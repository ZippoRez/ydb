#!/usr/bin/env python3
"""Converts YDB traces from OTLP/JSON to a Perfetto trace with a lane per logical flow.

A span stays on the lane of its parent unless it starts a flow of its own: it
is marked with the attribute ydb.flow = "async" (work concurrent with its
parent, e.g. a request served by another actor), it runs on another node
(node_id), or its parent is not in the input. A flow's lane is nested under the
lane that started it; the lane of a flow on another node is in that node's
process.

The slices of a lane always form a stack. A span that does not nest in its
parent's lane, or overlaps a sibling there, is not clamped: it is reported and
moved to a lane of its own marked as a violation. Such a span runs concurrently
without being marked as async, or its clock is misordered.

Arrows show where a flow starts (fork), where a span waited for another one
(link with ydb.link.relation = depends_on) and where a span started once
another one completed (follows_from).

Input: OTLP/JSON as written by the file exporter of the OpenTelemetry collector,
one request per line or a single document. Timestamps are kept in nanoseconds.

Usage:
    trace_to_perfetto.py traces.json -o trace.pftrace [--trace TRACE_ID] [--strict]

Open the result at https://ui.perfetto.dev.
"""

import argparse
import base64
import binascii
import json
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass, field

FLOW_ATTRIBUTE = 'ydb.flow'
ASYNC_FLOW = 'async'
RELATION_ATTRIBUTE = 'ydb.link.relation'
DEPENDS_ON = 'depends_on'
FOLLOWS_FROM = 'follows_from'
STATUS_ERROR = 2
STATUS_CODES = {'STATUS_CODE_UNSET': 0, 'STATUS_CODE_OK': 1, 'STATUS_CODE_ERROR': 2}


@dataclass
class Link:
    trace_id: str
    span_id: str
    attrs: dict


@dataclass(eq=False)
class Span:
    trace_id: str
    span_id: str
    parent_id: str
    name: str
    start: int
    end: int
    status: int
    status_message: str
    attrs: dict
    links: list
    service: str
    parent: 'Span' = None
    children: list = field(default_factory=list)
    lane: 'Lane' = None
    lane_children: list = field(default_factory=list)
    begin_event: dict = None

    @property
    def node(self):
        return self.attrs.get('node_id')


@dataclass(eq=False)
class Lane:
    uuid: int
    root: Span
    parent: 'Lane'
    violation: str = None


# OTLP/JSON parsing.

def get(obj, camel):
    """Reads a field in its lowerCamelCase or snake_case spelling."""
    if camel in obj:
        return obj[camel]
    return obj.get(re.sub(r'(?<!^)([A-Z])', r'_\1', camel).lower())


def parse_id(value):
    # OTLP/JSON encodes ids in hex; the generic protobuf JSON mapping uses base64.
    if not value:
        return ''
    if re.fullmatch(r'[0-9a-fA-F]{16}|[0-9a-fA-F]{32}', value):
        return value.lower()
    try:
        return base64.b64decode(value, validate=True).hex()
    except (binascii.Error, ValueError):
        return value


def parse_value(value):
    for key in ('stringValue', 'boolValue', 'doubleValue', 'bytesValue'):
        item = get(value, key)
        if item is not None:
            return item
    item = get(value, 'intValue')
    if item is not None:
        return int(item)
    item = get(value, 'arrayValue')
    if item is not None:
        return [parse_value(v) for v in item.get('values', [])]
    item = get(value, 'kvlistValue')
    if item is not None:
        return parse_attributes(item.get('values', []))
    return None


def parse_attributes(attributes):
    return {kv['key']: parse_value(kv.get('value') or {}) for kv in attributes or []}


def parse_status(status):
    code = (status or {}).get('code', 0)
    if isinstance(code, str):
        code = STATUS_CODES.get(code, 0)
    return int(code), (status or {}).get('message', '')


def parse_request(request, spans):
    for resource_spans in get(request, 'resourceSpans') or []:
        resource = parse_attributes((get(resource_spans, 'resource') or {}).get('attributes'))
        service = resource.get('service.name', '')
        scopes = get(resource_spans, 'scopeSpans') or get(resource_spans, 'instrumentationLibrarySpans') or []
        for scope in scopes:
            for span in scope.get('spans', []):
                status, message = parse_status(span.get('status'))
                spans.append(Span(
                    trace_id=parse_id(get(span, 'traceId')),
                    span_id=parse_id(get(span, 'spanId')),
                    parent_id=parse_id(get(span, 'parentSpanId')),
                    name=span.get('name', ''),
                    start=int(get(span, 'startTimeUnixNano') or 0),
                    end=int(get(span, 'endTimeUnixNano') or 0),
                    status=status,
                    status_message=message,
                    attrs=parse_attributes(span.get('attributes')),
                    links=[Link(parse_id(get(link, 'traceId')), parse_id(get(link, 'spanId')),
                                parse_attributes(link.get('attributes'))) for link in span.get('links', [])],
                    service=service,
                ))


def read_spans(path):
    with open(path) as f:
        text = f.read()
    try:
        documents = [json.loads(text)]
    except json.JSONDecodeError:
        documents = [json.loads(line) for line in text.splitlines() if line.strip()]
    spans = []
    for document in documents:
        for request in document if isinstance(document, list) else [document]:
            parse_request(request, spans)
    return spans


# Lanes.

def build_lanes(spans):
    """Assigns every span to a lane and returns the lanes and the violations."""
    by_id = {(span.trace_id, span.span_id): span for span in spans}
    for span in spans:
        span.parent = by_id.get((span.trace_id, span.parent_id))
        if span.parent:
            span.parent.children.append(span)

    lanes = []
    violations = []

    def new_lane(span, parent, violation=None):
        lane = Lane(uuid=len(lanes) + 1, root=span, parent=parent, violation=violation)
        lanes.append(lane)
        span.lane = lane
        return lane

    def starts_flow(span):
        return (span.attrs.get(FLOW_ATTRIBUTE) == ASYNC_FLOW or span.node != span.parent.node)

    stack = []
    for root in sorted((span for span in spans if not span.parent), key=lambda span: span.start):
        new_lane(root, None)
        stack.append(root)
    while stack:
        span = stack.pop()
        last = None
        for child in sorted(span.children, key=lambda child: (child.start, child.end)):
            if starts_flow(child):
                new_lane(child, span.lane)
            elif child.start < span.start or child.end > span.end:
                by = max(span.start - child.start, child.end - span.end)
                violation = f'sticks out of {span.name} by {by} ns'
                violations.append((child, violation))
                new_lane(child, span.lane, violation)
            elif last and child.start < last.end:
                violation = f'overlaps {last.name} by {last.end - child.start} ns'
                violations.append((child, violation))
                new_lane(child, span.lane, violation)
            else:
                child.lane = span.lane
                span.lane_children.append(child)
                last = child
            stack.append(child)
    return lanes, violations


# Perfetto protobuf encoding (protos/perfetto/trace in the Perfetto repository).

def varint(value):
    value &= (1 << 64) - 1
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def field_varint(number, value):
    return varint(number << 3) + varint(value)


def field_bytes(number, value):
    if isinstance(value, str):
        value = value.encode()
    return varint(number << 3 | 2) + varint(len(value)) + value


def field_fixed64(number, value):
    return varint(number << 3 | 1) + struct.pack('<Q', value)


def field_double(number, value):
    return varint(number << 3 | 1) + struct.pack('<d', value)


# TracePacket
PACKET_TIMESTAMP = 8
PACKET_SEQUENCE_ID = 10
PACKET_TRACK_EVENT = 11
PACKET_SEQUENCE_FLAGS = 13
PACKET_TRACK_DESCRIPTOR = 60
SEQ_INCREMENTAL_STATE_CLEARED = 1
# TrackDescriptor
TRACK_UUID = 1
TRACK_NAME = 2
TRACK_PROCESS = 3
TRACK_PARENT_UUID = 5
TRACK_CHILD_ORDERING = 11
TRACK_SIBLING_MERGE_BEHAVIOR = 15
CHILD_ORDERING_CHRONOLOGICAL = 2
SIBLING_MERGE_BEHAVIOR_NONE = 2
# ProcessDescriptor
PROCESS_PID = 1
PROCESS_NAME = 6
# TrackEvent
EVENT_DEBUG_ANNOTATIONS = 4
EVENT_TYPE = 9
EVENT_TRACK_UUID = 11
EVENT_NAME = 23
EVENT_FLOW_IDS = 47
EVENT_TERMINATING_FLOW_IDS = 48
TYPE_SLICE_BEGIN = 1
TYPE_SLICE_END = 2
TYPE_INSTANT = 3
# DebugAnnotation
ANNOTATION_BOOL = 2
ANNOTATION_INT = 4
ANNOTATION_DOUBLE = 5
ANNOTATION_STRING = 6
ANNOTATION_NAME = 10

SEQUENCE_ID = 1


def debug_annotation(name, value):
    out = field_bytes(ANNOTATION_NAME, name)
    if isinstance(value, bool):
        out += field_varint(ANNOTATION_BOOL, int(value))
    elif isinstance(value, int):
        out += field_varint(ANNOTATION_INT, value)
    elif isinstance(value, float):
        out += field_double(ANNOTATION_DOUBLE, value)
    elif isinstance(value, str):
        out += field_bytes(ANNOTATION_STRING, value)
    else:
        out += field_bytes(ANNOTATION_STRING, json.dumps(value))
    return out


def packet(body, timestamp=None, flags=None):
    out = field_varint(PACKET_SEQUENCE_ID, SEQUENCE_ID)
    if timestamp is not None:
        out += field_varint(PACKET_TIMESTAMP, timestamp)
    if flags is not None:
        out += field_varint(PACKET_SEQUENCE_FLAGS, flags)
    return field_bytes(1, out + body)  # Trace.packet


def track_event(event):
    out = field_varint(EVENT_TYPE, event['type']) + field_varint(EVENT_TRACK_UUID, event['track'])
    if 'name' in event:
        out += field_bytes(EVENT_NAME, event['name'])
    for name, value in event.get('args', {}).items():
        out += field_bytes(EVENT_DEBUG_ANNOTATIONS, debug_annotation(name, value))
    for flow in event.get('flows', []):
        out += field_fixed64(EVENT_FLOW_IDS, flow)
    for flow in event.get('terminating_flows', []):
        out += field_fixed64(EVENT_TERMINATING_FLOW_IDS, flow)
    return field_bytes(PACKET_TRACK_EVENT, out)


def lane_name(lane):
    name = lane.root.name
    table = lane.root.attrs.get('db.collection.name')
    if table:
        name += f' {table}'
    if lane.violation:
        name = f'⚠ {name}: {lane.violation}'
    return name


def convert(spans, lanes):
    processes = {}
    descriptors = []
    process_uuid_base = len(lanes) + 1

    def process_track(span):
        node = span.node
        if node not in processes:
            uuid = process_uuid_base + len(processes)
            pid = node if isinstance(node, int) and node > 0 else 0
            name = f'node {node}' if node is not None else 'unknown node'
            if span.service:
                name = f'{span.service} {name}'
            processes[node] = uuid
            process = field_varint(PROCESS_PID, pid) + field_bytes(PROCESS_NAME, name)
            descriptors.append(field_bytes(PACKET_TRACK_DESCRIPTOR,
                field_varint(TRACK_UUID, uuid) + field_bytes(TRACK_PROCESS, process)))
        return processes[node]

    for lane in lanes:
        # The process track first: a track must be described before its children.
        nested = lane.parent and lane.parent.root.node == lane.root.node
        parent = lane.parent.uuid if nested else process_track(lane.root)
        descriptors.append(field_bytes(PACKET_TRACK_DESCRIPTOR,
            field_varint(TRACK_UUID, lane.uuid)
            + field_varint(TRACK_PARENT_UUID, parent)
            + field_bytes(TRACK_NAME, lane_name(lane))
            + field_varint(TRACK_CHILD_ORDERING, CHILD_ORDERING_CHRONOLOGICAL)
            # Every flow keeps its own lane, also among flows with the same name.
            + field_varint(TRACK_SIBLING_MERGE_BEHAVIOR, SIBLING_MERGE_BEHAVIOR_NONE)))

    events = []

    def add(event, order=None):
        # Events with equal timestamps keep this order.
        event['order'] = len(events) if order is None else order
        events.append(event)
        return event

    def emit(span):
        name = span.name + (' (error)' if span.status == STATUS_ERROR else '')
        args = {key: value for key, value in span.attrs.items() if value is not None}
        args['span_id'] = span.span_id
        if span.status_message:
            args['status_message'] = span.status_message
        if span.lane.root is span and span.lane.violation:
            args['violation'] = span.lane.violation
        span.begin_event = add({'ts': span.start, 'type': TYPE_SLICE_BEGIN, 'track': span.lane.uuid,
                                'name': name, 'args': args})
        for child in span.lane_children:
            emit(child)
        add({'ts': span.end, 'type': TYPE_SLICE_END, 'track': span.lane.uuid})

    for lane in lanes:
        emit(lane.root)

    by_id = {(span.trace_id, span.span_id): span for span in spans}
    flows = 0

    def connect(source, destination):
        nonlocal flows
        flows += 1
        source.setdefault('flows', []).append(flows)
        destination.setdefault('terminating_flows', []).append(flows)

    for lane in lanes:
        if lane.parent:
            # Where the flow started, on the lane of the span that started it. The
            # source of an arrow must precede its destination, also at the same time.
            begin = lane.root.begin_event
            fork = add({'ts': lane.root.start, 'type': TYPE_INSTANT, 'track': lane.root.parent.lane.uuid,
                        'name': f'start {lane.root.name}'}, order=begin['order'] - 0.5)
            connect(fork, begin)
    for span in spans:
        dependencies = []
        for link in span.links:
            target = by_id.get((link.trace_id, link.span_id))
            relation = link.attrs.get(RELATION_ATTRIBUTE)
            if not target:
                continue
            if relation == DEPENDS_ON:
                dependencies.append(target)
            elif relation == FOLLOWS_FROM:
                connect(target.begin_event, span.begin_event)
        if dependencies:
            # The span got the outcomes of its dependencies: arrows from their ends.
            name = f'got {dependencies[0].name}' if len(dependencies) == 1 else f'got {len(dependencies)} results'
            join = add({'ts': span.end, 'type': TYPE_INSTANT, 'track': span.lane.uuid, 'name': name})
            for dependency in dependencies:
                connect(dependency.begin_event, join)

    out = bytearray(packet(b'', flags=SEQ_INCREMENTAL_STATE_CLEARED))
    for descriptor in descriptors:
        out += packet(descriptor)
    for event in sorted(events, key=lambda event: (event['ts'], event['order'])):
        out += packet(track_event(event), timestamp=event['ts'])
    return bytes(out), flows


def main():
    sys.setrecursionlimit(100_000)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input', help='OTLP/JSON traces')
    parser.add_argument('-o', '--output', required=True, help='Perfetto trace to write')
    parser.add_argument('--trace', help='convert only the trace with this id (hex)')
    parser.add_argument('--strict', action='store_true', help='fail if a span does not nest in its lane')
    args = parser.parse_args()

    spans = read_spans(args.input)
    if args.trace:
        spans = [span for span in spans if span.trace_id == args.trace.lower()]
    if not spans:
        sys.exit('no spans to convert')
    lanes, violations = build_lanes(spans)
    trace, flows = convert(spans, lanes)
    with open(args.output, 'wb') as f:
        f.write(trace)

    for span, violation in violations:
        print(f'violation: {span.name} ({span.trace_id}/{span.span_id}) {violation}', file=sys.stderr)
    print(f'{len(spans)} spans, {len(lanes)} lanes, {flows} arrows, {len(violations)} violations: {args.output}',
          file=sys.stderr)
    if args.strict and violations:
        sys.exit(1)


if __name__ == '__main__':
    main()
