#!/usr/bin/env python3
"""Tests of the lanes of trace_to_perfetto: python3 -m unittest test_trace_to_perfetto"""

import unittest

from trace_to_perfetto import Span, build_lanes, convert


def make_span(name, parent, start, end, node=1, flow=None):
    attrs = {'node_id': node}
    if flow:
        attrs['ydb.flow'] = flow
    return Span(trace_id='t', span_id=name, parent_id=parent or '', name=name, start=start, end=end,
                status=1, status_message='', attrs=attrs, links=[], service='')


class TestLanes(unittest.TestCase):
    def lanes_of(self, *spans):
        lanes, violations = build_lanes(list(spans))
        return {span.name: span.lane.root.name for span in spans}, [(s.name, v) for s, v in violations]

    def test_synchronous_children_stay_on_the_parent_lane(self):
        lanes, violations = self.lanes_of(
            make_span('compile', None, 0, 100),
            make_span('parse', 'compile', 10, 20),
            make_span('optimize', 'compile', 20, 90),
            make_span('rule', 'optimize', 30, 40))
        self.assertEqual(set(lanes.values()), {'compile'})
        self.assertEqual(violations, [])

    def test_async_flow_gets_its_own_lane_with_its_synchronous_children(self):
        lanes, violations = self.lanes_of(
            make_span('compile', None, 0, 100),
            make_span('request', 'compile', 10, 200, flow='async'),
            make_span('handle', 'request', 20, 190),
            make_span('next step', 'compile', 30, 40))
        self.assertEqual(lanes, {'compile': 'compile', 'request': 'request', 'handle': 'request',
                                 'next step': 'compile'})
        self.assertEqual(violations, [])

    def test_unmarked_concurrency_is_reported_not_clamped(self):
        spans = [
            make_span('compile', None, 0, 100),
            make_span('rule', 'compile', 10, 50),
            make_span('unmarked', 'compile', 40, 60),
            make_span('late', 'compile', 70, 120)]
        lanes, violations = self.lanes_of(*spans)
        self.assertEqual(lanes['unmarked'], 'unmarked')
        self.assertEqual(lanes['late'], 'late')
        self.assertEqual(violations, [('unmarked', 'overlaps rule by 10 ns'),
                                      ('late', 'sticks out of compile by 20 ns')])
        self.assertEqual((spans[2].start, spans[2].end), (40, 60))

    def test_other_node_starts_a_flow(self):
        lanes, _ = self.lanes_of(
            make_span('compile', None, 0, 100),
            make_span('remote', 'compile', 10, 20, node=2))
        self.assertEqual(lanes['remote'], 'remote')

    def test_conversion_produces_a_trace(self):
        spans = [make_span('compile', None, 0, 100), make_span('request', 'compile', 10, 200, flow='async')]
        lanes, _ = build_lanes(spans)
        trace, arrows = convert(spans, lanes)
        self.assertTrue(trace)
        self.assertEqual(arrows, 1)


if __name__ == '__main__':
    unittest.main()
