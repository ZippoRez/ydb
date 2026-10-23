#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <utility>
#include <memory>
#include <type_traits>

namespace NKikimr::NKqp {

struct TCompileTraceLevels {
    enum : ui8 {
        // Compilation phases: parsing, annotation, optimization, physical query compilation.
        Phase = TComponentTracingLevels::TQueryProcessor::Detailed,
        // Steps of a phase: RBO stages, physical conversion steps.
        Step = TComponentTracingLevels::TQueryProcessor::Diagnostic,
        // Applied RBO rules and recomputed plan properties.
        Detail = TComponentTracingLevels::TQueryProcessor::Trace,
    };
};

// A future can complete after its compiler has been cancelled. Only this span
// is shared with its callback; compiler state stays on the actor's thread.
class TCompileTraceContinuation {
public:
    explicit TCompileTraceContinuation(NWilson::TSpan span);
    NWilson::TTraceId Complete(bool success = true);

private:
    friend class TCompileTrace;
    void Cancel();

    const NWilson::TTraceId Id;
    TAdaptiveLock Lock;
    NWilson::TSpan Span;
};

// Spans of a single query compilation. Compiler turns run serially but may
// migrate between worker threads. Logical phases may stay open across waits;
// scope spans record synchronous work. The innermost open logical phase or
// scope parents new compiler work.
class TCompileTrace : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCompileTrace>;

    TCompileTrace(NWilson::TTraceId root, NActors::TActorSystem* actorSystem);
    ~TCompileTrace();

    bool IsEnabled(ui8 verbosity) const {
        return Root && verbosity <= Root.GetVerbosity();
    }

    NWilson::TSpan StartSpan(ui8 verbosity, TString name) const;
    static void EndSpan(NWilson::TSpan& span, bool success = true);

    // Makes the span the parent of the spans started until it is popped.
    void Push(const NWilson::TSpan& span);
    void Pop(const NWilson::TSpan& span);

    // Parent for spans that other components start during the compilation,
    // e.g. metadata requests of the gateway. Safe to call from any thread.
    NWilson::TTraceId GetCurrentParent() const;

    // Register only futures actually returned by GetAsyncFuture to the pipeline.
    void AwaitDependency(const NWilson::TTraceId& dependency);
    std::shared_ptr<TCompileTraceContinuation> StartContinuation();

    // Elapsed wall-clock intervals of compiler turns, including preemption.
    // These are not CPU measurements or physical worker tracks.
    void Resume(const NWilson::TTraceId& readyFrom = {});
    void Suspend();

    // A region is a span opened and closed by separate calls, e.g. by marker
    // stages placed around groups of transformers of a pipeline. Regions of one
    // group follow each other: opening a region ends the open regions of its
    // group, unless it is open itself (after a restart of the pipeline).
    void BeginRegion(ui8 verbosity, const TString& name, const void* group);
    // Ends the region together with the regions opened inside it.
    void EndRegion(TStringBuf name);

    // Ends remaining regions. This trace records no further regions.
    void Finish(bool success);

private:
    friend class TCompileTraceRuleScope;
    void Push(const NWilson::TTraceId& id);
    bool Pop(const NWilson::TTraceId& id);
    void EndRegions(size_t count, bool success);

private:
    const NWilson::TTraceId Root;
    NActors::TActorSystem* const ActorSystem;

    struct TParent {
        NWilson::TTraceId Id;
        mutable bool HasChildren = false;
    };
    TVector<TParent> Parents;
    // Guards Parents for GetCurrentParent.
    TAdaptiveLock ParentsLock;

    struct TRegion {
        TString Name;
        const void* Group;
        NWilson::TSpan Span;
    };
    TVector<TRegion> Regions;
    TVector<NWilson::TTraceId> AwaitedDependencies;
    std::shared_ptr<TCompileTraceContinuation> Continuation;
    NWilson::TSpan ExecutionSpan;
    ui32 Running = 0;
    bool Finished = false;
};

// Ends a dependency span. A fulfilled future may contain an error result. Keep
// this distinct from an exception: only the latter can short-circuit
// WaitExceptionOrAll.
void FinishCompileDependency(NWilson::TSpan& span, bool futureSuccess, bool operationSuccess = true);

// Ends the dependency span on the future's callback thread, before releasing its
// consumer: end(span, result) ends it, where result points to the value or is
// null for an exception (a bool for a void future).
// For a value future this transfers its result to the returned future: the
// caller must consume the returned future instead of retaining the original.
// Captures no compiler/transformer state, which may be destroyed while waiting.
template <class T, class TEnd>
NThreading::TFuture<T> TraceCompileDependencyWith(NThreading::TFuture<T> future, NWilson::TSpan span, TEnd end) {
    if (!span) {
        return future;
    }
    return future.Apply([span = std::make_shared<NWilson::TSpan>(std::move(span)), end = std::move(end)]
        (const NThreading::TFuture<T>& ready) mutable -> T {
        try {
            ready.TryRethrow();
        } catch (...) {
            span->Attribute("ydb.compile.future_outcome", TString("exception"));
            if constexpr (std::is_void_v<T>) {
                end(*span, false);
            } else {
                end(*span, static_cast<const T*>(nullptr));
            }
            throw;
        }
        span->Attribute("ydb.compile.future_outcome", TString("value"));
        if constexpr (std::is_void_v<T>) {
            end(*span, true);
        } else {
            auto owned = ready;
            auto result = owned.ExtractValue();
            end(*span, &std::as_const(result));
            return result;
        }
    });
}

template <class T, class TSuccess>
NThreading::TFuture<T> TraceCompileDependency(NThreading::TFuture<T> future, NWilson::TSpan span, TSuccess success) {
    return TraceCompileDependencyWith(std::move(future), std::move(span),
        [success = std::move(success)](NWilson::TSpan& span, auto result) mutable {
            if constexpr (std::is_void_v<T>) {
                FinishCompileDependency(span, result, result && success());
            } else {
                FinishCompileDependency(span, result != nullptr, result && success(*result));
            }
        });
}

template <class T>
NThreading::TFuture<T> TraceCompileDependency(NThreading::TFuture<T> future, NWilson::TSpan span) {
    return TraceCompileDependency(std::move(future), std::move(span), [](const auto&...) { return true; });
}

// Span that is open for the lifetime of the scope and parents the spans started in it.
class TCompileTraceScope {
public:
    TCompileTraceScope(TCompileTrace* trace, ui8 verbosity, TString name);
    ~TCompileTraceScope();

    TCompileTraceScope(const TCompileTraceScope&) = delete;
    TCompileTraceScope& operator=(const TCompileTraceScope&) = delete;

    // For operations that return an error result without throwing.
    void SetSuccess(bool success) {
        Success = success;
    }

private:
    TCompileTrace* const Trace;
    const int UncaughtExceptions;
    NWilson::TSpan Span;
    bool Success = true;
};

class TCompileTraceRunScope {
public:
    explicit TCompileTraceRunScope(TCompileTrace::TPtr trace, const NWilson::TTraceId& readyFrom = {});
    ~TCompileTraceRunScope();

    TCompileTraceRunScope(const TCompileTraceRunScope&) = delete;
    TCompileTraceRunScope& operator=(const TCompileTraceRunScope&) = delete;

private:
    // Completion/fallback may replace the actor's trace before this scope ends.
    const TCompileTrace::TPtr Trace;
};

// Reserve the parent before applying a rule, but emit only applied rules,
// failed rules, or rules whose children need a parent. Unproductive leaf
// attempts do not create exported spans.
class TCompileTraceRuleScope {
public:
    TCompileTraceRuleScope(TCompileTrace* trace, const TString& name);
    ~TCompileTraceRuleScope();

    TCompileTraceRuleScope(const TCompileTraceRuleScope&) = delete;
    TCompileTraceRuleScope& operator=(const TCompileTraceRuleScope&) = delete;

    void SetApplied(bool applied) { Applied = applied; }

private:
    TCompileTrace* const Trace;
    const TString Name;
    const int UncaughtExceptions;
    NWilson::TTraceId Parent;
    NWilson::TTraceId Id;
    TInstant Start;
    bool Applied = false;
};

} // namespace NKikimr::NKqp
