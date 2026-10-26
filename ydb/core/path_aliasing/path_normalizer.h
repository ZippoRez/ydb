#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimrConfig {
    class TPathRewriteConfig;
} // namespace NKikimrConfig

namespace NKikimr::NPathAliasing {

    class TPathNormalizer {
    public:
        // Owned by one request, accessed by its actors sequentially.
        class TRequestCounter {
        public:
            void Initialize(NMonitoring::TDynamicCounterPtr counters, TStringBuf rpcMethod, const TString& database);

        private:
            friend class TPathNormalizer;
            NMonitoring::TDynamicCounters::TCounterPtr Counter;
            bool Used = false;
        };

        TPathNormalizer() noexcept = default;
        explicit TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config);

        TString NormalizePath(TStringBuf path, TRequestCounter* requestCounter = nullptr) const;

    private:
        struct TImpl;
        std::shared_ptr<const TImpl> Impl;
    };

} // namespace NKikimr::NPathAliasing
