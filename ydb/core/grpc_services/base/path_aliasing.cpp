#include "base.h"

#include <ydb/core/base/database_kind.h>
#include <ydb/core/base/path.h>

#include <utility>

namespace NKikimr::NGRpcService {

    void IRequestCtxBaseMtSafe::EnablePathNormalization() noexcept {
        PathNormalizationEnabled_ = true;
    }

    void IRequestCtxBaseMtSafe::DisablePathNormalization() noexcept {
        PathNormalizationEnabled_ = false;
        PathNormalizer_.reset();
    }

    TString IRequestCtxBaseMtSafe::NormalizePath(TStringBuf path) const {
        return PathNormalizer_ ? PathNormalizer_->NormalizePath(path, &PathAliasCounter_) : TString(path);
    }

    void IRequestProxyCtx::InitializePathNormalization(
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer,
        NMonitoring::TDynamicCounterPtr counters,
        const NKikimrScheme::TEvDescribeSchemeResult* database)
    {
        if (!PathNormalizationEnabled_) {
            return;
        }

        if (!PathNormalizationInitialized_) {
            const TString method = GetRpcMethodName();
            if (method.StartsWith("Ydb.PersQueue.V1.") ||
                method.StartsWith("Ydb.Cms.V1.CmsService/")) {
                DisablePathNormalization();
                return;
            }

            auto databaseName = GetDatabaseNameFromRequest();
            PathNormalizer_ = std::move(normalizer);
            if (databaseName) {
                databaseName = NormalizePath(*databaseName);
            }
            EffectiveDatabaseName_ = std::move(databaseName);
            PathNormalizationInitialized_ = true;
        }

        if (!PathNormalizer_ || !counters || !database || PathAliasCounter_.IsInitialized()
            || database->GetStatus() != NKikimrScheme::StatusSuccess || !IsDatabase(*database)) {
            return;
        }
        const auto effectiveDatabase = GetDatabaseName();
        if (effectiveDatabase && !effectiveDatabase->empty()
            && CanonizePath(*effectiveDatabase) != CanonizePath(database->GetPath())) {
            return;
        }
        PathAliasCounter_.Initialize(std::move(counters), GetRpcMethodName(), GetDatabaseNameFromRequest().GetOrElse(""));
    }

} // namespace NKikimr::NGRpcService
