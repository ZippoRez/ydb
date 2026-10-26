#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/database_kind.h>
#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NKikimr::NGRpcService {
    namespace {

        std::shared_ptr<const NPathAliasing::TPathNormalizer> MakeNormalizer() {
            NKikimrConfig::TPathRewriteConfig config;
            auto* rule = config.AddRules();
            rule->SetSrc("/raw");
            rule->SetDst("/rewritten");
            return std::make_shared<const NPathAliasing::TPathNormalizer>(config);
        }

        std::unique_ptr<TEvHttpRequestAuthAndCheck> MakeRequest() {
            return std::make_unique<TEvHttpRequestAuthAndCheck>(
                "/raw", TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id");
        }

        NKikimrScheme::TEvDescribeSchemeResult MakeDatabase(const TString& path) {
            NKikimrScheme::TEvDescribeSchemeResult database;
            database.SetStatus(NKikimrScheme::StatusSuccess);
            database.SetPath(path);
            database.MutablePathDescription()->MutableSelf()->SetPathType(NKikimrSchemeOp::EPathTypeSubDomain);
            return database;
        }

        void InitializeCounter(IRequestProxyCtx& request, NMonitoring::TDynamicCounterPtr counters,
            const NKikimrScheme::TEvDescribeSchemeResult& database) {
            request.InitializePathNormalization({}, std::move(counters), &database);
        }

        class TNamedRequest final : public TEvHttpRequestAuthAndCheck {
        public:
            explicit TNamedRequest(TString method, TString database = "/raw")
                : TEvHttpRequestAuthAndCheck(
                    database, TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id")
                , Method_(std::move(method))
            {}

            TString GetRpcMethodName() const override {
                return Method_;
            }

        private:
            TString Method_;
        };

        ui64 GetPathAliasCount(const NMonitoring::TDynamicCounterPtr& root, const char* database = "/raw") {
            auto group = root;
            const std::pair<TString, TString> labels[] = {
                {"counters", "ydb"}, {"api_service", "Ydb.Topic.V1.TopicService"},
                {"method", "CreateTopic"}, {"database", database},
            };
            for (const auto& [name, value] : labels) {
                if (!group) {
                    return 0;
                }
                group = group->FindSubgroup(name, value);
            }
            auto counter = group ? group->FindNamedCounter("name", "api.grpc.request.path_alias_count") : nullptr;
            return counter ? counter->Val() : 0;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathAliasingRequestContext) {
        Y_UNIT_TEST(AliasesCountOncePerRequest) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            auto normalizer = MakeNormalizer();
            ui64 count = 0;
            for (const auto* database : {"/raw", "//raw/"}) {
                TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic", database);
                request.EnablePathNormalization();
                request.InitializePathNormalization(normalizer);
                auto unavailable = MakeDatabase("/rewritten");
                unavailable.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
                InitializeCounter(request, root, unavailable);
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root), count);
                InitializeCounter(request, root, MakeDatabase("/rewritten"));
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root), ++count);
                request.NormalizePath("/raw/a");
                request.NormalizePath("/raw/b");
                request.InitializePathNormalization(normalizer);
                InitializeCounter(request, root, MakeDatabase("/rewritten"));
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root), count);
            }
        }

        Y_UNIT_TEST(ResourceAliasesCountAfterBinding) {
            auto normalizer = MakeNormalizer();
            for (const auto* database : {"/physical", ""}) {
                NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
                TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic", database);
                request.EnablePathNormalization();
                request.InitializePathNormalization(normalizer);
                InitializeCounter(request, root, MakeDatabase(*database ? database : "/Root"));
                request.NormalizePath("/unmatched/a");
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, database), 0);
                request.NormalizePath("/raw/a");
                request.NormalizePath("/raw/b");
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, database), 1);
            }
        }

        Y_UNIT_TEST(UnvalidatedDatabasesCreateNoCounters) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            auto normalizer = MakeNormalizer();
            for (const auto* database : {"/raw/missing", "/unknown"}) {
                TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic", database);
                request.EnablePathNormalization();
                request.InitializePathNormalization(normalizer);
                InitializeCounter(request, root, MakeDatabase("/rewritten"));
                auto invalid = MakeDatabase(request.GetDatabaseName().GetOrElse(""));
                invalid.MutablePathDescription()->MutableSelf()->SetPathType(NKikimrSchemeOp::EPathTypeTable);
                InitializeCounter(request, root, invalid);
            }
            UNIT_ASSERT(!root->FindSubgroup("counters", "ydb"));
        }

        Y_UNIT_TEST(InternalRequestKeepsLiveDatabaseAndIdentityPaths) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            auto request = MakeRequest();
            request->InitializePathNormalization(MakeNormalizer());
            InitializeCounter(*request, root, MakeDatabase("/raw"));
            UNIT_ASSERT_VALUES_EQUAL(request->NormalizePath("/raw"), "/raw");
            UNIT_ASSERT(!root->FindSubgroup("counters", "ydb"));

            request->UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request->GetDatabaseName().GetOrElse(""), "/resolved");
        }

        Y_UNIT_TEST(ExplicitlyDisabledRequestKeepsLiveDatabase) {
            auto request = MakeRequest();
            request->EnablePathNormalization();
            request->DisablePathNormalization();
            request->InitializePathNormalization(MakeNormalizer());

            request->UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request->GetDatabaseName().GetOrElse(""), "/resolved");
        }

        Y_UNIT_TEST(EnabledRequestCachesRewrittenDatabase) {
            TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic");
            request.EnablePathNormalization();
            request.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw"), "/rewritten");

            request.UseDatabase("/resolved");
            request.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseName().GetOrElse(""), "/rewritten");
        }

        Y_UNIT_TEST(EmptyMappingKeepsCachedDatabase) {
            TNamedRequest request("Ydb.Query.V1.QueryService/ExecuteQuery");
            request.EnablePathNormalization();
            request.InitializePathNormalization(nullptr);
            UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw"), "/raw");
            request.UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseName().GetOrElse(""), "/raw");
        }

        Y_UNIT_TEST(LegacyServicesKeepRawDatabaseAndIdentityPaths) {
            const TString methods[] = {
                "Ydb.PersQueue.V1.PersQueueService/CreateTopic",
                "Ydb.PersQueue.V1.ClusterDiscoveryService/DiscoverClusters",
                "Ydb.Cms.V1.CmsService/CreateDatabase",
            };
            for (const auto& method : methods) {
                TNamedRequest request(method);
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer());
                UNIT_ASSERT_VALUES_EQUAL_C(request.NormalizePath("/raw"), "/raw", method);
                UNIT_ASSERT_VALUES_EQUAL_C(request.GetDatabaseName().GetOrElse(""), "/raw", method);

                request.UseDatabase("/resolved");
                UNIT_ASSERT_VALUES_EQUAL_C(request.GetDatabaseName().GetOrElse(""), "/resolved", method);
            }
        }
    }

} // namespace NKikimr::NGRpcService
