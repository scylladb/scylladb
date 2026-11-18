/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of ImportTable's tables in system_distributed, which db::alternator_import_table_helper
// reads and writes, and of the execution engine of ImportTable (alternator/import.cc): ImportTable
// runs the import in the background, and these tests check its result - the import row in
// system_distributed.alternator_import_from_s3_imports, the target table and its items. The source
// of the imports is in memory (make_in_memory_import_source()), so the tests need no S3.

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/error_injection.hh"

#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/export.hh"
#include "alternator/import.hh"
#include "alternator/rmw_operation.hh"
#include "cdc/metadata.hh"
#include "cql3/untyped_result_set.hh"
#include "db/system_distributed_keyspace.hh"
#include "gms/gossiper.hh"
#include "replica/database.hh"
#include "service/client_state.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "sstables/dictionary_service.hh"
#include "utils/alien_worker.hh"
#include "utils/error_injection.hh"
#include "transport/messages/result_message.hh"
#include "utils/rjson.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

static seastar::logger dict_trainer_logger("alternator_import_test_dict_trainer");

// Wraps a sharded<alternator::executor>, taking DynamoDB-style JSON requests.
class alternator_test_executor {
    sharded<cdc::metadata> _cdc_md;
    sharded<abort_source> _as;
    utils::alien_worker _dict_trainer{dict_trainer_logger, 19, "test-dict"};
    sharded<sstables::dictionary_service> _dicts;
    sharded<alternator::executor> _exec;
    alternator::rmw_operation::write_isolation _saved_write_isolation;
    bool _stopped = false;

    std::variant<rjson::value, alternator::api_error> call(auto method, rjson::value request) {
        service::client_state cs(service::client_state::internal_tag{});
        tracing::trace_state_ptr ts;
        std::unique_ptr<audit::audit_info_alternator> ai;
        // An operation returns some errors, and throws others.
        try {
            auto result = (_exec.local().*method)(cs, ts, empty_service_permit(), std::move(request), ai).get();
            if (auto* err = std::get_if<alternator::api_error>(&result)) {
                return std::move(*err);
            }
            return rjson::parse(std::get<std::string>(result));
        } catch (const alternator::api_error& err) {
            return err;
        }
    }

    rjson::value call_successfully(auto method, std::string_view name, rjson::value request) {
        auto result = call(method, std::move(request));
        if (auto* err = std::get_if<alternator::api_error>(&result)) {
            BOOST_FAIL(fmt::format("{} failed: {}", name, err->what()));
        }
        return std::move(std::get<rjson::value>(result));
    }
public:
    void start(cql_test_env& e) {
        // FORBID_RMW is enough for unconditional writes, and fails loudly on an accidental RMW.
        _saved_write_isolation = alternator::rmw_operation::default_write_isolation;
        alternator::rmw_operation::set_default_write_isolation("forbid_rmw");
        // The caller calls stop() only once start() returned: if a step fails, undo the earlier ones.
        auto restore_write_isolation = defer([this] noexcept {
            alternator::rmw_operation::default_write_isolation = _saved_write_isolation;
        });
        _cdc_md.start().get();
        auto stop_cdc_md = defer([this] noexcept { _cdc_md.stop().get(); });
        _as.start().get();
        auto stop_as = defer([this] noexcept { _as.stop().get(); });
        _dicts.start(std::ref(_as), std::ref(e.db()), std::ref(e.get_messaging_service()), std::ref(e.get_system_keyspace()), std::ref(e.get_feature_service()), std::ref(_dict_trainer)).get();
        auto stop_dicts = defer([this] noexcept { _dicts.stop().get(); });
        _exec.start(
            std::ref(e.gossiper()),
            std::ref(e.get_storage_proxy()),
            std::ref(_dicts),
            std::ref(e.migration_manager()),
            std::ref(e.get_system_distributed_keyspace()),
            std::ref(e.get_system_keyspace()),
            std::ref(_cdc_md),
            std::ref(e.vector_store_client()),
            default_smp_service_group(),
            utils::updateable_value<uint32_t>(10000)).get();
        auto stop_exec = defer([this] noexcept {
            _exec.invoke_on_all(&alternator::executor::request_stop).get();
            _exec.stop().get();
        });
        // As alternator::server::init() does.
        _exec.invoke_on_all(&alternator::executor::start).get();
        stop_exec.cancel();
        stop_dicts.cancel();
        stop_as.cancel();
        stop_cdc_md.cancel();
        restore_write_isolation.cancel();
    }

    // Can be called again: a test may stop the executor itself, before with_import_executor() does.
    void stop() {
        if (_stopped) {
            return;
        }
        _stopped = true;
        _exec.invoke_on_all(&alternator::executor::request_stop).get();
        _exec.stop().get();
        _dicts.stop().get();
        _as.stop().get();
        _cdc_md.stop().get();
        alternator::rmw_operation::default_write_isolation = _saved_write_isolation;
    }

    std::variant<rjson::value, alternator::api_error> import_table(rjson::value request) {
        return call(&alternator::executor::import_table, std::move(request));
    }
    // Sends ImportTable to `shard`. Returns the response, or throws the error.
    future<sstring> import_table_on_shard(unsigned shard, rjson::value request) {
        return _exec.invoke_on(shard, [request = sstring(rjson::print(request))] (alternator::executor& exec) -> future<sstring> {
            service::client_state cs(service::client_state::internal_tag{});
            std::unique_ptr<audit::audit_info_alternator> ai;
            auto result = co_await exec.import_table(cs, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(request), ai);
            if (auto* err = std::get_if<alternator::api_error>(&result)) {
                throw *err;
            }
            co_return sstring(std::get<std::string>(result));
        });
    }
    rjson::value import_table_successfully_on_shard(unsigned shard, rjson::value request) {
        return rjson::parse(import_table_on_shard(shard, std::move(request)).get());
    }
    rjson::value import_table_successfully(rjson::value request) {
        return call_successfully(&alternator::executor::import_table, "ImportTable", std::move(request));
    }
    std::variant<rjson::value, alternator::api_error> describe_table(std::string_view table_name) {
        rjson::value request = rjson::empty_object();
        rjson::add(request, "TableName", rjson::from_string(table_name));
        return call(&alternator::executor::describe_table, std::move(request));
    }
};

// A table with the hash key "p" (S) and the sort key "c" (N), as in test/alternator/test_import.py.
constexpr std::string_view table_creation_parameters = R"(
    "BillingMode": "PAY_PER_REQUEST",
    "AttributeDefinitions": [
        {"AttributeName": "p", "AttributeType": "S"},
        {"AttributeName": "c", "AttributeType": "N"}
    ],
    "KeySchema": [
        {"AttributeName": "p", "KeyType": "HASH"},
        {"AttributeName": "c", "KeyType": "RANGE"}
    ])";

rjson::value make_import_table_request(std::string_view table_name, std::string_view client_token) {
    return rjson::parse(fmt::format(R"({{
        "ClientToken": "{}",
        "InputFormat": "DYNAMODB_JSON",
        "S3BucketSource": {{"S3Bucket": "bucket", "S3KeyPrefix": "prefix/"}},
        "TableCreationParameters": {{"TableName": "{}", {}}}
    }})", client_token, table_name, table_creation_parameters));
}

std::string get_string(const rjson::value& v, std::string_view member) {
    return std::string(rjson::to_string_view(rjson::get(v, member)));
}

// Waits until the import of `import_arn` ends, and returns its row.
alternator::import_row wait_for_import(cql_test_env& e, const sstring& import_arn) {
    db::alternator_import_table_helper helper(e.qp().local());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(2);
    for (;;) {
        auto row = helper.get_import(import_arn).get();
        BOOST_REQUIRE_MESSAGE(row, fmt::format("import {} has no row", import_arn));
        if (row->import_status == "COMPLETED" || row->import_status == "FAILED") {
            return std::move(*row);
        }
        BOOST_REQUIRE_MESSAGE(std::chrono::steady_clock::now() < deadline, fmt::format("import {} did not end", import_arn));
        seastar::sleep(std::chrono::milliseconds(10)).get();
    }
}

// Waits until the import which `response`, the response of ImportTable, describes ends, and returns
// its row.
alternator::import_row wait_for_import(cql_test_env& e, const rjson::value& response) {
    return wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
}

cql_test_config import_test_config() {
    cql_test_config cfg;
    // The import metadata is written with LWT.
    cfg.need_remote_proxy = true;
    return cfg;
}

// Runs `f` in a cql_test_env with `cfg`, with a started executor, which it stops after `f`.
future<> with_import_executor(std::function<void(cql_test_env&, alternator_test_executor&)> f, cql_test_config cfg = import_test_config()) {
    return do_with_cql_env_thread([f = std::move(f)] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });
        f(e, exec);
    }, std::move(cfg));
}

// Checks that the columns of the import row `import_arn` expire in `ttl`, less the few minutes the
// test may have taken: each write of the row sets its TTL, as the table has no default TTL.
void check_import_row_ttl(cql_test_env& e, std::string_view import_arn, std::chrono::seconds ttl) {
    auto rs = e.qp().local().execute_internal(fmt::format("SELECT TTL(import_status) AS status_ttl, TTL(request) AS request_ttl,"
            " TTL(node_id) AS node_ttl FROM {}.{} WHERE import_arn = ?",
            db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_IMPORT_FROM_S3_IMPORTS),
            {sstring(import_arn)}, cql3::query_processor::cache_internal::no).get();
    BOOST_REQUIRE_EQUAL(rs->size(), 1u);
    for (auto column : {"status_ttl", "request_ttl", "node_ttl"}) {
        const auto remaining = rs->one().get_as<int32_t>(column);
        BOOST_REQUIRE_MESSAGE(remaining > ttl.count() - 300 && remaining <= ttl.count(),
                fmt::format("{} of import {} is {}, expected at most {}", column, import_arn, remaining, ttl.count()));
    }
}

} // anonymous namespace

// The garbage collector takes an import only if its row did not change since it was read: a
// progress write of an import which still runs, which keeps its status and node, is not undone.
SEASTAR_TEST_CASE(test_import_take_import_checks_counters) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        db::alternator_import_table_helper helper(e.qp().local());
        const db::alternator_import_row row{
            .import_arn = "arn:scylla:alternator:alternator_take:scylla:table/take/import/01700000000000-00000001",
            .client_token = "take",
            .request = "{}",
            .import_status = "IN_PROGRESS",
            .processed_item_count = 5,
            .imported_item_count = 4,
            .error_count = 1,
            .accepted_at = db_clock::now(),
            .node_id = "running-node",
        };
        BOOST_REQUIRE(!helper.insert_import(row, std::chrono::hours(1)).get());
        auto taken = row;
        taken.import_status = "FAILING";
        taken.node_id = "collector";
        auto read_before_progress = row;
        read_before_progress.processed_item_count = 3;
        BOOST_REQUIRE(!helper.take_import(taken, read_before_progress, std::chrono::hours(1)).get());
        BOOST_REQUIRE_EQUAL(helper.get_import(row.import_arn).get()->node_id, "running-node");
        auto read_with_other_bytes = row;
        read_with_other_bytes.processed_size_bytes = 10;
        BOOST_REQUIRE(!helper.take_import(taken, read_with_other_bytes, std::chrono::hours(1)).get());
        BOOST_REQUIRE_EQUAL(helper.get_import(row.import_arn).get()->node_id, "running-node");
        BOOST_REQUIRE(helper.take_import(taken, row, std::chrono::hours(2)).get());
        auto after = helper.get_import(row.import_arn).get();
        BOOST_REQUIRE_EQUAL(after->node_id, "collector");
        BOOST_REQUIRE_EQUAL(after->import_status, "FAILING");
        BOOST_REQUIRE_EQUAL(after->processed_item_count, 5);
        // The take sets the TTL it is given, not the one of the insert.
        check_import_row_ttl(e, row.import_arn, std::chrono::hours(2));
    }, import_test_config());
}

// A replay of a ClientToken whose import row is not written yet - the node which accepted the
// import stopped before writing it, or did not write it yet - answers IN_PROGRESS with the TableId
// allocated when the import was accepted, also when no table of that name exists.
SEASTAR_TEST_CASE(test_import_client_token_replay_before_import_row) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        // The import fails without creating the table.
        auto request = make_import_table_request("no_import_row", "token-no-import-row");
        auto response = exec.import_table_successfully(rjson::copy(request));
        const auto& description = rjson::get(response, "ImportTableDescription");
        const auto import_arn = get_string(description, "ImportArn");
        const auto table_id = get_string(description, "TableId");
        wait_for_import(e, response);
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("no_import_row")));
        e.execute_cql(fmt::format("DELETE FROM {}.{} WHERE import_arn = '{}'", db::system_distributed_keyspace::NAME,
                db::system_distributed_keyspace::ALTERNATOR_IMPORT_FROM_S3_IMPORTS, import_arn)).get();

        auto replay = exec.import_table_successfully(std::move(request));
        const auto& replay_description = rjson::get(replay, "ImportTableDescription");
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "ImportStatus"), "IN_PROGRESS");
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "TableId"), table_id);
    });
}
