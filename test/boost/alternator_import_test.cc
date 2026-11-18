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

#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/export.hh"
#include "alternator/import.hh"
#include "alternator/rmw_operation.hh"
#include "cdc/metadata.hh"
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

// The scheduling group in which the last import on this shard made its source, see set_source().
thread_local sstring import_source_scheduling_group;

// Wraps a sharded<alternator::executor>, taking DynamoDB-style JSON requests.
class alternator_test_executor {
    sharded<cdc::metadata> _cdc_md;
    sharded<abort_source> _as;
    utils::alien_worker _dict_trainer{dict_trainer_logger, 19, "test-dict"};
    sharded<sstables::dictionary_service> _dicts;
    sharded<alternator::executor> _exec;
    alternator::rmw_operation::write_isolation _saved_write_isolation;

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

    void stop() {
        _exec.invoke_on_all(&alternator::executor::request_stop).get();
        _exec.stop().get();
        _dicts.stop().get();
        _as.stop().get();
        _cdc_md.stop().get();
        alternator::rmw_operation::default_write_isolation = _saved_write_isolation;
    }

    // Makes every import, on every shard, read `objects` instead of S3.
    void set_source(std::vector<std::pair<sstring, std::string>> objects) {
        _exec.invoke_on_all([objects] (alternator::executor& exec) {
            exec.get_s3_imports().set_source_factory([objects] (sstring, sstring, abort_source&) {
                import_source_scheduling_group = current_scheduling_group().name();
                std::vector<std::pair<sstring, std::shared_ptr<alternator::in_memory_test_storage>>> storages;
                for (const auto& [key, content] : objects) {
                    auto storage = std::make_shared<alternator::in_memory_test_storage>();
                    storage->append(std::as_bytes(std::span(content)));
                    storages.emplace_back(key, std::move(storage));
                }
                return make_ready_future<std::unique_ptr<alternator::import_source>>(
                        alternator::make_in_memory_import_source(std::move(storages)));
            });
        }).get();
    }

    std::variant<rjson::value, alternator::api_error> import_table(rjson::value request) {
        return call(&alternator::executor::import_table, std::move(request));
    }
    rjson::value import_table_successfully_on_shard(unsigned shard, rjson::value request) {
        auto result = _exec.invoke_on(shard, [request = sstring(rjson::print(request))] (alternator::executor& exec) -> future<sstring> {
            service::client_state cs(service::client_state::internal_tag{});
            std::unique_ptr<audit::audit_info_alternator> ai;
            auto result = co_await exec.import_table(cs, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(request), ai);
            if (auto* err = std::get_if<alternator::api_error>(&result)) {
                throw *err;
            }
            co_return sstring(std::get<std::string>(result));
        }).get();
        return rjson::parse(result);
    }
    rjson::value import_table_successfully(rjson::value request) {
        return call_successfully(&alternator::executor::import_table, "ImportTable", std::move(request));
    }
    void create_table(rjson::value request) {
        call_successfully(&alternator::executor::create_table, "CreateTable", std::move(request));
    }
    void put_item(rjson::value request) {
        call_successfully(&alternator::executor::put_item, "PutItem", std::move(request));
    }
    rjson::value update_table_successfully(rjson::value request) {
        return call_successfully(&alternator::executor::update_table, "UpdateTable", std::move(request));
    }
    std::variant<rjson::value, alternator::api_error> describe_table(std::string_view table_name) {
        rjson::value request = rjson::empty_object();
        rjson::add(request, "TableName", rjson::from_string(table_name));
        return call(&alternator::executor::describe_table, std::move(request));
    }
    // Returns the item of hash key `p` and sort key `c` (a number), or nullopt if there is none.
    std::optional<rjson::value> get_item(std::string_view table_name, std::string_view p, std::string_view c) {
        auto request = rjson::parse(fmt::format(R"({{"TableName": "{}", "ConsistentRead": true,
                "Key": {{"p": {{"S": "{}"}}, "c": {{"N": "{}"}}}}}})", table_name, p, c));
        auto response = call_successfully(&alternator::executor::get_item, "GetItem", std::move(request));
        if (const rjson::value* item = rjson::find(response, "Item")) {
            return rjson::copy(*item);
        }
        return std::nullopt;
    }
    // Returns the string attribute "v" of the item of hash key `p` and sort key `c`, failing the
    // test if there is no such item.
    std::string get_item_v(std::string_view table_name, std::string_view p, std::string_view c) {
        auto item = get_item(table_name, p, c);
        BOOST_REQUIRE_MESSAGE(item, fmt::format("the table {} has no item {}/{}", table_name, p, c));
        return std::string(rjson::to_string_view(rjson::get(rjson::get(*item, "v"), "S")));
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

rjson::value make_create_table_request(std::string_view table_name) {
    return rjson::parse(fmt::format(R"({{"TableName": "{}", {}}})", table_name, table_creation_parameters));
}

rjson::value make_import_table_request(std::string_view table_name, std::string_view client_token) {
    return rjson::parse(fmt::format(R"({{
        "ClientToken": "{}",
        "InputFormat": "DYNAMODB_JSON",
        "S3BucketSource": {{"S3Bucket": "bucket", "S3KeyPrefix": "prefix/"}},
        "TableCreationParameters": {{"TableName": "{}", {}}}
    }})", client_token, table_name, table_creation_parameters));
}

// A line of an object of DYNAMODB_JSON: the item {p: <p>, c: <c>, v: <v>}.
std::string item_line(std::string_view p, int c, std::string_view v) {
    return fmt::format(R"({{"Item": {{"p": {{"S": "{}"}}, "c": {{"N": "{}"}}, "v": {{"S": "{}"}}}}}})", p, c, v) + "\n";
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

// Waits until done() returns true, failing the test after two minutes.
template <typename Done>
void wait_until(Done done, std::string_view what) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(2);
    while (!done()) {
        BOOST_REQUIRE_MESSAGE(std::chrono::steady_clock::now() < deadline, fmt::format("timed out waiting for {}", what));
        seastar::sleep(std::chrono::milliseconds(10)).get();
    }
}

cql_test_config import_test_config() {
    cql_test_config cfg;
    // The import metadata is written with LWT.
    cfg.need_remote_proxy = true;
    return cfg;
}

} // anonymous namespace

// An Alternator extension: an import into an existing table writes into it, and keeps its items and
// its id.
SEASTAR_TEST_CASE(test_import_into_existing_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        exec.create_table(make_create_table_request("existing"));
        exec.put_item(rjson::parse(R"({"TableName": "existing", "Item": {"p": {"S": "old"}, "c": {"N": "1"}, "v": {"S": "kept"}}})"));
        auto table_id = get_string(rjson::get(std::get<rjson::value>(exec.describe_table("existing")), "Table"), "TableId");

        exec.set_source({{"0", item_line("new", 1, "imported")}});
        auto response = exec.import_table_successfully(make_import_table_request("existing", "token-existing"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(response, "ImportTableDescription"), "TableId"), table_id);
        auto row = wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE(row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.table_id.to_sstring(), table_id);
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing", "old", "1"), "kept");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing", "new", "1"), "imported");
    }, import_test_config());
}

// An import into an existing table whose TableCreationParameters give another key schema: the live
// table's schema governs the import - the items are validated against it - and the table keeps its
// id.
SEASTAR_TEST_CASE(test_import_into_existing_table_with_other_parameters) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        exec.create_table(make_create_table_request("existing_other"));
        auto table_id = get_string(rjson::get(std::get<rjson::value>(exec.describe_table("existing_other")), "Table"), "TableId");

        exec.set_source({{"0", item_line("a", 1, "x") + item_line("a", 2, "y")}});
        auto response = exec.import_table_successfully(rjson::parse(R"({
            "ClientToken": "token-existing-other",
            "InputFormat": "DYNAMODB_JSON",
            "S3BucketSource": {"S3Bucket": "bucket", "S3KeyPrefix": "prefix/"},
            "TableCreationParameters": {
                "TableName": "existing_other",
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "B"}],
                "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}]
            }
        })"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(response, "ImportTableDescription"), "TableId"), table_id);
        auto row = wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE(row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 2);
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing_other", "a", "1"), "x");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing_other", "a", "2"), "y");
    }, import_test_config());
}

// An import into a table with a GSI fills its view. The import writes in the scheduling group of
// exports and imports, which therefore has its own view update concurrency semaphore (see
// replica::database::start()) - without it, the view updates would log an error and use another
// group's semaphore.
SEASTAR_TEST_CASE(test_import_into_table_with_gsi) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        auto& db = e.local_db();
        auto view_update_sem = [&] (scheduling_group sg) {
            return seastar::with_scheduling_group(sg, [&] { return &db.get_view_update_concurrency_sem(); }).get();
        };
        BOOST_REQUIRE(view_update_sem(db.get_export_import_scheduling_group()) != view_update_sem(default_scheduling_group()));

        auto create = make_create_table_request("with_gsi");
        rjson::push_back(rjson::get(create, "AttributeDefinitions"), rjson::parse(R"({"AttributeName": "v", "AttributeType": "S"})"));
        rjson::add(create, "GlobalSecondaryIndexes", rjson::parse(R"([{
            "IndexName": "by_v",
            "KeySchema": [{"AttributeName": "v", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }])"));
        exec.create_table(std::move(create));

        exec.set_source({{"0", item_line("a", 1, "x") + item_line("a", 2, "y")}});
        auto response = exec.import_table_successfully(make_import_table_request("with_gsi", "token-with-gsi"));
        auto row = wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 2);
        wait_until([&] {
            auto res = e.execute_cql(R"(SELECT * FROM alternator_with_gsi."with_gsi:by_v")").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size() == 2;
        }, "the view of the GSI to have the imported items");
    }, import_test_config());
}

// Reading S3 is not implemented yet, so the default source fails as soon as it is made, and the
// import fails with NotImplemented without creating a table.
SEASTAR_TEST_CASE(test_import_source_failure_creates_no_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        auto response = exec.import_table_successfully(make_import_table_request("no_source", "token-no-source"));
        auto row = wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "NotImplemented");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 0);
        auto description = alternator::make_import_table_description(row);
        BOOST_REQUIRE(rjson::find(description, "FailureMessage"));
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("no_source")));
    }, import_test_config());
}

// A request which CreateTable would refuse because of the keyspace of the new table - here a
// vector index in a keyspace using vnodes - is refused when it is made, and writes no rows.
SEASTAR_TEST_CASE(test_import_refuses_what_create_table_refuses) {
    auto cfg = import_test_config();
    cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::disabled);
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        auto request = make_import_table_request("vnodes_vector", "token-vnodes-vector");
        rjson::add(rjson::get(request, "TableCreationParameters"), "VectorIndexes", rjson::parse(R"([{
            "IndexName": "vector_index",
            "VectorAttribute": {"AttributeName": "x"},
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        }])"));
        auto result = exec.import_table(std::move(request));
        auto* err = std::get_if<alternator::api_error>(&result);
        BOOST_REQUIRE(err);
        BOOST_REQUIRE_EQUAL(err->_type, "ValidationException");
        BOOST_REQUIRE(err->_msg.contains("vnodes"));
        db::alternator_import_table_helper helper(e.qp().local());
        BOOST_REQUIRE(!helper.get_client_token("token-vnodes-vector").get());
    }, std::move(cfg));
}

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
        BOOST_REQUIRE(helper.take_import(taken, row, std::chrono::hours(1)).get());
        auto after = helper.get_import(row.import_arn).get();
        BOOST_REQUIRE_EQUAL(after->node_id, "collector");
        BOOST_REQUIRE_EQUAL(after->import_status, "FAILING");
        BOOST_REQUIRE_EQUAL(after->processed_item_count, 5);
    }, import_test_config());
}

// A replay of a ClientToken whose import row is not written yet - the node which accepted the
// import stopped before writing it, or did not write it yet - answers IN_PROGRESS with the TableId
// allocated when the import was accepted, also when no table of that name exists.
SEASTAR_TEST_CASE(test_import_client_token_replay_before_import_row) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        alternator_test_executor exec;
        exec.start(e);
        auto stop_exec = defer([&] noexcept { try { exec.stop(); } catch (...) { std::terminate(); } });

        // The default source fails the import before it creates the table.
        auto request = make_import_table_request("no_import_row", "token-no-import-row");
        auto response = exec.import_table_successfully(rjson::copy(request));
        const auto& description = rjson::get(response, "ImportTableDescription");
        const auto import_arn = get_string(description, "ImportArn");
        const auto table_id = get_string(description, "TableId");
        wait_for_import(e, sstring(import_arn));
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("no_import_row")));
        e.execute_cql(fmt::format("DELETE FROM {}.{} WHERE import_arn = '{}'", db::system_distributed_keyspace::NAME,
                db::system_distributed_keyspace::ALTERNATOR_IMPORT_FROM_S3_IMPORTS, import_arn)).get();

        auto replay = exec.import_table_successfully(std::move(request));
        const auto& replay_description = rjson::get(replay, "ImportTableDescription");
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "ImportStatus"), "IN_PROGRESS");
        BOOST_REQUIRE_EQUAL(get_string(replay_description, "TableId"), table_id);
    }, import_test_config());
}
