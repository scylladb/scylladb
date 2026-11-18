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
#include "service/cas_shard.hh"
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
#include <zlib.h>

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

    // Makes the executor refuse new work, as the server does when it starts to shut down.
    void request_stop() {
        _exec.invoke_on_all(&alternator::executor::request_stop).get();
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

    // Makes the imports run on this shard - the shard of the requests of import_table() - read the
    // sources which `factory` makes.
    void set_source_factory_on_this_shard(alternator::import_source_factory factory) {
        _exec.local().get_s3_imports().set_source_factory(std::move(factory));
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
    void create_table(rjson::value request) {
        call_successfully(&alternator::executor::create_table, "CreateTable", std::move(request));
    }
    // Starts BatchWriteItem on this shard and returns its future, failed with the error of the
    // request, so that a test can act while the write waits.
    future<> batch_write_item_in_background(rjson::value request) {
        return do_with(service::client_state(service::client_state::internal_tag{}), std::unique_ptr<audit::audit_info_alternator>(),
                [this, request = std::move(request)] (service::client_state& cs, std::unique_ptr<audit::audit_info_alternator>& ai) mutable {
            return _exec.local().batch_write_item(cs, tracing::trace_state_ptr(), empty_service_permit(), std::move(request), ai).then(
                    [] (alternator::executor::request_return_type result) {
                if (auto* err = std::get_if<alternator::api_error>(&result)) {
                    return make_exception_future<>(*err);
                }
                return make_ready_future<>();
            });
        });
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

// Waits until the import which `response`, the response of ImportTable, describes ends, and returns
// its row.
alternator::import_row wait_for_import(cql_test_env& e, const rjson::value& response) {
    return wait_for_import(e, sstring(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn")));
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

// An import into a new table: ImportTable answers IN_PROGRESS at once, and the import creates
// the table with the TableId it reported, and fills it with the items of all the objects, blank
// lines and empty objects being skipped - also with the item of a last line without a newline,
// which the pipeline delivers when it is closed.
SEASTAR_TEST_CASE(test_import_into_new_table) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        const std::string first = item_line("a", 1, "x") + "\n  \n" + item_line("a", 2, "y");
        std::string second = item_line("b", 1, "z");
        second.pop_back();
        exec.set_source({{"prefix/00000.json", first}, {"prefix/empty.json", ""}, {"prefix/00001.json", second}});

        auto response = exec.import_table_successfully(make_import_table_request("new_table", "token-new-table"));
        const rjson::value& initial = rjson::get(response, "ImportTableDescription");
        BOOST_REQUIRE_EQUAL(get_string(initial, "ImportStatus"), "IN_PROGRESS");
        BOOST_REQUIRE_EQUAL(get_string(initial, "ClientToken"), "token-new-table");
        BOOST_REQUIRE_EQUAL(rjson::get(initial, "ProcessedItemCount").GetInt64(), 0);
        BOOST_REQUIRE(!rjson::find(initial, "ProcessedSizeBytes"));
        BOOST_REQUIRE(!rjson::find(initial, "EndTime"));
        const auto import_arn = get_string(initial, "ImportArn");
        BOOST_REQUIRE_EQUAL(import_arn.substr(0, import_arn.rfind("/import/")), get_string(initial, "TableArn"));

        auto row = wait_for_import(e, sstring(import_arn));
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        // The import ran in the scheduling group of exports and imports, not in the request's.
        BOOST_REQUIRE_EQUAL(import_source_scheduling_group, "export_import");
        BOOST_REQUIRE(!row.failure_code);
        BOOST_REQUIRE(!row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 3);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 3);
        BOOST_REQUIRE_EQUAL(row.error_count, 0);
        // The uncompressed size of the objects, blank lines included.
        BOOST_REQUIRE_EQUAL(row.processed_size_bytes.value_or(-1), int64_t(first.size() + second.size()));
        BOOST_REQUIRE(row.completed_at);
        check_import_row_ttl(e, import_arn, std::chrono::days(90));

        auto final_description = alternator::make_import_table_description(row);
        BOOST_REQUIRE_EQUAL(get_string(final_description, "ImportStatus"), "COMPLETED");
        BOOST_REQUIRE(rjson::find(final_description, "EndTime"));
        BOOST_REQUIRE_EQUAL(get_string(final_description, "TableId"), get_string(initial, "TableId"));

        auto described = std::get<rjson::value>(exec.describe_table("new_table"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableStatus"), "ACTIVE");
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableId"), get_string(initial, "TableId"));

        BOOST_REQUIRE_EQUAL(exec.get_item_v("new_table", "a", "1"), "x");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("new_table", "a", "2"), "y");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("new_table", "b", "1"), "z");
    });
}

// Items which fail the validation of PutItem are counted and skipped. A line which is not JSON, or
// is JSON without an Item member, also ends the reading of its object, as in DynamoDB. The import
// ends as FAILED with ItemValidationError, and keeps the valid items.
SEASTAR_TEST_CASE(test_import_counts_invalid_items) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.set_source({
            // Invalid items do not end the reading of their object.
            {"0", item_line("good", 1, "v")
                    + R"({"Item": {"c": {"N": "1"}}})" "\n"                           // missing hash key
                    + R"({"Item": {"p": {"S": "x"}, "c": {"S": "1"}}})" "\n"          // sort key of a wrong type
                    + R"({"Item": {"p": {"S": "x"}, "c": {"N": "1"}, "": {"S": "v"}}})" "\n" // empty attribute name
                    + R"({"Item": 5})" "\n"                                           // Item is not an object
                    + item_line("good", 2, "v")},
            // Not JSON: the rest of the object is skipped.
            {"1", R"({"Item": )" "\n" + item_line("skipped", 1, "v")},
            // No Item member: the rest of the object is skipped.
            {"2", R"({"NotAnItem": {}})" "\n" + item_line("skipped", 2, "v")},
            {"3", item_line("good", 3, "v")},
        });

        auto response = exec.import_table_successfully(make_import_table_request("invalid_items", "token-invalid-items"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ItemValidationError");
        BOOST_REQUIRE_EQUAL(row.error_count, 6);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 3);
        BOOST_REQUIRE_EQUAL(row.processed_item_count, row.imported_item_count + row.error_count);

        for (auto c : {"1", "2", "3"}) {
            BOOST_REQUIRE(exec.get_item("invalid_items", "good", c));
        }
        BOOST_REQUIRE(!exec.get_item("invalid_items", "skipped", "1"));
        BOOST_REQUIRE(!exec.get_item("invalid_items", "skipped", "2"));
        // A failed import leaves its table usable, and ACTIVE.
        auto described = std::get<rjson::value>(exec.describe_table("invalid_items"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableStatus"), "ACTIVE");
    });
}

// Imports items with duplicate keys, and checks that of the items with one key, the last one in
// the source wins. ImportedItemCount counts all of them, as DynamoDB's does.
static void check_duplicate_keys_last_wins(cql_test_env& e, alternator_test_executor& exec, std::string_view table_name) {
    // More keys than one batch has, so that a key comes again while an earlier batch with it may
    // still be written. Without LWT, a later batch wins by its timestamp also then: the wait for
    // the keys in flight is tested by test_import_duplicate_keys_last_wins_after_retried_write.
    // In the last round, each key also comes twice in a row, and both items fall into one batch,
    // where the later one replaces the earlier.
    constexpr int keys = 150;
    constexpr int rounds = 3;
    std::string object = std::string(R"({"Item": {"p": {"S": "merge"}, "c": {"N": "0"}, "v": {"S": "first"}, "x": {"S": "extra"}}})") + "\n"
            + item_line("merge", 0, "second");
    for (int round = 0; round < rounds; ++round) {
        for (int k = 0; k < keys; ++k) {
            if (round == rounds - 1) {
                object += item_line("dup", k, "before-last");
            }
            object += item_line("dup", k, fmt::format("{}", round));
        }
    }
    exec.set_source({{"0", object}});

    auto response = exec.import_table_successfully(make_import_table_request(table_name, fmt::format("token-{}", table_name)));
    auto row = wait_for_import(e, response);
    BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
    BOOST_REQUIRE_EQUAL(row.processed_item_count, (rounds + 1) * keys + 2);
    BOOST_REQUIRE_EQUAL(row.imported_item_count, (rounds + 1) * keys + 2);
    auto merged = exec.get_item(table_name, "merge", "0");
    BOOST_REQUIRE(merged);
    BOOST_REQUIRE_EQUAL(get_string(rjson::get(*merged, "v"), "S"), "second");
    BOOST_REQUIRE(!rjson::find(*merged, "x"));
    for (int k = 0; k < keys; ++k) {
        auto item = exec.get_item(table_name, "dup", fmt::format("{}", k));
        BOOST_REQUIRE(item);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(*item, "v"), "S"), fmt::format("{}", rounds - 1));
    }
}

// The last item with a key wins, without LWT (FORBID_RMW, the default of these tests) and with LWT
// (do_batch_write() then writes each partition of a batch with cas_write()).
SEASTAR_TEST_CASE(test_import_duplicate_keys_last_wins) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        for (auto [isolation, table] : {std::pair{"forbid_rmw", "duplicates"}, std::pair{"always_use_lwt", "duplicates_lwt"}}) {
            alternator::rmw_operation::set_default_write_isolation(isolation);
            check_duplicate_keys_last_wins(e, exec, table);
        }
    });
}

// Compresses `content` into a gzip stream (windowBits 16 + MAX_WBITS selects the gzip format).
static std::string gzip_compress(std::string_view content) {
    z_stream zs = {};
    BOOST_REQUIRE_EQUAL(deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY), Z_OK);
    std::string compressed(deflateBound(&zs, content.size()), '\0');
    zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(content.data()));
    zs.avail_in = content.size();
    zs.next_out = reinterpret_cast<Bytef*>(compressed.data());
    zs.avail_out = compressed.size();
    BOOST_REQUIRE_EQUAL(deflate(&zs, Z_FINISH), Z_STREAM_END);
    compressed.resize(compressed.size() - zs.avail_out);
    deflateEnd(&zs);
    return compressed;
}

// An object which is not a gzip stream counts as one error, and the other objects are still
// imported.
SEASTAR_TEST_CASE(test_import_corrupt_gzip_object) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.set_source({{"0", "this is not a gzip stream\n"}, {"1", gzip_compress(item_line("g", 1, "x"))}});
        auto request = make_import_table_request("corrupt_gzip", "token-corrupt-gzip");
        rjson::add(request, "InputCompressionType", rjson::from_string("GZIP"));
        auto response = exec.import_table_successfully(std::move(request));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ItemValidationError");
        BOOST_REQUIRE_EQUAL(row.error_count, 1);
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 1);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 1);
        BOOST_REQUIRE(exec.get_item("corrupt_gzip", "g", "1"));
    });
}

// An import of a GZIP source: ProcessedSizeBytes counts the decompressed bytes.
SEASTAR_TEST_CASE(test_import_gzip) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        const std::string content = item_line("g", 1, "x") + "\n" + item_line("g", 2, "y");
        exec.set_source({{"0", gzip_compress(content)}});

        auto request = make_import_table_request("gzipped", "token-gzip");
        rjson::add(request, "InputCompressionType", rjson::from_string("GZIP"));
        auto response = exec.import_table_successfully(std::move(request));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 2);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 2);
        BOOST_REQUIRE_EQUAL(row.processed_size_bytes.value_or(-1), int64_t(content.size()));
        auto item = exec.get_item("gzipped", "g", "2");
        BOOST_REQUIRE(item);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(*item, "v"), "S"), "y");
    });
}

// An Alternator extension: an import into an existing table writes into it, and keeps its items and
// its id.
SEASTAR_TEST_CASE(test_import_into_existing_table) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.create_table(make_create_table_request("existing"));
        exec.put_item(rjson::parse(R"({"TableName": "existing", "Item": {"p": {"S": "old"}, "c": {"N": "1"}, "v": {"S": "kept"}}})"));
        auto table_id = get_string(rjson::get(std::get<rjson::value>(exec.describe_table("existing")), "Table"), "TableId");

        exec.set_source({{"0", item_line("new", 1, "imported")}});
        auto response = exec.import_table_successfully(make_import_table_request("existing", "token-existing"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(response, "ImportTableDescription"), "TableId"), table_id);
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE(row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.table_id.to_sstring(), table_id);
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing", "old", "1"), "kept");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing", "new", "1"), "imported");
    });
}

// An import into an existing table whose TableCreationParameters give another key schema: the live
// table's schema governs the import - the items are validated against it - and the table keeps its
// id.
SEASTAR_TEST_CASE(test_import_into_existing_table_with_other_parameters) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
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
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE(row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 2);
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing_other", "a", "1"), "x");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("existing_other", "a", "2"), "y");
    });
}

// The live table's schema governs an import into an existing table: an item with the key of the
// request's TableCreationParameters, but without the key of the live table, is an item error.
SEASTAR_TEST_CASE(test_import_into_existing_table_with_another_key) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.create_table(rjson::parse(R"({"TableName": "other_key", "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})"));
        exec.set_source({{"0", item_line("a", 1, "x") + R"({"Item": {"id": {"S": "b"}}})" "\n"}});
        auto response = exec.import_table_successfully(make_import_table_request("other_key", "token-other-key"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ItemValidationError");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 2);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 1);
        BOOST_REQUIRE_EQUAL(row.error_count, 1);
    });
}

// An import into a table with a GSI fills its view. The import writes in the scheduling group of
// exports and imports, which therefore has its own view update concurrency semaphore (see
// replica::database::start()) - without it, the view updates of the replicas on this node would log
// an error and use another group's semaphore.
SEASTAR_TEST_CASE(test_import_into_table_with_gsi) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        auto& db = e.local_db();
        auto view_update_sem = [&] (scheduling_group sg) {
            return seastar::with_scheduling_group(sg, [&] { return &db.get_view_update_concurrency_sem(); }).get();
        };
        // The group has a semaphore of its own. (Asking in a group without one, such as the default
        // group, would log an error and give the semaphore of the default service level.)
        BOOST_REQUIRE(view_update_sem(db.get_export_import_scheduling_group()) != view_update_sem(get_scheduling_groups().get().statement_scheduling_group));

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
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 2);
        wait_until([&] {
            auto res = e.execute_cql(R"(SELECT * FROM alternator_with_gsi."with_gsi:by_v")").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size() == 2;
        }, "the view of the GSI to have the imported items");
    });
}

// A ClientToken makes ImportTable idempotent: a replay returns the import of the token, also with
// other parameters, unless it names another table - then it is ImportConflictException.
SEASTAR_TEST_CASE(test_import_client_token_replay) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.set_source({{"0", item_line("a", 1, "x")}});
        auto request = make_import_table_request("replayed", "token-replay");
        auto first = exec.import_table_successfully(rjson::copy(request));
        const auto import_arn = get_string(rjson::get(first, "ImportTableDescription"), "ImportArn");
        wait_for_import(e, sstring(import_arn));

        // The same request, and one with another source, return the same import.
        auto replay = exec.import_table_successfully(rjson::copy(request));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay, "ImportTableDescription"), "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay, "ImportTableDescription"), "ImportStatus"), "COMPLETED");
        auto other_source = rjson::copy(request);
        rjson::replace_with_string_name(rjson::get(other_source, "S3BucketSource"), "S3Bucket", rjson::from_string("other-bucket"));
        auto replay_of_other_source = exec.import_table_successfully(std::move(other_source));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay_of_other_source, "ImportTableDescription"), "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(rjson::get(replay_of_other_source, "ImportTableDescription"), "S3BucketSource"), "S3Bucket"), "bucket");
        auto csv = rjson::copy(request);
        rjson::replace_with_string_name(csv, "InputFormat", rjson::from_string("CSV"));
        auto replay_of_csv = exec.import_table_successfully(rjson::copy(csv));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay_of_csv, "ImportTableDescription"), "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay_of_csv, "ImportTableDescription"), "InputFormat"), "DYNAMODB_JSON");
        // A new import in a format Alternator does not support is refused.
        rjson::replace_with_string_name(csv, "ClientToken", rjson::from_string("token-csv"));
        auto csv_result = exec.import_table(std::move(csv));
        auto* csv_err = std::get_if<alternator::api_error>(&csv_result);
        BOOST_REQUIRE(csv_err);
        BOOST_REQUIRE_EQUAL(csv_err->_type, "ValidationException");

        // Another table conflicts.
        auto result = exec.import_table(make_import_table_request("another_table", "token-replay"));
        auto* err = std::get_if<alternator::api_error>(&result);
        BOOST_REQUIRE(err);
        BOOST_REQUIRE_EQUAL(err->_type, "ImportConflictException");
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("another_table")));

        // Two requests with a new ClientToken at once, on two shards, get one import.
        auto request_at_once = make_import_table_request("replayed_at_once", "token-at-once");
        auto [response_a, response_b] = when_all_succeed(exec.import_table_on_shard(0, rjson::copy(request_at_once)),
                exec.import_table_on_shard(smp::count - 1, rjson::copy(request_at_once))).get();
        const auto parsed_a = rjson::parse(response_a);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(parsed_a, "ImportTableDescription"), "ImportArn"),
                get_string(rjson::get(rjson::parse(response_b), "ImportTableDescription"), "ImportArn"));
        BOOST_REQUIRE_EQUAL(wait_for_import(e, parsed_a).import_status, "COMPLETED");
    });
}

// A source which fails before its first object is listed fails the import before the table is
// created, so no table is left behind - as DynamoDB does for a missing bucket. Reading S3 is not
// implemented yet, so the default source fails as soon as it is made.
SEASTAR_TEST_CASE(test_import_source_failure_creates_no_table) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        auto response = exec.import_table_successfully(make_import_table_request("no_source", "token-no-source"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "NotImplemented");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 0);
        auto description = alternator::make_import_table_description(row);
        BOOST_REQUIRE(rjson::find(description, "FailureMessage"));
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("no_source")));
    });
}

// While the node shuts down, ImportTable refuses a new import with an error which the client
// retries, and writes no rows, so that the ClientToken stays free for the retry.
SEASTAR_TEST_CASE(test_import_refused_at_shutdown) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.request_stop();
        auto result = exec.import_table(make_import_table_request("at_shutdown", "token-at-shutdown"));
        auto* err = std::get_if<alternator::api_error>(&result);
        BOOST_REQUIRE(err);
        BOOST_REQUIRE_EQUAL(err->_type, "InternalServerError");
        db::alternator_import_table_helper helper(e.qp().local());
        BOOST_REQUIRE(!helper.get_client_token("token-at-shutdown").get());
    });
}

// A source whose listing fails with import_failure fails the import with its FailureCode. The source
// is listed before the table is created, so no table is left behind.
SEASTAR_TEST_CASE(test_import_listing_failure_creates_no_table) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        class unlistable_source : public alternator::import_source {
        public:
            future<std::optional<alternator::import_source_object>> next_object() override {
                return make_exception_future<std::optional<alternator::import_source_object>>(
                        alternator::import_failure("S3NoSuchBucket", "The bucket does not exist"));
            }
            future<std::unique_ptr<alternator::import_pipeline_interface>> open(const alternator::import_source_object&,
                    std::function<future<>(rjson::value)>, alternator::compression_type) override {
                BOOST_FAIL("nothing was listed");
                return make_ready_future<std::unique_ptr<alternator::import_pipeline_interface>>();
            }
            future<> close() override {
                return make_ready_future<>();
            }
        };
        exec.set_source_factory_on_this_shard([] (sstring, sstring, abort_source&) {
            return make_ready_future<std::unique_ptr<alternator::import_source>>(std::make_unique<unlistable_source>());
        });

        auto response = exec.import_table_successfully(make_import_table_request("unlistable", "token-unlistable"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "S3NoSuchBucket");
        BOOST_REQUIRE(std::holds_alternative<alternator::api_error>(exec.describe_table("unlistable")));
    });
}

// An import of a prefix with no objects completes with zero counters and ProcessedSizeBytes 0, and
// leaves an ACTIVE table with the TableId it reported, as DynamoDB does.
SEASTAR_TEST_CASE(test_import_of_no_objects) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.set_source({});
        auto response = exec.import_table_successfully(make_import_table_request("no_objects", "token-no-objects"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 0);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 0);
        BOOST_REQUIRE_EQUAL(row.error_count, 0);
        BOOST_REQUIRE_EQUAL(row.processed_size_bytes.value_or(-1), 0);
        auto described = std::get<rjson::value>(exec.describe_table("no_objects"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableStatus"), "ACTIVE");
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableId"),
                get_string(rjson::get(response, "ImportTableDescription"), "TableId"));
    });
}

// A table with the name of the target created after the import was accepted, and before the import
// creates its table (while the import waits for its source, which the test holds), fails the
// import with TableAlreadyExists. That table is left as it is: ACTIVE, with its own id and items.
SEASTAR_TEST_CASE(test_import_table_created_meanwhile) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        auto table_created = make_lw_shared<promise<>>();
        bool table_created_set = false;
        // Releases the source before the executor stops, so that the import ends also if the test
        // fails before it releases the source itself.
        auto release_source = defer([&] () noexcept {
            if (!table_created_set) {
                table_created->set_value();
            }
        });
        exec.set_source_factory_on_this_shard([table_created] (sstring, sstring, abort_source&) {
            return table_created->get_future().then([] {
                std::vector<std::pair<sstring, std::shared_ptr<alternator::in_memory_test_storage>>> storages;
                auto storage = std::make_shared<alternator::in_memory_test_storage>();
                const std::string content = item_line("a", 1, "imported");
                storage->append(std::as_bytes(std::span(content)));
                storages.emplace_back("prefix/0", std::move(storage));
                return alternator::make_in_memory_import_source(std::move(storages));
            });
        });

        auto response = exec.import_table_successfully(make_import_table_request("created_meanwhile", "token-created-meanwhile"));
        exec.create_table(make_create_table_request("created_meanwhile"));
        exec.put_item(rjson::parse(R"({"TableName": "created_meanwhile",
                "Item": {"p": {"S": "a"}, "c": {"N": "1"}, "v": {"S": "user"}}})"));
        table_created->set_value();
        table_created_set = true;

        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "TableAlreadyExists");
        BOOST_REQUIRE(!row.target_table_existed);
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 0);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 0);
        auto described = std::get<rjson::value>(exec.describe_table("created_meanwhile"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableStatus"), "ACTIVE");
        BOOST_REQUIRE_NE(get_string(rjson::get(described, "Table"), "TableId"),
                get_string(rjson::get(response, "ImportTableDescription"), "TableId"));
        BOOST_REQUIRE_EQUAL(exec.get_item_v("created_meanwhile", "a", "1"), "user");
    });
}

// Waits until `as` is aborted: a source factory which holds the import before it creates its table.
static future<std::unique_ptr<alternator::import_source>> wait_for_abort(lw_shared_ptr<bool> entered, abort_source& as) {
    *entered = true;
    co_await seastar::sleep_abortable(std::chrono::minutes(5), as);
    BOOST_FAIL("the import was not stopped");
    co_return nullptr;
}

// While an import waits for its source, before it creates its table, a replay of its ClientToken
// answers IN_PROGRESS from its row, and a shutdown fails it with ImportInterrupted, leaving no table.
SEASTAR_TEST_CASE(test_import_stopped_before_table_creation) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        auto entered = make_lw_shared<bool>(false);
        exec.set_source_factory_on_this_shard([entered] (sstring, sstring, abort_source& as) {
            return wait_for_abort(entered, as);
        });

        auto request = make_import_table_request("stopped_early", "token-stopped-early");
        auto response = exec.import_table_successfully(rjson::copy(request));
        const auto import_arn = get_string(rjson::get(response, "ImportTableDescription"), "ImportArn");
        wait_until([&] { return *entered; }, "the import to make its source");
        auto replay = exec.import_table_successfully(std::move(request));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay, "ImportTableDescription"), "ImportArn"), import_arn);
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(replay, "ImportTableDescription"), "ImportStatus"), "IN_PROGRESS");

        exec.stop();
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ImportInterrupted");
        BOOST_REQUIRE(!e.local_db().has_schema("alternator_stopped_early", "stopped_early"));
    });
}

// Items larger than a batch may be (import_batch_bytes) are imported, also one larger than all the
// batches being written may be together (import_bytes_in_flight): its batch takes the whole budget.
SEASTAR_TEST_CASE(test_import_large_items) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        const std::string big(2 * 1024 * 1024, 'b');
        const std::string huge(5 * 1024 * 1024, 'h');
        std::string content;
        for (int c = 1; c <= 6; ++c) {
            content += item_line("a", c, big);
        }
        content += item_line("a", 7, huge) + item_line("a", 8, "small");
        exec.set_source({{"0", content}});

        auto response = exec.import_table_successfully(make_import_table_request("large_items", "token-large-items"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 8);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 8);
        BOOST_REQUIRE_EQUAL(row.processed_size_bytes.value_or(-1), int64_t(content.size()));
        for (int c = 1; c <= 6; ++c) {
            BOOST_REQUIRE(exec.get_item_v("large_items", "a", fmt::to_string(c)) == big);
        }
        BOOST_REQUIRE(exec.get_item_v("large_items", "a", "7") == huge);
        BOOST_REQUIRE_EQUAL(exec.get_item_v("large_items", "a", "8"), "small");
    });
}

// A table dropped after the import's last write - here there is none, its only object is empty -
// and before the import ends still fails it with TableDeleted. The source waits at its end until
// the test dropped the table.
SEASTAR_TEST_CASE(test_import_table_deleted_after_the_last_write) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        class source_waiting_at_its_end : public alternator::import_source {
            future<> _end;
            bool _listed_empty_object = false;
        public:
            explicit source_waiting_at_its_end(future<> end) : _end(std::move(end)) {}
            future<std::optional<alternator::import_source_object>> next_object() override {
                if (!_listed_empty_object) {
                    _listed_empty_object = true;
                    co_return alternator::import_source_object{ .key = "prefix/empty", .size = 0 };
                }
                co_await std::move(_end);
                co_return std::nullopt;
            }
            future<std::unique_ptr<alternator::import_pipeline_interface>> open(const alternator::import_source_object&,
                    std::function<future<>(rjson::value)>, alternator::compression_type) override {
                BOOST_FAIL("an object of size 0 is not read");
                co_return nullptr;
            }
            future<> close() override {
                return make_ready_future<>();
            }
        };
        auto table_dropped = make_lw_shared<promise<>>();
        bool table_dropped_set = false;
        // Releases the source before the executor stops, so that the import ends also if the test
        // fails before it releases the source itself.
        auto release_source = defer([&] () noexcept {
            if (!table_dropped_set) {
                table_dropped->set_value();
            }
        });
        exec.set_source_factory_on_this_shard([table_dropped] (sstring, sstring, abort_source&) {
            return make_ready_future<std::unique_ptr<alternator::import_source>>(
                    std::make_unique<source_waiting_at_its_end>(table_dropped->get_future()));
        });

        auto response = exec.import_table_successfully(make_import_table_request("deleted_at_end", "token-deleted-at-end"));
        wait_until([&] { return std::holds_alternative<rjson::value>(exec.describe_table("deleted_at_end")); }, "the import to create its table");
        e.execute_cql("DROP TABLE alternator_deleted_at_end.deleted_at_end").get();
        table_dropped->set_value();
        table_dropped_set = true;

        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "TableDeleted");
    });
}

// A request which CreateTable would refuse because of the keyspace of the new table - here a
// vector index in a keyspace using vnodes - is refused when it is made, and writes no rows.
SEASTAR_TEST_CASE(test_import_refuses_what_create_table_refuses) {
    auto cfg = import_test_config();
    cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::disabled);
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
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
        // The default source fails the import before it creates the table.
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

// A string which is not valid UTF-8, which the parser of requests lets through, is refused with
// ValidationException before any row is written: the text columns of the import tables could not
// store it. Here in the ClientToken (the key of the client token row), and in S3Bucket.
SEASTAR_TEST_CASE(test_import_refuses_invalid_utf8) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        auto require_refused = [&] (rjson::value request) {
            auto result = exec.import_table(std::move(request));
            auto* err = std::get_if<alternator::api_error>(&result);
            BOOST_REQUIRE(err);
            BOOST_REQUIRE_EQUAL(err->_type, "ValidationException");
        };
        auto invalid_token = make_import_table_request("invalid_utf8", "token");
        rjson::replace_with_string_name(invalid_token, "ClientToken", rjson::from_string("token-\xff"));
        require_refused(std::move(invalid_token));
        auto invalid_bucket = make_import_table_request("invalid_utf8", "token-invalid-bucket");
        rjson::replace_with_string_name(rjson::get(invalid_bucket, "S3BucketSource"), "S3Bucket", rjson::from_string("bucket-\xff"));
        require_refused(std::move(invalid_bucket));
        BOOST_REQUIRE(!db::alternator_import_table_helper(e.qp().local()).get_client_token("token-invalid-bucket").get());
    });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

// Stopping the node during an import fails it with ImportInterrupted.
SEASTAR_TEST_CASE(test_import_interrupted_by_shutdown) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        constexpr auto pause = "alternator_import_from_s3_pause_in_item_handler";
        scoped_error_injection pause_injection(pause);
        exec.set_source({{"0", item_line("a", 1, "x")}});

        auto response = exec.import_table_successfully(make_import_table_request("shutdown", "token-shutdown"));
        const sstring import_arn(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn"));
        wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() > 0; }, "the import to pause");

        exec.stop();
        auto row = wait_for_import(e, import_arn);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ImportInterrupted");
    });
}

// Items are validated against the schema current when their batch started: an item whose attribute
// does not match the key type of a GSI added during the import is counted as an error.
SEASTAR_TEST_CASE(test_import_validates_against_gsi_added_during_the_import) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        exec.create_table(make_create_table_request("gsi_added"));
        constexpr auto pause = "alternator_import_from_s3_pause_in_item_handler";
        scoped_error_injection pause_injection(pause);
        exec.set_source({{"0", std::string(R"({"Item": {"p": {"S": "a"}, "c": {"N": "1"}, "v": {"N": "5"}}})") + "\n"}});

        auto response = exec.import_table_successfully(make_import_table_request("gsi_added", "token-gsi-added"));
        const sstring import_arn(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn"));
        wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() > 0; }, "the import to pause");
        exec.update_table_successfully(rjson::parse(R"({
            "TableName": "gsi_added",
            "AttributeDefinitions": [{"AttributeName": "v", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "by_v",
                "KeySchema": [{"AttributeName": "v", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        })"));
        utils::get_local_injector().disable_on_all(pause).get();

        auto row = wait_for_import(e, import_arn);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "ItemValidationError");
        BOOST_REQUIRE_EQUAL(row.error_count, 1);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 0);
    });
}

// An item keeps its value when a CQL ALTER adds a column with the name of one of its attributes
// after the import read the item, and the item's LWT write moves to the shard of its partition.
// Alternator keeps the attribute in :attrs or in a column of its name, encoded differently in each,
// so the moved write must be built with the schema the item was encoded with, not with the
// current one - which would write the value encoded for :attrs into the new column.
SEASTAR_TEST_CASE(test_import_lwt_write_on_another_shard_after_alter) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        if (smp::count < 2) {
            BOOST_TEST_MESSAGE("Skipping: an LWT write moves to another shard only with 2 shards or more");
            return;
        }
        alternator::rmw_operation::set_default_write_isolation("always_use_lwt");

        exec.create_table(make_create_table_request("altered"));
        // The import runs on this shard: find a partition whose LWT writes run on another one.
        auto schema = e.local_db().find_schema("alternator_altered", "altered");
        std::string moved_p;
        for (int i = 0; moved_p.empty(); ++i) {
            auto p = fmt::format("p{}", i);
            auto dk = dht::decorate_key(*schema, partition_key::from_singular(*schema, data_value(sstring(p))));
            if (service::cas_shard(*schema, dk.token()).shard() != this_shard_id()) {
                moved_p = p;
            }
        }

        constexpr auto pause = "alternator_import_from_s3_pause_in_item_handler";
        scoped_error_injection pause_injection(pause);
        exec.set_source({{"0", item_line(moved_p, 1, "x") + item_line("last", 2, "y")}});
        auto response = exec.import_table_successfully(make_import_table_request("altered", "token-altered"));
        const sstring import_arn(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn"));
        // Let the first item go on - it is encoded and added to the batch - and pause at the second.
        wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() != 0; }, "the import to pause");
        utils::error_injection<true>::receive_message_on_all(pause).get();
        wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() >= 2; }, "the import to pause at its second item");

        e.execute_cql(R"(ALTER TABLE alternator_altered.altered ADD v text)").get();
        utils::get_local_injector().disable_on_all(pause).get();

        auto row = wait_for_import(e, import_arn);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("altered", moved_p, "1"), "x");
        BOOST_REQUIRE_EQUAL(exec.get_item_v("altered", "last", "2"), "y");
    });
}

// Like test_import_lwt_write_on_another_shard_after_alter, for a plain BatchWriteItem: its LWT write
// which moves to the shard of its partition is built with the schema which encoded the item, so a
// CQL ALTER which adds a column with the name of an attribute while the write waits to move does not
// change the value read back. The write waits in the injection alternator_executor_batch_write_wait.
SEASTAR_TEST_CASE(test_batch_write_lwt_write_on_another_shard_after_alter) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        if (smp::count < 2) {
            BOOST_TEST_MESSAGE("Skipping: an LWT write moves to another shard only with 2 shards or more");
            return;
        }
        alternator::rmw_operation::set_default_write_isolation("always_use_lwt");

        exec.create_table(make_create_table_request("batch_altered"));
        // Find a partition whose LWT writes run on another shard than this one, which coordinates
        // the BatchWriteItem.
        auto schema = e.local_db().find_schema("alternator_batch_altered", "batch_altered");
        std::string moved_p;
        for (int i = 0; moved_p.empty(); ++i) {
            auto p = fmt::format("p{}", i);
            auto dk = dht::decorate_key(*schema, partition_key::from_singular(*schema, data_value(sstring(p))));
            if (service::cas_shard(*schema, dk.token()).shard() != this_shard_id()) {
                moved_p = p;
            }
        }

        // If the test fails before it gets the result of the write, waits for the write, which uses
        // the executor. Declared before wait_injection, whose destructor releases the write first.
        std::optional<future<>> write;
        auto join_write = defer([&] () noexcept {
            if (write) {
                write->wait();
                write->ignore_ready_future();
            }
        });
        constexpr auto wait = "alternator_executor_batch_write_wait";
        scoped_error_injection wait_injection(wait, {{"keyspace", "alternator_batch_altered"}, {"table", "batch_altered"},
                {"shard", fmt::to_string(this_shard_id())}});
        write = exec.batch_write_item_in_background(rjson::parse(fmt::format(R"({{"RequestItems": {{"batch_altered": [
                {{"PutRequest": {{"Item": {{"p": {{"S": "{}"}}, "c": {{"N": "1"}}, "v": {{"S": "x"}}}}}}}}]}}}})", moved_p)));
        wait_until([&] { return utils::get_local_injector().enter_count(wait) != 0; }, "the write to wait");
        e.execute_cql("ALTER TABLE alternator_batch_altered.batch_altered ADD v text").get();
        utils::error_injection<true>::receive_message_on_all(wait).get();
        std::exchange(write, std::nullopt)->get();
        BOOST_REQUIRE_EQUAL(exec.get_item_v("batch_altered", moved_p, "1"), "x");
    });
}

// A table deleted during the import fails it with TableDeleted, and a new table with the same name
// gets none of its items: when the table is dropped while the import waits at its first item - the
// next lookup of the schema finds the drop - and while that item waits in its batch - the write of
// the batch finds it.
SEASTAR_TEST_CASE(test_import_table_deleted_during_the_import) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        constexpr auto pause = "alternator_import_from_s3_pause_in_item_handler";
        exec.set_source({{"0", item_line("a", 1, "x") + item_line("a", 2, "y")}});
        for (bool item_in_batch : {false, true}) {
            const std::string name = item_in_batch ? "deleted_at_write" : "deleted";
            // Enabled anew in each round, so that its enter count starts at 0.
            scoped_error_injection pause_injection(pause);
            auto response = exec.import_table_successfully(make_import_table_request(name, "token-" + name));
            const sstring import_arn(get_string(rjson::get(response, "ImportTableDescription"), "ImportArn"));
            wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() != 0; }, "the import to pause");
            if (item_in_batch) {
                // The first item goes into the batch, and the import pauses at the second one.
                utils::error_injection<true>::receive_message_on_all(pause).get();
                wait_until([&] { return utils::error_injection<true>::enter_count_on_all(pause).get() >= 2; }, "the import to pause at its second item");
            }
            // DeleteTable revokes the permissions on the table, which needs the auth service which an
            // internal client does not have, so drop the table with CQL - which an import has to handle
            // as well.
            e.execute_cql(fmt::format("DROP TABLE alternator_{0}.{0}", name)).get();
            // A new table with the same name is not the import's table.
            exec.create_table(make_create_table_request(name));
            utils::get_local_injector().disable_on_all(pause).get();
            auto row = wait_for_import(e, import_arn);
            BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
            BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "TableDeleted");
            BOOST_REQUIRE(!exec.get_item(name, "a", "1"));
            BOOST_REQUIRE(!exec.get_item(name, "a", "2"));
        }
    });
}

// The last item with a key wins also when the write of a batch with an earlier item with the key
// fails and is retried: a later item with that key waits for the writes in flight, the retry
// included.
SEASTAR_TEST_CASE(test_import_duplicate_keys_last_wins_after_retried_write) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        constexpr auto fail_write = "alternator_import_from_s3_write";
        scoped_error_injection fail_write_injection(fail_write, true, {});
        check_duplicate_keys_last_wins(e, exec, "duplicates_retried");
        // The one-shot injection is disabled once it failed the write. The import ran on this shard.
        BOOST_REQUIRE(!utils::get_local_injector().is_enabled(fail_write));
    });
}

// A write which keeps failing for longer than the retry budget - shortened here to 2 seconds - fails
// the import with WriteFailed.
SEASTAR_TEST_CASE(test_import_write_failure) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        constexpr auto fail_writes = "alternator_import_from_s3_write";
        scoped_error_injection fail_writes_injection(fail_writes);
        scoped_error_injection budget_injection("alternator_import_from_s3_write_retry_budget_s", {{"value", "2"}});
        exec.set_source({{"0", item_line("a", 1, "x")}});

        auto response = exec.import_table_successfully(make_import_table_request("write_failure", "token-write-failure"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "FAILED");
        BOOST_REQUIRE_EQUAL(row.failure_code.value_or(""), "WriteFailed");
        BOOST_REQUIRE_EQUAL(row.processed_item_count, 1);
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 0);
        // The table is kept, and the import does not leave it CREATING.
        auto described = std::get<rjson::value>(exec.describe_table("write_failure"));
        BOOST_REQUIRE_EQUAL(get_string(rjson::get(described, "Table"), "TableStatus"), "ACTIVE");
    });
}

// A write of the final state of an import which fails is retried in the background.
SEASTAR_TEST_CASE(test_import_final_write_is_retried) {
    return with_import_executor([] (cql_test_env& e, alternator_test_executor& exec) {
        constexpr auto fail = "alternator_import_from_s3_final_write";
        scoped_error_injection fail_injection(fail, true, {});
        exec.set_source({{"0", item_line("a", 1, "x")}});

        auto response = exec.import_table_successfully(make_import_table_request("final_write", "token-final-write"));
        auto row = wait_for_import(e, response);
        BOOST_REQUIRE_EQUAL(row.import_status, "COMPLETED");
        BOOST_REQUIRE_EQUAL(row.imported_item_count, 1);
        // The one-shot injection is disabled once it failed the write. The import ran on this shard.
        BOOST_REQUIRE(!utils::get_local_injector().is_enabled(fail));
    });
}

#endif // SCYLLA_ENABLE_ERROR_INJECTION
