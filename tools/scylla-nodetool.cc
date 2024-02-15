/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <chrono>
#include <fmt/chrono.h>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/core/units.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>

#include <stdexcept>
#include <yaml-cpp/yaml.h>
#include <ranges>
#include <unordered_map>

#include "api/scrub_status.hh"
#include "gms/application_state.hh"
#include "db_clock.hh"
#include "log.hh"
#include "tools/utils.hh"
#include "utils/http.hh"
#include "utils/human_readable.hh"
#include "utils/pretty_printers.hh"
#include "utils/rjson.hh"
#include "utils/UUID.hh"

namespace bpo = boost::program_options;

using namespace std::chrono_literals;
using namespace tools::utils;

// mimic the behavior of FileUtils::stringifyFileSize
struct file_size_printer {
    uint64_t value;
};

template <>
struct fmt::formatter<file_size_printer> : fmt::formatter<std::string_view> {
    auto format(file_size_printer size, auto& ctx) const {
        using unit_t = std::pair<uint64_t, std::string_view>;
        const unit_t units[] = {
            {1UL << 40, "TB"},
            {1UL << 30, "GB"},
            {1UL << 20, "MB"},
            {1UL << 10, "KB"},
        };
        for (auto [n, prefix] : units) {
            if (size.value > n) {
                auto d = static_cast<float>(size.value) / n;
                return fmt::format_to(ctx.out(), "{:.2f} {}", d, prefix);
            }
        }
        return fmt::format_to(ctx.out(), "{} bytes", size.value);
    }
};

namespace {

const auto app_name = "nodetool";

logging::logger nlog(format("scylla-{}", app_name));

struct operation_failed_on_scylladb : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct operation_failed_with_status : public std::runtime_error {
    const int exit_status;
    explicit operation_failed_with_status(int status)
        : std::runtime_error::runtime_error("exit")
        , exit_status(status)
    {}
};

struct api_request_failed : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

class scylla_rest_client {
    sstring _host;
    uint16_t _port;
    sstring _host_name;
    http::experimental::client _api_client;

    rjson::value do_request(sstring type, sstring path, std::unordered_map<sstring, sstring> params) {
        auto req = http::request::make(type, _host_name, path);
        auto url = req.get_url();
        req.query_parameters = params;

        nlog.trace("Making {} request to {} with parameters {}", type, url, params);


        http::reply::status_type status = http::reply::status_type::ok;
        sstring res;

        _api_client.make_request(std::move(req), seastar::coroutine::lambda([&] (const http::reply& r, input_stream<char> body) -> future<> {
            status = r._status;
            res = co_await util::read_entire_stream_contiguous(body);
        })).get();

        if (status != http::reply::status_type::ok) {
            sstring message;
            try {
                message = sstring(rjson::to_string_view(rjson::parse(res)["message"]));
            } catch (...) {
                message = res;
            }
            throw api_request_failed(fmt::format("error executing {} request to {} with parameters {}: remote replied with status code {}:\n{}",
                    type, url, params, status, message));
        }

        if (res.empty()) {
            nlog.trace("Got empty response");
            return rjson::null_value();
        } else {
            nlog.trace("Got response:\n{}", res);
            return rjson::parse(res);
        }
    }

public:
    scylla_rest_client(sstring host, uint16_t port)
        : _host(std::move(host))
        , _port(port)
        , _host_name(format("{}:{}", _host, _port))
        , _api_client(std::make_unique<utils::http::dns_connection_factory>(_host, _port, false, nlog), 1)
    { }

    ~scylla_rest_client() {
        _api_client.close().get();
    }

    rjson::value post(sstring path, std::unordered_map<sstring, sstring> params = {}) {
        return do_request("POST", std::move(path), std::move(params));
    }

    rjson::value get(sstring path, std::unordered_map<sstring, sstring> params = {}) {
        return do_request("GET", std::move(path), std::move(params));
    }

    // delete is a reserved keyword, using del instead
    rjson::value del(sstring path, std::unordered_map<sstring, sstring> params = {}) {
        return do_request("DELETE", std::move(path), std::move(params));
    }
};

std::vector<sstring> get_keyspaces(scylla_rest_client& client, std::optional<sstring> type = {}) {
    std::unordered_map<sstring, sstring> params;
    if (type) {
        params["type"] = *type;
    }
    auto keyspaces_json = client.get("/storage_service/keyspaces", std::move(params));
    std::vector<sstring> keyspaces;
    for (const auto& keyspace_json : keyspaces_json.GetArray()) {
        keyspaces.emplace_back(rjson::to_string_view(keyspace_json));
    }
    return keyspaces;
}

struct keyspace_and_tables {
    sstring keyspace;
    std::vector<sstring> tables;
};

keyspace_and_tables parse_keyspace_and_tables(scylla_rest_client& client, const bpo::variables_map& vm, const char* common_keyspace_table_arg_name) {
    keyspace_and_tables ret;

    const auto args = vm[common_keyspace_table_arg_name].as<std::vector<sstring>>();

    ret.keyspace = args.at(0);

    const auto all_keyspaces = get_keyspaces(client);
    if (std::ranges::find(all_keyspaces, ret.keyspace) == all_keyspaces.end()) {
        throw std::invalid_argument(fmt::format("keyspace {} does not exist", ret.keyspace));
    }

    if (args.size() > 1) {
        ret.tables.insert(ret.tables.end(), args.begin() + 1, args.end());
    }

    return ret;
}

keyspace_and_tables parse_keyspace_and_tables(scylla_rest_client& client, const bpo::variables_map& vm, bool keyspace_required = true) {
    keyspace_and_tables ret;

    if (vm.contains("keyspace")) {
        ret.keyspace = vm["keyspace"].as<sstring>();
    } else if (keyspace_required) {
        throw std::invalid_argument(fmt::format("keyspace must be specified"));
    } else {
        return ret;
    }

    const auto all_keyspaces = get_keyspaces(client);
    if (std::ranges::find(all_keyspaces, ret.keyspace) == all_keyspaces.end()) {
        throw std::invalid_argument(fmt::format("keyspace {} does not exist", ret.keyspace));
    }

    if (vm.count("table")) {
        ret.tables = vm["table"].as<std::vector<sstring>>();
    }

    return ret;
}

using operation_func = void(*)(scylla_rest_client&, const bpo::variables_map&);

std::map<operation, operation_func> get_operations_with_func();

void cleanup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.count("cleanup_arg")) {
        const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, "cleanup_arg");
        std::unordered_map<sstring, sstring> params;
        if (!tables.empty()) {
            params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
        }
        client.post(format("/storage_service/keyspace_cleanup/{}", keyspace), std::move(params));
    } else {
        client.post("/storage_service/cleanup_all");
    }
}

void clearsnapshot_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::unordered_map<sstring, sstring> params;

    if (vm.count("keyspaces")) {
        std::vector<sstring> keyspaces;
        const auto all_keyspaces = get_keyspaces(client);
        for (const auto& keyspace : vm["keyspaces"].as<std::vector<sstring>>()) {
            if (std::ranges::find(all_keyspaces, keyspace) == all_keyspaces.end()) {
                throw std::invalid_argument(fmt::format("keyspace {} does not exist", keyspace));
            }
            keyspaces.push_back(keyspace);
        }

        if (!keyspaces.empty()) {
            params["kn"] = fmt::to_string(fmt::join(keyspaces.begin(), keyspaces.end(), ","));
        }
    }

    if (vm.count("tag")) {
        params["tag"] = vm["tag"].as<sstring>();
    }

    client.del("/storage_service/snapshots", std::move(params));
}

void compact_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.count("user-defined")) {
        throw std::invalid_argument("--user-defined flag is unsupported");
    }

    std::unordered_map<sstring, sstring> params;
    if (vm.count("flush-memtables")) {
        params["flush_memtables"] = vm["flush-memtables"].as<bool>() ? "true" : "false";
    }

    if (vm.count("compaction_arg")) {
        const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, "compaction_arg");
        if (!tables.empty()) {
            params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
        }
        client.post(format("/storage_service/keyspace_compaction/{}", keyspace), std::move(params));
    } else {
        client.post("/storage_service/compact", std::move(params));
    }
}

void compactionhistory_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto format = vm["format"].as<sstring>();

    static const std::vector<std::string_view> recognized_formats{"text", "json", "yaml"};
    if (std::ranges::find(recognized_formats, format) == recognized_formats.end()) {
        throw std::invalid_argument(fmt::format("invalid format {}, valid formats are: {}", format, recognized_formats));
    }

    const auto history_json = client.get("/compaction_manager/compaction_history");

    struct history_entry {
        utils::UUID id;
        std::string table;
        std::string keyspace;
        int64_t compacted_at;
        int64_t bytes_in;
        int64_t bytes_out;
    };
    std::vector<history_entry> history;

    for (const auto& history_entry_json : history_json.GetArray()) {
        const auto& history_entry_json_object = history_entry_json.GetObject();

        history.emplace_back(history_entry{
                .id = utils::UUID(rjson::to_string_view(history_entry_json_object["id"])),
                .table = std::string(rjson::to_string_view(history_entry_json_object["cf"])),
                .keyspace = std::string(rjson::to_string_view(history_entry_json_object["ks"])),
                .compacted_at = history_entry_json_object["compacted_at"].GetInt64(),
                .bytes_in = history_entry_json_object["bytes_in"].GetInt64(),
                .bytes_out = history_entry_json_object["bytes_out"].GetInt64()});
    }

    std::ranges::sort(history, [] (const history_entry& a, const history_entry& b) { return a.compacted_at > b.compacted_at; });

    const auto format_compacted_at = [] (int64_t compacted_at) {
        const auto compacted_at_time = std::time_t(compacted_at / 1000);
        const auto milliseconds = compacted_at % 1000;
        return fmt::format("{:%FT%T}.{}", fmt::localtime(compacted_at_time), milliseconds);
    };

    if (format == "text") {
        std::array<std::string, 7> header_row{"id", "keyspace_name", "columnfamily_name", "compacted_at", "bytes_in", "bytes_out", "rows_merged"};
        std::array<size_t, 7> max_column_length{};
        for (size_t c = 0; c < header_row.size(); ++c) {
            max_column_length[c] = header_row[c].size();
        }

        std::vector<std::array<std::string, 7>> rows;
        rows.reserve(history.size());
        for (const auto& e : history) {
            rows.push_back({fmt::to_string(e.id), e.keyspace, e.table, format_compacted_at(e.compacted_at), fmt::to_string(e.bytes_in),
                    fmt::to_string(e.bytes_out), ""});
            for (size_t c = 0; c < rows.back().size(); ++c) {
                max_column_length[c] = std::max(max_column_length[c], rows.back()[c].size());
            }
        }

        const auto header_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}}\n", max_column_length[0],
                max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4], max_column_length[5], max_column_length[6]);
        const auto regular_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:>{}}} {{:>{}}} {{:>{}}}\n", max_column_length[0],
                max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4], max_column_length[5], max_column_length[6]);

        fmt::print(std::cout, "Compaction History:\n");
        fmt::print(std::cout, fmt::runtime(header_row_format.c_str()), header_row[0], header_row[1], header_row[2], header_row[3], header_row[4],
                header_row[5], header_row[6]);
        for (const auto& r : rows) {
            fmt::print(std::cout, fmt::runtime(regular_row_format.c_str()), r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        }
    } else if (format == "json") {
        rjson::streaming_writer writer;

        writer.StartObject();
        writer.Key("CompactionHistory");
        writer.StartArray();

        for (const auto& e : history) {
            writer.StartObject();
            writer.Key("id");
            writer.String(fmt::to_string(e.id));
            writer.Key("columnfamily_name");
            writer.String(e.table);
            writer.Key("keyspace_name");
            writer.String(e.keyspace);
            writer.Key("compacted_at");
            writer.String(format_compacted_at(e.compacted_at));
            writer.Key("bytes_in");
            writer.Int64(e.bytes_in);
            writer.Key("bytes_out");
            writer.Int64(e.bytes_out);
            writer.Key("rows_merged");
            writer.String("");
            writer.EndObject();
        }

        writer.EndArray();
        writer.EndObject();
    } else if (format == "yaml") {
        YAML::Emitter yout(std::cout);

        yout << YAML::BeginMap;
        yout << YAML::Key << "CompactionHistory";
        yout << YAML::BeginSeq;

        for (const auto& e : history) {
            yout << YAML::BeginMap;
            yout << YAML::Key << "id";
            yout << YAML::Value << fmt::to_string(e.id);
            yout << YAML::Key << "columnfamily_name";
            yout << YAML::Value << e.table;
            yout << YAML::Key << "keyspace_name";
            yout << YAML::Value << e.keyspace;
            yout << YAML::Key << "compacted_at";
            yout << YAML::Value << YAML::SingleQuoted << format_compacted_at(e.compacted_at);
            yout << YAML::Key << "bytes_in";
            yout << YAML::Value << e.bytes_in;
            yout << YAML::Key << "bytes_out";
            yout << YAML::Value << e.bytes_out;
            yout << YAML::Key << "rows_merged";
            yout << YAML::Value << YAML::SingleQuoted << "";
            yout << YAML::EndMap;
        }

        yout << YAML::EndSeq;
        yout << YAML::EndMap;
    }
}

class Tabulate {
    static constexpr std::string_view COLUMN_DELIMITER = " ";
    std::vector<std::vector<std::string>> _rows;

    int max_column_width(int col) const {
        auto row = std::ranges::max_element(_rows, [col] (const auto& lhs, const auto& rhs) {
            return lhs[col].size() < rhs[col].size();
        });
        return std::max((*row)[col].size(), 1UL);
    }
public:
    template <typename... Fields>
    void add(Fields... fields) {
        if (!_rows.empty()) {
            const auto& header = _rows.front();
            if (header.size() != sizeof...(Fields)) {
                throw std::logic_error("mismatched column number");
            }
        }
        std::vector<std::string> row;
        (row.push_back(fmt::to_string(fields)), ...);
        _rows.push_back(std::move(row));
    }

    void print() const {
        if (_rows.empty()) {
            return;
        }
        const auto nr_cols = _rows.front().size();
        std::vector<unsigned> max_column_widths;
        for (unsigned col = 0; col < nr_cols; col++) {
            max_column_widths.push_back(max_column_width(col));
        }
        for (auto& row : _rows) {
            for (unsigned col = 0; col < nr_cols; col++) {
                auto width = max_column_widths[col];
                fmt::print("{:<{}}{}", row[col], width, col < nr_cols - 1 ? COLUMN_DELIMITER : "");
            }
            fmt::print("\n");
        }
    }
};

std::string format_size(bool human_readable, size_t size, std::string_view unit) {
    if (human_readable && unit == "bytes") {
        return fmt::to_string(utils::pretty_printed_data_size(size));
    } else {
        return fmt::to_string(size);
    }
}

std::string format_percent(uint64_t completed, uint64_t total) {
    if (total == 0) {
        return "n/a";
    }
    auto percent = static_cast<float>(completed) / total * 100;
    std::string formatted;
    fmt::format_to(std::back_inserter(formatted), "{:.2f}%", percent);
    return formatted;
}

void report_compaction_remaining_time(scylla_rest_client& client, uint64_t remaining_bytes) {
    std::string fmt_remaining_time;
    auto res = client.get("/storage_service/compaction_throughput");
    int compaction_throughput_mb_per_sec = res.GetInt();
    if (compaction_throughput_mb_per_sec != 0) {
        auto remaining_time_in_secs = remaining_bytes / (compaction_throughput_mb_per_sec * 1_MiB);
        std::chrono::hh_mm_ss remaining_time{std::chrono::seconds(remaining_time_in_secs)};
        fmt::format_to(std::back_inserter(fmt_remaining_time), "{}h{:0>2}m{:0>2}s",
                       remaining_time.hours().count(),
                       remaining_time.minutes().count(),
                       remaining_time.seconds().count());
    } else {
        fmt_remaining_time = "n/a";
    }
    fmt::print("Active compaction remaining time : {:>10}\n", fmt_remaining_time);
}

void report_compaction_table(scylla_rest_client& client, bool human_readable) {
    auto res = client.get("/compaction_manager/compactions");
    const auto& compactions = res.GetArray();
    if (compactions.Empty()) {
        return;
    }
    uint64_t remaining_bytes = 0;
    Tabulate table;
    table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");
    for (auto& element : compactions) {
        const auto& compaction = element.GetObject();
        std::string_view id = "n/a";
        if (compaction.HasMember("id")) {
            id = rjson::to_string_view(compaction["id"]);
        }
        auto unit = rjson::to_string_view(compaction["unit"]);
        auto completed = compaction["completed"].GetInt64();
        auto total = compaction["total"].GetInt64();
        table.add(id,
                  rjson::to_string_view(compaction["task_type"]),
                  rjson::to_string_view(compaction["ks"]),
                  rjson::to_string_view(compaction["cf"]),
                  format_size(human_readable, completed, unit),
                  format_size(human_readable, total, unit),
                  unit,
                  format_percent(completed, total));
        remaining_bytes += total - completed;
    }
    table.print();
    report_compaction_remaining_time(client, remaining_bytes);
}

void compactionstats_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto res = client.get("/compaction_manager/metrics/pending_tasks_by_table");
    std::map<std::string_view, std::map<std::string_view, long>> pending_task_num_by_table;
    long num_total_pending_tasks = 0;
    for (auto& element : res.GetArray()) {
        const auto& compaction = element.GetObject();
        auto ks = rjson::to_string_view(compaction["ks"]);
        auto cf = rjson::to_string_view(compaction["cf"]);
        auto task = compaction["task"].GetInt64();
        pending_task_num_by_table[ks][cf] = task;
        num_total_pending_tasks += task;
    }
    fmt::print("pending tasks: {}\n", num_total_pending_tasks);
    for (auto& [ks, table_tasks] : pending_task_num_by_table) {
        for (auto& [cf, pending_task_num] : table_tasks) {
            fmt::print("- {}.{}: {}\n", ks, cf, pending_task_num);
        }
    }
    fmt::print("\n");
    report_compaction_table(client, vm["human-readable"].as<bool>());
}

void decommission_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/decommission");
}

void describering_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto keyspace = vm["keyspace"].as<sstring>();
    const auto schema_version_res = client.get("/storage_service/schema_version");

    std::unordered_map<sstring, sstring> params;
    if (vm.count("table")) {
        auto tables = vm["table"].as<std::vector<sstring>>();
        if (tables.size() != 1) {
            throw std::invalid_argument(fmt::format("expected a single table parameter, got {}", tables.size()));
        }
        params["table"] = tables.front();
    }
    const auto ring_res = client.get(format("/storage_service/describe_ring/{}", keyspace), std::move(params));

    fmt::print(std::cout, "Schema Version:{}\n", rjson::to_string_view(schema_version_res));
    fmt::print(std::cout, "TokenRange: \n");
    for (const auto& ring_info : ring_res.GetArray()) {
        const auto start_token = rjson::to_string_view(ring_info["start_token"]);
        const auto end_token = rjson::to_string_view(ring_info["end_token"]);
        const auto endpoints = ring_info["endpoints"].GetArray() | std::views::transform([] (const auto& ep) { return rjson::to_string_view(ep); });
        const auto rpc_endpoints = ring_info["rpc_endpoints"].GetArray() | std::views::transform([] (const auto& ep) { return rjson::to_string_view(ep); });
        const auto endpoint_details = ring_info["endpoint_details"].GetArray() | std::views::transform([] (const auto& ep_details) {
            return format("EndpointDetails(host:{}, datacenter:{}, rack:{})",
                    rjson::to_string_view(ep_details["host"]),
                    rjson::to_string_view(ep_details["datacenter"]),
                    rjson::to_string_view(ep_details["rack"]));
        });
        fmt::print(std::cout, "\tTokenRange(start_token:{}, end_token:{}, endpoints:[{}], rpc_endpoints:[{}], endpoint_details:[{}])\n",
                start_token,
                end_token,
                fmt::join(std::begin(endpoints), std::end(endpoints), ", "),
                fmt::join(std::begin(rpc_endpoints), std::end(rpc_endpoints), ", "),
                fmt::join(std::begin(endpoint_details), std::end(endpoint_details), ", "));
    }
}

void describecluster_operation(scylla_rest_client& client, const bpo::variables_map&) {
    const auto cluster_name_res = client.get("/storage_service/cluster_name");
    const auto snitch_name_res = client.get("/snitch/name");
    // ScyllaDB does not support Dynamic Snitching, and will not.
    // see https://github.com/scylladb/scylladb/issues/208
    const auto dynamic_snitch = "disabled";
    const auto partitioner_name_res = client.get("/storage_service/partitioner_name");
    fmt::print("Cluster Information:\n"
               "\tName: {}\n"
               "\tSnitch: {}\n"
               "\tDynamicEndPointSnitch: {}\n"
               "\tPartitioner: {}\n",
               rjson::to_string_view(cluster_name_res),
               rjson::to_string_view(snitch_name_res),
               dynamic_snitch,
               rjson::to_string_view(partitioner_name_res));

    const auto schema_versions = client.get("/storage_proxy/schema_versions");
    fmt::print("\tSchema versions:\n");
    for (auto& element : schema_versions.GetArray()) {
        const auto& version_hosts = element.GetObject();
        const auto schema_version = rjson::to_string_view(version_hosts["key"]);
        std::vector<std::string_view> hosts;
        for (auto& host : version_hosts["value"].GetArray()) {
            hosts.push_back(rjson::to_string_view(host));
        }
        // Java does not quote when formatting strings in a List, while {fmt}
        // does when formatting a sequence. so we cannot use {fmt} to format
        // the vector<string_view> here.
        fmt::print("\t\t{}: [{}]\n\n", schema_version, fmt::join(hosts, ", "));
    }

}

void disableautocompaction_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("keyspace")) {
        for (const auto& keyspace :  get_keyspaces(client)) {
            client.del(format("/storage_service/auto_compaction/{}", keyspace));
        }
    } else {
        const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm);
        std::unordered_map<sstring, sstring> params;
        if (!tables.empty()) {
            params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
        }
        client.del(format("/storage_service/auto_compaction/{}", keyspace), std::move(params));
    }
}

void disablebackup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/incremental_backups", {{"value", "false"}});
}

void disablebinary_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.del("/storage_service/native_transport");
}

void disablegossip_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.del("/storage_service/gossiping");
}

void drain_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/drain");
}

void enableautocompaction_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("keyspace")) {
        for (const auto& keyspace :  get_keyspaces(client)) {
            client.post(format("/storage_service/auto_compaction/{}", keyspace));
        }
    } else {
        const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm);
        std::unordered_map<sstring, sstring> params;
        if (!tables.empty()) {
            params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
        }
        client.post(format("/storage_service/auto_compaction/{}", keyspace), std::move(params));
    }
}

void enablebackup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/incremental_backups", {{"value", "true"}});
}

void enablebinary_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/native_transport");
}

void enablegossip_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/gossiping");
}

void flush_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, false);
    if (keyspace.empty()) {
        client.post("/storage_service/flush");
        return;
    }
    std::unordered_map<sstring, sstring> params;
    if (!tables.empty()) {
        params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }
    client.post(format("/storage_service/keyspace_flush/{}", keyspace), std::move(params));
}

void getendpoints_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("keyspace") || !vm.count("table") || !vm.count("key")) {
        throw std::invalid_argument("getendpoint requires keyspace, table and partition key arguments");
    }
    auto res = client.get(seastar::format("/storage_service/natural_endpoints/{}",
                                          vm["keyspace"].as<sstring>()),
                          {{"cf", vm["table"].as<sstring>()},
                           {"key", vm["key"].as<sstring>()}});
    for (auto& inet_address : res.GetArray()) {
        fmt::print("{}\n", rjson::to_string_view(inet_address));
    }
}

void getlogginglevels_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto res = client.get("/storage_service/logging_level");
    const auto row_format = "{:<50}{:>10}\n";
    fmt::print(std::cout, "\n");
    fmt::print(std::cout, fmt::runtime(row_format), "Logger Name", "Log Level");
    for (const auto& element : res.GetArray()) {
        const auto& logger_obj = element.GetObject();
        fmt::print(
                std::cout,
                fmt::runtime(row_format),
                rjson::to_string_view(logger_obj["key"]),
                rjson::to_string_view(logger_obj["value"]));
    }
}

void gettraceprobability_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto res = client.get("/storage_service/trace_probability");
    fmt::print(std::cout, "Current trace probability: {}\n", res.GetDouble());
}

void gossipinfo_operation(scylla_rest_client& client, const bpo::variables_map&) {
    auto res = client.get("/failure_detector/endpoints");
    for (const auto& element : res.GetArray()) {
        const auto& endpoint = element.GetObject();
        fmt::print("/{}\n"
                   "  generation:{}\n"
                   "  heartbeat:{}\n",
                   rjson::to_string_view(endpoint["addrs"]),
                   endpoint["generation"].GetInt64(),
                   endpoint["version"].GetInt64());

        for (auto& element : endpoint["application_state"].GetArray()) {
            const auto& obj = element.GetObject();
            auto state = static_cast<gms::application_state>(obj["application_state"].GetInt());
            if (state == gms::application_state::TOKENS) {
                // skip tokens' state
                continue;
            }
            fmt::print("  {}:{}\n", state, rjson::to_string_view(obj["value"]));
        }
    }
}

void listsnapshots_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto snapshots = client.get("/storage_service/snapshots");
    const auto true_size = client.get("/storage_service/snapshots/size/true").GetInt64();

    std::array<std::string, 5> header_row{"Snapshot name", "Keyspace name", "Column family name", "True size", "Size on disk"};
    std::array<size_t, 5> max_column_length{};
    for (size_t c = 0; c < header_row.size(); ++c) {
        max_column_length[c] = header_row[c].size();
    }

    auto format_hr_size = [] (const utils::human_readable_value hrv) {
        if (!hrv.suffix || hrv.suffix == 'B') {
            return fmt::format("{} B  ", hrv.value);
        }
        return fmt::format("{} {}iB", hrv.value, hrv.suffix);
    };

    std::vector<std::array<std::string, 5>> rows;
    for (const auto& snapshot_by_name : snapshots.GetArray()) {
        const auto snapshot_name = std::string(rjson::to_string_view(snapshot_by_name.GetObject()["key"]));
        for (const auto& snapshot : snapshot_by_name.GetObject()["value"].GetArray()) {
            rows.push_back({
                    snapshot_name,
                    std::string(rjson::to_string_view(snapshot["ks"])),
                    std::string(rjson::to_string_view(snapshot["cf"])),
                    format_hr_size(utils::to_hr_size(snapshot["live"].GetInt64())),
                    format_hr_size(utils::to_hr_size(snapshot["total"].GetInt64()))});

            for (size_t c = 0; c < rows.back().size(); ++c) {
                max_column_length[c] = std::max(max_column_length[c], rows.back()[c].size());
            }
        }
    }

    const auto header_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}}\n", max_column_length[0],
            max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4]);
    const auto regular_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:>{}}} {{:>{}}}\n", max_column_length[0],
            max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4]);

    fmt::print(std::cout, "Snapshot Details:\n");
    fmt::print(std::cout, fmt::runtime(header_row_format.c_str()), header_row[0], header_row[1], header_row[2], header_row[3], header_row[4]);
    for (const auto& r : rows) {
        fmt::print(std::cout, fmt::runtime(regular_row_format.c_str()), r[0], r[1], r[2], r[3], r[4]);
    }

    fmt::print(std::cout, "\nTotal TrueDiskSpaceUsed: {}\n\n", format_hr_size(utils::to_hr_size(true_size)));
}

void move_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    throw std::invalid_argument("This operation is not supported");
}

void help_operation(const tool_app_template::config& cfg, const bpo::variables_map& vm) {
    if (vm.count("command")) {
        const auto command = vm["command"].as<sstring>();
        auto ops = get_operations_with_func();
        auto keys = ops | boost::adaptors::map_keys;
        auto it = std::ranges::find_if(keys, [&] (const operation& op) { return op.name() == command; });
        if (it == keys.end()) {
            throw std::invalid_argument(fmt::format("unknown command {}", command));
        }

        const auto& op = *it;

        fmt::print(std::cout, "{}\n\n", op.summary());
        fmt::print(std::cout, "{}\n\n", op.description());

        // FIXME
        // The below code is needed because we don't have complete access to the
        // internal options descriptions inside the app-template.
        // This will be addressed once https://github.com/scylladb/seastar/pull/1762
        // goes in.

        bpo::options_description opts_desc(fmt::format("scylla-{} options", app_name));
        opts_desc.add_options()
                ("help,h", "show help message")
                ;
        opts_desc.add_options()
                ("help-seastar", "show help message about seastar options")
                ;
        opts_desc.add_options()
                ("help-loggers", "print a list of logger names and exit")
                ;
        if (cfg.global_options) {
            for (const auto& go : *cfg.global_options) {
                go.add_option(opts_desc);
            }
        }
        if (cfg.global_positional_options) {
            for (const auto& gpo : *cfg.global_positional_options) {
                gpo.add_option(opts_desc);
            }
        }

        bpo::options_description op_opts_desc(op.name());
        for (const auto& opt : op.options()) {
            opt.add_option(op_opts_desc);
        }
        for (const auto& opt : op.positional_options()) {
            opt.add_option(opts_desc);
        }
        if (!op.options().empty()) {
            opts_desc.add(op_opts_desc);
        }

        fmt::print(std::cout, "{}\n", opts_desc);
    } else {
        fmt::print(std::cout, "usage: nodetool [(-p <port> | --port <port>)] [(-h <host> | --host <host>)] <command> [<args>]\n\n");
        fmt::print(std::cout, "The most commonly used nodetool commands are:\n");
        for (auto [op, _] : get_operations_with_func()) {
            fmt::print(std::cout, "    {:<26} {}\n", op.name(), op.summary());
        }
        fmt::print(std::cout, "\nSee 'nodetool help <command>' for more information on a specific command.\n\n");
    }
}

void rebuild_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::unordered_map<sstring, sstring> params;
    if (vm.count("source-dc")) {
        params["source_dc"] = vm["source-dc"].as<sstring>();
    }
    client.post("/storage_service/rebuild", std::move(params));
}

void refresh_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("keyspace") || !vm.count("table")) {
        throw std::invalid_argument("required parameters are missing: keyspace and/or table");
    }
    std::unordered_map<sstring, sstring> params{{"cf", vm["table"].as<sstring>()}};
    if (vm.count("load-and-stream")) {
        params["load_and_stream"] = "true";
    }
    if (vm.count("primary-replica-only")) {
        if (!vm.count("load-and-stream")) {
            throw std::invalid_argument("--primary-replica-only|-pro takes no effect without --load-and-stream|-las");
        }
        params["primary_replica_only"] = "true";
    }
    client.post(format("/storage_service/sstables/{}", vm["keyspace"].as<sstring>()), std::move(params));
}

void removenode_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("remove-operation")) {
        throw std::invalid_argument("required parameters are missing: remove-operation, pass one of (status, force or a host id)");
    }
    const auto op = vm["remove-operation"].as<sstring>();
    if (op == "status" || op == "force") {
        if (vm.count("ignore-dead-nodes")) {
            throw std::invalid_argument("cannot use --ignore-dead-nodes with status or force");
        }
        fmt::print(std::cout, "RemovalStatus: {}\n", rjson::to_string_view(client.get("/storage_service/removal_status")));
        if (op == "force") {
            client.post("/storage_service/force_remove_completion");
        }
    } else {
        std::unordered_map<sstring, sstring> params{{"host_id", op}};
        if (vm.count("ignore-dead-nodes")) {
            params["ignore_nodes"] = vm["ignore-dead-nodes"].as<sstring>();
        }
        client.post("/storage_service/remove_node", std::move(params));
    }
}

void repair_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::vector<sstring> keyspaces, tables;
    if (vm.contains("keyspace")) {
        auto res = parse_keyspace_and_tables(client, vm, true);
        keyspaces.push_back(std::move(res.keyspace));
        tables = std::move(res.tables);
    } else {
        keyspaces = get_keyspaces(client, "non_local_strategy");
    }

    if (vm.contains("partitioner-range") && (vm.contains("in-dc") || vm.contains("in-hosts"))) {
        throw std::invalid_argument("primary range repair should be performed on all nodes in the cluster");
    }

    // Use the same default param-set that the legacy nodetool uses, makes testing easier.
    std::unordered_map<sstring, sstring> repair_params{
            {"trace", "false"},
            {"ignoreUnreplicatedKeyspaces", "false"},
            {"parallelism", "parallel"},
            {"incremental", "false"},
            {"pullRepair", "false"},
            {"primaryRange", "false"},
            {"jobThreads", "1"}};

    if (!tables.empty()) {
        repair_params["columnFamilies"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }

    if (vm.contains("trace")) {
        repair_params["trace"] = "true";
    }

    if (vm.contains("start-token") || vm.contains("end-token")) {
        const auto st = vm.contains("start-token") ? fmt::to_string(vm["start-token"].as<uint64_t>()) : "";
        const auto et = vm.contains("end-token") ? fmt::to_string(vm["end-token"].as<uint64_t>()) : "";
        repair_params["ranges"] = format("{}:{}", st, et);
    }

    if (vm.contains("ignore-unreplicated-keyspaces")) {
        repair_params["ignoreUnreplicatedKeyspaces"] = "true";
    }

    if (vm.contains("in-hosts")) {
        const auto hosts = vm["in-hosts"].as<std::vector<sstring>>();
        repair_params["hosts"] = fmt::to_string(fmt::join(hosts.begin(), hosts.end(), ","));
    }

    if (vm.contains("sequential")) {
        repair_params["parallelism"] = "sequential";
    } else if (vm.contains("dc-parallel")) {
        repair_params["parallelism"] = "dc_parallel";
    }

    if (vm.contains("in-local-dc")) {
        const auto res = client.get("/snitch/datacenter");
        repair_params["dataCenters"] = sstring(rjson::to_string_view(res));
    } else if (vm.contains("in-dc")) {
        const auto dcs = vm["in-dc"].as<std::vector<sstring>>();
        repair_params["dataCenters"] = fmt::to_string(fmt::join(dcs.begin(), dcs.end(), ","));
    }

    if (vm.contains("pull")) {
        repair_params["pullRepair"] = "true";
    }

    if (vm.contains("partitioner-range")) {
        repair_params["primaryRange"] = "true";
    }

    auto log = [&] (const char* fmt, auto&&... param) {
        const auto msg = fmt::format(fmt::runtime(fmt), param...);
        using clock = std::chrono::system_clock;
        const auto n = clock::now();
        const auto t = clock::to_time_t(n);
        const auto ms = (n - clock::from_time_t(t)) / 1ms;
        fmt::print("[{:%F %T},{:03d}] {}\n", fmt::localtime(t), ms, msg);
    };

    size_t failed = 0;
    for (const auto& keyspace : keyspaces) {
        const auto url = format("/storage_service/repair_async/{}", keyspace);

        const auto id = client.post(url, repair_params).GetInt();

        log("Starting repair command #{}, repairing 1 ranges for keyspace {} (parallelism=SEQUENTIAL, full=true)", id, keyspace);
        log("Repair session {}", id);

        std::unordered_map<sstring, sstring> poll_params{{"id", fmt::to_string(id)}};

        auto get_status = [&] {
            const auto res = client.get(url, poll_params);
            return sstring(rjson::to_string_view(res));
        };

        auto status = get_status();

        while (status == "RUNNING") {
            sleep(std::chrono::milliseconds(200)).get();
            status = get_status();
        }

        if (status == "SUCCESSFUL") {
            log("Repair session {} finished", id);
        } else {
            log("Repair session {} failed", id);
            ++failed;
        }
    }

    if (failed) {
        throw operation_failed_on_scylladb(format("{} out of {} repair session(s) failed", failed, keyspaces.size()));
    }
}

struct host_stat {
    sstring endpoint;
    std::optional<float> ownership;
    sstring token;
};

class SnitchInfo {
    // memorize the snitch info
    scylla_rest_client& _client;
    std::map<std::string, sstring> _endpoint_to_dc;
    std::map<std::string, sstring> _endpoint_to_rack;

public:
    SnitchInfo(scylla_rest_client& client)
        : _client{client}
    {}
    sstring get_datacenter(const std::string& host) {
        if (auto found = _endpoint_to_dc.find(host);
            found != _endpoint_to_dc.end()) {
            return found->second;
        }
        auto res = _client.get("/snitch/datacenter", {{"host", sstring(host)}});
        auto dc = sstring(rjson::to_string_view(res));
        _endpoint_to_dc.emplace(host, dc);
        return dc;
    }
    sstring get_rack(const std::string& host) {
        if (auto found = _endpoint_to_rack.find(host);
            found != _endpoint_to_rack.end()) {
            return found->second;
        }
        auto res = _client.get("/snitch/rack", {{"host", sstring(host)}});
        auto rack = sstring(rjson::to_string_view(res));
        _endpoint_to_rack.emplace(host, rack);
        return rack;
    }
};

using ownership_by_dc_t = std::map<sstring, std::vector<host_stat>>;
ownership_by_dc_t get_ownership_by_dc(SnitchInfo& snitch,
                                      const std::vector<std::pair<sstring, std::string>>& token_to_endpoint,
                                      const std::map<sstring, float>& endpoint_to_ownership) {
    ownership_by_dc_t ownership_by_dc;
    std::map<std::string_view, std::string_view> endpoint_to_dc;
    for (auto& [token, endpoint] : token_to_endpoint) {
        sstring dc = snitch.get_datacenter(endpoint);
        std::optional<float> ownership;
        if (auto found = endpoint_to_ownership.find(endpoint);
            found != endpoint_to_ownership.end()) {
            ownership = found->second;
        }
        ownership_by_dc[dc].emplace_back(host_stat{endpoint, ownership, token});
    }
    return ownership_by_dc;
}

auto get_endpoints_of_status(scylla_rest_client& client, std::string_view status) {
    auto res = client.get(seastar::format("/gossiper/endpoint/{}", status));
    std::set<sstring> endpoints;
    for (auto& element : res.GetArray()) {
        endpoints.insert(sstring(rjson::to_string_view(element)));
    }
    return endpoints;
}

auto get_nodes_of_state(scylla_rest_client& client, std::string_view state) {
    auto res = client.get(seastar::format("/storage_service/nodes/{}", state));
    std::set<sstring> nodes;
    for (auto& element : res.GetArray()) {
        nodes.insert(sstring(rjson::to_string_view(element)));
    }
    return nodes;
}

// deserialize httpd::storage_service_json back to a map
template<typename Value>
std::map<sstring, Value> rjson_to_map(const rjson::value& v) {
    std::map<sstring, Value> result;
    for (auto& element : v.GetArray()) {
        const auto& object = element.GetObject();
        auto key = rjson::to_string_view(object["key"]);
        Value v;
        try {
            v = object["value"].Get<Value>();
        } catch (const rjson::error&) {
            v = boost::lexical_cast<Value>(object["value"].Get<std::string>());
        }
        result.emplace(sstring(key), v);
    }
    return result;
}

template<typename Value>
std::vector<std::pair<sstring, Value>> rjson_to_vector(const rjson::value& v) {
    std::vector<std::pair<sstring, Value>> result;
    for (auto& element : v.GetArray()) {
        const auto& object = element.GetObject();
        auto key = rjson::to_string_view(object["key"]);
        Value v = object["value"].Get<Value>();
        result.emplace_back(sstring(key), v);
    }
    return result;
}

std::string last_token_in_hosts(const std::vector<std::pair<sstring, std::string>>& tokens_endpoint,
                                const std::vector<host_stat>& host_stats) {
    if (host_stats.size() <= 2) {
        return {};
    }
    auto& last_host = host_stats.back();
    auto last_token = std::find_if(tokens_endpoint.rbegin(),
                                   tokens_endpoint.rend(),
                                   [&last_host] (auto& token_endpoint) {
                                       return token_endpoint.second == last_host.endpoint;
                                   });
    assert(last_token != tokens_endpoint.rend());
    return last_token->first;
}

void print_dc(scylla_rest_client& client,
              SnitchInfo& snitch,
              const sstring& dc,
              unsigned max_endpoint_width,
              bool resolve_ip,
              const sstring& last_token,
              const std::multimap<std::string_view, std::string_view>& endpoints_to_tokens,
              const std::vector<host_stat>& host_stats) {
    fmt::print("\n"
               "Datacenter: {}\n"
               "==========\n", dc);
    const auto fmt_str = fmt::runtime("{:<{}}  {:<12}{:<7}{:<8}{:<16}{:<20}{:<44}\n");
    fmt::print(fmt_str,
               "Address", max_endpoint_width,
               "Rack", "Status", "State", "Load", "Owns", "Token");

    if (host_stats.size() > 1) {
        fmt::print(fmt_str,
                   "", max_endpoint_width,
                   "", "", "", "", "", last_token);
    } else {
        fmt::print("\n");
    }

    const auto live_nodes = get_endpoints_of_status(client, "live");
    const auto down_nodes = get_endpoints_of_status(client, "down");
    const auto joining_nodes = get_nodes_of_state(client, "joining");
    const auto leaving_nodes = get_nodes_of_state(client, "leaving");
    const auto moving_nodes = get_nodes_of_state(client, "moving");
    const auto load_map = rjson_to_map<double>(client.get("/storage_service/load_map"));
    for (auto& stat : host_stats) {
        auto addr = stat.endpoint;
        if (resolve_ip) {
            addr = net::dns::resolve_addr(net::inet_address(addr)).get();
        }
        auto rack = snitch.get_rack(stat.endpoint);

        sstring status = "?";
        if (live_nodes.contains(stat.endpoint)) {
            status = "Up";
        } else if (down_nodes.contains(stat.endpoint)) {
            status = "Down";
        }

        sstring state;
        if (joining_nodes.contains(stat.endpoint)) {
            state = "Joining";
        } else if (leaving_nodes.contains(stat.endpoint)) {
            state = "Leaving";
        } else if (moving_nodes.contains(stat.endpoint)) {
            state = "Moving";
        } else {
            state = "Normal";
        }

        sstring load = "?";
        if (auto found = load_map.find(stat.endpoint); found != load_map.end()) {
            load = fmt::to_string(file_size_printer(found->second));
        }

        sstring ownership = "?";
        if (stat.ownership) {
            ownership = fmt::format("{:.2f}%", *stat.ownership * 100);
        }
        fmt::print(fmt_str,
                   stat.endpoint, max_endpoint_width,
                   rack, status, state, load, ownership, stat.token);
    }
}

void ring_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("keyspace")) {
        throw std::invalid_argument("required parameters are missing: keyspace");
    }
    bool resolve_ip = vm["resolve-ip"].as<bool>();

    std::multimap<std::string_view, std::string_view> endpoints_to_tokens;
    bool have_vnodes = false;
    size_t max_endpoint_width = 0;
    auto tokens_endpoint = rjson_to_vector<std::string>(client.get("/storage_service/tokens_endpoint"));
    for (auto& [token, endpoint] : tokens_endpoint) {
        if (endpoints_to_tokens.contains(endpoint)) {
            have_vnodes = true;
        }
        endpoints_to_tokens.emplace(endpoint, token);
        max_endpoint_width = std::max(max_endpoint_width, endpoint.size());
    }
    // Calculate per-token ownership of the ring
    std::string_view warnings;
    std::map<sstring, float> endpoint_to_ownership;
    try {
        auto ownership_res = client.get(seastar::format("/storage_service/ownership/{}",
                                                        vm["keyspace"].as<sstring>()));
        endpoint_to_ownership = rjson_to_map<float>(ownership_res);
    } catch (const api_request_failed&) {
        warnings = "Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless\n";
    }

    SnitchInfo snitch{client};
    for (auto& [dc, host_stats] : get_ownership_by_dc(snitch,
                                                      tokens_endpoint,
                                                      endpoint_to_ownership)) {
        auto last_token = last_token_in_hosts(tokens_endpoint, host_stats);
        print_dc(client, snitch, dc, max_endpoint_width, resolve_ip,
                 last_token, endpoints_to_tokens, host_stats);
    }

    if (have_vnodes) {
        fmt::print(R"(
  Warning: "nodetool ring" is used to output all the tokens of a node.
  To view status related info of a node use "nodetool status" instead.


)");
    }
    fmt::print("  {}", warnings);
}

void scrub_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, false);
    if (keyspace.empty()) {
        throw std::invalid_argument("missing mandatory positional argument: keyspace");
    }
    if (vm.count("skip-corrupted") && vm.count("mode")) {
        throw std::invalid_argument("cannot use --skip-corrupted when --mode is used");
    }

    std::unordered_map<sstring, sstring> params;

    if (!tables.empty()) {
        params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }

    if (vm.count("mode")) {
        params["scrub_mode"] = vm["mode"].as<sstring>();
    } else if (vm.count("skip-corrupted")) {
        params["scrub_mode"] = "SKIP";
    }

    if (vm.count("quarantine-mode")) {
        params["quarantine_mode"] = vm["quarantine-mode"].as<sstring>();
    }

    if (vm.count("no-snapshot")) {
        params["disable_snapshot"] = "true";
    }

    auto res = client.get(format("/storage_service/keyspace_scrub/{}", keyspace), std::move(params)).GetInt();

    switch (api::scrub_status(res)) {
        case api::scrub_status::successful:
        case api::scrub_status::unable_to_cancel:
            return;
        case api::scrub_status::aborted:
            throw operation_failed_on_scylladb("scrub failed: aborted");
        case api::scrub_status::validation_errors:
            throw operation_failed_on_scylladb("scrub failed: there are invalid sstables");
    }
}

void setlogginglevel_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("logger") || !vm.count("level")) {
        throw std::invalid_argument("resetting logger(s) is not supported yet, the logger and level parameters are required");
    }
    client.post(format("/system/logger/{}", vm["logger"].as<sstring>()), {{"level", vm["level"].as<sstring>()}});
}

void settraceprobability_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("trace_probability")) {
        throw std::invalid_argument("required parameters are missing: trace_probability");
    }
    const auto value = vm["trace_probability"].as<double>();
    if (value < 0.0 or value > 1.0) {
        throw std::invalid_argument("trace probability must be between 0 and 1");
    }
    client.post("/storage_service/trace_probability", {{"probability", fmt::to_string(value)}});
}

void snapshot_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::unordered_map<sstring, sstring> params;

    sstring kn_msg;

    if (vm.count("keyspace-table-list")) {
        if (vm.count("table")) {
            throw std::invalid_argument("when specifying the keyspace-table list for a snapshot, you should not specify table(s)");
        }
        if (vm.count("keyspaces")) {
            throw std::invalid_argument("when specifying the keyspace-table list for a snapshot, you should not specify keyspace(s)");
        }

        const auto kt_list_str = vm["keyspace-table-list"].as<sstring>();
        std::vector<sstring> kt_list;
        boost::split(kt_list, kt_list_str, boost::algorithm::is_any_of(","));

        std::vector<sstring> components;
        for (const auto& kt : kt_list) {
            components.clear();
            boost::split(components, kt, boost::algorithm::is_any_of("."));
            if (components.size() != 2) {
                throw std::invalid_argument(fmt::format("invalid keyspace.table: {}, keyspace and table must be separated by exactly one dot", kt));
            }
        }

        if (kt_list.size() == 1) {
            params["kn"] = components[0];
            params["cf"] = components[1];
            kn_msg = format("{}.{}", params["kn"], params["cf"]);
        } else {
            params["kn"] = kt_list_str;
        }
    } else {
        if (vm.count("keyspaces")) {
            const auto keyspaces = vm["keyspaces"].as<std::vector<sstring>>();

            if (keyspaces.size() > 1 && vm.count("table")) {
                throw std::invalid_argument("when specifying the table for the snapshot, you must specify one and only one keyspace");
            }

            params["kn"] = fmt::to_string(fmt::join(keyspaces.begin(), keyspaces.end(), ","));
        } else {
            kn_msg = "all keyspaces";
        }

        if (vm.count("table")) {
            params["cf"] = vm["table"].as<sstring>();
        }
    }

    if (vm.count("tag")) {
        params["tag"] = vm["tag"].as<sstring>();
    } else {
        params["tag"] = fmt::to_string(db_clock::now().time_since_epoch().count());
    }

    if (vm.count("skip-flush")) {
        params["sf"] = "true";
    } else {
        params["sf"] = "false";
    }

    client.post("/storage_service/snapshots", params);

    if (kn_msg.empty()) {
        kn_msg = params["kn"];
    }

    fmt::print(std::cout, "Requested creating snapshot(s) for [{}] with snapshot name [{}] and options {{skipFlush={}}}\n",
            kn_msg,
            params["tag"],
            params["sf"]);
    fmt::print(std::cout, "Snapshot directory: {}\n", params["tag"]);
}

void statusbackup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto status = client.get("/storage_service/incremental_backups");
    fmt::print(std::cout, "{}\n", status.GetBool() ? "running" : "not running");
}

void statusbinary_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto status = client.get("/storage_service/native_transport");
    fmt::print(std::cout, "{}\n", status.GetBool() ? "running" : "not running");
}

void statusgossip_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto status = client.get("/storage_service/gossiping");
    fmt::print(std::cout, "{}\n", status.GetBool() ? "running" : "not running");
}

void stop_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.count("id")) {
        throw std::invalid_argument("stopping compactions by id is not implemented");
    }
    if (!vm.count("compaction_type")) {
        throw std::invalid_argument("missing required parameter: compaction_type");
    }

    static const std::vector<std::string_view> recognized_compaction_types{"COMPACTION", "CLEANUP", "SCRUB", "RESHAPE", "RESHARD", "UPGRADE"};

    const auto compaction_type = vm["compaction_type"].as<sstring>();

    if (std::ranges::find(recognized_compaction_types, compaction_type) == recognized_compaction_types.end()) {
        throw std::invalid_argument(fmt::format("invalid compaction type: {}", compaction_type));
    }

    client.post("/compaction_manager/stop_compaction", {{"type", compaction_type}});
}

void toppartitions_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    // sanity check the arguments
    auto list_size = vm["size"].as<int>();
    auto capacity = vm["capacity"].as<int>();
    if (list_size >= capacity) {
        throw std::invalid_argument("TopK count (-k) option must be smaller than the summary capacity (-s)");
    }
    {
        // scylla's API server does not use samplers, but let's verify them anyway
        std::vector<sstring> samplers;
        boost::split(samplers, vm["samplers"].as<sstring>(), boost::algorithm::is_any_of(","));
        for (auto& sampler : samplers) {
            auto sampler_lo = boost::to_lower_copy(sampler);
            if (sampler_lo != "reads" && sampler_lo != "writes") {
                throw std::invalid_argument(
                    fmt::format("{} is not a valid sampler, choose one of: READS, WRITES", sampler));
            }
        }
    }
    // prepare the query params
    int duration_in_milli = vm["duration"].as<int>();
    sstring table_filters = vm["cf-filters"].as<sstring>();
    if (vm.contains("keyspace") && vm.contains("table") && vm.contains("duration-milli")) {
        table_filters = seastar::format("{}:{}",
                                        vm["keyspace"].as<sstring>(),
                                        vm["table"].as<sstring>());
        duration_in_milli = vm["duration-milli"].as<int>();
    } else if (vm.contains("keyspace")) {
        throw std::invalid_argument("toppartitions requires either a keyspace, column family name and duration or no arguments at all");
    }
    std::unordered_map<sstring, sstring> params{
        {"duration", fmt::to_string(duration_in_milli)},
        {"capacity", fmt::to_string(capacity)},
        {"list_size", fmt::to_string(list_size)}
    };
    if (vm.contains("ks-filters")) {
        params.emplace("keyspace_filters", vm["ks-filters"].as<sstring>());
    }
    if (!table_filters.empty()) {
        params.emplace("table_filters", table_filters);
    }
    auto res = client.get("/storage_service/toppartitions", std::move(params));
    const auto& toppartitions = res.GetObject();
    struct record {
        std::string_view partition;
        int64_t count;
        int64_t error;
    };
    // format the query result
    bool first = true;
    for (auto operation : {"read", "write"}) {
        auto cardinality = toppartitions[fmt::format("{}_cardinality", operation)].GetInt64();
        fmt::print("{}S Sampler:\n", boost::to_upper_copy(std::string(operation)));
        fmt::print("  Cardinality: ~{} ({} capacity)\n", cardinality, capacity);
        fmt::print("  Top {} partitions:\n", list_size);

        std::vector<record> topk;
        if (toppartitions.HasMember(operation)) {
            auto counters = toppartitions[operation].GetArray();
            topk.reserve(counters.Size());
            for (auto& element : counters) {
                const auto& counter = element.GetObject();
                topk.push_back(record(rjson::to_string_view(counter["partition"]),
                                      counter["count"].GetInt64(),
                                      counter["error"].GetInt64()));
            }
            std::ranges::sort(topk, [](auto& lhs, auto& rhs) {
                return lhs.count > rhs.count;
            });
        }
        if (topk.empty()) {
            fmt::print("\tNothing recorded during sampling period...\n");
        } else {
            auto max_width_record = std::ranges::max_element(
                topk,
                [] (auto& lhs, auto& rhs) {
                    return lhs.partition.size() < rhs.partition.size();
                });
            auto width = max_width_record->partition.size();
            fmt::print("\t{:<{}}{:>10}{:>10}\n", "Partition", width, "Count", "+/-");
            for (auto& record : topk) {
                fmt::print("\t{:<{}}{:>10}{:>10}\n", record.partition, width, record.count, record.error);
            }
            if (std::exchange(first, false)) {
                fmt::print("\n");
            }
        }
    }
}

void upgradesstables_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, false);

    std::vector<sstring> keyspaces;
    if (keyspace.empty()) {
        keyspaces = get_keyspaces(client);
    } else {
        keyspaces.push_back(keyspace);
    }

    std::unordered_map<sstring, sstring> params;

    if (!vm.count("include-all-sstables")) {
        params["exclude_current_version"] = "true";
    }

    if (!tables.empty()) {
        params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }

    for (const auto& keyspace : keyspaces) {
        client.get(format("/storage_service/keyspace_upgrade_sstables/{}", keyspace), params);
    }
}

void viewbuildstatus_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto invalid_argument_message = "viewbuildstatus requires keyspace and view name arguments";
    if (!vm.contains("keyspace_view")) {
        throw std::invalid_argument(invalid_argument_message);
    }
    const auto keyspace_view_args = vm["keyspace_view"].as<std::vector<sstring>>();
    sstring keyspace;
    sstring view;
    if (keyspace_view_args.size() == 1) {
        // Usually, keyspace name and table is are separated by a dot, but to
        // allow names which themselves contain a dot (this is allowed in
        // Alternator), also allow to separate the two parts with a slash
        // instead.
        auto keyspace_view = keyspace_view_args[0];
        auto sep = std::ranges::find_first_of(keyspace_view, "/.");
        if (sep == keyspace_view.end()) {
            throw std::invalid_argument(invalid_argument_message);
        }
        keyspace = sstring(keyspace_view.begin(), sep++);
        view = sstring(sep, keyspace_view.end());
    } else if (keyspace_view_args.size() == 2) {
        keyspace = keyspace_view_args[0];
        view = keyspace_view_args[1];
    } else {
        throw std::invalid_argument(invalid_argument_message);
    }

    auto res = client.get(seastar::format("/storage_service/view_build_statuses/{}/{}", keyspace, view));
    Tabulate table;
    bool succeed = true;
    table.add("Host", "Info");
    for (auto& element : res.GetArray()) {
        const auto& object = element.GetObject();
        auto endpoint = rjson::to_string_view(object["key"]);
        auto status = rjson::to_string_view(object["value"]);
        if (status != "SUCCESS") {
            succeed = false;
        }
        table.add(endpoint, status);
    }
    if (succeed) {
        fmt::print("{}.{} has finished building\n", keyspace, view);
    } else {
        fmt::print("{}.{} has not finished building; node status is below.\n", keyspace, view);
        fmt::print("\n");
        table.print();
        throw operation_failed_with_status(1);
    }
}

void version_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto version_json = client.get("/storage_service/release_version");
    fmt::print(std::cout, "ReleaseVersion: {}\n", rjson::to_string_view(version_json));
}

const std::vector<operation_option> global_options{
    typed_option<sstring>("host,h", "localhost", "the hostname or ip address of the ScyllaDB node"),
    typed_option<uint16_t>("port,p", 10000, "the port of the REST API of the ScyllaDB node"),
    typed_option<sstring>("password", "Remote jmx agent password (unused)"),
    typed_option<sstring>("password-file", "Path to the JMX password file (unused)"),
    typed_option<sstring>("username,u", "Remote jmx agent username (unused)"),
    typed_option<>("print-port", "Operate in 4.0 mode with hosts disambiguated by port number (unused)"),
};

const std::map<std::string_view, std::string_view> option_substitutions{
    {"-h", "--host"},
    {"-pw", "--password"},
    {"-pwf", "--password-file"},
    {"-pp", "--print-port"},
    {"-st", "--start-token"},
    {"-et", "--end-token"},
    {"-id", "--id"},
    {"-cf", "--table"},
    {"--column-family", "--table"},
    {"-kt", "--keyspace-table-list"},
    {"--kt-list", "--keyspace-table-list"},
    {"-kc", "--keyspace-table-list"},
    {"--kc.list", "--keyspace-table-list"},
    {"-las", "--load-and-stream"},
    {"-pro", "--primary-replica-only"},
    {"-ns", "--no-snapshot"},
    {"-m", "--mode"}, // FIXME: this clashes with seastar option for memory
    {"-tr", "--trace"},
    {"-iuk", "--ignore-unreplicated-keyspaces"},
    {"-seq", "--sequential"},
    {"-dcpar", "--dc-parallel"},
    {"-dc", "--in-dc"},
    {"-local", "--in-local-dc"},
    {"-pl", "--pull"},
    {"-pr", "--partitioner-range"},
    {"-hosts", "--in-hosts"},
};

std::map<operation, operation_func> get_operations_with_func() {

    const static std::map<operation, operation_func> operations_with_func {
        {
            {
                "cleanup",
                "Triggers removal of data that the node no longer owns",
R"(
You should run nodetool cleanup whenever you scale-out (expand) your cluster, and
new nodes are added to the same DC. The scale out process causes the token ring
to get re-distributed. As a result, some of the nodes will have replicas for
tokens that they are no longer responsible for (taking up disk space). This data
continues to consume diskspace until you run nodetool cleanup. The cleanup
operation deletes these replicas and frees up disk space.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/cleanup.html
)",
                {
                    typed_option<int64_t>("jobs,j", "The number of compaction jobs to be used for the cleanup (unused)"),
                },
                {
                    typed_option<std::vector<sstring>>("cleanup_arg", "[<keyspace> <tables>...]", -1),
                }
            },
            cleanup_operation
        },
        {
            {
                "clearsnapshot",
                "Remove snapshots",
R"(
By default all snapshots are removed for all keyspaces.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/clearsnapshot.html
)",
                {
                    typed_option<sstring>("tag,t", "The snapshot to remove"),
                },
                {
                    typed_option<std::vector<sstring>>("keyspaces", "[<keyspaces>...]", -1),
                }
            },
            clearsnapshot_operation
        },
        {
            {
                "compact",
                "Force a (major) compaction on one or more tables",
R"(
Forces a (major) compaction on one or more tables. Compaction is an optimization
that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the keyspaces and tables. Major
compactions will take all the SSTables for a column family and merge them into a
single SSTable per shard. If a keyspace is provided, the compaction will run on
all of the tables within that keyspace. If one or more tables are provided as
command-line arguments, the compaction will run on these tables.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/compact.html
)",
                {
                    typed_option<bool>("flush-memtables", "Control flushing of tables before major compaction (true by default)"),

                    typed_option<>("split-output,s", "Don't create a single big file (unused)"),
                    typed_option<>("user-defined", "Submit listed SStable files for user-defined compaction (unused)"),
                    typed_option<int64_t>("start-token", "Specify a token at which the compaction range starts (unused)"),
                    typed_option<int64_t>("end-token", "Specify a token at which the compaction range end (unused)"),
                },
                {
                    typed_option<std::vector<sstring>>("compaction_arg", "[<keyspace> <tables>...] or [<SStable files>...] ", -1),
                }
            },
            compact_operation
        },
        {
            {
                "compactionhistory",
                "Provides the history of compaction operations",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/compactionhistory.html
)",
                {
                    typed_option<sstring>("format,F", "text", "Output format, one of: (json, yaml or text); defaults to text"),
                },
            },
            compactionhistory_operation
        },
        {
            {
                "compactionstats",
                "Print statistics on compactions",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/compactionstats.html
)",
                {
                    typed_option<bool>("human-readable,H", false, "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"),
                },
            },
            compactionstats_operation
        },
        {
            {
                "decommission",
                "Deactivate a selected node by streaming its data to the next node in the ring",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/decommission.html
)",
            },
            decommission_operation
        },
        {
            {
                "describering",
                "Shows the partition ranges for the given keyspace or table",
R"(
For vnode (legacy) keyspaces, describering describes all tables in the keyspace.
For tablet keyspaces, describering needs the table to describe.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/describering.html
)",
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to describe the ring for", 1),
                    typed_option<std::vector<sstring>>("table", "The table to describe the ring for (for tablet keyspaces)", -1),
                },
            },
            describering_operation
        },
        {
            {
                "describecluster",
                "Print the name, snitch, partitioner and schema version of a cluster",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/describecluster.html
)",
            },
            describecluster_operation
        },
        {
            {
                "disableautocompaction",
                "Disables automatic compaction for the given keyspace and table(s)",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/disableautocompaction.html
)",
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to disable automatic compaction for", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to disable automatic compaction for", -1),
                }
            },
            disableautocompaction_operation
        },
        {
            {
                "disablebackup",
                "Disables incremental backup",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/disablebackup.html
)",
            },
            disablebackup_operation
        },
        {
            {
                "disablebinary",
                "Disable the CQL native protocol",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/disablebinary.html
)",
            },
            disablebinary_operation
        },
        {
            {
                "disablegossip",
                "Disable the gossip protocol",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/disablegossip.html
)",
            },
            disablegossip_operation
        },
        {
            {
                "drain",
                "Drain the node (stop accepting writes and flush all tables)",
R"(
Flushes all memtables from a node to the SSTables that are on the disk. Scylla
stops listening for connections from the client and other nodes. You need to
restart Scylla after running this command. This command is usually executed
before upgrading a node to a new version or before any maintenance action is
performed. When you want to simply flush memtables to disk, use the nodetool
flush command.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/drain.html
)",
            },
            drain_operation
        },
        {
            {
                "enableautocompaction",
                "Enables automatic compaction for the given keyspace and table(s)",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/enableautocompaction.html
)",
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to enable automatic compaction for", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to enable automatic compaction for", -1),
                }
            },
            enableautocompaction_operation
        },
        {
            {
                "enablebackup",
                "Enables incremental backup",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/enablebackup.html
)",
            },
            enablebackup_operation
        },
        {
            {
                "enablebinary",
                "Enables the CQL native protocol",
R"(
The native protocol is enabled by default.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/enablebinary.html
)",
            },
            enablebinary_operation
        },
        {
            {
                "enablegossip",
                "Enables the gossip protocol",
R"(
The gossip protocol is enabled by default.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/enablegossip.html
)",
            },
            enablegossip_operation
        },
        {
            {
                "flush",
                "Flush one or more tables",
R"(
Flush memtables to on-disk SSTables in the specified keyspace and table(s).
If no keyspace is specified, all keyspaces are flushed.
If no table(s) are specified, all tables in the specified keyspace are flushed.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/flush.html
)",
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to flush", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to flush", -1),
                }
            },
            flush_operation
        },
        {
            {
                "getendpoints",
                "Print the end points that owns the key",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/getendpoints.html
)",
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to query", 1),
                    typed_option<sstring>("table", "The table to query", 1),
                    typed_option<sstring>("key", "The partition key for which we need to find the endpoint", 1),
                },
            },
            getendpoints_operation
        },
        {
            {
                "getlogginglevels",
                "Get the runtime logging levels",
R"(
Prints a table with the name and current logging level for each logger in ScyllaDB.
)",
            },
            getlogginglevels_operation
        },
        {
            {
                "gettraceprobability",
                "Displays the current trace probability value",
R"(
This value is the probability for tracing a request. To change this value see settraceprobability.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/gettraceprobability.html
)",
            },
            gettraceprobability_operation
        },
        {
            {
                "gossipinfo",
                "Shows the gossip information for the cluster",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/gossipinfo.html
)",
            },
            gossipinfo_operation
        },
        {
            {
                "help",
                "Displays the list of all available nodetool commands",
                "",
                { },
                {
                    typed_option<sstring>("command", "The command to get more information about", 1),
                },
            },
            [] (scylla_rest_client&, const bpo::variables_map&) {}
        },
        {
            {
                "listsnapshots",
                "Lists all the snapshots along with the size on disk and true size",
R"(
Dropped tables (column family) will not be part of the listsnapshots.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/listsnapshots.html
)",
                { },
                { },
            },
            listsnapshots_operation
        },
        {
            {
                "move",
                "Move the token to this node",
R"(
This operation is not supported.
)",
                { },
                {
                    typed_option<sstring>("new-token", "The new token to move to this node", 1),
                },
            },
            move_operation
        },
        {
            {
                "rebuild",
                "Rebuilds a node’s data by streaming data from other nodes in the cluster (similarly to bootstrap)",
R"(
Rebuild operates on multiple nodes in a Scylla cluster. It streams data from a
single source replica when rebuilding a token range. When executing the command,
Scylla first figures out which ranges the local node (the one we want to rebuild)
is responsible for. Then which node in the cluster contains the same ranges.
Finally, Scylla streams the data to the local node.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/rebuild.html
)",
                { },
                {
                    typed_option<sstring>("source-dc", "DC from which to stream data (default: any DC)", 1),
                },
            },
            rebuild_operation
        },
        {
            {
                "refresh",
                "Load newly placed SSTables to the system without a restart",
R"(
Add the files to the upload directory, by default it is located under
/var/lib/scylla/data/keyspace_name/table_name-UUID/upload.
Materialized Views (MV) and Secondary Indexes (SI) of the upload table, and if
they exist, they are automatically updated. Uploading MV or SI SSTables is not
required and will fail.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html
)",
                {
                    typed_option<>("load-and-stream", "Allows loading sstables that do not belong to this node, in which case they are automatically streamed to the owning nodes"),
                    typed_option<>("primary-replica-only", "Load the sstables and stream to primary replica node that owns the data. Repair is needed after the load and stream process"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to load sstable(s) into", 1),
                    typed_option<sstring>("table", "The table to load sstable(s) into", 1),
                },
            },
            refresh_operation
        },
        {
            {
                "removenode",
                "Remove a node from the cluster when the status of the node is Down Normal (DN) and all attempts to restore the node have failed",
R"(
Provide the Host ID of the node to specify which node you want to remove.

Important: use this command *only* on nodes that are not reachable by other nodes
by any means!

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/removenode.html
)",
                {
                    typed_option<sstring>("ignore-dead-nodes", "Comma-separated list of dead nodes to ignore during removenode"),
                },
                {
                    typed_option<sstring>("remove-operation", "status|force|$HOST_ID - show status of current node removal, force completion of pending removal, or remove provided ID", 1),
                },
            },
            removenode_operation
        },
        {
            {
                "repair",
                "Synchronize data between nodes in the background",
R"(
When running nodetool repair on a single node, it acts as the repair master.
Only the data contained in the master node and its replications will be
repaired. Typically, this subset of data is replicated on many nodes in
the cluster, often all, and the repair process syncs between all the
replicas until the master data subset is in-sync.

To repair all of the data in the cluster, you need to run a repair on
all of the nodes in the cluster, or let ScyllaDB Manager do it for you.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/repair.html
)",
                {
                    typed_option<>("dc-parallel", "Repair datacenters in parallel"),
                    typed_option<uint64_t>("end-token", "Repair data up to this token"),
                    typed_option<std::vector<sstring>>("in-dc", "Constrain repair to specific datacenter(s)"),
                    typed_option<>("in-local-dc", "Constrain repair to the local datacenter only"),
                    typed_option<std::vector<sstring>>("in-hosts", "Constrain repair to the specific host(s)"),
                    typed_option<>("ignore-unreplicated-keyspaces", "Ignore keyspaces which are not replicated, without this repair will fail on such keyspaces"),
                    typed_option<unsigned>("jobs", "Number of threads to run repair on. "),
                    typed_option<>("partitioner-range", "Repair only the first range returned by the partitioner"),
                    typed_option<>("pull", "Fix local node only"),
                    typed_option<>("sequential", "Perform repair sequentially"),
                    typed_option<uint64_t>("start-token", "Repair data starting from this token"),
                    typed_option<>("trace", "Trace repair, traces are stored in system.traces"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to repair, if missing all keyspaces are repaired", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to repair, if missing all tables are repaired", -1),
                },
            },
            repair_operation
        },
        {
            {
                "ring",
                "Print information about the token ring",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/ring.html
)",
                {
                    typed_option<bool>("resolve-ip,r", false, "Show node domain names instead of IPs")
                },
                {
                    typed_option<sstring>("keyspace", "Specify a keyspace for accurate ownership information (topology awareness)", 1),
                },
            },
            ring_operation
        },
        {
            {
                "scrub",
                "Scrub the SSTable files in the specified keyspace or table(s)",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/scrub.html
)",
                {
                    typed_option<>("no-snapshot", "Do not take a snapshot of scrubbed tables before starting scrub (default false)"),
                    typed_option<>("skip-corrupted,s", "Skip corrupted rows or partitions, even when scrubbing counter tables (deprecated, use ‘–-mode’ instead, default false)"),
                    typed_option<sstring>("mode,m", "How to handle corrupt data (one of: ABORT|SKIP|SEGREGATE|VALIDATE, default ABORT; overrides ‘–-skip-corrupted’)"),
                    typed_option<sstring>("quarantine-mode,q", "How to handle quarantined sstables (one of: INCLUDE|EXCLUDE|ONLY, default INCLUDE)"),
                    typed_option<>("no-validate,n", "Do not validate columns using column validator (unused)"),
                    typed_option<>("reinsert-overflowed-ttl,r", "Rewrites rows with overflowed expiration date (unused)"),
                    typed_option<int64_t>("jobs,j", "The number of sstables to be scrubbed concurrently (unused)"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to scrub", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to scrub (if unspecified, all tables in the keyspace are scrubbed)", -1),
                },
            },
            scrub_operation
        },
        {
            {
                "setlogginglevel",
                "Sets the level log threshold for a given logger during runtime",
R"(
Resetting the log level of one or all loggers is not supported yet.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/setlogginglevel.html
)",
                { },
                {
                    typed_option<sstring>("logger", "The logger to set the log level for, if unspecified, all loggers are reset to the default level", 1),
                    typed_option<sstring>("level", "The log level to set, one of (trace, debug, info, warn and error), if unspecified, default level is reset to default log level", 1),
                },
            },
            setlogginglevel_operation
        },
        {
            {
                "settraceprobability",
                "Sets the probability for tracing a request",
R"(
Value is trace probability between 0 and 1. 0 the trace will never happen and 1
the trace will always happen. Anything in between is a percentage of the time,
converted into a decimal. For example, 60% would be 0.6.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/settraceprobability.html
)",
                { },
                {
                    typed_option<double>("trace_probability", "trace probability value, must between 0 and 1, e.g. 0.2", 1),
                },
            },
            settraceprobability_operation
        },
        {
            {
                "snapshot",
                "Take a snapshot of specified keyspaces or a snapshot of the specified table",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/snapshot.html
)",
                {
                    typed_option<sstring>("table", "The table(s) to snapshot, multiple ones can be joined with ','"),
                    typed_option<sstring>("keyspace-table-list", "The keyspace.table pair(s) to snapshot, multiple ones can be joined with ','"),
                    typed_option<sstring>("tag,t", "The name of the snapshot"),
                    typed_option<>("skip-flush", "Do not flush memtables before snapshotting (snapshot will not contain unflushed data)"),
                },
                {
                    typed_option<std::vector<sstring>>("keyspaces", "The keyspaces to snapshot", -1),
                },
            },
            snapshot_operation
        },
        {
            {
                "statusbackup",
                "Displays the incremental backup status",
R"(
Results can be one of the following: `running` or `not running`.

By default, the incremental backup status is `not running`.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/statusbackup.html
)",
            },
            statusbackup_operation
        },
        {
            {
                "statusbinary",
                "Displays the incremental backup status",
R"(
Provides the status of native transport - CQL (binary protocol).
In case that you don’t want to use CQL you can disable it using the disablebinary
command.
Results can be one of the following: `running` or `not running`.

By default, the native transport is `running`.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/statusbinary.html
)",
            },
            statusbinary_operation
        },
        {
            {
                "statusgossip",
                "Displays the gossip status",
R"(
Provides the status of gossip.
Results can be one of the following: `running` or `not running`.

By default, the gossip protocol is `running`.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/statusgossip.html
)",
            },
            statusgossip_operation
        },
        {
            {
                "stop",
                "Stops a compaction operation",
R"(
This command is usually used to stop compaction that has a negative impact on the performance of a node.

Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/stop.html
)",
                {
                    typed_option<int>("id", "The id of the compaction operation to stop (not implemented)"),
                },
                {
                    typed_option<sstring>("compaction_type", "The type of compaction to be stopped", 1),
                },
            },
            stop_operation
        },
        {
            {
                "toppartitions",
                "Sample and print the most active partitions for a given column family",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/toppartitions.html
)",
                {
                    typed_option<int>("duration,d", 5000, "Duration in milliseconds"),
                    typed_option<sstring>("ks-filters", "Comma separated list of keyspaces to include in the query (Default: all)"),
                    typed_option<sstring>("cf-filters", "", "Comma separated list of column families to include in the query in the form of 'ks:cf' (Default: all)"),
                    typed_option<int>("capacity,s", 256, "Capacity of stream summary, closer to the actual cardinality of partitions will yield more accurate results (Default: 256)"),
                    typed_option<int>("size,k", 10, "Number of the top partitions to list (Default: 10)"),
                    typed_option<sstring>("samplers,a", "READS,WRITES", "Comma separated list of samplers to use (Default: all)"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to filter", 1),
                    typed_option<sstring>("table", "The column family to filter", 1),
                    typed_option<int>("duration-milli", "Duration in milliseconds", 1),
                },
            },
            toppartitions_operation
        },
        {
            {
                "upgradesstables",
                "Upgrades sstables to the latest available sstable version and applies the current options",
R"(
Run this command if you want to force-apply changes that are relevant to sstables, like changing the compression settings, or enabling EAR (enterprise only).
Can also be used to upgrade all sstables to the latest sstable version.

Note that this command is not needed for changes described above to take effect. They take effect gradually as new sstables are written and old ones are compacted.
This command should be used when it is desired that such changes take effect right away.

For more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/upgradesstables.html
)",
                {
                    typed_option<>("include-all-sstables,a", "Include all sstables, even those already on the current version"),
                    typed_option<unsigned>("jobs,j", "Number of sstables to upgrade simultanously (unused)"),
                },
                {
                    typed_option<sstring>("keyspace", "Keyspace to upgrade sstables for, if missing, all sstables are upgraded", 1),
                    typed_option<std::vector<sstring>>("table", "Table to upgrade sstables for, if missing, all tables in the keyspace are upgraded", -1),
                },
            },
            upgradesstables_operation
        },
        {
            {
                "viewbuildstatus",
                "Show progress of a materialized view build",
R"(
Fore more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/viewbuildstatus.html
)",
                {},
                {
                    typed_option<std::vector<sstring>>("keyspace_view", "<keyspace> <view> | <keyspace.view>, The keyspace and view name ", -1),
                },
            },
            viewbuildstatus_operation
        },
        {
            {
                "version",
                "Displays the Apache Cassandra version which your version of Scylla is most compatible with",
R"(
Displays the Apache Cassandra version which your version of Scylla is most
compatible with, not your current Scylla version. To display the Scylla version,
run `scylla --version`.

For more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/version.html
)",
            },
            version_operation
        },
    };

    return operations_with_func;
}

// boost::program_options doesn't allow multi-char option short-form,
// e.g. -pw, that C*'s nodetool uses. We silently map these to the
// respective long-form and pass the transformed argv to tool_app_template.
// Furthermore, C* nodetool allows for assigning values to short-form
// arguments with =, e.g. -h=localhost, something which boost::program_options
// also doesn't support. We silently replace all = with space to support this.
// So, e.g. "-h=localhost" becomes "-h localhost".
std::vector<char*> massage_argv(int argc, char** argv) {
    static std::vector<std::string> argv_holder;
    argv_holder.reserve(argc);

    for (int i = 0; i < argc; ++i) {
        if (argv[i][0] != '-') {
            argv_holder.push_back(argv[i]);
            continue;
        }

        std::string arg = argv[i];
        std::string arg_key;
        std::optional<std::string> arg_value;

        if (auto pos = arg.find('='); pos == std::string::npos) {
            arg_key = std::move(arg);
        } else {
            arg_key = arg.substr(0, pos);
            arg_value = arg.substr(pos + 1);
        }

        const auto it = option_substitutions.find(arg_key);
        if (it != option_substitutions.end()) {
            nlog.trace("Substituting cmd-line arg {} with {}", arg_key, it->second);
            arg_key = it->second;
        }

        argv_holder.push_back(std::move(arg_key));
        if (arg_value) {
            argv_holder.push_back(std::move(*arg_value));
        }
    }

    std::vector<char*> new_argv;
    new_argv.reserve(argv_holder.size());
    std::ranges::transform(argv_holder, std::back_inserter(new_argv), [] (std::string& arg) -> char* { return arg.data(); });
    return new_argv;
}

} // anonymous namespace

namespace tools {

int scylla_nodetool_main(int argc, char** argv) {
    auto replacement_argv = massage_argv(argc, argv);
    nlog.debug("replacement argv: {}", replacement_argv);

    constexpr auto description_template =
R"(scylla-{} - a command-line tool to administer local or remote ScyllaDB nodes

# Operations

The operation to execute is the mandatory, first positional argument.
Operations write their output to stdout. Logs are written to stderr,
with a logger called {}.

Supported Nodetool operations:
{}

For more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.html
)";

    const auto operations = boost::copy_range<std::vector<operation>>(get_operations_with_func() | boost::adaptors::map_keys);
    tool_app_template::config app_cfg{
            .name = app_name,
            .description = format(description_template, app_name, nlog.name(), boost::algorithm::join(operations | boost::adaptors::transformed([] (const auto& op) {
                return format("* {}: {}", op.name(), op.summary());
            }), "\n")),
            .logger_name = nlog.name(),
            .lsa_segment_pool_backend_size_mb = 1,
            .operations = std::move(operations),
            .global_options = &global_options};
    tool_app_template app(std::move(app_cfg));

    return app.run_async(replacement_argv.size(), replacement_argv.data(), [&app] (const operation& operation, const bpo::variables_map& app_config) {
        try {
            // Help operation is special (and weird), add special path for it
            // instead of making all other commands:
            // * make client param optional
            // * take an additional param
            if (operation.name() == "help") {
                help_operation(app.get_config(), app_config);
            } else {
                scylla_rest_client client(app_config["host"].as<sstring>(), app_config["port"].as<uint16_t>());
                get_operations_with_func().at(operation)(client, app_config);
            }
        } catch (std::invalid_argument& e) {
            fmt::print(std::cerr, "error processing arguments: {}\n", e.what());
            return 1;
        } catch (operation_failed_on_scylladb& e) {
            fmt::print(std::cerr, "{}\n", e.what());
            return 3;
        } catch (api_request_failed& e) {
            fmt::print(std::cerr, "{}\n", e.what());
            return 4;
        } catch (operation_failed_with_status& e) {
            return e.exit_status;
        } catch (...) {
            fmt::print(std::cerr, "error running operation: {}\n", std::current_exception());
            return 2;
        }

        return 0;
    });
}

} // namespace tools
