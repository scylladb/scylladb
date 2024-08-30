/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <cctype>
#include <chrono>
#include <concepts>
#include <future>
#include <limits>
#include <iterator>
#include <numeric>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <fmt/chrono.h>
#include <fmt/ranges.h>
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
#include <ranges>
#include <unordered_map>

#include "api/scrub_status.hh"
#include "gms/application_state.hh"
#include "db_clock.hh"
#include "log.hh"
#include "release.hh"
#include "tools/format_printers.hh"
#include "tools/utils.hh"
#include "utils/assert.hh"
#include "utils/estimated_histogram.hh"
#include "utils/http.hh"
#include "utils/pretty_printers.hh"
#include "utils/rjson.hh"
#include "utils/UUID.hh"

namespace bpo = boost::program_options;

using namespace std::chrono_literals;
using namespace tools::utils;

namespace std {
// required by boost::lexical_cast<std::string>(vector<string>), which is in turn used
// by boost::program_option for printing out the default value of an option
static std::ostream& operator<<(std::ostream& os, const std::vector<sstring>& v) {
    return os << fmt::format("{{{}}}", fmt::join(v, ", "));
}
}

// mimic the behavior of FileUtils::stringifyFileSize
struct file_size_printer {
    uint64_t value;
    bool human_readable;
    bool use_correct_units;
    // Cassandra nodetool uses base_2 and base_10 units interchangeably, some
    // commands use this, some that. Let's accomodate this for now, and maybe
    // fix this mess at one point in the future, after the rewrite is done.
    file_size_printer(uint64_t value, bool human_readable = true, bool use_correct_units = false)
        : value{value}
        , human_readable{human_readable}
        , use_correct_units{use_correct_units}
    {}
};

template <>
struct fmt::formatter<file_size_printer> : fmt::formatter<string_view> {
    auto format(file_size_printer size, auto& ctx) const {
        if (!size.human_readable) {
            return fmt::format_to(ctx.out(), "{}", size.value);
        }

        using unit_t = std::tuple<uint64_t, std::string_view, std::string_view>;
        const unit_t units[] = {
            {1UL << 40, "TiB", "TB"},
            {1UL << 30, "GiB", "GB"},
            {1UL << 20, "MiB", "MB"},
            {1UL << 10, "KiB", "KB"},
        };
        for (auto [n, base_2, base_10] : units) {
            if (size.value > n) {
                auto d = static_cast<float>(size.value) / n;
                auto postfix = size.use_correct_units ? base_2 : base_10;
                return fmt::format_to(ctx.out(), "{:.2f} {}", d, postfix);
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
    http::reply::status_type status;

    using std::runtime_error::runtime_error;
    api_request_failed(http::reply::status_type s, sstring message)
        : std::runtime_error(std::move(message))
        , status(s)
    {}
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
            throw api_request_failed(status, fmt::format("error executing {} request to {} with parameters {}: remote replied with status code {}:\n{}",
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

// Based on origin's org.apache.cassandra.tools.NodeProbe.BufferSamples
class buffer_samples {
    std::vector<uint64_t> _samples;

public:
    explicit buffer_samples(std::vector<uint64_t> samples) : _samples(std::move(samples)) {
        std::sort(_samples.begin(), _samples.end());
    }

    const std::vector<uint64_t>& values() const { return _samples; }

    double min() const { return _samples.empty() ? 0.0 : double(_samples.front()); }

    double max() const { return _samples.empty() ? 0.0 : double(_samples.back()); }

    double value(double quantile) const {
        if (quantile < 0.0 || quantile > 1.0) {
            throw std::runtime_error(fmt::format("quantile {} is not in [0..1]", quantile));
        }

        if (_samples.empty()) {
            return 0.0;
        }

        const double pos = quantile * (_samples.size() + 1);

        if (pos < 1) {
            return _samples.front();
        }

        if (pos >= _samples.size()) {
            return _samples.back();
        }

        const double lower = _samples.at(size_t(pos) - 1);
        const double upper = _samples.at(size_t(pos));
        return lower + (pos - floor(pos)) * (upper - lower);
    }

    static buffer_samples retrieve_from_api(scylla_rest_client& client, sstring path) {
        const auto res = client.get(std::move(path));
        const auto histogram_object = res["hist"].GetObject();
        std::vector<uint64_t> samples;
        if (histogram_object.HasMember("sample")) {
            for (const auto& sample : histogram_object["sample"].GetArray()) {
                samples.push_back(sample.GetInt());
            }
        }
        return buffer_samples(std::move(samples));
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

std::map<sstring, std::vector<sstring>> get_ks_to_cfs(scylla_rest_client& client) {
    auto res = client.get("/column_family/");
    std::map<sstring, std::vector<sstring>> keyspaces;
    for (auto& element : res.GetArray()) {
        const auto& cf_info = element.GetObject();
        auto ks = rjson::to_string_view(cf_info["ks"]);
        auto cf = rjson::to_string_view(cf_info["cf"]);
        keyspaces[sstring(ks)].push_back(sstring(cf));
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

    if (vm.contains("table")) {
        ret.tables = vm["table"].as<std::vector<sstring>>();
    }

    return ret;
}

struct keyspace_and_table {
    sstring keyspace;
    sstring table;
};

// Parses keyspace and table for commands which take a single keyspace and table.
// The accepted formats are:
// command keyspace table
// command keyspace.table
// command keyspace/table
//
// keyspace is allowed to contain a ".", if the keyspace and table is separated by a /
keyspace_and_table parse_keyspace_and_table(scylla_rest_client& client, const bpo::variables_map& vm, std::string_view option_name) {
    auto args = vm[std::string(option_name)].as<std::vector<sstring>>();

    const auto error_msg = "single argument must be keyspace and table separated by \".\" or \"/\"";

    auto split_by = [error_msg] (std::string_view arg, std::string_view by) {
        const auto pos = arg.find(by);
        if (pos == sstring::npos || pos == 0 || pos == arg.size() - 1) {
            throw std::invalid_argument(error_msg);
        }
        return std::pair(sstring(arg.substr(0, pos)), sstring(arg.substr(pos + 1)));
    };

    sstring keyspace, table;
    switch (args.size()) {
        case 1:
            if (args[0].find("/") == sstring::npos) {
                std::tie(keyspace, table) = split_by(args[0], ".");
            } else {
                std::tie(keyspace, table) = split_by(args[0], "/");
            }
            break;
        case 2:
            keyspace = args[0];
            table = args[1];
            break;
        default:
            throw std::invalid_argument(error_msg);
    }

    return {keyspace, table};
}

using operation_func = void(*)(scylla_rest_client&, const bpo::variables_map&);
struct operation_action{
    std::optional<operation_func> func = std::nullopt;
    std::map<sstring, operation_action> suboperation_funcs = {};

    operation_action(operation_func f)
        : func(std::move(f))
    {}

    operation_action(std::map<sstring, operation_action> subfs)
        : suboperation_funcs(std::move(subfs))
    {}
};

const std::map<operation, operation_action>& get_operations_with_func();

void backup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::unordered_map<sstring, sstring> params;
    for (auto required_param : {"endpoint", "bucket", "keyspace"}) {
        if (!vm.contains(required_param)) {
            throw std::invalid_argument(fmt::format("missing required parameter: {}", required_param));
        }
        params[required_param] = vm[required_param].as<sstring>();
    }
    if (vm.contains("snapshot")) {
        params["snapshot"] = vm["snapshot"].as<sstring>();
    }
    const auto backup_res = client.post("/storage_service/backup", std::move(params));
    if (vm.contains("nowait")) {
        return;
    }

    const auto task_id = rjson::to_string_view(backup_res);
    const auto url = seastar::format("/task_manager/wait_task/{}", task_id);
    const auto wait_res = client.get(url);
    const auto& status = wait_res.GetObject();
    auto state = rjson::to_string_view(status["state"]);
    fmt::print("{}", state);
    if (state != "done") {
        fmt::print(": {}", rjson::to_string_view(status["error"]));
    }
    fmt::print(R"(
start: {}
end: {}
)",
               rjson::to_string_view(status["start_time"]),
               rjson::to_string_view(status["end_time"]));
}

void checkandrepaircdcstreams_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/cdc_streams_check_and_repair");
}

void cleanup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.contains("cleanup_arg")) {
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

    if (vm.contains("keyspaces")) {
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

    if (vm.contains("tag")) {
        params["tag"] = vm["tag"].as<sstring>();
    }

    client.del("/storage_service/snapshots", std::move(params));
}

void compact_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.contains("user-defined")) {
        throw std::invalid_argument("--user-defined flag is unsupported");
    }

    std::unordered_map<sstring, sstring> params;
    if (vm.contains("flush-memtables")) {
        params["flush_memtables"] = vm["flush-memtables"].as<bool>() ? "true" : "false";
    }

    if (vm.contains("compaction_arg")) {
        const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, "compaction_arg");
        if (!tables.empty()) {
            params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
        }
        client.post(format("/storage_service/keyspace_compaction/{}", keyspace), std::move(params));
    } else {
        client.post("/storage_service/compact", std::move(params));
    }
}

std::string format_compacted_at(int64_t compacted_at) {
    const auto compacted_at_time = std::time_t(compacted_at / 1000);
    const auto milliseconds = compacted_at % 1000;
    return fmt::format("{:%FT%T}.{}", fmt::localtime(compacted_at_time), milliseconds);
}

template<typename Writer, typename Entry>
void print_compactionhistory(const std::vector<Entry>& history) {
    Writer writer(std::cout);
    auto root = writer.map();
    auto seq = root.add_seq("CompactionHistory");
    for (const auto& e : history) {
        auto output = seq->add_map();
        output->add_item("id", fmt::to_string(e.id));
        output->add_item("columnfamily_name", e.table);
        output->add_item("keyspace_name", e.keyspace);
        output->add_item("compacted_at", format_compacted_at(e.compacted_at));
        output->add_item("bytes_in", e.bytes_in);
        output->add_item("bytes_out", e.bytes_out);
        output->add_item("rows_merged", "");
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
        print_compactionhistory<json_writer>(history);
    } else if (format == "yaml") {
        print_compactionhistory<yaml_writer>(history);
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

void resetlocalschema_operation(scylla_rest_client& client, const bpo::variables_map&) {
    client.post("/storage_service/relocal_schema");
}

void describering_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("keyspace")) {
        throw std::invalid_argument("keyspace must be specified");
    }

    const auto keyspace = vm["keyspace"].as<sstring>();
    const auto schema_version_res = client.get("/storage_service/schema_version");

    std::unordered_map<sstring, sstring> params;
    if (vm.contains("table")) {
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
    if (!vm.contains("keyspace")) {
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
    if (!vm.contains("keyspace")) {
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
    if (!vm.contains("keyspace") || !vm.count("table") || !vm.count("key")) {
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
    constexpr auto row_format = "{:<50}{:>10}\n";
    fmt::print(std::cout, "\n");
    fmt::print(std::cout, row_format, "Logger Name", "Log Level");
    for (const auto& element : res.GetArray()) {
        const auto& logger_obj = element.GetObject();
        fmt::print(
                std::cout,
                row_format,
                rjson::to_string_view(logger_obj["key"]),
                rjson::to_string_view(logger_obj["value"]));
    }
}

void getsstables_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("keyspace") || !vm.count("table") || !vm.count("key")) {
        throw std::invalid_argument("getsstables requires keyspace, table and partition key arguments");
    }

    const auto keyspace = vm["keyspace"].as<sstring>();
    const auto table = vm["table"].as<sstring>();
    const auto ks_to_cfs = get_ks_to_cfs(client);
    if (!ks_to_cfs.contains(keyspace)) {
        throw std::invalid_argument(format("unknown keyspace: {}", keyspace));
    }
    const auto tables = ks_to_cfs.at(keyspace);
    if (auto it = std::find(tables.begin(), tables.end(), table); it == tables.end()) {
        throw std::invalid_argument(format("unknown table: {}", table));
    }

    auto params = std::unordered_map<sstring, sstring>{{"key", vm["key"].as<sstring>()}};
    if (vm.contains("hex-format")) {
        params["format"] = "hex";
    }

    auto res = client.get(seastar::format("/column_family/sstables/by_key/{}:{}", keyspace, table), std::move(params));
    for (auto& sst : res.GetArray()) {
        fmt::print("{}\n", rjson::to_string_view(sst));
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

static uint64_t get_off_heap_memory_used(scylla_rest_client& client) {
    uint64_t used = 0;
    for (auto& [ks_name, cf_names] : get_ks_to_cfs(client)) {
        for (auto& cf_name : cf_names) {
            for (auto name : {"memtable_off_heap_size",
                              "bloom_filter_off_heap_memory_used",
                              "index_summary_off_heap_memory_used",
                              "compression_metadata_off_heap_memory_used"}) {
                used += client.get(fmt::format("/column_family/metrics/{}/{}:{}",
                                               name, ks_name, cf_name)).GetUint64();
            }
        }
    }
    return used;
}

static double get_cache_count(scylla_rest_client& client,
                              std::string_view cache_name,
                              std::string_view metrics_name) {
    auto res = client.get(fmt::format("/cache_service/metrics/{}/{}_moving_avrage",
                                      cache_name, metrics_name));
    auto moving_average = res.GetObject();
    return moving_average["count"].GetInt64();
}

void info_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto gossip_running = client.get("/storage_service/gossiping").GetBool();
    fmt::print("{:<23}: {}\n", "ID", rjson::to_string_view(client.get("/storage_service/hostid/local")));
    fmt::print("{:<23}: {}\n", "Gossip active", gossip_running);
    fmt::print("{:<23}: {}\n", "Thrift active", client.get("/storage_service/rpc_server").GetBool());
    fmt::print("{:<23}: {}\n", "Native Transport active", client.get("/storage_service/native_transport").GetBool());
    fmt::print("{:<23}: {}\n", "Load", file_size_printer(client.get("/storage_service/load").GetDouble()));
    if (gossip_running) {
        fmt::print("{:<23}: {}\n", "Generation No", client.get("/storage_service/generation_number").GetInt());
    } else {
        fmt::print("{:<23}: {}\n", "Generation No", 0);
    }
    auto uptime_ms = std::chrono::milliseconds(client.get("/system/uptime_ms").GetInt64());
    fmt::print("{:<23}: {}\n", "Uptime (seconds)",
               std::chrono::duration_cast<std::chrono::seconds>(uptime_ms).count());
    // the JVM heap memory usage is meaningless for Scylla
    const double mem_used = 0;
    const double mem_max = 0;
    fmt::print("{:<23}: {:.2f} / {:.2f}\n", "Heap Memory (MB)", mem_used, mem_max);
    fmt::print("{:<23}: {:.2f}\n", "Off Heap Memory (MB)",
               static_cast<float>(get_off_heap_memory_used(client)) / 1_MiB);
    fmt::print("{:<23}: {}\n", "Data Center", rjson::to_string_view(client.get("/snitch/datacenter")));
    fmt::print("{:<23}: {}\n", "Rack", rjson::to_string_view(client.get("/snitch/rack")));
    // scylla always returns 0 though.
    fmt::print("{:<23}: {}\n", "Exceptions", client.get("/storage_service/metrics/exceptions").GetInt64());
    using namespace std::literals;
    for (auto name : {"key"sv, "row"sv, "counter"sv}) {
        std::string capitalized_name = fmt::format("{}{} Cache", static_cast<char>(toupper(name[0])), name.substr(1));
        fmt::print("{:<23}: entries {}, size {}, capacity {}, {} hits, {} requests, {:.3f} recent hit rate, {} save period in seconds\n",
                   capitalized_name,
                   client.get(seastar::format("/cache_service/metrics/{}/entries", name)).GetInt64(),
                   file_size_printer(client.get(seastar::format("/cache_service/metrics/{}/size", name)).GetInt64()),
                   file_size_printer(client.get(seastar::format("/cache_service/metrics/{}/capacity", name)).GetInt64()),
                   get_cache_count(client, name, "hits"),
                   get_cache_count(client, name, "requests"),
                   client.get(seastar::format("/cache_service/metrics/{}/hit_rate", name)).GetDouble(),
                   client.get(seastar::format("/cache_service/{}_cache_save_period", name)).GetInt64());
    }
    // this is a dummy value, to be compatible with cassandra nodetool.
    fmt::print("{:<23}: {:.1f}%\n", "Percent Repaired", 0.0);
    const bool display_all_tokens = vm.contains("tokens");
    if (client.get("/storage_service/join_ring").GetBool()) {
        auto res = client.get("/storage_service/tokens");
        auto tokens = res.GetArray();
        const auto nr_tokens = tokens.Size();
        if (nr_tokens == 1 || display_all_tokens) {
            for (auto& token : tokens) {
                fmt::print("{:<23}: {}\n", "Token", rjson::to_string_view(token));
            }
        } else {
            fmt::print("{:<23}: (invoke with -T/--tokens to see all {} tokens)\n",
                       "Token", nr_tokens);
        }
    } else {
        fmt::print("{:<23}: (node is not joined to the cluster)\n", "Token");
    }
}

void listsnapshots_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto snapshots = client.get("/storage_service/snapshots");

    if (snapshots.GetArray().Empty()) {
        fmt::print("There are no snapshots\n");
        return;
    }

    const auto true_size = client.get("/storage_service/snapshots/size/true").GetInt64();

    std::array<std::string, 5> header_row{"Snapshot name", "Keyspace name", "Column family name", "True size", "Size on disk"};
    std::array<size_t, 5> max_column_length{};
    for (size_t c = 0; c < header_row.size(); ++c) {
        max_column_length[c] = header_row[c].size();
    }

    auto format_hr_size = [] (uint64_t val, bool use_alternative_units) {
        const char* const units[] = {"bytes", "KB", "MB", "GB", "TB", "PB", "EB"};
        const char* const alternative_units[] = {"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};

        unsigned i = 0;
        const uint64_t step = 1024;
        uint64_t nominator = 1;

        for (; i < std::size(units); ++i) {
            if (val / nominator < step) {
                break;
            }
            nominator *= step;
        }

        auto formatted_number = fmt::format("{:.2f}", double(val) / double(nominator));
        // Legacy nodetool omits decimal parts if they are "00"
        if (formatted_number.ends_with(".00")) {
            formatted_number.erase(formatted_number.size() - 3);
        }
        return fmt::format("{} {}", formatted_number, use_alternative_units ? alternative_units[i] : units[i]);
    };

    std::vector<std::array<std::string, 5>> rows;
    for (const auto& snapshot_by_name : snapshots.GetArray()) {
        const auto snapshot_name = std::string(rjson::to_string_view(snapshot_by_name.GetObject()["key"]));
        for (const auto& snapshot : snapshot_by_name.GetObject()["value"].GetArray()) {
            rows.push_back({
                    snapshot_name,
                    std::string(rjson::to_string_view(snapshot["ks"])),
                    std::string(rjson::to_string_view(snapshot["cf"])),
                    format_hr_size(snapshot["live"].GetInt64(), false),
                    format_hr_size(snapshot["total"].GetInt64(), false)});

            for (size_t c = 0; c < rows.back().size(); ++c) {
                max_column_length[c] = std::max(max_column_length[c], rows.back()[c].size());
            }
        }
    }

    const auto header_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}}\n", max_column_length[0],
            max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4]);
    const auto regular_row_format = fmt::format("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}}\n", max_column_length[0],
            max_column_length[1], max_column_length[2], max_column_length[3], max_column_length[4]);

    fmt::print(std::cout, "Snapshot Details: \n");
    fmt::print(std::cout, fmt::runtime(header_row_format.c_str()), header_row[0], header_row[1], header_row[2], header_row[3], header_row[4]);
    for (const auto& r : rows) {
        fmt::print(std::cout, fmt::runtime(regular_row_format.c_str()), r[0], r[1], r[2], r[3], r[4]);
    }

    fmt::print(std::cout, "\nTotal TrueDiskSpaceUsed: {}\n\n", format_hr_size(true_size, true));
}

void move_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    throw std::invalid_argument("This operation is not supported");
}

void print_stream_session(
        const rjson::value& summaries,
        const rjson::value& files,
        std::string_view action_continuous,
        std::string_view action_perfect,
        std::string_view target,
        bool human_readable) {
    if (summaries.GetArray().Empty()) {
        return;
    }

    uint64_t total_count{}, total_size{}, done_count{}, done_size{};
    for (const auto& tbl : summaries.GetArray()) {
        total_count += tbl["files"].GetInt();
        total_size += tbl["total_size"].GetInt();
    }
    for (const auto& file_entry : files.GetArray()) {
        const auto& file = file_entry["value"];
        if (file["current_bytes"].GetInt() == file["total_bytes"].GetInt()) {
            ++done_count;
        }
        done_size += file["current_bytes"].GetInt();
    }

    auto format_bytes = [] (uint64_t value, bool human_readable) {
        if (!human_readable) {
            return format("{} bytes", value);
        }
        return format("{}", file_size_printer(value, true, true));
    };

    fmt::print(std::cout, "        {} {} files, {} total. Already {} {} files, {} total\n",
            action_continuous,
            total_count,
            format_bytes(total_size, human_readable),
            action_perfect,
            done_count,
            format_bytes(done_size, human_readable));

    for (const auto& file_entry : files.GetArray()) {
        const auto& file = file_entry["value"];
        fmt::print(std::cout, "            {} {}/{} bytes({}%) {} {} idx:{}/{}\n",
                rjson::to_string_view(file["file_name"]),
                file["current_bytes"].GetInt(),
                file["total_bytes"].GetInt(),
                uint64_t(file["current_bytes"].GetDouble() / file["total_bytes"].GetDouble() * 100.0),
                action_perfect,
                target,
                file["session_index"].GetInt(),
                rjson::to_string_view(file["peer"]));
    }
}

void netstats_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto mode = sstring(rjson::to_string_view(client.get("/storage_service/operation_mode")));
    const bool human_readable = vm.contains("human-readable");

    fmt::print(std::cout, "Mode: {}\n", mode);

    const auto streams_res = client.get("/stream_manager/");
    if (streams_res.GetArray().Empty()) {
        fmt::print(std::cout, "Not sending any streams.\n");
    } else {
        for (const auto& stream : streams_res.GetArray()) {
            fmt::print(std::cout, "{} {}\n", rjson::to_string_view(stream["description"]), rjson::to_string_view(stream["plan_id"]));

            for (const auto& session : stream["sessions"].GetArray()) {
                const auto ip = rjson::to_string_view(session["peer"]);
                const auto private_ip = rjson::to_string_view(session["connecting"]);
                fmt::print(std::cout, "    /{}", ip);
                if (ip != private_ip) {
                    fmt::print(std::cout, " (using /{})", private_ip);
                }
                fmt::print(std::cout, "\n");
                print_stream_session(session["receiving_summaries"], session["receiving_files"], "Receiving", "received", "from", human_readable);
                print_stream_session(session["sending_summaries"], session["sending_files"], "Sending", "sent", "to", human_readable);
            }
        }
    }

    if (client.get("/storage_service/is_starting").GetBool()) {
        return;
    }

    fmt::print(std::cout, "Read Repair Statistics:\n");
    fmt::print(std::cout, "Attempted: {}\n", client.get("/storage_proxy/read_repair_attempted").GetInt());
    fmt::print(std::cout, "Mismatch (Blocking): {}\n", client.get("/storage_proxy/read_repair_repaired_blocking").GetInt());
    fmt::print(std::cout, "Mismatch (Background): {}\n", client.get("/storage_proxy/read_repair_repaired_background").GetInt());

    constexpr auto line_fmt = "{:<25}{:>10}{:>10}{:>15}{:>10}\n";

    auto sum_nodes = [] (auto&& res) {
        uint64_t sum = 0;
        for (const auto& node : res.GetArray()) {
            sum += node["value"].GetInt();
        }
        return sum;
    };

    fmt::print(std::cout, line_fmt, "Pool Name", "Active", "Pending", "Completed", "Dropped");
    fmt::print(std::cout, line_fmt, "Large messages", "n/a",
            sum_nodes(client.get("/messaging_service/messages/pending")),
            sum_nodes(client.get("/messaging_service/messages/sent")),
            0);
    fmt::print(std::cout, line_fmt, "Small messages", "n/a",
            sum_nodes(client.get("/messaging_service/messages/respond_pending")),
            sum_nodes(client.get("/messaging_service/messages/respond_completed")),
            0);
    fmt::print(std::cout, line_fmt, "Gossip messages", "n/a", 0, 0, 0);
}

void proxyhistograms_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto read_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/read/moving_average_histogram");
    const auto write_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/write/moving_average_histogram");
    const auto range_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/range/moving_average_histogram");
    const auto cas_read_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/cas_read/moving_average_histogram");
    const auto cas_write_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/cas_write/moving_average_histogram");
    const auto view_write_hg = buffer_samples::retrieve_from_api(client, "/storage_proxy/metrics/view_write/moving_average_histogram");

    fmt::print(std::cout, "proxy histograms\n");
    fmt::print(std::cout, "{:>10}{:>19}{:>19}{:>19}{:>19}{:>19}{:>19}\n", "Percentile", "Read Latency", "Write Latency", "Range Latency", "CAS Read Latency", "CAS Write Latency", "View Write Latency");
    fmt::print(std::cout, "{:>10}{:>19}{:>19}{:>19}{:>19}{:>19}{:>19}\n", "", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)");
    for (const auto percentile : {0.5, 0.75, 0.95, 0.98, 0.99}) {
        fmt::print(
                std::cout,
                "{}%       {:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}\n",
                int(percentile * 100),
                read_hg.value(percentile),
                write_hg.value(percentile),
                range_hg.value(percentile),
                cas_read_hg.value(percentile),
                cas_write_hg.value(percentile),
                view_write_hg.value(percentile));
    }
    fmt::print(std::cout, "{:<10}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}\n", "Min", read_hg.min(), write_hg.min(), range_hg.min(),
            cas_read_hg.min(), cas_write_hg.min(), view_write_hg.min());
    fmt::print(std::cout, "{:<10}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}{:>19.2f}\n", "Max", read_hg.max(), write_hg.max(), range_hg.max(),
            cas_read_hg.max(), cas_write_hg.max(), view_write_hg.max());
    fmt::print(std::cout, "\n");
}

sstring get_command_name(const std::vector<sstring>& cmds) {
    sstring res = "";
    sstring delim = "";
    for (const auto& cmd: cmds) {
        res += delim + cmd;
        delim = " ";
    }
    return res;
}

void help_operation(const tool_app_template::config& cfg, const bpo::variables_map& vm) {
    if (vm.contains("command")) {
        const auto command = vm["command"].as<std::vector<sstring>>();
        const auto& ops = get_operations_with_func();
        auto keys = ops | boost::adaptors::map_keys;
        auto it = std::ranges::find_if(keys, [&] (const operation& op) { return op.name() == command[0]; });
        if (it == keys.end()) {
            throw std::invalid_argument(fmt::format("unknown command {}", get_command_name(command)));
        }
        // Search the subcommands.
        const operation* op = &*it;
        for (size_t i = 1; i < command.size(); ++i) {
            auto subcommand = op->suboperations();
            auto it = std::ranges::find_if(subcommand, [&] (const operation& subop) { return subop.name() == command[i]; });
            if (it == subcommand.end()) {
                throw std::invalid_argument(fmt::format("unknown command {}", get_command_name(command)));
            }
            op = &*it;
        }

        fmt::print(std::cout, "{}\n\n", op->summary());
        fmt::print(std::cout, "{}\n\n", op->description());

        // FIXME
        // The below code is needed because we don't have complete access to the
        // internal options descriptions inside the app-template.
        // This will be addressed once https://github.com/scylladb/seastar/pull/1762
        // goes in.

        bpo::options_description opts_desc(fmt::format("scylla-{} options", app_name));
        for (const auto& [option, help] : tool_app_template::help_arguments) {
            opts_desc.add_options()(option, help);
        }
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

        bpo::options_description op_opts_desc(op->name());
        for (const auto& opt : op->options()) {
            opt.add_option(op_opts_desc);
        }
        for (const auto& opt : op->positional_options()) {
            opt.add_option(opts_desc);
        }
        if (!op->options().empty()) {
            opts_desc.add(op_opts_desc);
        }

        fmt::print(std::cout, "{}\n", fmt::streamed(opts_desc));
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
    if (vm.contains("source-dc")) {
        params["source_dc"] = vm["source-dc"].as<sstring>();
    }
    if (vm.contains("force")) {
        params["force"] = "true";
    }
    client.post("/storage_service/rebuild", std::move(params));
}

void refresh_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("keyspace")) {
        throw std::invalid_argument("required parameters are missing: keyspace and table");
    }
    if (!vm.contains("table")) {
        throw std::invalid_argument("required parameter is missing: table");
    }
    std::unordered_map<sstring, sstring> params{{"cf", vm["table"].as<sstring>()}};
    if (vm.contains("load-and-stream")) {
        params["load_and_stream"] = "true";
    }
    if (vm.contains("primary-replica-only")) {
        if (!vm.contains("load-and-stream")) {
            throw std::invalid_argument("--primary-replica-only|-pro takes no effect without --load-and-stream|-las");
        }
        params["primary_replica_only"] = "true";
    }
    client.post(format("/storage_service/sstables/{}", vm["keyspace"].as<sstring>()), std::move(params));
}

void removenode_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("remove-operation")) {
        throw std::invalid_argument("required parameters are missing: remove-operation, pass one of (status, force or a host id)");
    }
    const auto op = vm["remove-operation"].as<sstring>();
    if (op == "status" || op == "force") {
        if (vm.contains("ignore-dead-nodes")) {
            throw std::invalid_argument("cannot use --ignore-dead-nodes with status or force");
        }
        fmt::print(std::cout, "RemovalStatus: {}\n", rjson::to_string_view(client.get("/storage_service/removal_status")));
        if (op == "force") {
            client.post("/storage_service/force_remove_completion");
        }
    } else {
        std::unordered_map<sstring, sstring> params{{"host_id", op}};
        if (vm.contains("ignore-dead-nodes")) {
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

    if (vm.contains("job-threads")) {
        repair_params["jobThreads"] = fmt::to_string(vm["job-threads"].as<unsigned>());
    }

    auto log = [&]<typename... Args> (fmt::format_string<Args...> fmt, Args&&... param) {
        const auto msg = fmt::format(fmt, param...);
        using clock = std::chrono::system_clock;
        const auto n = clock::now();
        const auto t = clock::to_time_t(n);
        const auto ms = (n - clock::from_time_t(t)) / 1ms;
        fmt::print("[{:%F %T},{:03d}] {}\n", fmt::localtime(t), ms, msg);
    };

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
            throw operation_failed_on_scylladb(format("Repair session {} failed", id));
        }
    }
}

void restore_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::unordered_map<sstring, sstring> params;
    for (auto required_param : {"endpoint", "bucket", "snapshot", "keyspace"}) {
        if (!vm.count(required_param)) {
            throw std::invalid_argument(fmt::format("missing required parameter: {}", required_param));
        }
        params[required_param] = vm[required_param].as<sstring>();
    }
    if (vm.count("table")) {
        params["table"] = vm["table"].as<sstring>();
    }
    const auto restore_res = client.post("/storage_service/restore", std::move(params));
    if (vm.count("nowait")) {
        return;
    }

    const auto task_id = rjson::to_string_view(restore_res);
    const auto url = seastar::format("/task_manager/wait_task/{}", task_id);
    const auto wait_res = client.get(url);
    const auto& status = wait_res.GetObject();
    auto state = rjson::to_string_view(status["state"]);
    fmt::print("{}", state);
    if (state != "done") {
        fmt::print(": {}", rjson::to_string_view(status["error"]));
    }
    fmt::print(R"(
start: {}
end: {}
)",
               rjson::to_string_view(status["start_time"]),
               rjson::to_string_view(status["end_time"]));
}

struct host_stat {
    sstring endpoint;
    sstring address;
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
                                      const std::map<sstring, float>& endpoint_to_ownership,
                                      bool resolve_ip) {
    ownership_by_dc_t ownership_by_dc;
    std::map<std::string_view, std::string_view> endpoint_to_dc;
    for (auto& [token, endpoint] : token_to_endpoint) {
        sstring dc = snitch.get_datacenter(endpoint);
        std::optional<float> ownership;
        if (auto found = endpoint_to_ownership.find(endpoint);
            found != endpoint_to_ownership.end()) {
            ownership = found->second;
        }
        host_stat stat {
            .endpoint = endpoint,
            .address = endpoint,
            .ownership = ownership,
            .token = token,
        };
        if (resolve_ip) {
            stat.address = net::dns::resolve_addr(net::inet_address(endpoint)).get();
        }
        ownership_by_dc[dc].emplace_back(std::move(stat));
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
            if constexpr (std::is_same_v<Value, sstring>) {
                v = sstring(rjson::to_string_view(object["value"]));
            } else {
                v = object["value"].Get<Value>();
            }
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
    SCYLLA_ASSERT(last_token != tokens_endpoint.rend());
    return last_token->first;
}

void print_dc(scylla_rest_client& client,
              SnitchInfo& snitch,
              const sstring& dc,
              bool resolve_ip,
              const sstring& last_token,
              const std::multimap<std::string_view, std::string_view>& endpoints_to_tokens,
              const std::vector<host_stat>& host_stats) {
    fmt::print("\n"
               "Datacenter: {}\n"
               "==========\n", dc);
    constexpr auto fmt_str = "{:<{}}  {:<12}{:<7}{:<8}{:<16}{:<20}{:<44}\n";
    const auto max_endpoint_width = std::invoke([&] {
        auto stat_with_max_addr_width = std::ranges::max_element(
            host_stats,
            [](auto& lhs, auto&& rhs) {
                return lhs.address.size() < rhs.address.size();
            });
        if (stat_with_max_addr_width == host_stats.end()) {
            return size_t(0);
        }
        return stat_with_max_addr_width->address.size();
    });
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
                   stat.address, max_endpoint_width,
                   rack, status, state, load, ownership, stat.token);
    }
}

static std::map<sstring, float> get_effective_ownership(scylla_rest_client& client, const sstring& keyspace, const std::optional<sstring>& table) {
    const sstring request_str = format("/storage_service/ownership/{}", keyspace);
    std::unordered_map<sstring, sstring> params{};
    if (table) {
        params["cf"] = *table;
    }

    return rjson_to_map<float>(client.get(request_str, params));
}

static bool keyspace_uses_tablets(scylla_rest_client& client, const sstring& keyspace) {
    const std::unordered_map<sstring, sstring> params = {{"replication", "tablets"}};
    const auto res = client.get("/storage_service/keyspaces", params);

    const auto& ks_array = res.GetArray();
    const auto is_same_ks = [&] (const auto& json_ks) { return rjson::to_string_view(json_ks) == keyspace; };
    return std::find_if(ks_array.begin(), ks_array.end(), is_same_ks) != ks_array.end();
}

void ring_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const bool resolve_ip = vm.contains("resolve-ip");

    const std::optional<sstring> keyspace = vm.contains("keyspace") ? std::optional(vm["keyspace"].as<sstring>()) : std::nullopt;
    const std::optional<sstring> table = vm.contains("table") ? std::optional(vm["table"].as<sstring>()) : std::nullopt;

    if (keyspace && !table && keyspace_uses_tablets(client, *keyspace)) {
        throw std::invalid_argument("need a table to obtain ring for tablet keyspace");
    }

    std::unordered_map<sstring, sstring> tokens_endpoint_params;
    if (keyspace && table) {
        tokens_endpoint_params["keyspace"] = *keyspace;
        tokens_endpoint_params["cf"] = *table;
    }
    auto tokens_endpoint = rjson_to_vector<std::string>(client.get("/storage_service/tokens_endpoint", std::move(tokens_endpoint_params)));

    std::multimap<std::string_view, std::string_view> endpoints_to_tokens;
    bool have_vnodes_or_tablets = false;
    for (auto& [token, endpoint] : tokens_endpoint) {
        if (endpoints_to_tokens.contains(endpoint)) {
            have_vnodes_or_tablets = true;
        }
        endpoints_to_tokens.emplace(endpoint, token);
    }
    // Calculate per-token ownership of the ring
    std::string_view warnings;
    std::map<sstring, float> endpoint_to_ownership;
    if (keyspace) {
        endpoint_to_ownership = get_effective_ownership(client, *keyspace, table);
    } else {
        warnings = "Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless\n";
    }

    SnitchInfo snitch{client};
    for (auto& [dc, host_stats] : get_ownership_by_dc(snitch,
                                                      tokens_endpoint,
                                                      endpoint_to_ownership,
                                                      resolve_ip)) {
        auto last_token = last_token_in_hosts(tokens_endpoint, host_stats);
        print_dc(client, snitch, dc, resolve_ip,
                 last_token, endpoints_to_tokens, host_stats);
    }

    if (have_vnodes_or_tablets) {
        fmt::print(R"(
  Warning: "nodetool ring" is used to output all the tokens of a node.
  To view status related info of a node use "nodetool status" instead.


)");
    }
    fmt::print("  {}", warnings);
}

void scrub_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::vector<sstring> keyspaces;
    const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm, false);
    if (keyspace.empty()) {
        keyspaces = get_keyspaces(client);
    } else {
        keyspaces.push_back(std::move(keyspace));
    }
    if (vm.contains("skip-corrupted") && vm.count("mode")) {
        throw std::invalid_argument("cannot use --skip-corrupted when --mode is used");
    }

    std::unordered_map<sstring, sstring> params;

    if (!tables.empty()) {
        params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }

    if (vm.contains("mode")) {
        params["scrub_mode"] = vm["mode"].as<sstring>();
    } else if (vm.contains("skip-corrupted")) {
        params["scrub_mode"] = "SKIP";
    }

    if (vm.contains("quarantine-mode")) {
        params["quarantine_mode"] = vm["quarantine-mode"].as<sstring>();
    }

    if (vm.contains("no-snapshot")) {
        params["disable_snapshot"] = "true";
    }

    std::vector<api::scrub_status> statuses;
    for (const auto& keyspace : keyspaces) {
        statuses.push_back(api::scrub_status(client.get(format("/storage_service/keyspace_scrub/{}", keyspace), params).GetInt()));
    }

    for (const auto status : statuses) {
        switch (status) {
            case api::scrub_status::successful:
            case api::scrub_status::unable_to_cancel:
                continue;
            case api::scrub_status::aborted:
                throw operation_failed_on_scylladb("scrub failed: aborted");
            case api::scrub_status::validation_errors:
                throw operation_failed_on_scylladb("scrub failed: there are invalid sstables");
        }
    }
}

void setlogginglevel_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("logger") || !vm.count("level")) {
        throw std::invalid_argument("resetting logger(s) is not supported yet, the logger and level parameters are required");
    }
    client.post(format("/system/logger/{}", vm["logger"].as<sstring>()), {{"level", vm["level"].as<sstring>()}});
}

void settraceprobability_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("trace_probability")) {
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

    auto split_kt = [] (const std::string_view kt) -> std::optional<std::pair<sstring, sstring>> {
        std::vector<sstring> components;
        boost::split(components, kt, boost::algorithm::is_any_of("/"));
        if (components.size() == 2) {
            return std::pair(components[0], components[1]);
        }
        components.clear();
        boost::split(components, kt, boost::algorithm::is_any_of("."));
        if (components.size() == 2) {
            return std::pair(components[0], components[1]);
        }
        return {};
    };

    std::vector<sstring> kt_list;
    if (vm.contains("keyspace-table-list")) {
        if (vm.contains("table")) {
            throw std::invalid_argument("when specifying the keyspace-table list for a snapshot, you should not specify table(s)");
        }
        if (vm.contains("keyspaces")) {
            throw std::invalid_argument("when specifying the keyspace-table list for a snapshot, you should not specify keyspace(s)");
        }

        const auto kt_list_str = vm["keyspace-table-list"].as<sstring>();
        boost::split(kt_list, kt_list_str, boost::algorithm::is_any_of(","));
    } else if (vm.contains("keyspaces")) {
        kt_list = vm["keyspaces"].as<std::vector<sstring>>();

        if (kt_list.size() > 1 && vm.contains("table")) {
            throw std::invalid_argument("when specifying the table for the snapshot, you must specify one and only one keyspace");
        }
    }

    if (kt_list.empty()) {
        kn_msg = "all keyspaces";
    } else {
        if (kt_list.size() == 1 && split_kt(kt_list.front())) {
            auto res = split_kt(kt_list.front());
            params["kn"] = std::move(res->first);
            params["cf"] = std::move(res->second);
            kn_msg = kt_list.front();
        } else {
            params["kn"] = fmt::to_string(fmt::join(kt_list.begin(), kt_list.end(), ","));
            if (vm.contains("table")) {
                params["cf"] = vm["table"].as<sstring>();
            }
        }
    }

    if (vm.contains("tag")) {
        params["tag"] = vm["tag"].as<sstring>();
    } else {
        params["tag"] = fmt::to_string(db_clock::now().time_since_epoch().count());
    }

    if (vm.contains("skip-flush")) {
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

void sstableinfo_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::vector<keyspace_and_table> requests;
    if (vm.contains("table")) {
        const auto keyspace = vm["keyspace"].as<sstring>();
        for (const auto& table : vm["table"].as<std::vector<sstring>>()) {
            requests.push_back(keyspace_and_table{.keyspace = keyspace, .table = table});
        }
    } else if (vm.contains("keyspace")) {
        requests.push_back(keyspace_and_table{.keyspace = vm["keyspace"].as<sstring>()});
    } else {
        requests.push_back(keyspace_and_table{});
    }

    fmt::print("\n");

    for (const auto& req : requests) {
        std::unordered_map<sstring, sstring> params;
        if (!req.keyspace.empty()) {
            params["keyspace"] = req.keyspace;
        }
        if (!req.table.empty()) {
            params["cf"] = req.table;
        }
        auto res = client.get("/storage_service/sstable_info", std::move(params));

        for (const auto& entry : res.GetArray()) {
            fmt::print("{:>8} : {}\n", "keyspace", rjson::to_string_view(entry["keyspace"]));
            fmt::print("{:>8} : {}\n", "table", rjson::to_string_view(entry["table"]));
            if (!entry.HasMember("sstables")) {
                continue;
            }
            fmt::print("sstables :\n");
            unsigned i = 0;
            for (const auto& sstable : entry["sstables"].GetArray()) {
                fmt::print("{:>8} :\n", i++);

                // Keep the same order as the Java nodetool.
                // NOTE: we keep the timestamp field as-is, while the Java nodetool re-formats it.
                for (const auto& key : {"data_size", "filter_size", "index_size", "level", "size", "generation", "version", "timestamp"}) {
                    std::string print_key = key;
                    std::replace(print_key.begin(), print_key.end(), '_', ' ');
                    if (sstable[key].IsNumber()) {
                        fmt::print("{:>23} : {}\n", print_key, sstable[key]);
                    } else {
                        fmt::print("{:>23} : {}\n", print_key, rjson::to_string_view(sstable[key]));
                    }
                }

                if (sstable.HasMember("properties")) {
                    fmt::print("{:>23} :\n", "properties");
                    for (const auto& property : sstable["properties"].GetArray()) {
                        fmt::print("{:>16} : {}\n", rjson::to_string_view(property["key"]), rjson::to_string_view(property["value"]));
                    }
                }

                if (sstable.HasMember("extended_properties")) {
                    fmt::print("{:>23} :\n", "extended properties");
                    for (const auto& extended_property : sstable["extended_properties"].GetArray()) {
                        fmt::print("{:>35} :\n", rjson::to_string_view(extended_property["group"]));
                        for (const auto& attribute : extended_property["attributes"].GetArray()) {
                            fmt::print("{:>43} : {}\n", rjson::to_string_view(attribute["key"]), rjson::to_string_view(attribute["value"]));
                        }
                    }
                }
            }
        }
    }
}

void status_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::optional<sstring> keyspace;
    if (vm.contains("keyspace")) {
        keyspace.emplace(vm["keyspace"].as<sstring>());
    }

    std::optional<sstring> table;
    if (vm.contains("table")) {
        table.emplace(vm["table"].as<sstring>());
    }

    const auto resolve_ips = vm.contains("resolve-ip");

    const auto live = get_endpoints_of_status(client, "live");
    const auto down = get_endpoints_of_status(client, "down");
    const auto joining = get_nodes_of_state(client, "joining");
    const auto leaving = get_nodes_of_state(client, "leaving");
    const auto moving = get_nodes_of_state(client, "moving");
    const auto endpoint_load = rjson_to_map<size_t>(client.get("/storage_service/load_map"));
    const auto endpoint_host_id = rjson_to_map<sstring>(client.get("/storage_service/host_id"));

    const auto tablets_keyspace = keyspace && keyspace_uses_tablets(client, *keyspace);

    const auto is_effective_ownership_unknown = (!keyspace || (!table && tablets_keyspace));

    const auto endpoint_ownership = is_effective_ownership_unknown
        ? std::map<sstring, float>{}
        : get_effective_ownership(client, *keyspace, table);

    std::unordered_map<sstring, sstring> endpoint_rack;
    std::map<sstring, std::set<sstring>> dc_endpoints;
    std::unordered_map<sstring, size_t> endpoint_tokens;
    std::unordered_map<sstring, sstring> tokens_endpoint_params;
    if (tablets_keyspace && table) {
        tokens_endpoint_params["keyspace"] = *keyspace;
        tokens_endpoint_params["cf"] = *table;
    }
    const auto tokens_endpoint_res = client.get("/storage_service/tokens_endpoint", std::move(tokens_endpoint_params));
    for (const auto& te : tokens_endpoint_res.GetArray()) {
        const auto ep = sstring(rjson::to_string_view(te["value"]));
         // We are not printing the actual tokens, so it is enough just to count them.
        ++endpoint_tokens[ep];
        if (endpoint_rack.contains(ep)) {
            continue;
        }
        const auto dc = sstring(rjson::to_string_view(client.get("/snitch/datacenter", {{"host", ep}})));
        const auto rack = sstring(rjson::to_string_view(client.get("/snitch/rack", {{"host", ep}})));
        endpoint_rack.emplace(ep, rack);
        dc_endpoints[dc].insert(ep);
    }

    const bool token_count_unknown = tablets_keyspace && !table;

    for (const auto& [dc, endpoints] : dc_endpoints) {
        const auto dc_header = fmt::format("Datacenter: {}", dc);
        fmt::print("{}\n", dc_header);
        fmt::print("{}\n", std::string(dc_header.size(), '='));
        fmt::print("Status=Up/Down\n");
        fmt::print("|/ State=Normal/Leaving/Joining/Moving\n");
        Tabulate table;
        if (keyspace) {
            table.add("--", "Address", "Load", "Tokens", "Owns (effective)", "Host ID", "Rack");
        } else {
            table.add("--", "Address", "Load", "Tokens", "Owns", "Host ID", "Rack");
        }
        for (const auto& ep : endpoints) {
            char status, state;
            if (live.contains(ep)) {
                status = 'U';
            } else if (down.contains(ep)) {
                status = 'D';
            } else {
                status = '?';
            }
            if (joining.contains(ep)) {
                state = 'J';
            } else if (leaving.contains(ep)) {
                state = 'L';
            } else if (moving.contains(ep)) {
                state = 'M';
            } else {
                state = 'N';
            }
            sstring address = resolve_ips ? net::dns::resolve_addr(net::inet_address(ep)).get() : ep;
            const std::string load = endpoint_load.contains(ep) ? fmt::to_string(file_size_printer(endpoint_load.at(ep))) : "?";
            table.add(
                    fmt::format("{}{}", status, state),
                    address,
                    load,
                    token_count_unknown ? "?" : fmt::to_string(endpoint_tokens.at(ep)),
                    !is_effective_ownership_unknown ? format("{:.1f}%", endpoint_ownership.at(ep) * 100) : "?",
                    endpoint_host_id.contains(ep) ? endpoint_host_id.at(ep) : "?",
                    endpoint_rack.at(ep));
        }
        table.print();
    }

    fmt::print("\n");
    if (!keyspace) {
        fmt::print("Note: Non-system keyspaces don't have the same replication settings, "
                "effective ownership information is meaningless\n");
    }
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
    if (vm.contains("id")) {
        throw std::invalid_argument("stopping compactions by id is not implemented");
    }
    if (!vm.contains("compaction_type")) {
        throw std::invalid_argument("missing required parameter: compaction_type");
    }

    static const std::vector<std::string_view> recognized_compaction_types{"COMPACTION", "CLEANUP", "SCRUB", "RESHAPE", "RESHARD", "UPGRADE"};

    const auto compaction_type = vm["compaction_type"].as<sstring>();

    if (std::ranges::find(recognized_compaction_types, compaction_type) == recognized_compaction_types.end()) {
        throw std::invalid_argument(fmt::format("invalid compaction type: {}", compaction_type));
    }

    client.post("/compaction_manager/stop_compaction", {{"type", compaction_type}});
}

void tablehistograms_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto [keyspace, table] = parse_keyspace_and_table(client, vm, "table");

    auto get_estimated_histogram = [&client, &keyspace, &table] (std::string_view histogram) {
        const auto res = client.get(format("/column_family/metrics/{}/{}:{}", histogram, keyspace, table));
        const auto res_object = res.GetObject();
        if (!res.HasMember("bucket_offsets")) {
            return utils::estimated_histogram(0);
        }
        const auto& buckets_array = res["buckets"].GetArray();
        const auto& bucket_offsets_array = res["bucket_offsets"].GetArray();

        if (bucket_offsets_array.Size() + 1 != buckets_array.Size()) {
            throw std::runtime_error(format("invalid estimated histogram {}, buckets must have one more element than bucket_offsets", histogram));
        }

        std::vector<int64_t> bucket_offsets, buckets;
        for (size_t i = 0; i < bucket_offsets_array.Size(); ++i) {
            buckets.emplace_back(buckets_array[i].GetInt64());
            bucket_offsets.emplace_back(bucket_offsets_array[i].GetInt64());
        }
        buckets.emplace_back(buckets_array[buckets_array.Size() - 1].GetInt64());

        return utils::estimated_histogram(std::move(bucket_offsets), std::move(buckets));
    };

    const auto row_size_hg = get_estimated_histogram("estimated_row_size_histogram");
    const auto column_count_hg = get_estimated_histogram("estimated_column_count_histogram");
    const auto read_latency_hg = buffer_samples::retrieve_from_api(client,
            format("/column_family/metrics/read_latency/moving_average_histogram/{}:{}", keyspace, table));
    const auto write_latency_hg = buffer_samples::retrieve_from_api(client,
            format("/column_family/metrics/write_latency/moving_average_histogram/{}:{}", keyspace, table));
    const auto sstables_per_read_hg = get_estimated_histogram("sstables_per_read_histogram");

    fmt::print(std::cout, "{}/{} histograms\n", keyspace, table);
    fmt::print(std::cout, "{:>10}{:>10}{:>18}{:>18}{:>18}{:>18}\n", "Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count");
    fmt::print(std::cout, "{:>10}{:>10}{:>18}{:>18}{:>18}{:>18}\n", "", " ", "(micros)", "(micros)", "(bytes)", "");
    for (const auto percentile : {0.5, 0.75, 0.95, 0.98, 0.99}) {
        fmt::print(
                std::cout,
                "{}%       {:>10.2f}{:>18.2f}{:>18.2f}{:>18}{:>18}\n",
                int(percentile * 100),
                double(sstables_per_read_hg.percentile(percentile)),
                write_latency_hg.value(percentile),
                read_latency_hg.value(percentile),
                row_size_hg.percentile(percentile),
                column_count_hg.percentile(percentile));
    }
    fmt::print(std::cout, "{:<10}{:>10.2f}{:>18.2f}{:>18.2f}{:>18}{:>18}\n", "Min", double(sstables_per_read_hg.min()), write_latency_hg.min(),
            read_latency_hg.min(), row_size_hg.min(), column_count_hg.min());
    fmt::print(std::cout, "{:<10}{:>10.2f}{:>18.2f}{:>18.2f}{:>18}{:>18}\n", "Max", double(sstables_per_read_hg.max()), write_latency_hg.max(),
            read_latency_hg.max(), row_size_hg.max(), column_count_hg.max());
    fmt::print(std::cout, "\n");
}

class table_metrics {
    scylla_rest_client& _client;
    std::string _ks_name;
    std::string _cf_name;
    template<typename T>
    T get(std::string_view metric_name) {
        auto res = _client.get(fmt::format("/column_family/metrics/{}/{}:{}",
                                           metric_name, _ks_name, _cf_name));
        return res.Get<T>();
    }
    struct histogram {
        uint64_t count;
        uint64_t sum;
        uint64_t min;
        uint64_t max;
        double variance;
        double mean;
    };
    histogram get_histogram(std::string_view metric_name) {
        auto res = _client.get(fmt::format("/column_family/metrics/{}/{}:{}",
                                           metric_name, _ks_name, _cf_name));
        const auto& obj = res.GetObject();
        // we don't use sample here
        return histogram(obj["count"].GetUint64(),
                         obj["sum"].GetUint64(),
                         obj["min"].GetUint64(),
                         obj["max"].GetUint64(),
                         obj["variance"].GetDouble(),
                         obj["mean"].GetDouble());
    }

public:
    table_metrics(scylla_rest_client& client,
                  std::string_view keyspace_name,
                  std::string_view table_name)
        : _client{client}
        , _ks_name{keyspace_name}
        , _cf_name{table_name}
    {}
    const std::string& name() const {
        return _cf_name;
    }
    uint64_t live_ss_table_count() {
        return get<uint64_t>("live_ss_table_count");
    }
    std::vector<int64_t> ss_table_count_per_level() {
        auto res = _client.get(fmt::format("/column_family/sstables/per_level/{}:{}",
                                           _ks_name, _cf_name));
        std::vector<int64_t> levels;
        for (auto& element : res.GetArray()) {
            levels.push_back(element.GetInt64());
        }
        return levels;
    }
    uint64_t space_used_live() {
        return get<uint64_t>("live_disk_space_used");
    }
    uint64_t space_used_total() {
        return get<uint64_t>("total_disk_space_used");
    }
    uint64_t space_used_by_snapshots_total() {
        return get<uint64_t>("snapshots_size");
    }
    uint64_t memtable_off_heap_size() {
        return get<uint64_t>("memtable_off_heap_size");
    }
    uint64_t bloom_filter_off_heap_memory_used() {
        return get<uint64_t>("bloom_filter_off_heap_memory_used");
    }
    uint64_t index_summary_off_heap_memory_used() {
        return get<uint64_t>("index_summary_off_heap_memory_used");
    }
    uint64_t compression_metadata_off_heap_memory_used() {
        return get<uint64_t>("compression_metadata_off_heap_memory_used");
    }
    double compression_ratio() {
        return get<double>("compression_ratio");
    }
    uint64_t estimated_row_count() {
        return get<uint64_t>("estimated_row_count");
    }
    uint64_t memtable_columns_count() {
        return get<uint64_t>("memtable_columns_count");
    }
    uint64_t memtable_live_data_size() {
        return get<uint64_t>("memtable_live_data_size");
    }
    uint64_t memtable_switch_count() {
        return get<uint64_t>("memtable_switch_count");
    }
    uint64_t read() {
        return get<uint64_t>("read");
    }
    uint64_t read_latency() {
        return get<uint64_t>("read_latency");
    }
    uint64_t write() {
        return get<uint64_t>("write");
    }
    uint64_t write_latency() {
        return get<uint64_t>("write_latency");
    }
    uint64_t pending_flushes() {
        return get<uint64_t>("pending_flushes");
    }
    uint64_t bloom_filter_false_positives() {
        return get<uint64_t>("bloom_filter_false_positives");
    }
    double recent_bloom_filter_false_ratio() {
        return get<double>("recent_bloom_filter_false_ratio");
    }
    uint64_t bloom_filter_disk_space_used() {
        return get<uint64_t>("bloom_filter_disk_space_used");
    }
    uint64_t min_row_size() {
        return get<uint64_t>("min_row_size");
    }
    uint64_t max_row_size() {
        return get<uint64_t>("max_row_size");
    }
    uint64_t mean_row_size() {
        return get<uint64_t>("mean_row_size");
    }
    histogram live_scanned_histogram() {
        return get_histogram("live_scanned_histogram");
    }
    histogram tombstone_scanned_histogram() {
        return get_histogram("tombstone_scanned_histogram");
    }

    struct latency_hist {
        int64_t count;
        float latency;
    };

    latency_hist latency_histogram(std::string_view name) {
        auto res = _client.get(fmt::format("/column_family/metrics/{}/moving_average_histogram/{}:{}",
                                           name, _ks_name, _cf_name));
        const auto& moving_avg_and_hist = res.GetObject();
        const auto& hist = moving_avg_and_hist["hist"].GetObject();
        return latency_hist{
            hist["count"].GetInt64(),
            hist["mean"].GetDouble() / 1000
        };
    }
    static std::vector<std::string> print_ss_table_levels(const std::vector<int64_t>& counts) {
        constexpr int LEVEL_FANOUT_SIZE = 10;
        int64_t max_count;
        std::vector<std::string> levels;
        for (unsigned level = 0; level < counts.size(); level++) {
            std::string fmt_sstable;
            auto out = std::back_inserter(fmt_sstable);
            auto count = counts[level];
            if (level == 0) {
                max_count = 4;
            } else if (level == 1) {
                max_count = LEVEL_FANOUT_SIZE;
            } else {
                max_count *= LEVEL_FANOUT_SIZE;
            }
            out = fmt::format_to(out, "{}", count);
            if (count > max_count) {
                fmt::format_to(out, "/{}", max_count);
            }
            levels.push_back(std::move(fmt_sstable));
        }
        return levels;
    }

    static std::pair<double, double> mean_max(const std::vector<double> v) {
        if (v.empty()) {
            return {0, 0};
        }
        auto sum = std::accumulate(v.begin(), v.end(), .0);
        auto max_element = std::ranges::max_element(v);
        return {sum / v.size(), *max_element};
    }

    std::map<std::string, metrics_value> to_map(bool human_readable) {
        std::map<std::string, metrics_value> map;
        if (auto levels = ss_table_count_per_level(); !levels.empty()) {
            map.emplace("sstables_in_each_level", print_ss_table_levels(levels));
        }
        map.emplace("space_used_live",
                    fmt::to_string(file_size_printer(space_used_live(), human_readable)));
        map.emplace("space_used_total",
                    fmt::to_string(file_size_printer(space_used_total(), human_readable)));
        map.emplace("space_used_by_snapshots_total",
                    fmt::to_string(file_size_printer(space_used_by_snapshots_total(), human_readable)));
        auto memtable_off_heap_mem_size = memtable_off_heap_size();
        auto bloom_filter_off_heap_mem_size = bloom_filter_off_heap_memory_used();
        auto index_summary_off_heap_mem_size = index_summary_off_heap_memory_used();
        auto compression_metadata_off_heap_mem_size = compression_metadata_off_heap_memory_used();
        auto total_off_heap_size = (memtable_off_heap_mem_size +
                                    bloom_filter_off_heap_mem_size +
                                    index_summary_off_heap_mem_size +
                                    compression_metadata_off_heap_mem_size);
        map.emplace("off_heap_memory_used_total",
                    fmt::to_string(file_size_printer(total_off_heap_size, human_readable)));
        map.emplace("sstable_compression_ratio", compression_ratio());
        map.emplace("number_of_partitions_estimate", estimated_row_count());
        map.emplace("memtable_cell_count", memtable_columns_count());
        map.emplace("memtable_data_size",
                    fmt::to_string(file_size_printer(memtable_live_data_size(), human_readable)));
        map.emplace("memtable_off_heap_memory_used",
                    fmt::to_string(file_size_printer(memtable_off_heap_mem_size, human_readable)));
        map.emplace("memtable_switch_count", memtable_switch_count());
        auto local_reads = latency_histogram("read_latency");
        map.emplace("local_read_count", local_reads.count);
        map.emplace("local_read_latency_ms",
                    fmt::format("{:.3f}", local_reads.latency));
        auto local_writes = latency_histogram("write_latency");
        map.emplace("local_write_count", local_writes.count);
        map.emplace("local_write_latency_ms",
                    fmt::format("{:.3f}", local_writes.latency));
        map.emplace("pending_flushes", pending_flushes());
        // scylla does not support it.
        map.emplace("percent_repaired", 0.0);
        map.emplace("bloom_filter_false_positives", bloom_filter_false_positives());
        map.emplace("bloom_filter_false_ratio",
                    fmt::format("{:01.5f}", recent_bloom_filter_false_ratio()));
        map.emplace("bloom_filter_space_used",
                    fmt::to_string(file_size_printer(bloom_filter_disk_space_used(), human_readable)));
        map.emplace("bloom_filter_off_heap_memory_used",
                    fmt::to_string(file_size_printer(bloom_filter_off_heap_mem_size, human_readable)));
        map.emplace("index_summary_off_heap_memory_used",
                    fmt::to_string(file_size_printer(index_summary_off_heap_mem_size, human_readable)));
        map.emplace("compression_metadata_off_heap_memory_used",
                    fmt::to_string(file_size_printer(compression_metadata_off_heap_mem_size, human_readable)));
        map.emplace("compacted_partition_minimum_bytes", min_row_size());
        map.emplace("compacted_partition_maximum_bytes", max_row_size());
        map.emplace("compacted_partition_mean_bytes", mean_row_size());
        auto live_cells_per_slice = live_scanned_histogram();
        map.emplace("average_live_cells_per_slice_last_five_minutes", live_cells_per_slice.mean);
        map.emplace("maximum_live_cells_per_slice_last_five_minutes", live_cells_per_slice.max);
        auto tombstones_per_slice = tombstone_scanned_histogram();
        map.emplace("average_tombstones_per_slice_last_five_minutes", tombstones_per_slice.mean);
        map.emplace("maximum_tombstones_per_slice_last_five_minutes", live_cells_per_slice.max);
        // scylla does not support it.
        map.emplace("dropped_mutations", fmt::to_string(0));
        return map;
    }

    void print(bool human_readable) {
        fmt::print("\t\tTable: {}\n", name());
        fmt::print("\t\tSSTable count: {}\n", live_ss_table_count());
        if (auto levels = ss_table_count_per_level(); !levels.empty()) {
            fmt::print("\t\tSSTables in each level: [{}]\n",
                       fmt::join(print_ss_table_levels(levels), ", "));
        }
        fmt::print("\t\tSpace used (live): {}\n",
                   file_size_printer(space_used_live(), human_readable));
        fmt::print("\t\tSpace used (total): {}\n",
                   file_size_printer(space_used_total(), human_readable));
        fmt::print("\t\tSpace used by snapshots (total): {}\n",
                   file_size_printer(space_used_by_snapshots_total(), human_readable));
        auto memtable_off_heap_mem_size = memtable_off_heap_size();
        auto bloom_filter_off_heap_mem_size = bloom_filter_off_heap_memory_used();
        auto index_summary_off_heap_mem_size = index_summary_off_heap_memory_used();
        auto compression_metadata_off_heap_mem_size = compression_metadata_off_heap_memory_used();
        auto total_off_heap_size = (memtable_off_heap_mem_size +
                                    bloom_filter_off_heap_mem_size +
                                    index_summary_off_heap_mem_size +
                                    compression_metadata_off_heap_mem_size);
        fmt::print("\t\tOff heap memory used (total): {}\n", file_size_printer(total_off_heap_size, human_readable));
        fmt::print("\t\tSSTable Compression Ratio: {:.1f}\n", compression_ratio());
        fmt::print("\t\tNumber of partitions (estimate): {}\n", estimated_row_count());
        fmt::print("\t\tMemtable cell count: {}\n", memtable_columns_count());
        fmt::print("\t\tMemtable data size: {}\n", memtable_live_data_size());
        fmt::print("\t\tMemtable off heap memory used: {}\n", file_size_printer(memtable_off_heap_mem_size, human_readable));
        fmt::print("\t\tMemtable switch count: {}\n", memtable_switch_count());
        auto local_reads = latency_histogram("read_latency");
        fmt::print("\t\tLocal read count: {}\n", local_reads.count);
        fmt::print("\t\tLocal read latency: {:.3f} ms\n", local_reads.latency);
        auto local_writes = latency_histogram("write_latency");
        fmt::print("\t\tLocal write count: {}\n", local_writes.count);
        fmt::print("\t\tLocal write latency: {:.3f} ms\n", local_writes.latency);
        fmt::print("\t\tPending flushes: {}\n", pending_flushes());
        // scylla does not support it.
        fmt::print("\t\tPercent repaired: {:.1f}\n", 0.0);
        fmt::print("\t\tBloom filter false positives: {}\n", bloom_filter_false_positives());
        fmt::print("\t\tBloom filter false ratio: {:01.5f}\n", recent_bloom_filter_false_ratio());
        fmt::print("\t\tBloom filter space used: {}\n",
                   file_size_printer(bloom_filter_disk_space_used(), human_readable));
        fmt::print("\t\tBloom filter off heap memory used: {}\n",
                   file_size_printer(bloom_filter_off_heap_mem_size, human_readable));
        fmt::print("\t\tIndex summary off heap memory used: {}\n",
                   file_size_printer(index_summary_off_heap_mem_size, human_readable));
        fmt::print("\t\tCompression metadata off heap memory used: {}\n",
                   file_size_printer(compression_metadata_off_heap_mem_size, human_readable));
        fmt::print("\t\tCompacted partition minimum bytes: {}\n", min_row_size());
        fmt::print("\t\tCompacted partition maximum bytes: {}\n", max_row_size());
        fmt::print("\t\tCompacted partition mean bytes: {}\n", mean_row_size());
        auto live_cells_per_slice = live_scanned_histogram();
        fmt::print("\t\tAverage live cells per slice (last five minutes): {:.1f}\n",
                   live_cells_per_slice.mean);
        fmt::print("\t\tMaximum live cells per slice (last five minutes): {}\n",
                   live_cells_per_slice.max);
        auto tombstones_per_slice = tombstone_scanned_histogram();
        fmt::print("\t\tAverage tombstones per slice (last five minutes): {:.1f}\n",
                   tombstones_per_slice.mean);
        fmt::print("\t\tMaximum tombstones per slice (last five minutes): {}\n",
                   tombstones_per_slice.max);
        // scylla does not support it.
        fmt::print("\t\tDropped Mutations: {}\n\n", 0);
    }
};

struct keyspace_stats {
    sstring name;
    uint64_t read_count = 0;
    double total_read_time = .0;
    uint64_t write_count = 0;
    double total_write_time = .0;
    uint64_t pending_flushes = 0;
    std::vector<table_metrics> tables;

    void add(table_metrics&& metrics) {
        auto write_hist = metrics.latency_histogram("write_latency");
        if (write_hist.count > 0) {
            write_count += write_hist.count;
            total_write_time += metrics.write_latency();
        }
        auto read_hist = metrics.latency_histogram("read_latency");
        if (read_hist.count > 0) {
            read_count += read_hist.count;
            total_read_time += metrics.read_latency();
        }
        pending_flushes += metrics.pending_flushes();
        tables.push_back(std::move(metrics));
    }
    double read_latency() const {
        if (read_count == 0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return total_read_time / read_count / 1000;
    }

    double write_latency() const {
        if (write_count == 0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return total_write_time / write_count / 1000;
    }

    std::map<std::string, metrics_value> to_map() {
        std::map<std::string, metrics_value> map;
        map.emplace("read_count", read_count);
        map.emplace("read_latency_ms", read_latency());
        map.emplace("write_count", write_count);
        map.emplace("write_latency_ms", write_latency());
        map.emplace("pending_flushes", pending_flushes);
        return map;
    }
};

class table_filter {
    enum class mode {
      inclusive,
      exclusive,
    };
    mode _mode;
    std::map<sstring, std::set<sstring>> _filter;

    static std::map<sstring, std::set<sstring>> init_filter(const std::vector<sstring>& specs) {
        std::map<sstring, std::set<sstring>> filter;
        for (auto& s : specs) {
            // Usually, keyspace name and table is are separated by a
            // dot, but to allow names which themselves contain a dot
            // (this is allowed in Alternator), also allow to separate
            // the two parts with a slash instead:
            // Allow the syntax "keyspace.name/" to represent a
            // keyspace with a dot in its name.
            auto delim = s.find('/');
            if (delim == s.npos) {
                delim = s.find('.');
            }
            auto first = s.substr(0, delim);
            auto [ks, inserted] = filter.try_emplace(first);
            if (delim != s.npos) {
                auto second = s.substr(++delim);
                if (!second.empty()) {
                    ks->second.insert(second);
                }
            }
        }
        return filter;
    }

    bool belongs_to_filter(const sstring& ks, const sstring& cf) const {
        if (_filter.empty()) {
            // an empty _filter implies the universal set
            return true;
        }
        auto found = _filter.find(ks);
        if (found == _filter.end()) {
            return false;
        }
        auto& tables = found->second;
        if (tables.empty()) {
            // this implies all tables in this ks
            return true;
        }
        return tables.contains(cf);
    }

public:
    table_filter(bool ignore_specified, const std::vector<sstring>& specs)
        : _mode(ignore_specified ? mode::exclusive : mode::inclusive)
        , _filter(init_filter(specs))
    {}

    bool operator()(const sstring& ks, const sstring& cf) const {
        if (belongs_to_filter(ks, cf)) {
            return _mode == mode::inclusive;
        } else {
            return _mode == mode::exclusive;
        }
    }
};

void table_stats_print_plain(scylla_rest_client& client,
                             const table_filter& is_included,
                             bool human_readable) {
    fmt::print("Total number of tables: {}\n", client.get("/column_family/").GetArray().Size());
    fmt::print("----------------\n");
    for (auto& [keyspace_name, table_names] : get_ks_to_cfs(client)) {
        keyspace_stats keyspace;
        for (auto& table_name : table_names) {
            if (is_included(keyspace_name, table_name)) {
                keyspace.add(table_metrics(client, keyspace_name, table_name));
            }
        }
        if (keyspace.tables.empty()) {
            continue;
        }
        fmt::print("Keyspace : {}\n", keyspace_name);
        fmt::print("\tRead Count: {}\n", keyspace.read_count);
        fmt::print("\tRead Latency: {:.15E} ms\n", keyspace.read_latency());
        fmt::print("\tWrite Count: {}\n", keyspace.write_count);
        fmt::print("\tWrite Latency: {:.15E} ms\n", keyspace.write_latency());
        fmt::print("\tPending Flushes: {}\n", keyspace.pending_flushes);

        for (auto& table : keyspace.tables) {
            table.print(human_readable);
        }
        fmt::print("----------------\n");
    }
}

template<typename Writer>
void table_stats_print(scylla_rest_client& client,
                       const table_filter& is_included,
                       bool human_readable) {
    // {
    //     'total_number_of_tables': ...,
    //     'keyspace1' : {
    //         'read_latency': ...,
    //         # ...
    //         'tables': {
    //             'cf1': {
    //                 'sstables_in_each_level': ...
    //                 # ...
    //             },
    //             # ...
    //         }
    //     }
    //     # ...
    // }
    Writer writer(std::cout);
    auto keyspaces_out = writer.map();
    keyspaces_out.add_item("total_number_of_tables",
                           client.get("/column_family/").GetArray().Size());
    for (auto& [keyspace_name, table_names] : get_ks_to_cfs(client)) {
        keyspace_stats keyspace;
        for (auto& table_name : table_names) {
            if (is_included(keyspace_name, table_name)) {
                keyspace.add(table_metrics(client, keyspace_name, table_name));
            }
        }
        auto ks_out = keyspaces_out.add_map(keyspace_name);
        for (auto& [key, value] : keyspace.to_map()) {
            ks_out->add_item(key, value);
        }
        auto cfs_out = ks_out->add_map("tables");
        for (auto& table : keyspace.tables) {
            auto cf_out = cfs_out->add_map(table.name());
            for (auto& [key, value] : table.to_map(human_readable)) {
                cf_out->add_item(key, value);
            }
        }
    }
}

void table_stats_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::vector<sstring> match_set;
    if (vm.contains("tables")) {
        match_set = vm["tables"].as<std::vector<sstring>>();
    }
    table_filter is_included(vm["ignore"].as<bool>(), match_set);

    const auto format = vm["format"].as<sstring>();
    const auto human_readable = vm["human-readable"].as<bool>();
    if (format == "json") {
        table_stats_print<json_writer>(client, is_included, human_readable);
    } else if (format == "yaml") {
        table_stats_print<yaml_writer>(client, is_included, human_readable);
    } else {
        table_stats_print_plain(client, is_included, human_readable);
    }
}

void tasks_print_status(const rjson::value& res) {
    auto status = res.GetObject();
    for (const auto& x: status) {
        if (x.value.IsString()) {
            fmt::print("{}: {}\n", x.name.GetString(), x.value.GetString());
        } else if (x.value.IsArray()) {
            fmt::print("{}: [", x.name.GetString());
            sstring delim = "";
            for (const auto& el : x.value.GetArray()) {
                fmt::print("{}{{", delim);
                sstring ident_delim = "";
                for (const auto& child_info : el.GetObject()) {
                    fmt::print("{}{}: {}", ident_delim, child_info.name.GetString(), child_info.value.GetString());
                    ident_delim = ", ";
                }
                fmt::print(" }}");
                delim = ", ";
            }
            fmt::print("]\n");
        } else {
            fmt::print("{}: {}\n", x.name.GetString(), x.value);
        }
    }
}

void tasks_add_tree_to_statuses_lists(Tabulate& table, const rjson::value& res) {
    auto statuses = res.GetArray();
    for (auto& element : statuses) {
        const auto& status = element.GetObject();

        sstring children_ids = "[";
        sstring delim = "";
        if (status.HasMember("children_ids")) {
            for (const auto& el : status["children_ids"].GetArray()) {
                children_ids += format("{}{{", delim);
                sstring ident_delim = "";
                for (const auto& child_info : el.GetObject()) {
                    children_ids += format("{}{}: {}", ident_delim, child_info.name.GetString(), child_info.value.GetString());
                    ident_delim = ", ";
                }
                children_ids += format(" }}");
                delim = ", ";
            }
        }
        children_ids += "]";

        table.add(rjson::to_string_view(status["id"]),
                rjson::to_string_view(status["type"]),
                rjson::to_string_view(status["kind"]),
                rjson::to_string_view(status["scope"]),
                rjson::to_string_view(status["state"]),
                status["is_abortable"].GetBool(),
                rjson::to_string_view(status["start_time"]),
                rjson::to_string_view(status["end_time"]),
                rjson::to_string_view(status["error"]),
                rjson::to_string_view(status["parent_id"]),
                status["sequence_number"].GetUint64(),
                status["shard"].GetUint(),
                rjson::to_string_view(status["keyspace"]),
                rjson::to_string_view(status["table"]),
                rjson::to_string_view(status["entity"]),
                rjson::to_string_view(status["progress_units"]),
                status["progress_total"].GetDouble(),
                status["progress_completed"].GetDouble(),
                children_ids);
    }
}

void tasks_print_trees(const std::vector<rjson::value>& res) {
    Tabulate table;
    table.add("id", "type", "kind", "scope", "state",
        "is_abortable", "start_time", "end_time", "error", "parent_id",
        "sequence_number", "shard", "keyspace", "table", "entity",
        "progress_units", "total", "completed", "children_ids");

    for (const auto& res_el : res) {
        tasks_add_tree_to_statuses_lists(table, res_el);
    }

    table.print();
}

void tasks_print_stats_list(const rjson::value& res) {
    auto stats = res.GetArray();
    Tabulate table;
    table.add("task_id", "type", "kind", "scope", "state", "sequence_number", "keyspace", "table", "entity");
    for (auto& element : stats) {
        const auto& s = element.GetObject();

        table.add(rjson::to_string_view(s["task_id"]),
                rjson::to_string_view(s["type"]),
                rjson::to_string_view(s["kind"]),
                rjson::to_string_view(s["scope"]),
                rjson::to_string_view(s["state"]),
                s["sequence_number"].GetUint64(),
                rjson::to_string_view(s["keyspace"]),
                rjson::to_string_view(s["table"]),
                rjson::to_string_view(s["entity"]));
    }
    table.print();
}

void tasks_abort_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("id")) {
        throw std::invalid_argument("required parameter is missing: id");
    }
    auto id = vm["id"].as<sstring>();

    try {
        auto res = client.post(format("/task_manager/abort_task/{}", id));
    } catch (const api_request_failed& e) {
        if (e.status != http::reply::status_type::forbidden) {
            throw;
        }

        fmt::print("Task with id {} is not abortable\n", id);
    }
}

void tasks_list_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("module")) {
        throw std::invalid_argument("required parameter is missing: module");
    }
    auto module = vm["module"].as<sstring>();
    std::unordered_map<sstring, sstring> params;
    if (vm.contains("keyspace")) {
        params["keyspace"] = vm["keyspace"].as<sstring>();
    }
    if (vm.contains("table")) {
        params["table"] = vm["table"].as<sstring>();
    }
    if (vm.contains("internal")) {
        params["internal"] = "true";
    }
    int interval = vm["interval"].as<int>();
    int iterations = vm["iterations"].as<int>();

    auto res = client.get(format("/task_manager/list_module_tasks/{}", module), params);
    tasks_print_stats_list(res);

    for (auto i = 0; i < iterations; ++i) {
        sleep(interval);
        auto res = client.get(format("/task_manager/list_module_tasks/{}", module), params);
        fmt::print("\n\n");
        tasks_print_stats_list(res);
    }
}

void tasks_modules_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto res = client.get("/task_manager/list_modules");

    for (auto& x: res.GetArray()) {
        fmt::print("{}\n", rjson::to_string_view(x));
    }
}

void tasks_status_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("id")) {
        throw std::invalid_argument("required parameter is missing: id");
    }
    auto id = vm["id"].as<sstring>();

    auto res = client.get(format("/task_manager/task_status/{}", id));

    tasks_print_status(res);
}

void tasks_tree_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    std::vector<rjson::value> res;
    if (vm.contains("id")) {
        auto id = vm["id"].as<sstring>();

        res.push_back(client.get(format("/task_manager/task_status_recursive/{}", id)));

        tasks_print_trees(res);
        return;
    }

    auto module_res = client.get("/task_manager/list_modules");
    for (const auto& module : module_res.GetArray()) {
        auto list_res = client.get(format("/task_manager/list_module_tasks/{}", module.GetString()));
        for (const auto& stats : list_res.GetArray()) {
            try {
                res.push_back(client.get(format("/task_manager/task_status_recursive/{}", stats.GetObject()["task_id"].GetString())));
            } catch (const api_request_failed& e) {
                if (e.status == http::reply::status_type::bad_request) {
                    // Task has already been unregistered.
                    continue;
                }
                throw;
            }
        }
    }
    tasks_print_trees(res);
}

void tasks_ttl_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.contains("set")) {
        auto res = client.get("/task_manager/ttl");
        fmt::print("Current ttl: {}\n", res);
        return;
    }
    auto new_ttl = vm["set"].as<uint32_t>();
    std::unordered_map<sstring, sstring> params = {{ "ttl", fmt::to_string(new_ttl) }};

    auto res = client.post("/task_manager/ttl", std::move(params));
}

enum class tasks_exit_code {
    ok = 0,
    failed = 123,
    timeout = 124,
    request_error = 125
};

void tasks_wait_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (!vm.count("id")) {
        throw std::invalid_argument("required parameter is missing: id");
    }
    std::unordered_map<sstring, sstring> params;
    auto id = vm["id"].as<sstring>();
    bool quiet = vm.count("quiet");
    if (vm.count("timeout")) {
        params["timeout"] = fmt::format("{}", vm["timeout"].as<uint32_t>());
    }

    std::exception_ptr ex = nullptr;
    tasks_exit_code exit_code = tasks_exit_code::ok;
    try {
        auto res = client.get(format("/task_manager/wait_task/{}", id), params);
        if (!quiet) {
            tasks_print_status(res);
            return;
        }
        exit_code = res.GetObject()["state"] == "failed" ? tasks_exit_code::failed : tasks_exit_code::ok;
    } catch (const api_request_failed& e) {
        if (e.status == http::reply::status_type::request_timeout) {
            if (!quiet) {
                fmt::print("Operation timed out");
                return;
            }
            exit_code = tasks_exit_code::timeout;
        } else {
            exit_code = tasks_exit_code::request_error;
            ex = std::current_exception();
        }
    } catch (...) {
        exit_code = tasks_exit_code::request_error;
        ex = std::current_exception();
    }

    if (quiet) {
        if (exit_code == tasks_exit_code::ok) {
            return;
        }
        throw operation_failed_with_status(int(exit_code));
    } else if (ex) {
        std::rethrow_exception(ex);
    }
}

void toppartitions_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    // sanity check the arguments
    sstring table_filters;
    int duration_in_milli = 0;
    const auto positional_args = {"keyspace", "table", "duration-milli"};
    if (std::ranges::all_of(positional_args,
                            [&] (auto& arg) { return vm.contains(arg); })) {
        table_filters = seastar::format("{}:{}",
                                        vm["keyspace"].as<sstring>(),
                                        vm["table"].as<sstring>());
        duration_in_milli = vm["duration-milli"].as<int>();
    } else if (std::ranges::none_of(positional_args,
                                    [&] (auto& arg) { return vm.contains(arg); })) {
        table_filters = vm["cf-filters"].as<sstring>();
        duration_in_milli = vm["duration"].as<int>();
    } else {
        // either use positional args, or none of them. but not some of them.
        throw std::invalid_argument("toppartitions requires either a keyspace, column family name and duration or no arguments at all");
    }

    auto list_size = vm["size"].as<int>();
    auto capacity = vm["capacity"].as<int>();
    if (list_size >= capacity) {
        throw std::invalid_argument("TopK count (-k) option must be smaller than the summary capacity (-s)");
    }
    std::vector<std::string> operations;
    {
        // the API of /storage_service/toppartition/ does not use samplers, but
        // we need to check and normalize them
        std::vector<sstring> samplers;
        boost::split(samplers, vm["samplers"].as<sstring>(), boost::algorithm::is_any_of(","));
        for (auto& sampler : samplers) {
            boost::to_lower(sampler);
            if (sampler != "reads" && sampler != "writes") {
                throw std::invalid_argument(
                    fmt::format("{} is not a valid sampler, choose one of: READS, WRITES", sampler));
            }
            // remove the trailing "s"
            sampler.resize(sampler.size() - 1);
            operations.push_back(sampler);
        }
    }
    // prepare the query params
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
    auto res = client.get("/storage_service/toppartitions/", std::move(params));
    const auto& toppartitions = res.GetObject();
    struct record {
        std::string_view partition;
        int64_t count;
        int64_t error;
    };
    // format the query result
    bool first = true;
    for (const auto& operation : operations) {
        // add a separator
        if (!std::exchange(first, false)) {
            fmt::print("\n");
        }
        auto cardinality = toppartitions[fmt::format("{}_cardinality", operation)].GetInt64();
        fmt::print("{}S Sampler:\n", boost::to_upper_copy(operation));
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

    if (!vm.contains("include-all-sstables")) {
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
    typed_option<uint16_t>("rest-api-port", "the port of the REST API of the ScyllaDB node; takes precedence over --port|-p"),
    typed_option<sstring>("password", "Remote jmx agent password (unused)"),
    typed_option<sstring>("password-file", "Path to the JMX password file (unused)"),
    typed_option<sstring>("username,u", "Remote jmx agent username (unused)"),
    typed_option<>("print-port", "Operate in 4.0 mode with hosts disambiguated by port number (unused)"),
};

const std::map<std::string_view, std::string_view> option_substitutions{
    {"-h", "--host"},
    {"-Dcom.scylladb.apiPort", "--rest-api-port"},
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
    {"-full", "--full"},
    {"-local", "--in-local-dc"},
    {"-pl", "--pull"},
    {"-pr", "--partitioner-range"},
    {"-hosts", "--in-hosts"},
    {"-hf", "--hex-format"},
};

const std::map<operation, operation_action>& get_operations_with_func() {

    const static std::map<operation, operation_action> operations_with_func {
        {
            {
                "backup",
                "copy SSTables from a specified keyspace's snapshot to a designated bucket in object storage",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/backup.html")),
                {
                    typed_option<sstring>("keyspace", "Name of a keyspace to copy SSTables from"),
                    typed_option<sstring>("snapshot", "Name of a snapshot to copy SSTables from"),
                    typed_option<sstring>("endpoint", "ID of the configured object storage endpoint to copy SSTables to"),
                    typed_option<sstring>("bucket", "Name of the bucket to backup SSTables to"),
                    typed_option<>("nowait", "Don't wait on the backup process"),
                },
            },
            backup_operation
        },
        {
            {
                "checkAndRepairCdcStreams",
                "Checks that CDC streams reflect current cluster topology and regenerates them if not",
fmt::format(R"(
Warning: DO NOT use this while performing other administrative tasks, like
bootstrapping or decommissioning a node.

For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/checkandrepaircdcstreams.html")),
            },
            {
                checkandrepaircdcstreams_operation
            }
        },
        {
            {
                "cleanup",
                "Triggers removal of data that the node no longer owns",
fmt::format(R"(
You should run nodetool cleanup whenever you scale-out (expand) your cluster, and
new nodes are added to the same DC. The scale out process causes the token ring
to get re-distributed. As a result, some of the nodes will have replicas for
tokens that they are no longer responsible for (taking up disk space). This data
continues to consume diskspace until you run nodetool cleanup. The cleanup
operation deletes these replicas and frees up disk space.

For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/cleanup.html")),
                {
                    typed_option<int64_t>("jobs,j", "The number of compaction jobs to be used for the cleanup (unused)"),
                },
                {
                    typed_option<std::vector<sstring>>("cleanup_arg", "[<keyspace> <tables>...]", -1),
                }
            },
            {
                cleanup_operation
            }
        },
        {
            {
                "clearsnapshot",
                "Remove snapshots",
fmt::format(R"(
By default all snapshots are removed for all keyspaces.

For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/clearsnapshot.html")),
                {
                    typed_option<sstring>("tag,t", "The snapshot to remove"),
                },
                {
                    typed_option<std::vector<sstring>>("keyspaces", "[<keyspaces>...]", -1),
                }
            },
            {
                clearsnapshot_operation
            }
        },
        {
            {
                "compact",
                "Force a (major) compaction on one or more tables",
fmt::format(R"(
Forces a (major) compaction on one or more tables. Compaction is an optimization
that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the keyspaces and tables. Major
compactions will take all the SSTables for a column family and merge them into a
single SSTable per shard. If a keyspace is provided, the compaction will run on
all of the tables within that keyspace. If one or more tables are provided as
command-line arguments, the compaction will run on these tables.

For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/compact.html")),
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
            {
                compact_operation
            }
        },
        {
            {
                "compactionhistory",
                "Provides the history of compaction operations",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/compactionhistory.html")),
                {
                    typed_option<sstring>("format,F", "text", "Output format, one of: (json, yaml or text); defaults to text"),
                },
            },
            {
                compactionhistory_operation
            }
        },
        {
            {
                "compactionstats",
                "Print statistics on compactions",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/compactionstats.html")),
                {
                    typed_option<bool>("human-readable,H", false, "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"),
                },
            },
            {
                compactionstats_operation
            }
        },
        {
            {
                "decommission",
                "Deactivate a selected node by streaming its data to the next node in the ring",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/decommission.html")),
            },
            {
                decommission_operation
            }
        },
        {
            {
                "describering",
                "Shows the partition ranges for the given keyspace or table",
fmt::format(R"(
For vnode (legacy) keyspaces, describering describes all tables in the keyspace.
For tablet keyspaces, describering needs the table to describe.

For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/describering.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to describe the ring for", 1),
                    typed_option<std::vector<sstring>>("table", "The table to describe the ring for (for tablet keyspaces)", -1),
                },
            },
            {
                describering_operation
            }
        },
        {
            {
                "describecluster",
                "Print the name, snitch, partitioner and schema version of a cluster",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/describecluster.html")),
            },
            {
                describecluster_operation
            }
        },
        {
            {
                "disableautocompaction",
                "Disables automatic compaction for the given keyspace and table(s)",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/disableautocompaction.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to disable automatic compaction for", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to disable automatic compaction for", -1),
                }
            },
            {
                disableautocompaction_operation
            }
        },
        {
            {
                "disablebackup",
                "Disables incremental backup",
fmt::format(R"(
For more information, see: {}
)", doc_link("operating-scylla/nodetool-commands/disablebackup.html")),
            },
            {
                disablebackup_operation
            }
        },
        {
            {
                "disablebinary",
                "Disable the CQL native protocol",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/disablebinary.html")),
            },
            {
                disablebinary_operation
            }
        },
        {
            {
                "disablegossip",
                "Disable the gossip protocol",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/disablegossip.html")),
            },
            {
                disablegossip_operation
            }
        },
        {
            {
                "drain",
                "Drain the node (stop accepting writes and flush all tables)",
fmt::format(R"(
Flushes all memtables from a node to the SSTables that are on the disk. Scylla
stops listening for connections from the client and other nodes. You need to
restart Scylla after running this command. This command is usually executed
before upgrading a node to a new version or before any maintenance action is
performed. When you want to simply flush memtables to disk, use the nodetool
flush command.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/drain.html")),
            },
            {
                drain_operation
            }
        },
        {
            {
                "enableautocompaction",
                "Enables automatic compaction for the given keyspace and table(s)",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/enableautocompaction.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to enable automatic compaction for", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to enable automatic compaction for", -1),
                }
            },
            {
                enableautocompaction_operation
            }
        },
        {
            {
                "enablebackup",
                "Enables incremental backup",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/enablebackup.html")),
            },
            {
                enablebackup_operation
            }
        },
        {
            {
                "enablebinary",
                "Enables the CQL native protocol",
fmt::format(R"(
The native protocol is enabled by default.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/enablebinary.html")),
            },
            {
                enablebinary_operation
            }
        },
        {
            {
                "enablegossip",
                "Enables the gossip protocol",
fmt::format(R"(
The gossip protocol is enabled by default.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/enablegossip.html")),
            },
            {
                enablegossip_operation
            }
        },
        {
            {
                "flush",
                "Flush one or more tables",
fmt::format(R"(
Flush memtables to on-disk SSTables in the specified keyspace and table(s).
If no keyspace is specified, all keyspaces are flushed.
If no table(s) are specified, all tables in the specified keyspace are flushed.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/flush.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to flush", 1),
                    typed_option<std::vector<sstring>>("table", "The table(s) to flush", -1),
                }
            },
            {
                flush_operation
            }
        },
        {
            {
                "getendpoints",
                "Print the end points that owns the key",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/getendpoints.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace to query", 1),
                    typed_option<sstring>("table", "The table to query", 1),
                    typed_option<sstring>("key", "The partition key for which we need to find the endpoint", 1),
                },
            },
            {
                getendpoints_operation
            }
        },
        {
            {
                "getlogginglevels",
                "Get the runtime logging levels",
R"(
Prints a table with the name and current logging level for each logger in ScyllaDB.
)",
            },
            {
                getlogginglevels_operation
            }
        },
        {
            {
                "getsstables",
                "Get the sstables that contain the given key",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/getsstables.html")),
                {
                    typed_option<>("hex-format", "The key is given in hex dump format"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to query", 1),
                    typed_option<sstring>("table", "The table to query", 1),
                    typed_option<sstring>("key", "The partition key for which we need to find the sstables", 1),
                },
            },
            {
                getsstables_operation
            }
        },
        {
            {
                "gettraceprobability",
                "Displays the current trace probability value",
fmt::format(R"(
This value is the probability for tracing a request. To change this value see settraceprobability.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/gettraceprobability.html")),
            },
            {
                gettraceprobability_operation
            }
        },
        {
            {
                "gossipinfo",
                "Shows the gossip information for the cluster",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/gossipinfo.html")),
            },
            {
                gossipinfo_operation
            }
        },
        {
            {
                "help",
                "Displays the list of all available nodetool commands",
                "",
                { },
                {
                    typed_option<std::vector<sstring>>("command", "The command to get more information about", -1),
                },
            },
            {
                [] (scylla_rest_client&, const bpo::variables_map&) {}
            }
        },
        {
            {
                "info",
                "Print node information (uptime, load, ...)",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/info.html")),
                {
                    typed_option<>("tokens,T", "Display all tokens"),
                },
            },
            {
                info_operation
            }
        },
        {
            {
                "listsnapshots",
                "Lists all the snapshots along with the size on disk and true size",
fmt::format(R"(
Dropped tables (column family) will not be part of the listsnapshots.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/listsnapshots.html")),
                { },
                { },
            },
            {
                listsnapshots_operation
            }
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
            {
                move_operation
            }
        },
        {
            {
                "netstats",
                "Print network information on provided host (connecting node by default)",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/netstats.html")),
                {
                    typed_option<>("human-readable,H", "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"),
                },
                { },
            },
            {
                netstats_operation
            }
        },
        {
            {
                "proxyhistograms",
                "Print statistic histograms for network operations",
fmt::format(R"(
Provide the latency request that is recorded by the coordinator.
This command is helpful if you encounter slow node operations.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/proxyhistograms.html")),
                { },
                {
                    typed_option<std::vector<sstring>>("table", "<keyspace> <table>, <keyspace>.<table> or <keyspace>-<table>", 2),
                }
            },
            {
                proxyhistograms_operation
            }
        },
        {
            {
                "rebuild",
                "Rebuilds a node’s data by streaming data from other nodes in the cluster (similarly to bootstrap)",
fmt::format(R"(
Rebuild operates on multiple nodes in a Scylla cluster. It streams data from a
single source replica when rebuilding a token range. When executing the command,
Scylla first figures out which ranges the local node (the one we want to rebuild)
is responsible for. Then which node in the cluster contains the same ranges.
Finally, Scylla streams the data to the local node.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/rebuild.html")),
                {
                    typed_option<>("force", "Enforce the source_dc option, even if it unsafe to use for rebuild"),
                },
                {
                    typed_option<sstring>("source-dc", "DC from which to stream data (default: any DC)", 1),
                },
            },
            {
                rebuild_operation
            }
        },
        {
            {
                "refresh",
                "Load newly placed SSTables to the system without a restart",
fmt::format(R"(
Add the files to the upload directory, by default it is located under
/var/lib/scylla/data/keyspace_name/table_name-UUID/upload.
Materialized Views (MV) and Secondary Indexes (SI) of the upload table, and if
they exist, they are automatically updated. Uploading MV or SI SSTables is not
required and will fail.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/refresh.html")),
                {
                    typed_option<>("load-and-stream", "Allows loading sstables that do not belong to this node, in which case they are automatically streamed to the owning nodes"),
                    typed_option<>("primary-replica-only", "Load the sstables and stream to primary replica node that owns the data. Repair is needed after the load and stream process"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace to load sstable(s) into", 1),
                    typed_option<sstring>("table", "The table to load sstable(s) into", 1),
                },
            },
            {
                refresh_operation
            }
        },
        {
            {
                "removenode",
                "Remove a node from the cluster when the status of the node is Down Normal (DN) and all attempts to restore the node have failed",
fmt::format(R"(
Provide the Host ID of the node to specify which node you want to remove.

Important: use this command *only* on nodes that are not reachable by other nodes
by any means!

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/removenode.html")),
                {
                    typed_option<sstring>("ignore-dead-nodes", "Comma-separated list of dead nodes to ignore during removenode"),
                },
                {
                    typed_option<sstring>("remove-operation", "status|force|$HOST_ID - show status of current node removal, force completion of pending removal, or remove provided ID", 1),
                },
            },
            {
                removenode_operation
            }
        },
        {
            {
                "repair",
                "Synchronize data between nodes in the background",
fmt::format(R"(
When running nodetool repair on a single node, it acts as the repair master.
Only the data contained in the master node and its replications will be
repaired. Typically, this subset of data is replicated on many nodes in
the cluster, often all, and the repair process syncs between all the
replicas until the master data subset is in-sync.

To repair all of the data in the cluster, you need to run a repair on
all of the nodes in the cluster, or let ScyllaDB Manager do it for you.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/repair.html")),
                {
                    typed_option<>("dc-parallel", "Repair datacenters in parallel"),
                    typed_option<uint64_t>("end-token", "Repair data up to this token"),
                    typed_option<>("full", "Issue a full repair, as opposed to an incremental one; note that repairs on ScyllaDB are always full"),
                    typed_option<std::vector<sstring>>("in-dc", "Constrain repair to specific datacenter(s)"),
                    typed_option<>("in-local-dc", "Constrain repair to the local datacenter only"),
                    typed_option<std::vector<sstring>>("in-hosts", "Constrain repair to the specific host(s)"),
                    typed_option<>("ignore-unreplicated-keyspaces", "Ignore keyspaces which are not replicated, without this repair will fail on such keyspaces"),
                    typed_option<unsigned>("job-threads,j", "Number of threads to run repair on"),
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
            {
                repair_operation
            }
        },
        {
            {
                "resetlocalschema",
                "Reset node's local schema and resync",
R"(
For more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/resetlocalschema.html
)",
                { },
                { },
            },
            {
                resetlocalschema_operation
            }
        },
        {
            {
                "restore",
                "Copy SSTables from a designated bucket in object store to a specified keyspace or table",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/restore.html")),
                {
                    typed_option<sstring>("endpoint", "ID of the configured object storage endpoint to copy SSTables from"),
                    typed_option<sstring>("bucket", "Name of the bucket to copy SSTables from"),
                    typed_option<sstring>("snapshot", "Name of a snapshot to copy sstables from"),
                    typed_option<sstring>("keyspace", "Name of a keyspace to copy SSTables to"),
                    typed_option<sstring>("table", "Name of a table to copy SSTables to"),
                    typed_option<>("nowait", "Don't wait on the restore process"),
                },

            },
            restore_operation
        },
        {
            {
                "ring",
                "Print information about the token ring",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/ring.html")),
                {
                    typed_option<>("resolve-ip,r", "Show node domain names instead of IPs")
                },
                {
                    typed_option<sstring>("keyspace", "Specify a keyspace for accurate ownership information (topology awareness)", 1),
                    typed_option<sstring>("table", "The table to print the ring for (needed for tablet keyspaces)", 1),
                },
            },
            {
                ring_operation
            }
        },
        {
            {
                "scrub",
                "Scrub the SSTable files in the specified keyspace or table(s)",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/scrub.html")),
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
            {
                scrub_operation
            }
        },
        {
            {
                "setlogginglevel",
                "Sets the level log threshold for a given logger during runtime",
fmt::format(R"(
Resetting the log level of one or all loggers is not supported yet.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/setlogginglevel.html")),
                { },
                {
                    typed_option<sstring>("logger", "The logger to set the log level for, if unspecified, all loggers are reset to the default level", 1),
                    typed_option<sstring>("level", "The log level to set, one of (trace, debug, info, warn and error), if unspecified, default level is reset to default log level", 1),
                },
            },
            {
                setlogginglevel_operation
            }
        },
        {
            {
                "settraceprobability",
                "Sets the probability for tracing a request",
fmt::format(R"(
Value is trace probability between 0 and 1. 0 the trace will never happen and 1
the trace will always happen. Anything in between is a percentage of the time,
converted into a decimal. For example, 60% would be 0.6.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/settraceprobability.html")),
                { },
                {
                    typed_option<double>("trace_probability", "trace probability value, must between 0 and 1, e.g. 0.2", 1),
                },
            },
            {
                settraceprobability_operation
            }
        },
        {
            {
                "snapshot",
                "Take a snapshot of specified keyspaces or a snapshot of the specified table",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/snapshot.html")),
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
            {
                snapshot_operation
            }
        },
        {
            {
                "sstableinfo",
                "Information about sstables per keyspace/table",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/sstableinfo.html")),
                { },
                {
                    typed_option<sstring>("keyspace", "The keyspace name", 1),
                    typed_option<std::vector<sstring>>("table", "The table names (optional)", -1),
                },
            },
            {
                sstableinfo_operation,
            }
        },
        {
            {
                "status",
                "Displays cluster information for a table in a keyspace, a single keyspace or all keyspaces",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/status.html")),
                {
                    typed_option<>("resolve-ip,r", "Show node domain names instead of IPs"),
                },
                {
                    typed_option<sstring>("keyspace", "The keyspace name", 1),
                    typed_option<sstring>("table", "The table name (needed to display load information, if the keyspace uses tablets)", 1),
                },
            },
            {
                status_operation
            }
        },
        {
            {
                "statusbackup",
                "Displays the incremental backup status",
fmt::format(R"(
Results can be one of the following: `running` or `not running`.

By default, the incremental backup status is `not running`.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/statusbackup.html")),
            },
            {
                statusbackup_operation
            }
        },
        {
            {
                "statusbinary",
                "Displays the incremental backup status",
fmt::format(R"(
Provides the status of native transport - CQL (binary protocol).
In case that you don’t want to use CQL you can disable it using the disablebinary
command.
Results can be one of the following: `running` or `not running`.

By default, the native transport is `running`.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/statusbinary.html")),
            },
            {
                statusbinary_operation
            }
        },
        {
            {
                "statusgossip",
                "Displays the gossip status",
fmt::format(R"(
Provides the status of gossip.
Results can be one of the following: `running` or `not running`.

By default, the gossip protocol is `running`.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/statusgossip.html")),
            },
            {
                statusgossip_operation
            }
        },
        {
            {
                "stop",
                "Stops a compaction operation",
fmt::format(R"(
This command is usually used to stop compaction that has a negative impact on the performance of a node.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/stop.html")),
                {
                    typed_option<int>("id", "The id of the compaction operation to stop (not implemented)"),
                },
                {
                    typed_option<sstring>("compaction_type", "The type of compaction to be stopped", 1),
                },
            },
            {
                stop_operation
            }
        },
        {
            {
                "tablehistograms",
                {"cfhistograms"},
                "Provides statistics about a table",
fmt::format(R"(
Provides statistics about a table, including number of SSTables, read/write
latency, partition size and column count. cfhistograms covers all operations
since the last time you ran the nodetool cfhistograms command.

Also invokable as "cfhistograms".

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/cfhistograms.html")),
                { },
                {
                    typed_option<std::vector<sstring>>("table", "<keyspace> <table>, <keyspace>.<table> or <keyspace>-<table>", 2),
                }
            },
            {
                tablehistograms_operation
            }
        },
        {
            {
                "tablestats",
                {"cfstats"},
                "Print statistics on tables",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tablestats.html")),
                {
                    typed_option<bool>("ignore,i", false, "Ignore the list of tables and display the remaining tables"),
                    typed_option<bool>("human-readable,H", false, "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"),
                    typed_option<sstring>("format,F", "plain", "Output format (json, yaml)"),
                },
                {
                    typed_option<std::vector<sstring>>("tables", "List of tables (or keyspace) names", -1),
                },
            },
            {
                table_stats_operation
            }
        },
        {
            {
                "tasks",
                "Manages task manager tasks",
                "",
                { },
                {
                    typed_option<sstring>("command", "The uuid of a task", 1),
                },
                {
                        {
                            "abort",
                            "Aborts the task",
fmt::format(R"(
Aborts a task with given id. If the task is not abortable, appropriate message
will be printed, depending on why the abort failed.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/abort.html")),
                            { },
                            {
                                typed_option<sstring>("id", "The uuid of a task", 1),
                            },
                        },
                        {
                            "list",
                            "Gets a list of tasks in a given module",
fmt::format(R"(
Lists short stats (including id, type, kind, scope, state, sequence_number,
keyspace, table, and entity) of tasks in a specified module.

Allows to monitor tasks for extended time.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/list.html")),
                            {
                                typed_option<>("internal", "Show internal tasks"),
                                typed_option<sstring>("keyspace,ks", "The keyspace name; if specified only the tasks for this keyspace are shown"),
                                typed_option<sstring>("table,t", "The table name; if specified only the tasks for this table are shown"),
                                typed_option<int>("interval", 10, "Re-print the task list after interval seconds. Can be used to monitor the task-list for extended period of time"),
                                typed_option<int>("iterations,i", 0, "The number of times the task list will be re-printed. Use interval to control the frequency of re-printing the list"),
                            },
                            {
                                typed_option<sstring>("module", "The module to query about", 1),
                            },
                        },
                        {
                            "modules",
                            "Gets a list of modules supported by task manager",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/modules.html")),
                            { },
                            { },
                        },
                        {
                            "status",
                            "Gets a status of the task",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/status.html")),
                            { },
                            {
                                typed_option<sstring>("id", "The uuid of a task", 1),
                            },
                        },
                        {
                            "tree",
                            "Gets statuses of the task tree with a given root",
fmt::format(R"(
Lists statuses of a specified task and all its descendants in BFS order.
If id param isn't specified, trees of all non-internal tasks are listed.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/tree.html")),
                            { },
                            {
                                typed_option<sstring>("id", "The uuid of a root task", -1),
                            },
                        },
                        {
                            "ttl",
                            "Gets or sets task ttl",
fmt::format(R"(
Gets or sets the time in seconds for which tasks will be kept in task manager after
they are finished.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/ttl.html")),
                            {
                                typed_option<uint32_t>("set", "New task_ttl value", -1),
                            },
                            { },
                        },
                        {
                            "wait",
                            "Waits for the task to finish and returns its status",
fmt::format(R"(
Waits until task is finished and returns it status. If task does not finish before
timeout, the appropriate message with failure reason will be printed.

If quiet flag is set, nothing is printed. Instead the right exit code is returned:
- 0, if the task finished successfully;
- 123, if the task failed;
- 124, if the operation timed out;
- 125, if there was an error.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/tasks/wait.html")),
                            {
                                typed_option<>("quiet,q", "If set, status isn't printed"),
                                typed_option<uint32_t>("timeout", "Timeout in seconds"),
                            },
                            {
                                typed_option<sstring>("id", "The uuid of a root task", 1),
                            },
                        }
                }
            },
            {
                {
                    {
                        "abort", { tasks_abort_operation }
                    },
                    {
                        "list", { tasks_list_operation }
                    },
                    {
                        "modules", { tasks_modules_operation }
                    },
                    {
                        "status", { tasks_status_operation }
                    },
                    {
                        "tree", { tasks_tree_operation }
                    },
                    {
                        "ttl", { tasks_ttl_operation }
                    },
                    {
                        "wait", { tasks_wait_operation }
                    },
                }
            }
        },
        {
            {
                "toppartitions",
                "Sample and print the most active partitions for a given column family",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/toppartitions.html")),
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
            {
                toppartitions_operation
            }
        },
        {
            {
                "upgradesstables",
                "Upgrades sstables to the latest available sstable version and applies the current options",
fmt::format(R"(
Run this command if you want to force-apply changes that are relevant to sstables, like changing the compression settings, or enabling EAR (enterprise only).
Can also be used to upgrade all sstables to the latest sstable version.

Note that this command is not needed for changes described above to take effect. They take effect gradually as new sstables are written and old ones are compacted.
This command should be used when it is desired that such changes take effect right away.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/upgradesstables.html")),
                {
                    typed_option<>("include-all-sstables,a", "Include all sstables, even those already on the current version"),
                    typed_option<unsigned>("jobs,j", "Number of sstables to upgrade simultanously (unused)"),
                },
                {
                    typed_option<sstring>("keyspace", "Keyspace to upgrade sstables for, if missing, all sstables are upgraded", 1),
                    typed_option<std::vector<sstring>>("table", "Table to upgrade sstables for, if missing, all tables in the keyspace are upgraded", -1),
                },
            },
            {
                upgradesstables_operation
            }
        },
        {
            {
                "viewbuildstatus",
                "Show progress of a materialized view build",
fmt::format(R"(
For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/viewbuildstatus.html")),
                {},
                {
                    typed_option<std::vector<sstring>>("keyspace_view", "<keyspace> <view> | <keyspace.view>, The keyspace and view name ", -1),
                },
            },
            {
                viewbuildstatus_operation
            }
        },
        {
            {
                "version",
                "Displays the Apache Cassandra version which your version of Scylla is most compatible with",
fmt::format(R"(
Displays the Apache Cassandra version which your version of Scylla is most
compatible with, not your current Scylla version. To display the Scylla version,
run `scylla --version`.

For more information, see: {}"
)", doc_link("operating-scylla/nodetool-commands/version.html")),
            },
            {
                version_operation
            }
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

        // Java JVM options, they look like -Dkey=value, or just -Dkey
        // We leave -Dcom.scylladb.apiPort, it will be substituted to --rest-api-port
        if (argv[i][1] == 'D' && !arg.starts_with("-Dcom.scylladb.apiPort=")) {
            continue;
        }

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

operation_func get_operation_function(const operation& op) noexcept {
    auto operations_with_func = get_operations_with_func();
    auto name = op.fullname();

    // Find main operation's action.
    auto it = std::find_if(operations_with_func.begin(), operations_with_func.end(), [&op_name = name.back()] (const auto& el) {
        return el.first.name() == op_name;
    });
    assert(it != operations_with_func.end());
    auto& action = it->second;
    name.pop_back();

    // Check suboperations.
    for (auto n : name | boost::adaptors::reversed) {
        action = action.suboperation_funcs.at(n);
    }

    return action.func.value();
}

} // anonymous namespace

#if FMT_VERSION == 100000
template <>
struct fmt::formatter<char*> : fmt::formatter<const char*> {};
#endif

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

For more information, see: {})";

    const auto operations = boost::copy_range<std::vector<operation>>(get_operations_with_func() | boost::adaptors::map_keys);
    tool_app_template::config app_cfg{
            .name = app_name,
            .description = format(description_template, app_name, nlog.name(), boost::algorithm::join(operations | boost::adaptors::transformed([] (const auto& op) {
                return format("* {}: {}", op.name(), op.summary());
            }), "\n"), doc_link("operating-scylla/nodetool.html")),
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
                uint16_t port{};
                if (app_config.count("rest-api-port")) {
                    port = app_config["rest-api-port"].as<uint16_t>();
                } else {
                    port = app_config["port"].as<uint16_t>();
                }
                scylla_rest_client client(app_config["host"].as<sstring>(), port);
                get_operation_function(operation)(client, app_config);
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
