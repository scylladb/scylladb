/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <fmt/chrono.h>
#include <seastar/core/thread.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>
#include <yaml-cpp/yaml.h>

#include "db_clock.hh"
#include "log.hh"
#include "tools/utils.hh"
#include "utils/http.hh"
#include "utils/rjson.hh"
#include "utils/UUID.hh"

namespace bpo = boost::program_options;

using namespace tools::utils;

namespace {

const auto app_name = "scylla-nodetool";

logging::logger nlog(app_name);

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

        sstring res;

        try {
            _api_client.make_request(std::move(req), seastar::coroutine::lambda([&] (const http::reply&, input_stream<char> body) -> future<> {
                res = co_await util::read_entire_stream_contiguous(body);
            })).get();
        } catch (httpd::unexpected_status_error& e) {
            throw std::runtime_error(fmt::format("error executing {} request to {} with parameters {}: remote replied with {}", type, url, params,
                        e.status()));
        }

        if (res.empty()) {
            return rjson::null_value();
        } else {
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

using operation_func = void(*)(scylla_rest_client&, const bpo::variables_map&);

std::map<operation, operation_func> get_operations_with_func();

void compact_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.count("user-defined")) {
        throw std::invalid_argument("--user-defined flag is unsupported");
    }

    auto keyspaces_json = client.get("/storage_service/keyspaces", {});
    std::vector<sstring> all_keyspaces;
    for (const auto& keyspace_json : keyspaces_json.GetArray()) {
        all_keyspaces.emplace_back(rjson::to_string_view(keyspace_json));
    }

    if (vm.count("compaction_arg")) {
        auto args = vm["compaction_arg"].as<std::vector<sstring>>();
        std::unordered_map<sstring, sstring> params;
        const auto keyspace = args[0];
        if (std::ranges::find(all_keyspaces, keyspace) == all_keyspaces.end()) {
            throw std::invalid_argument(fmt::format("keyspace {} does not exist", keyspace));
        }

        if (args.size() > 1) {
            params["cf"] = fmt::to_string(fmt::join(args.begin() + 1, args.end(), ","));
        }
        client.post(format("/storage_service/keyspace_compaction/{}", keyspace), std::move(params));
    } else {
        for (const auto& keyspace : all_keyspaces) {
            client.post(format("/storage_service/keyspace_compaction/{}", keyspace));
        }
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

void disablebackup_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.post("/storage_service/incremental_backups", {{"value", "false"}});
}

void disablebinary_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.del("/storage_service/native_transport");
}

void disablegossip_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    client.del("/storage_service/gossiping");
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

void gettraceprobability_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    auto res = client.get("/storage_service/trace_probability");
    fmt::print(std::cout, "Current trace probability: {}\n", res.GetDouble());
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

        bpo::options_description opts_desc(fmt::format("{} options", app_name));
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
};

std::map<operation, operation_func> get_operations_with_func() {

    const static std::map<operation, operation_func> operations_with_func {
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
R"(scylla-nodetool - a command-line tool to administer local or remote ScyllaDB nodes

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
            .description = format(description_template, app_name, boost::algorithm::join(operations | boost::adaptors::transformed([] (const auto& op) {
                return format("* {}: {}", op.name(), op.summary());
            }), "\n")),
            .logger_name = app_name,
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
        } catch (...) {
            fmt::print(std::cerr, "error running operation: {}\n", std::current_exception());
            return 2;
        }

        return 0;
    });
}

} // namespace tools
