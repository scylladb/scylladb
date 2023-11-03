/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/thread.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "log.hh"
#include "tools/utils.hh"
#include "utils/http.hh"
#include "utils/rjson.hh"

namespace bpo = boost::program_options;

using namespace tools::utils;

namespace {

const auto app_name = "nodetool";

logging::logger nlog(format("scylla-{}", app_name));

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

keyspace_and_tables parse_keyspace_and_tables(scylla_rest_client& client, const bpo::variables_map& vm) {
    keyspace_and_tables ret;

    ret.keyspace = vm["keyspace"].as<sstring>();

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
        for (const auto& keyspace : get_keyspaces(client, "non_local_strategy")) {
            client.post(format("/storage_service/keyspace_cleanup/{}", keyspace));
        }
    }
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
        for (const auto& keyspace : get_keyspaces(client)) {
            client.post(format("/storage_service/keyspace_compaction/{}", keyspace), params);
        }
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

void flush_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    const auto [keyspace, tables] = parse_keyspace_and_tables(client, vm);
    std::unordered_map<sstring, sstring> params;
    if (!tables.empty()) {
        params["cf"] = fmt::to_string(fmt::join(tables.begin(), tables.end(), ","));
    }
    client.post(format("/storage_service/keyspace_flush/{}", keyspace), std::move(params));
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
                "flush",
                "Flush one or more tables",
R"(
Specify a keyspace and one or more tables that you want to flush from the
memtable to on disk SSTables.

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
        } catch (...) {
            fmt::print(std::cerr, "error running operation: {}\n", std::current_exception());
            return 2;
        }

        return 0;
    });
}

} // namespace tools
