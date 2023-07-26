/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/thread.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "log.hh"
#include "tools/utils.hh"
#include "utils/http.hh"
#include "utils/rjson.hh"

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
        req.query_parameters = std::move(params);

        nlog.trace("Making {} request to {} with parameters {}", type, req.get_url(), req.query_parameters);

        sstring res;

        _api_client.make_request(std::move(req), seastar::coroutine::lambda([&] (const http::reply&, input_stream<char> body) -> future<> {
            res = co_await util::read_entire_stream_contiguous(body);
        })).get();

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
};

using operation_func = void(*)(scylla_rest_client&, const bpo::variables_map&);

enum class json_type {
    null, boolean, object, array, string, number
};

const rjson::value& check_json_type(const rjson::value& value, json_type type) {
    bool ok = false;
    sstring type_name = "unknown";
    switch (type) {
        case json_type::null:
            ok = value.IsNull();
            type_name = "null";
            break;
        case json_type::boolean:
            ok = value.IsBool();
            type_name = "bool";
            break;
        case json_type::object:
            ok = value.IsObject();
            type_name = "object";
            break;
        case json_type::array:
            ok = value.IsArray();
            type_name = "array";
            break;
        case json_type::string:
            ok = value.IsString();
            type_name = "string";
            break;
        case json_type::number:
            ok = value.IsNumber();
            type_name = "number";
            break;
        default:
            throw std::runtime_error(fmt::format("check_json_type(): unknown type: {}", static_cast<int>(type)));
    }
    if (!ok) {
        throw std::runtime_error(fmt::format("check_json_type(): json value is not of the expected type: {}", type_name));
    }
    return value;
}

void compact_operation(scylla_rest_client& client, const bpo::variables_map& vm) {
    if (vm.count("user-defined")) {
        throw std::invalid_argument("--user-defined flag is unsupported");
    }

    auto keyspaces_json = client.get("/storage_service/keyspaces", {});
    std::vector<sstring> all_keyspaces;
    for (const auto& keyspace_json : check_json_type(keyspaces_json, json_type::array).GetArray()) {
        all_keyspaces.emplace_back(rjson::to_string_view(check_json_type(keyspace_json, json_type::string)));
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

const std::map<operation, operation_func> operations_with_func{
    {{"compact",
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
                    typed_option<sstring>("partition", "String representation of the partition key to compact (unused)"),
            },
            {
                    {"compaction_arg", bpo::value<std::vector<sstring>>(), "[<keyspace> <tables>...] or [<SStable files>...] ", -1},
            }},
            compact_operation},
};

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

    for (size_t i = 0; i < argc; ++i) {
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

    const auto description_template =
R"(scylla-nodetool - a command-line tool to administer local or remote ScyllaDB nodes

# Operations

The operation to execute is the mandatory, first positional argument.
Operations write their output to stdout. Logs are written to stderr,
with a logger called {}.

Supported Nodetool operations:
{}

For more information, see: https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.html
)";

    const auto operations = boost::copy_range<std::vector<operation>>(operations_with_func | boost::adaptors::map_keys);
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

    return app.run_async(replacement_argv.size(), replacement_argv.data(), [] (const operation& operation, const bpo::variables_map& app_config) {
        scylla_rest_client client(app_config["host"].as<sstring>(), app_config["port"].as<uint16_t>());

        try {
            operations_with_func.at(operation)(client, app_config);
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
