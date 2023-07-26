/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/thread.hh>

#include "log.hh"
#include "tools/utils.hh"

namespace bpo = boost::program_options;

using namespace tools::utils;

namespace {

const auto app_name = "scylla-nodetool";

logging::logger nlog(app_name);

class scylla_rest_client {
};

using operation_func = void(*)(scylla_rest_client&, const bpo::variables_map&);

const std::vector<operation_option> global_options{
};

const std::map<operation, operation_func> operations_with_func{
};

} // anonymous namespace

namespace tools {

int scylla_nodetool_main(int argc, char** argv) {
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

    return app.run_async(argc, argv, [] (const operation& operation, const bpo::variables_map& app_config) {
        scylla_rest_client client;

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
