/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptors.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/distributed.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "log.hh"
#include "schema/schema_builder.hh"
#include "tools/utils.hh"
#include "test/lib/cql_test_env.hh"

using namespace seastar;
using namespace tools::utils;

namespace bpo = boost::program_options;

namespace {

const auto app_name = "commitlog";

using operation_func = void(*)(const bpo::variables_map&);
std::map<operation, operation_func> get_operations_with_func();

distributed<replica::database> db;
static sharded<db::system_keyspace> sys_ks;

const std::vector<operation_option> global_options{
    typed_option<sstring>("path", "the commitlog file absolute path)"),
};

void parse_commit_operation(const bpo::variables_map& vm) {
    fmt::print(std::cout, "\nscylla commitlog op.\n\n");
    // std::vector<sstring> paths = vm["path"].as<sstring>().split(',');
    auto path = vm["path"].as<sstring>();
    fmt::print(std::cout, "value {}\n\n", path);
    // TODO: initializ db and sys_ks
    auto rp = db::commitlog_replayer::create_replayer(db, sys_ks).get0();
    rp.recover(std::move(path), db::commitlog::descriptor::FILENAME_PREFIX).get();
    
}

std::map<operation, operation_func> get_operations_with_func() {

    const static std::map<operation, operation_func> operations_with_func {
        {
            {
                "parse",
                "Triggers removal of data that the node no longer owns",
                R"(commitlog parse)",
            },  
            parse_commit_operation
        },
    };

    return operations_with_func;
}
}

namespace tools {

int scylla_commitlog_main(int argc, char** argv) {
    auto description_template =
R"(scylla-{} - a command-line tool to examine values belonging to scylla types.
Usage: scylla {} {{action}} [--option1] [--option2] ... {{hex_value1}} [{{hex_value2}}] ...
The supported actions are:
{}
$ scylla types {{action}} --help
)";

    const auto operations = boost::copy_range<std::vector<operation>>(get_operations_with_func() | boost::adaptors::map_keys);
    tool_app_template::config app_cfg{
        .name = app_name,
        .description = format(description_template, app_name, app_name, boost::algorithm::join(operations | boost::adaptors::transformed(
                [] (const operation& op) { return format("* {} - {}", op.name(), op.summary()); } ), "\n")),
        .operations = std::move(operations),
        .global_options = &global_options,
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& operation, const boost::program_options::variables_map& app_config) {
        try {
            // go to the specific operation
            get_operations_with_func().at(operation)(app_config);
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
