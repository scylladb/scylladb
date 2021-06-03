/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <filesystem>
#include <fstream>
// use boost::regex instead of std::regex due
// to stack overflow in debug mode
#include <boost/regex.hpp>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include "transport/messages/result_message.hh"
#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "db/config.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "cql3/cql_config.hh"
#include "cql3/type_json.hh"
#include "test/lib/exception_utils.hh"
#include "alternator/tags_extension.hh"
#include "cdc/cdc_extension.hh"
#include <json/json.h>

static std::ofstream std_cout;

//
// A helper class to serialize result set output to a formatted JSON
//
class json_visitor final : public cql_transport::messages::result_message::visitor {
    Json::Value& _root;
public:
    json_visitor(Json::Value& root)
        : _root(root)
    {
    }

    virtual void visit(const cql_transport::messages::result_message::void_message&) override {
        _root["status"] = "ok";
    }

    virtual void visit(const cql_transport::messages::result_message::set_keyspace& m) override {
        _root["status"] = "ok";
    }

    virtual void visit(const cql_transport::messages::result_message::prepared::cql& m) override {
        _root["status"] = "ok";
    }

    virtual void visit(const cql_transport::messages::result_message::prepared::thrift& m) override {
        assert(false);
    }

    virtual void visit(const cql_transport::messages::result_message::schema_change& m) override {
        _root["status"] = "ok";
    }

    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard& m) override {
        assert(false);
    }

    virtual void visit(const cql_transport::messages::result_message::rows& m) override {
        Json::Value& output_rows = _root["rows"];
        const auto input_rows = m.rs().result_set().rows();
        const auto& meta = m.rs().result_set().get_metadata().get_names();
        for (auto&& in_row: input_rows) {
            Json::Value out_row;
            for (unsigned i = 0; i < meta.size(); ++i) {
                const cql3::column_specification& col = *meta[i];
                const bytes_opt& cell = in_row[i];
                if (cell.has_value()) {
                    out_row[col.name->text()] = fmt::format("{}", to_json_string(*col.type, cell));
                }
            }
            output_rows.append(out_row);
        }
    }
};

// Prepare query_options with serial consistency
std::unique_ptr<cql3::query_options> repl_options() {
    const auto& so = cql3::query_options::specific_options::DEFAULT;
    auto qo = std::make_unique<cql3::query_options>(
            db::consistency_level::ONE,
            std::vector<cql3::raw_value>{},
            // Ensure (optional) serial consistency is always specified.
            cql3::query_options::specific_options{
                so.page_size,
                so.state,
                db::consistency_level::SERIAL,
                so.timestamp,
            }
    );
    return qo;
}

// Read-evaluate-print-loop for CQL
void repl(seastar::app_template& app) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<alternator::tags_extension>(alternator::tags_extension::NAME);
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    ext->add_schema_extension<db::paxos_grace_seconds_extension>(db::paxos_grace_seconds_extension::NAME);
    auto db_cfg = ::make_shared<db::config>(std::move(ext));
    db_cfg->enable_user_defined_functions({true}, db::config::config_source::CommandLine);
    db_cfg->experimental_features(db::experimental_features_t::all(), db::config::config_source::CommandLine);
    do_with_cql_env_thread([] (cql_test_env& e) {

        // Comments allowed by CQL - -- and //
        const boost::regex comment_re("^[[:space:]]*((--|//).*)?$");
        // A comment is not a delimiter even if ends with one
        const boost::regex delimiter_re("^(?![[:space:]]*(--|//)).*;[[:space:]]*$");

        while (std::cin) {
            std::string line;
            std::ostringstream stmt;
            if (!std::getline(std::cin, line)) {
                break;
            }
            // Handle multiline input and comments
            if (boost::regex_match(line.begin(), line.end(), comment_re)) {
                std_cout << line << std::endl;
                continue;
            }
            stmt << line << std::endl;
            while (!boost::regex_match(line.begin(), line.end(), delimiter_re)) {
                // Read the rest of input until delimiter or EOF
                if (!std::getline(std::cin, line)) {
                    break;
                }
                stmt << line << std::endl;
            }
            // Print the statement
            std_cout << stmt.str();
            Json::Value json;

            auto execute = [&json, &stmt, &e] () mutable {
                return seastar::async([&] () mutable -> std::optional<unsigned> {
                    auto qo = repl_options();
                    auto msg = e.execute_cql(stmt.str(), std::move(qo)).get0();
                    if (msg->move_to_shard()) {
                        return *msg->move_to_shard();
                    }
                    json_visitor visitor(json);
                    msg->accept(visitor);
                    return std::nullopt;
                });
            };

            try {
                auto shard = execute().get0();
                if (shard) {
                    smp::submit_to(*shard, std::move(execute)).get();
                }
            } catch (std::exception& e) {
                json["status"] = "error";
                json["message"] = fmt::format("{}", e);
            }
            std_cout << json << std::endl;
        }
    }, db_cfg).get0();
}

// Reset stdin/stdout/log streams to locations pointed
// on the command line.
void apply_configuration(const boost::program_options::variables_map& cfg) {

    if (cfg.contains("input")) {
        static std::ifstream input(cfg["input"].as<std::string>());
        std::cin.rdbuf(input.rdbuf());
    }
    static std::ofstream log(cfg["log"].as<std::string>());
    // Seastar always logs to std::cout, hack this around
    // by redirecting std::cout to a file and capturing
    // the old std::cout in std_cout
    auto save_filebuf = std::cout.rdbuf(log.rdbuf());
    if (cfg.contains("output")) {
        std_cout.open(cfg["output"].as<std::string>());
    } else  {
        std_cout.std::ios::rdbuf(save_filebuf);
    }
}

int main(int argc, char* argv[]) {

    namespace bpo = boost::program_options;
    namespace fs = std::filesystem;

    seastar::app_template::config cfg;
    cfg.name = fmt::format(R"({} - An embedded single-node version of Scylla.

Runs read-evaluate-print loop, reading commands from stdin,
evaluating them and printing output, formatted as JSON, to stdout.
Creates a temporary database in /tmp and deletes it at exit.
Pre-configures a default keyspace, naturally, with replication
factor 1.

Used in unit tests as a test driver for .test.cql files.

Available )", argv[0]);

    seastar::app_template app(cfg);

    /* Define options for input, output and log file. */
    app.add_options()
        ("input", bpo::value<std::string>(),
         "Input file with CQL, defaults to stdin")
        ("output", bpo::value<std::string>(),
         "Output file for data, defaults to stdout")
        ("log", bpo::value<std::string>()->default_value(
                    fmt::format("{}.log", fs::path(argv[0]).stem().string())),
        "Output file for Scylla log");

    return app.run(argc, argv, [&app] {

        apply_configuration(app.configuration());

        return seastar::async([&app] {
            return repl(app);
        });
    });
}

