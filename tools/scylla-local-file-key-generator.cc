/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <filesystem>
#include <iostream>
#include <ranges>

#include <fmt/ranges.h>
#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "schema/schema_builder.hh"
#include "tools/utils.hh"
#include "dht/i_partitioner.hh"
#include "utils/managed_bytes.hh"
#include "ent/encryption/symmetric_key.hh"
#include "ent/encryption/local_file_provider.hh"

using namespace seastar;
using namespace tools::utils;

namespace bpo = boost::program_options;

namespace std {
// required by boost::lexical_cast<std::string>(vector<string>), which is in turn used
// by boost::program_option for printing out the default value of an option
static std::ostream& operator<<(std::ostream& os, const std::vector<std::string>& v) {
    return os << fmt::format("{}", v);
}
}

namespace {

const auto app_name = "local-file-key-generator";

const std::vector<operation_option> global_options{
    typed_option<std::string>("alg,a", "AES", "Key algorithm (i.e. AES, 3DES)"),
    typed_option<std::string>("block-mode,b", "CBC", "Algorithm block mode (i.e. CBC, EBC)"),
    typed_option<std::string>("padding,p", "PKCS5", "Algorithm padding method (i.e. PKCS5)"),
    typed_option<unsigned>("length,l", 128, "Key length in bits (i.e. 128, 256)"),
};

const std::vector<operation_option> global_positional_options{
    typed_option<std::vector<std::string>>("files", "key path|key name", -1),
};

const std::vector<operation> operations = {
    {"generate", "creates a new key and stores to a new file",
R"(
Generate a key suitable for a given algorithm and key length 
and store to a file readable by scylla encryption at rest 
local file key provider.
)"},
    {"append", "same as generate, but appends key to existing file",
R"(
Generate a key suitable for a given algorithm and key length 
and append to an existing file readable by scylla encryption at rest 
local file key provider.
)"},
};

}

namespace tools {

using namespace encryption;
using namespace std::string_literals;
namespace fs = std::filesystem;

int scylla_local_file_key_generator_main(int argc, char** argv) {
    constexpr auto description_template =
R"(scylla-{} - a command-line tool to generate file-based encryption keys.

Usage: scylla {} <op> [--option1] [--option2] ... [key path|key name]

Allows creating symmetric keys for use with scylla encryption at rest 
local key file provider.

Where <op> can be one of:

{}
)";

    auto op_str = std::ranges::to<std::string>(operations | std::views::transform([] (const operation& op) { 
        return fmt::format("* {} - {}\n{}", op.name(), op.summary(), op.description()); 
    }) | std::views::join_with('\n'));
    tool_app_template::config app_cfg{
        .name = app_name,
        .description = seastar::format(description_template, app_name, app_name, op_str),
        .operations = std::move(operations),
        .global_options = &global_options,
        .global_positional_options = &global_positional_options,
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& op, const boost::program_options::variables_map& app_config) {
        std::vector<std::string> files;

        if (app_config.contains("files")) {
            files = app_config["files"].as<std::vector<std::string>>();
        }
        if (files.size() > 1) {
            throw std::invalid_argument("Too many arguments");
        }
        auto alg = app_config["alg"].as<std::string>();
        auto mode = app_config["block-mode"].as<std::string>();
        auto padd = app_config["padding"].as<std::string>();
        auto len = app_config["length"].as<unsigned>();

        if (!padd.ends_with("Padding")) {
            padd = padd + "Padding";
        }

        auto java_sig = fmt::format("{}/{}/{}", alg, mode, padd);

        key_info info {
            .alg = java_sig, .len = len
        };

        symmetric_key k(info);
        auto key = k.key();
        auto hex = base64_encode(key);
        auto line = fmt::format("{}:{}:{}", java_sig, len, hex);
        auto key_name = "system_key"s;

        if (!files.empty()) {
            fs::path f(files.front());
            if (fs::is_directory(f)) {
                f = f / key_name;
            }
            if (!fs::exists(f)) {
                auto p = f.parent_path();
                if (!p.empty()) {
                    fs::create_directories(p);
                }
            }
            std::ios_base::openmode mode = std::ios_base::out;
            if (op.name() == "append") {
                mode |= std::ios_base::ate|std::ios_base::app;
            } else {
                mode |= std::ios_base::trunc;
            }

            if (!fs::exists(f) || op.name() != "append") {
                // create once so we can enforce proper
                // permissions. (neither seastar or c++ io is great here)
                std::ofstream os(f, mode);
            }

            fs::permissions(f, fs::perms::owner_read|fs::perms::owner_write);
            std::ofstream os(f, mode);

            os << line << std::endl;
        } else {
            std::cout << line << std::endl;
        }
        return 0;
    });
}

} // namespace tools
