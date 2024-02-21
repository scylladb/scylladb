/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/thread.hh>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "db/config.hh"
#include "db/extensions.hh"
#include "tools/utils.hh"
#include "utils/logalloc.hh"
#include "init.hh"

namespace tools::utils {

bool operation::matches(std::string_view name) const {
    return _name == name || std::ranges::find(_aliases, name) != _aliases.end();
}

namespace {

// Extract the operation from the argv.
//
// The operation is expected to be at argv[1].If found, it is shifted out to the
// end (effectively removed) and the corresponding operation* is returned.
// If not found or unrecognized an error is logged and exit() is called.
const operation& get_selected_operation(int& ac, char**& av, const std::vector<operation>& operations, std::string_view alias) {
    if (ac < 2) {
        fmt::print(std::cerr, "error: missing mandatory {} argument\n", alias);
        exit(1);
    }

    const char* op_name = av[1];
    if (auto found = std::ranges::find_if(operations, [op_name] (auto& op) {
            return op.matches(op_name);
        });
        found != operations.end()) {
        std::shift_left(av + 1, av + ac, 1);
        --ac;
        return *found;
    }

    std::vector<std::string_view> all_operation_names;
    for (const auto& op : operations) {
        all_operation_names.push_back(op.name());
        for (const auto& alias : op.aliases()) {
            all_operation_names.push_back(alias);
        }
    }

    fmt::print(std::cerr, "error: unrecognized {} argument: expected one of ({}), got {}\n", alias, all_operation_names, op_name);
    exit(100);
}

// Configure seastar with defaults more appropriate for a tool.
// Make seastar not act as if it owns the place, taking over all system resources.
// Set ERROR as the default log level, except for the logger \p logger_name, which
// is configured with INFO level.
void configure_tool_mode(app_template::seastar_options& opts, const sstring& logger_name) {
    opts.reactor_opts.blocked_reactor_notify_ms.set_value(60000);
    opts.reactor_opts.overprovisioned.set_value();
    opts.reactor_opts.idle_poll_time_us.set_value(0);
    opts.reactor_opts.poll_aio.set_value(false);
    opts.reactor_opts.relaxed_dma.set_value();
    opts.reactor_opts.unsafe_bypass_fsync.set_value(true);
    opts.reactor_opts.kernel_page_cache.set_value(true);
    opts.reactor_opts.reactor_backend.select_candidate("epoll");
    opts.smp_opts.thread_affinity.set_value(false);
    opts.smp_opts.mbind.set_value(false);
    opts.smp_opts.smp.set_value(1);
    opts.smp_opts.lock_memory.set_value(false);
    opts.smp_opts.memory_allocator = memory_allocator::standard;
    opts.log_opts.default_log_level.set_value(log_level::error);
    if (!logger_name.empty()) {
        opts.log_opts.logger_log_level.set_value({});
        opts.log_opts.logger_log_level.get_value()[logger_name] = log_level::info;
    }
}

} // anonymous namespace

db_config_and_extensions::db_config_and_extensions() 
    : extensions(std::make_shared<db::extensions>())
    , db_cfg(std::make_unique<db::config>(extensions))
{}

db_config_and_extensions::db_config_and_extensions(db_config_and_extensions&&) = default;
db_config_and_extensions::~db_config_and_extensions() = default;

tool_app_template::tool_app_template(config cfg)
    : _cfg(std::move(cfg))
{}

int tool_app_template::run_async(int argc, char** argv, noncopyable_function<int(const operation&, const boost::program_options::variables_map&)> main_func) {
    if (argc <= 1) {
        auto program_name = argc ? std::filesystem::path(argv[0]).filename().native() : "scylla";
        fmt::print(std::cerr, "Usage: {} {} OPERATION [OPTIONS] ...\nTry `{} {} --help` for more information.\n",
                program_name, _cfg.name, program_name, _cfg.name);
        return 2;
    }

    const operation* found_op = nullptr;
    if (std::strncmp(argv[1], "--help", 6) != 0 && std::strcmp(argv[1], "-h") != 0) {
        found_op = &tools::utils::get_selected_operation(argc, argv, _cfg.operations, "operation");
    }

    app_template::seastar_options app_cfg;
    app_cfg.name = format("scylla-{}", _cfg.name);

    if (found_op) {
        app_cfg.description = format("{}\n\n{}\n", found_op->summary(), found_op->description());
    } else {
        app_cfg.description = _cfg.description;
    }

    tools::utils::configure_tool_mode(app_cfg, _cfg.logger_name);

    app_template app(std::move(app_cfg));

    if (_cfg.global_options) {
        auto& global_desc = app.get_options_description();
        for (const auto& go : *_cfg.global_options) {
            go.add_option(global_desc);
        }
    }
    if (_cfg.global_positional_options) {
        for (const auto& gpo : *_cfg.global_positional_options) {
            app.add_positional_options({gpo.to_positional_option()});
        }
    }

    if (found_op) {
        boost::program_options::options_description op_desc(found_op->name());
        for (const auto& opt : found_op->options()) {
            opt.add_option(op_desc);
        }
        for (const auto& opt : found_op->positional_options()) {
            app.add_positional_options({opt.to_positional_option()});
        }
        if (!found_op->options().empty()) {
            app.get_options_description().add(op_desc);
        }
    }

    if (_cfg.db_cfg_ext) {
        auto init = app.get_options_description().add_options();
        configurable::append_all(*_cfg.db_cfg_ext->db_cfg, init);
    }

    return app.run(argc, argv, [this, &main_func, &app, found_op] {
        return async([this, &main_func, &app, found_op] {
            configurable::notify_set ns;

            if (_cfg.db_cfg_ext) {
                ns = configurable::init_all(*_cfg.db_cfg_ext->db_cfg, *_cfg.db_cfg_ext->extensions).get();
            }

            ns.notify_all(configurable::system_state::started).get();

            logalloc::use_standard_allocator_segment_pool_backend(_cfg.lsa_segment_pool_backend_size_mb * 1024 * 1024).get();
            assert(found_op);
            auto res = main_func(*found_op, app.configuration());

            ns.notify_all(configurable::system_state::stopped).get();

            return res;
        });
    });
}

} // namespace tools::utils
