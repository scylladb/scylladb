/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "tools/utils.hh"

namespace tools::utils {

void configure_tool_mode(app_template::seastar_options& opts, const sstring& logger_name) {
    opts.reactor_opts.blocked_reactor_notify_ms.set_value(60000);
    opts.reactor_opts.overprovisioned.set_value();
    opts.reactor_opts.idle_poll_time_us.set_value(0);
    opts.reactor_opts.poll_aio.set_value(false);
    opts.reactor_opts.relaxed_dma.set_value();
    opts.reactor_opts.unsafe_bypass_fsync.set_value(true);
    opts.reactor_opts.kernel_page_cache.set_value(true);
    opts.smp_opts.thread_affinity.set_value(false);
    opts.smp_opts.mbind.set_value(false);
    opts.smp_opts.smp.set_value(1);
    opts.smp_opts.lock_memory.set_value(false);
    opts.log_opts.default_log_level.set_value(log_level::warn);
    if (!logger_name.empty()) {
        opts.log_opts.logger_log_level.set_value({});
        opts.log_opts.logger_log_level.get_value()[logger_name] = log_level::info;
    }
}

} // namespace tools::utils
