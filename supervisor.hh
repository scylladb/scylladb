/*
 * Copyright (C) 2017-present ScyllaDB
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

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include "seastarx.hh"
#include <systemd/sd-daemon.h>
#include "log.hh"

extern logger startlog;

class supervisor {
public:
    static constexpr auto systemd_ready_msg = "READY=1";
    /** A systemd status message has a format <status message prefix>=<message> */
    static constexpr auto systemd_status_msg_prefix = "STATUS";
public:
    /**
     * @brief Notify the Supervisor with the given message.
     * @param msg message to notify the Supervisor with
     * @param ready set to TRUE when scylla service becomes ready
     */
    static inline void notify(sstring msg, bool ready = false) {
        startlog.info("{}", msg);
        try_notify_systemd(msg, ready);
    }

private:
    static inline void try_notify_systemd(sstring msg, bool ready) {
        if (ready) {
            sd_notify(0, format("{}\n{}={}\n", systemd_ready_msg, systemd_status_msg_prefix, msg).c_str());
        } else {
            sd_notify(0, format("{}={}\n", systemd_status_msg_prefix, msg).c_str());
        }
    }
};
