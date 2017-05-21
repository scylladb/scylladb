/*
 * Copyright (C) 2017 ScyllaDB
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
#include "seastarx.hh"

class supervisor {
public:
    static const sstring scylla_upstart_job_str;
    static const sstring upstart_job_env;
    static const sstring systemd_ready_msg;
    /** A systemd status message has a format <status message prefix>=<message> */
    static const sstring systemd_status_msg_prefix;
public:
    /**
     * @brief Notify the Supervisor with the given message.
     * @param msg message to notify the Supervisor with
     * @param ready set to TRUE when scylla service becomes ready
     */
    static void notify(sstring msg, bool ready = false);

private:
    static void try_notify_systemd(sstring msg, bool ready);
    static bool try_notify_upstart(sstring msg, bool ready);
    static sstring get_upstart_job_env();
};
