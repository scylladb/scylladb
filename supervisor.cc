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

#include "supervisor.hh"
#include "log.hh"
#include <seastar/core/print.hh>
#include <csignal>
#include <cstdlib>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

extern logger startlog;

const sstring supervisor::scylla_upstart_job_str("scylla-server");
const sstring supervisor::upstart_job_env("UPSTART_JOB");
const sstring supervisor::systemd_ready_msg("READY=1");
const sstring supervisor::systemd_status_msg_prefix("STATUS");

sstring supervisor::get_upstart_job_env() {
    const char* upstart_job = std::getenv(upstart_job_env.c_str());
    return !upstart_job ? "" : upstart_job;
}

bool supervisor::try_notify_upstart(sstring msg, bool ready) {
    static const sstring upstart_job_str(get_upstart_job_env());

    if (upstart_job_str != scylla_upstart_job_str) {
        return false;
    }

    if (ready) {
        std::raise(SIGSTOP);
    }

    return true;
}

void supervisor::try_notify_systemd(sstring msg, bool ready) {
#ifdef HAVE_LIBSYSTEMD
    if (ready) {
        sd_notify(0, sprint("%s\n%s=%s\n", systemd_ready_msg, systemd_status_msg_prefix, msg).c_str());
    } else {
        sd_notify(0, sprint("%s=%s\n", systemd_status_msg_prefix, msg).c_str());
    }
#endif
}

void supervisor::notify(sstring msg, bool ready) {
    startlog.trace("{}", msg);

    if (try_notify_upstart(msg, ready) == true) {
        return;
    } else {
        try_notify_systemd(msg, ready);
    }
}
