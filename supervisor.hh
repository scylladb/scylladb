/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/format.hh>
#include <seastar/util/log.hh>
#include "seastarx.hh"
#include <systemd/sd-daemon.h>

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
