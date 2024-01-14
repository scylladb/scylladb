/*
 * Copyright 2016-present ScyllaDB
 **/

/* SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/disk-error-handler.hh"
#include "utils/exceptions.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

thread_local io_error_handler commit_error_handler = default_io_error_handler(commit_error);
thread_local io_error_handler general_disk_error_handler = default_io_error_handler(general_disk_error);
thread_local io_error_handler sstable_write_error_handler = default_io_error_handler(sstable_write_error);

io_error_handler default_io_error_handler(disk_error_signal_type& signal) {
    return [&signal] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch(std::system_error& e) {
            if (should_stop_on_system_error(e)) {
                signal();
                throw storage_io_error(e);
            }
        }
    };
}

io_error_handler_gen default_io_error_handler_gen() {
    return [] (disk_error_signal_type& signal) {
        return default_io_error_handler(signal);
    };
}
