/*
 * Copyright 2016-present ScyllaDB
 **/

/* This file is part of Scylla.
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

#include "utils/disk-error-handler.hh"

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
