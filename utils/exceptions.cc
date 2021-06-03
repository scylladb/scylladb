/*
 * Copyright 2015-present ScyllaDB
 */

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

#include <seastar/core/print.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/log.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/core/abort_on_ebadf.hh>

#include <exception>
#include <system_error>
#include <atomic>
#include "exceptions.hh"

#include <iostream>

bool check_exception(system_error_lambda_t f)
{
    auto e = std::current_exception();
    if (!e) {
        return false;
    }

    try {
        std::rethrow_exception(e);
    } catch (std::system_error &e) {
        return f(e);
    } catch (...) {
        return false;
    }
    return false;
}

bool is_system_error_errno(int err_no)
{
    return check_exception([err_no] (const std::system_error &e) {
        auto code = e.code();
        return code.value() == err_no &&
               code.category() == std::system_category();
    });
}

bool should_stop_on_system_error(const std::system_error& e) {
    if (e.code().category() == std::system_category()) {
	// Whitelist of errors that don't require us to stop the server:
	switch (e.code().value()) {
        case EEXIST:
        case ENOENT:
            return false;
        default:
            break;
        }
    }
    return true;
}

bool is_timeout_exception(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (seastar::rpc::timeout_error& unused) {
        return true;
    } catch (seastar::semaphore_timed_out& unused) {
        return true;
    } catch (...) {
    }
    return false;
}
