/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include "redis/commands.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "types.hh"
#include "service_permit.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "redis/options.hh"
#include "redis/query_utils.hh"
#include "redis/mutation_utils.hh"
#include "redis/lolwut.hh"

namespace redis {

namespace commands {

shared_ptr<abstract_command> get::prepare(service::storage_proxy& proxy, request&& req) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return seastar::make_shared<get> (std::move(req._command), std::move(req._args[0]));
}

future<redis_message> get::execute(service::storage_proxy& proxy, redis::redis_options& options, service_permit permit) {
    return redis::read_strings(proxy, options, _key, permit).then([] (auto result) {
        if (result->has_result()) {
            return redis_message::make_strings_result(std::move(result->result()));
        }
        // return nil string if key does not exist
        return redis_message::nil();
    });
}

shared_ptr<abstract_command> set::prepare(service::storage_proxy& proxy, request&& req) {
    if (req.arguments_size() != 2) {
        throw wrong_arguments_exception(2, req.arguments_size(), req._command);
    }
    return seastar::make_shared<set> (std::move(req._command), std::move(req._args[0]), std::move(req._args[1]), 0);
}

future<redis_message> set::execute(service::storage_proxy& proxy, redis::redis_options& options, service_permit permit) {
    return redis::write_strings(proxy, options, std::move(_key), std::move(_data), _ttl, permit).then([] {
        return redis_message::ok();
    });
}

shared_ptr<abstract_command> del::prepare(service::storage_proxy& proxy, request&& req) {
    if (req.arguments_size() == 0) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    return seastar::make_shared<del> (std::move(req._command), std::move(req._args));
}

future<redis_message> del::execute(service::storage_proxy& proxy, redis::redis_options& options, service_permit permit) {
    //FIXME: We should return the count of the actually deleted keys.
    auto size = _keys.size();
    return redis::delete_objects(proxy, options, std::move(_keys), permit).then([size] {
       return redis_message::number(size);
    });
}

shared_ptr<abstract_command> select::prepare(service::storage_proxy& proxy, request&& req) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    long index = -1;
    try {
        index = std::stol(std::string(reinterpret_cast<const char*>(req._args[0].data()), req._args[0].size()));
    }
    catch (...) {
        throw invalid_db_index_exception();
    }
    return seastar::make_shared<select> (std::move(req._command), index);
}

future<redis_message> select::execute(service::storage_proxy&, redis::redis_options& options, service_permit) {
    if (_index < 0 || static_cast<size_t>(_index) >= options.get_total_redis_db_count()) {
        throw invalid_db_index_exception();
    }
    options.set_keyspace_name(sprint("REDIS_%zu", static_cast<size_t>(_index)));
    return redis_message::ok();
}

shared_ptr<abstract_command> unknown::prepare(service::storage_proxy& proxy, request&& req) {
    return seastar::make_shared<unknown> (std::move(req._command));
}

future<redis_message> unknown::execute(service::storage_proxy&, redis::redis_options&, service_permit) {
    return redis_message::unknown(_name);
}

shared_ptr<abstract_command> ping::prepare(service::storage_proxy& proxy, request&& req) {
    return seastar::make_shared<ping> (std::move(req._command));
}

future<redis_message> ping::execute(service::storage_proxy&, redis::redis_options&, service_permit) {
    return redis_message::pong();
}

shared_ptr<abstract_command> echo::prepare(service::storage_proxy& proxy, request&& req) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return seastar::make_shared<echo> (std::move(req._command), std::move(req._args[0]));
}

future<redis_message> echo::execute(service::storage_proxy&, redis::redis_options&, service_permit) {
    return redis_message::make_strings_result(std::move(_str));
}

shared_ptr<abstract_command> lolwut::prepare(service::storage_proxy& proxy, request&& req) {
    int cols = 66;
    int squares_per_row = 8;
    int squares_per_col = 12;
    try {
        if (req.arguments_size() >= 1) {
            cols = std::stoi(std::string(reinterpret_cast<const char*>(req._args[0].data()), req._args[0].size()));
            cols = std::clamp(cols, 1, 1000);
        }
        if (req.arguments_size() >= 2) {
            squares_per_row = std::stoi(std::string(reinterpret_cast<const char*>(req._args[1].data()), req._args[1].size()));
            squares_per_row = std::clamp(squares_per_row, 1, 200);
        }
        if (req.arguments_size() >= 3) {
            squares_per_col = std::stoi(std::string(reinterpret_cast<const char*>(req._args[2].data()), req._args[2].size()));
            squares_per_col = std::clamp(squares_per_col, 1, 200);
       }
    } catch (...) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return seastar::make_shared<lolwut> (std::move(req._command), cols, squares_per_row, squares_per_col);
}

future<redis_message> lolwut::execute(service::storage_proxy&, redis::redis_options& options, service_permit) {
    return redis::lolwut5(_cols, _squares_per_row, _squares_per_col).then([] (auto result) {
        return redis_message::make_strings_result(std::move(result));
    });
}

}

}
