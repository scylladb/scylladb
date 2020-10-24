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
#include "service/client_state.hh"
#include "redis/options.hh"
#include "redis/query_utils.hh"
#include "redis/mutation_utils.hh"
#include "redis/lolwut.hh"
#include "redis/keyspace_utils.hh"

namespace redis {

namespace commands {

future<redis_message> get(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return redis::read_strings(proxy, options, req._args[0], permit).then([] (auto result) {
        if (result->has_result()) {
            return redis_message::make_strings_result(std::move(result->result()));
        }
        // return nil string if key does not exist
        return redis_message::nil();
    });
}

future<redis_message> exists(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() < 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return do_with(size_t(0), [&proxy, &options, permit, &req] (size_t& count) {
        return seastar::do_for_each(req._args, [&proxy, &options, permit, &count] (auto& key) {
            return redis::read_strings(proxy, options, key, permit).then([&count] (lw_shared_ptr<strings_result> result) {
                if (result->has_result()) {
                    count++;
                }
            });
        }).then([&count] () {
            return redis_message::number(count);
        });
    });
}

future<redis_message> ttl(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return redis::read_strings(proxy, options, req._args[0], permit).then([] (auto result) {
        if (result->has_result()) {
            if (result->has_ttl()) {
                return redis_message::number(result->ttl().count());
            }else{
                return redis_message::number(-1);
            }
        }
        return redis_message::number(-2);
    });
}

future<redis_message> strlen(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return redis::read_strings(proxy, options, req._args[0], permit).then([] (auto result) {
        if (result->has_result()) {
            return redis_message::number(result->result().length());
        }
        // return 0 string if key does not exist
        return redis_message::zero();
    });
}

future<redis_message> hgetall(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 1) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    return redis::read_hashes(proxy, options, req._args[0], permit).then([] (auto result) {
        if (!result->empty()) {
            return redis_message::make_list_result(*result);
        }
        // return nil string if key does not exist
        return redis_message::nil();
    });
}

future<redis_message> hget(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 2) {
        throw wrong_arguments_exception(2, req.arguments_size(), req._command);
    }
    auto field = std::move(req._args[1]);
    return redis::read_hashes(proxy, options, req._args[0], field, permit).then([field] (auto result) {
        if (!result->empty()) {
            return redis_message::make_strings_result(std::move(result->at(field)));
        }
        // return nil string if key does not exist
        return redis_message::nil();
    });
}

future<redis_message> hset(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 3) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    return redis::write_hashes(proxy, options, std::move(req._args[0]), std::move(req._args[1]), std::move(req._args[2]), 0, permit).then([] {
        return redis_message::one();
    });
}

future<redis_message> hdel(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() < 2) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    //FIXME: We should return the count of the actually deleted fields.
    auto fields = std::vector<bytes>(req._args.begin() + 1, req._args.end());
    auto size = fields.size();
    return redis::delete_fields(proxy, options, std::move(req._args[0]), std::move(fields), permit).then([size] {
        return redis_message::number(size);
    });
}

future<redis_message> hexists(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 2) {
        throw wrong_arguments_exception(2, req.arguments_size(), req._command);
    }
    return redis::read_hashes(proxy, options, req._args[0], req._args[1], permit).then([] (auto result) {
        return redis_message::number(result->empty() ? 0 : 1);
    });
}

future<redis_message> set(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 2 && req.arguments_size() != 4) {
        throw invalid_arguments_exception(req._command);
    }
    long ttl = 0;
    if (req.arguments_size() == 4) {
        bytes opt;
        opt.resize(req._args[2].size());
        std::transform(req._args[2].begin(), req._args[2].end(), opt.begin(), ::tolower);
        if (opt == "ex") {
            try {
                ttl = std::stol(std::string(reinterpret_cast<const char*>(req._args[3].data()), req._args[3].size()));
            }
            catch (...) {
                throw invalid_arguments_exception(req._command);
            }
        }
    }
    return redis::write_strings(proxy, options, std::move(req._args[0]), std::move(req._args[1]), ttl, permit).then([] {
        return redis_message::ok();
    });
}

future<redis_message> setex(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() != 3) {
        throw wrong_arguments_exception(3, req.arguments_size(), req._command);
    }
    long ttl = std::stol(std::string(reinterpret_cast<const char*>(req._args[1].data()), req._args[1].size()));
    return redis::write_strings(proxy, options, std::move(req._args[0]), std::move(req._args[2]), ttl, permit).then([] {
        return redis_message::ok();
    });
}

future<redis_message> del(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit) {
    if (req.arguments_size() == 0) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    //FIXME: We should return the count of the actually deleted keys.
    auto keys = req._args;
    auto size = keys.size();
    return redis::delete_objects(proxy, options, std::move(keys), permit).then([size] {
       return redis_message::number(size);
    });
}

future<redis_message> select(service::storage_proxy&, request& req, redis::redis_options& options, service_permit) {
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
    if (index < 0 || static_cast<size_t>(index) >= options.get_total_redis_db_count()) {
        throw invalid_db_index_exception();
    }
    options.set_keyspace_name(sprint("REDIS_%zu", static_cast<size_t>(index)));
    return redis_message::ok();
}

future<redis_message> unknown(service::storage_proxy&, request& req, redis::redis_options&, service_permit) {
    return redis_message::unknown(std::move(req._command));
}

future<redis_message> ping(service::storage_proxy&, request& req, redis::redis_options&, service_permit) {
    return redis_message::pong();
}

future<redis_message> echo(service::storage_proxy&, request& req, redis::redis_options&, service_permit) {
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return redis_message::make_strings_result(req._args[0]);
}

future<redis_message> lolwut(service::storage_proxy&, request& req, redis::redis_options& options, service_permit) {
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
    return redis::lolwut5(cols, squares_per_row, squares_per_col).then([] (auto result) {
        return redis_message::make_strings_result(std::move(result));
    });
}

}

}
