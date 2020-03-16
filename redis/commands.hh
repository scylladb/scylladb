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

#pragma once

#include "redis/request.hh"
#include "redis/abstract_command.hh"

namespace redis {

namespace commands {

class get : public abstract_command {
    bytes _key;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    get(bytes&& name, bytes&& key) 
        : abstract_command(std::move(name)) 
        , _key(std::move(key)) {
    }
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class set : public abstract_command {
    bytes _key;
    bytes _data;
    long _ttl = 0;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    set(bytes&& name, bytes&& key, bytes&& data, long ttl) 
        : abstract_command(std::move(name)) 
        , _key(std::move(key))
        , _data(std::move(data))
        , _ttl(ttl) {
    }
    set(bytes&& name, bytes&& key, bytes&& data) : set(std::move(name), std::move(key), std::move(data), 0) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class del : public abstract_command {
    std::vector<bytes> _keys;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    del(bytes&& name, std::vector<bytes>&& keys) : abstract_command(std::move(name)), _keys(std::move(keys)) {} 
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class unknown : public abstract_command {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    unknown(bytes&& name) : abstract_command(std::move(name)) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit permit) override;
};

class echo : public abstract_command {
    bytes _str;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    echo(bytes&& name, bytes&& str) : abstract_command(std::move(name)) , _str(std::move(str)) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class ping : public abstract_command {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    ping(bytes&& name) : abstract_command(std::move(name)) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class select : public abstract_command {
    long _index;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    select(bytes&& name, long index) : abstract_command(std::move(name)), _index(index) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

class lolwut : public abstract_command {
    const int _cols, _squares_per_row, _squares_per_col;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    lolwut(bytes&& name, const int cols, const int squares_per_row, const int squares_per_col) : abstract_command(std::move(name)), _cols(cols), _squares_per_row(squares_per_row), _squares_per_col(squares_per_col) {}
    virtual future<redis_message> execute(service::storage_proxy&, redis_options&, service_permit) override;
};

}

}
