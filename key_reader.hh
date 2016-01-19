/*
 * Copyright 2015 ScyllaDB
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

#include "mutation_reader.hh"
#include "schema.hh"
#include "query-request.hh"

#include "dht/i_partitioner.hh"

class key_reader final {
public:
    class impl {
    public:
        virtual ~impl() {}
        virtual future<dht::decorated_key_opt> operator()() = 0;
    };
private:
    class null_impl final : public impl {
    public:
        virtual future<dht::decorated_key_opt> operator()() override { throw std::bad_function_call(); }
    };
private:
    std::unique_ptr<impl> _impl;
public:
    key_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}
    key_reader() : key_reader(std::make_unique<null_impl>()) {}
    key_reader(key_reader&&) = default;
    key_reader(const key_reader&) = delete;
    key_reader& operator=(key_reader&&) = default;
    key_reader& operator=(const key_reader&) = delete;
    future<dht::decorated_key_opt> operator()() { return _impl->operator()(); }
};

template<typename Impl, typename... Args>
inline key_reader make_key_reader(Args&&... args) {
    return key_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

key_reader make_combined_reader(schema_ptr s, std::vector<key_reader>);
key_reader make_key_from_mutation_reader(mutation_reader&&);

template<typename Filter>
class filtering_key_reader final : public key_reader::impl {
    key_reader _reader;
    Filter _filter;
public:
    filtering_key_reader(key_reader&& reader, Filter&& filter)
        : _reader(std::move(reader)), _filter(std::move(filter))
    { }
    virtual future<dht::decorated_key_opt> operator()() override {
        return _reader().then([this] (dht::decorated_key_opt&& dk) {
            if (!dk || _filter(*dk)) {
                return make_ready_future<dht::decorated_key_opt>(std::move(dk));
            }
            return operator()();
        });
    }
};

template<typename Filter>
key_reader make_filtering_reader(key_reader&& reader, Filter&& filter) {
    return make_key_reader<filtering_key_reader<Filter>>(std::move(reader), std::forward<Filter>(filter));
}

class key_source {
    std::function<key_reader(const query::partition_range& range, const io_priority_class& pc)> _fn;
public:
    key_source(std::function<key_reader(const query::partition_range& range, const io_priority_class& pc)> fn) : _fn(std::move(fn)) {}
    key_source(std::function<key_reader(const query::partition_range& range)> fn)
        : _fn([fn = std::move(fn)](const query::partition_range& range, const io_priority_class& pc) {
            return fn(range);
        }) {}
    key_reader operator()(const query::partition_range& range, const io_priority_class& pc) {
        return _fn(range, pc);
    }
    key_reader operator()(const query::partition_range& range) {
        return _fn(range, default_priority_class());
    }
};
