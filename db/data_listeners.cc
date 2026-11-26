/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/data_listeners.hh"
#include "replica/database.hh"
#include "readers/filtering.hh"
#include "utils/error_injection.hh"

#include <seastar/coroutine/exception.hh>

#include <tuple>

extern logging::logger dblog;

namespace db {

void data_listeners::install(data_listener* listener) {
    _listeners.emplace(listener);
    dblog.debug("data_listeners: install listener {}", fmt::ptr(listener));
}

void data_listeners::uninstall(data_listener* listener) {
    dblog.debug("data_listeners: uninstall listener {}", fmt::ptr(listener));
    _listeners.erase(listener);
}

bool data_listeners::exists(data_listener* listener) const {
    return _listeners.contains(listener);
}

mutation_reader data_listeners::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, mutation_reader&& rd) {
    for (auto&& li : _listeners) {
        rd = li->on_read(s, range, slice, std::move(rd));
    }
    return std::move(rd);
}

void data_listeners::on_write(const schema_ptr& s, const frozen_mutation& m) {
    for (auto&& li : _listeners) {
        li->on_write(s, m);
    }
}

toppartitions_item_key::operator sstring() const {
    return fmt::to_string(key.key().with_schema(*schema));
}

toppartitions_data_listener::toppartitions_data_listener(replica::database& db, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> table_filters,
        std::unordered_set<sstring> keyspace_filters) : _db(db), _table_filters(std::move(table_filters)), _keyspace_filters(std::move(keyspace_filters)) {
    dblog.debug("toppartitions_data_listener: installing {}", fmt::ptr(this));
    _db.data_listeners().install(this);
}

toppartitions_data_listener::~toppartitions_data_listener() {
    dblog.debug("toppartitions_data_listener: uninstalling {}", fmt::ptr(this));
    _db.data_listeners().uninstall(this);
}

future<> toppartitions_data_listener::stop() {
    dblog.debug("toppartitions_data_listener: stopping {}", fmt::ptr(this));
    return make_ready_future<>();
}

template <typename MakeKey>
void toppartitions_data_listener::sample(top_k& top, MakeKey&& make_key) noexcept {
    if (_sampling_error) {
        return;
    }
    try {
        utils::get_local_injector().inject("toppartitions_sample_fail", [] { throw std::bad_alloc(); });
        top.append(make_key());
    } catch (...) {
        set_sampling_error(std::current_exception());
    }
}

void toppartitions_data_listener::set_sampling_error(std::exception_ptr ex) noexcept {
    _sampling_error = std::move(ex);
    dblog.warn("toppartitions_data_listener: sampling failed, the query will report an error: {}", _sampling_error);
}

bool toppartitions_data_listener::should_sample(const schema& s) noexcept {
    if (_sampling_error) {
        return false;
    }
    if (_table_filters.empty() && _keyspace_filters.empty()) {
        return true;
    }
    try {
        // Building the (ks, cf) key allocates for long names.
        return _keyspace_filters.contains(s.ks_name()) || _table_filters.contains({s.ks_name(), s.cf_name()});
    } catch (...) {
        set_sampling_error(std::current_exception());
        return false;
    }
}

mutation_reader toppartitions_data_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, mutation_reader&& rd) {
    if (should_sample(*s)) {
        dblog.trace("toppartitions_data_listener::on_read: {}.{}", s->ks_name(), s->cf_name());
        return make_filtering_reader(std::move(rd), [zis = this->weak_from_this(), s = std::move(s)] (const dht::decorated_key& dk) {
            // The data query may be executing after the toppartitions_data_listener object has been removed, so check
            if (zis) {
                zis->sample(zis->_top_k_read, [&] { return toppartitions_item_key{s, dk}; });
            }
            return true;
        });
    }

    return std::move(rd);
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
    if (should_sample(*s)) {
        dblog.trace("toppartitions_data_listener::on_write: {}.{}", s->ks_name(), s->cf_name());
        sample(_top_k_write, [&] { return toppartitions_item_key{s, m.decorated_key(*s)}; });
    }
}

toppartitions_data_listener::global_top_k_results
toppartitions_data_listener::globalize(top_k::results&& r) {
    toppartitions_data_listener::global_top_k_results n;
    n.reserve(r.size());
    for (auto&& e : r) {
        n.emplace_back(global_top_k_result{toppartitions_global_item_key(std::move(e.item)), e.count, e.error});
    }
    return n;
}

future<toppartitions_data_listener::top_k::results>
toppartitions_data_listener::localize(const global_top_k_results& r) {
    toppartitions_data_listener::top_k::results n;
    n.reserve(r.size());
    for (auto&& e : r) {
        auto s = co_await e.item.schema.get(db::no_timeout);
        n.emplace_back(top_k::results::value_type{toppartitions_item_key(s, e.item.key), e.count, e.error});
    }
    co_return n;
}

toppartitions_query::toppartitions_query(sharded<replica::database>& xdb, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash>&& table_filters,
        std::unordered_set<sstring>&& keyspace_filters, std::chrono::milliseconds duration, size_t list_size, size_t capacity)
        : _xdb(xdb), _table_filters(std::move(table_filters)), _keyspace_filters(std::move(keyspace_filters)), _duration(duration), _list_size(list_size), _capacity(capacity),
          _query(std::make_unique<sharded<toppartitions_data_listener>>()) {
    dblog.debug("toppartitions_query on {} column families and {} keyspaces", !_table_filters.empty() ? std::to_string(_table_filters.size()) : "all",
                !_keyspace_filters.empty() ? std::to_string(_keyspace_filters.size()) : "all");
}

future<> toppartitions_query::scatter() {
    return _query->start(std::ref(_xdb), _table_filters, _keyspace_filters);
}

using top_t = toppartitions_data_listener::global_top_k_results;

future<toppartitions_query::results> toppartitions_query::gather(unsigned res_size) {
    dblog.debug("toppartitions_query::gather");

    auto map = [res_size] (toppartitions_data_listener& listener) {
        dblog.trace("toppartitions_query::map_reduce with listener {}", fmt::ptr(&listener));
        if (listener._sampling_error) {
            throw std::runtime_error(fmt::format("toppartitions sampling failed on shard {}: {}", this_shard_id(), listener._sampling_error));
        }
        top_t rd = toppartitions_data_listener::globalize(listener._top_k_read.top(res_size));
        top_t wr = toppartitions_data_listener::globalize(listener._top_k_write.top(res_size));
        return make_foreign(std::make_unique<std::tuple<top_t, top_t>>(std::move(rd), std::move(wr)));
    };
    results res{res_size};
    std::exception_ptr ex;
    try {
        // Localize on this shard rather than in a map_reduce reducer, since it may have to wait
        // for this shard to catch up with the schema of the other shards.
        auto shard_results = co_await _query->map(map);
        for (auto& rd_wr : shard_results) {
            res.read.append(co_await toppartitions_data_listener::localize(std::get<0>(*rd_wr)));
            res.write.append(co_await toppartitions_data_listener::localize(std::get<1>(*rd_wr)));
        }
    } catch (...) {
        ex = std::current_exception();
        dblog.error("toppartitions_query::gather: {}", ex);
    }
    dblog.debug("toppartitions_query::gather: stopping query");
    co_await _query->stop();
    dblog.debug("toppartitions_query::gather: query stopped");
    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }
    co_return res;
}

} // namespace db
