/*
 * Copyright (C) 2018 ScyllaDB
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

#include "db/data_listeners.hh"
#include "database.hh"
#include "db_clock.hh"

#include <tuple>

extern logging::logger dblog;

namespace db {

void data_listeners::install(data_listener* listener) {
    _listeners.emplace(listener);
    dblog.debug("data_listeners: install listener {}", listener);
}

void data_listeners::uninstall(data_listener* listener) {
    dblog.debug("data_listeners: uninstall listener {}", listener);
    _listeners.erase(listener);
}

bool data_listeners::exists(data_listener* listener) const {
    return _listeners.count(listener) != 0;
}

flat_mutation_reader data_listeners::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
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
    std::ostringstream oss;
    oss << key.key().with_schema(*schema);
    return oss.str();
}

toppartitions_data_listener::toppartitions_data_listener(database& db, sstring ks, sstring cf) : _db(db), _ks(ks), _cf(cf) {
    dblog.debug("toppartitions_data_listener: installing {}", this);
    _db.data_listeners().install(this);
}

toppartitions_data_listener::~toppartitions_data_listener() {
    dblog.debug("toppartitions_data_listener: uninstalling {}", this);
    _db.data_listeners().uninstall(this);
}

future<> toppartitions_data_listener::stop() {
    dblog.debug("toppartitions_data_listener: stopping {}", this);
    return make_ready_future<>();
}

flat_mutation_reader toppartitions_data_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
    if (s->ks_name() != _ks || s->cf_name() != _cf) {
        return std::move(rd);
    }
    dblog.trace("toppartitions_data_listener::on_read: {}.{}", s->ks_name(), s->cf_name());
    return make_filtering_reader(std::move(rd), [zis = this->weak_from_this(), &range, &slice, s = std::move(s)] (const dht::decorated_key& dk) {
        // The data query may be executing after the toppartitions_data_listener object has been removed, so check
        if (zis) {
            zis->_top_k_read.append(toppartitions_item_key{s, dk});
        }
        return true;
    });
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
    if (s->ks_name() != _ks || s->cf_name() != _cf) {
        return;
    }
    dblog.trace("toppartitions_data_listener::on_write: {}.{}", _ks, _cf);
    _top_k_write.append(toppartitions_item_key{s, m.decorated_key(*s)});
}

toppartitions_data_listener::global_top_k::results
toppartitions_data_listener::globalize(top_k::results&& r) {
    toppartitions_data_listener::global_top_k::results n;
    n.reserve(r.size());
    for (auto&& e : r) {
        n.emplace_back(global_top_k::results::value_type{toppartitions_global_item_key(std::move(e.item)), e.count, e.error});
    }
    return n;
}

toppartitions_data_listener::top_k::results
toppartitions_data_listener::localize(const global_top_k::results& r) {
    toppartitions_data_listener::top_k::results n;
    n.reserve(r.size());
    for (auto&& e : r) {
        n.emplace_back(top_k::results::value_type{toppartitions_item_key(e.item), e.count, e.error});
    }
    return n;
}

toppartitions_query::toppartitions_query(distributed<database>& xdb, sstring ks, sstring cf,
        std::chrono::milliseconds duration, size_t list_size, size_t capacity)
        : _xdb(xdb), _ks(ks), _cf(cf), _duration(duration), _list_size(list_size), _capacity(capacity),
          _query(std::make_unique<sharded<toppartitions_data_listener>>()) {
    dblog.debug("toppartitions_query on {}.{}", _ks, _cf);
}

future<> toppartitions_query::scatter() {
    return _query->start(std::ref(_xdb), _ks, _cf);
}

using top_t = toppartitions_data_listener::global_top_k::results;

future<toppartitions_query::results> toppartitions_query::gather(unsigned res_size) {
    dblog.debug("toppartitions_query::gather");

    auto map = [res_size, this] (toppartitions_data_listener& listener) {
        dblog.trace("toppartitions_query::map_reduce with listener {}", &listener);
        top_t rd = toppartitions_data_listener::globalize(listener._top_k_read.top(res_size));
        top_t wr = toppartitions_data_listener::globalize(listener._top_k_write.top(res_size));
        return make_foreign(std::make_unique<std::tuple<top_t, top_t>>(std::move(rd), std::move(wr)));
    };
    auto reduce = [this] (results res, foreign_ptr<std::unique_ptr<std::tuple<top_t, top_t>>> rd_wr) {
        res.read.append(toppartitions_data_listener::localize(std::get<0>(*rd_wr)));
        res.write.append(toppartitions_data_listener::localize(std::get<1>(*rd_wr)));
        return std::move(res);
    };
    return _query->map_reduce0(map, results{res_size}, reduce)
        .handle_exception([] (auto ep) {
            dblog.error("toppartitions_query::gather: {}", ep);
            return make_exception_future<results>(ep);
        }).finally([this] () {
            dblog.debug("toppartitions_query::gather: stopping query");
            return _query->stop().then([this] {
                dblog.debug("toppartitions_query::gather: query stopped");
            });
        });
}

} // namespace db
