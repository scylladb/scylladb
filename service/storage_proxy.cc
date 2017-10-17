/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "partition_range_compat.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "storage_proxy.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "query_result_merger.hh"
#include "core/do_with.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "storage_service.hh"
#include "core/future-util.hh"
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/none_of.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/empty.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "utils/latency.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "utils/joinpoint.hh"
#include <seastar/util/lazy.hh>
#include "core/metrics.hh"
#include <seastar/core/execution_stage.hh>

namespace service {

static logging::logger slogger("storage_proxy");
static logging::logger qlogger("query_result");
static logging::logger mlogger("mutation_data");

const sstring storage_proxy::COORDINATOR_STATS_CATEGORY("storage_proxy_coordinator");
const sstring storage_proxy::REPLICA_STATS_CATEGORY("storage_proxy_replica");

distributed<service::storage_proxy> _the_storage_proxy;

using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

static inline
const dht::token& start_token(const dht::partition_range& r) {
    static const dht::token min_token = dht::minimum_token();
    return r.start() ? r.start()->value().token() : min_token;
}

static inline
const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static inline
sstring get_dc(gms::inet_address ep) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(ep);
}

static inline
sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    return get_dc(local_addr);
}

class mutation_holder {
protected:
    size_t _size = 0;
    schema_ptr _schema;
public:
    virtual ~mutation_holder() {}
    virtual lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) = 0;
    virtual bool is_shared() = 0;
    size_t size() const {
        return _size;
    }
    const schema_ptr& schema() {
        return _schema;
    }
};

// different mutation for each destination (for read repairs)
class per_destination_mutation : public mutation_holder {
    std::unordered_map<gms::inet_address, lw_shared_ptr<const frozen_mutation>> _mutations;
    dht::token _token;
public:
    per_destination_mutation(const std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>& mutations) {
        for (auto&& m : mutations) {
            lw_shared_ptr<const frozen_mutation> fm;
            if (m.second) {
                _schema = m.second.value().schema();
                _token = m.second.value().token();
                fm = make_lw_shared<const frozen_mutation>(freeze(m.second.value()));
                _size += fm->representation().size();
            }
            _mutations.emplace(m.first, std::move(fm));
        }
    }
    lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) override {
        return _mutations[ep];
    }
    virtual bool is_shared() override {
        return false;
    }
    dht::token& token() {
        return _token;
    }
};

// same mutation for each destination
class shared_mutation : public mutation_holder {
    lw_shared_ptr<const frozen_mutation> _mutation;
public:
    shared_mutation(const mutation& m) : _mutation(make_lw_shared<const frozen_mutation>(freeze(m))) {
        _size = _mutation->representation().size();
        _schema = m.schema();
    };
    lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) override {
        return _mutation;
    }
    virtual bool is_shared() override {
        return true;
    }
};

class abstract_write_response_handler : public enable_shared_from_this<abstract_write_response_handler> {
protected:
    storage_proxy::response_id_type _id;
    promise<> _ready; // available when cl is achieved
    shared_ptr<storage_proxy> _proxy;
    tracing::trace_state_ptr _trace_state;
    db::consistency_level _cl;
    size_t _total_block_for = 0;
    db::write_type _type;
    std::unique_ptr<mutation_holder> _mutation_holder;
    std::unordered_set<gms::inet_address> _targets; // who we sent this mutation to
    size_t _pending_endpoints; // how many endpoints in bootstrap state there is
    // added dead_endpoints as a memeber here as well. This to be able to carry the info across
    // calls in helper methods in a convinient way. Since we hope this will be empty most of the time
    // it should not be a huge burden. (flw)
    std::vector<gms::inet_address> _dead_endpoints;
    size_t _cl_acks = 0;
    bool _cl_achieved = false;
    bool _timedout = false;
    bool _throttled = false;
protected:
    virtual void signal(gms::inet_address from) {
        signal();
    }
public:
    abstract_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets, tracing::trace_state_ptr trace_state,
            size_t pending_endpoints = 0, std::vector<gms::inet_address> dead_endpoints = {})
            : _id(p->_next_response_id++), _proxy(std::move(p)), _trace_state(trace_state), _cl(cl), _type(type), _mutation_holder(std::move(mh)), _targets(std::move(targets)),
              _pending_endpoints(pending_endpoints), _dead_endpoints(std::move(dead_endpoints)) {
        // original comment from cassandra:
        // during bootstrap, include pending endpoints in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        _total_block_for = db::block_for(ks, _cl) + _pending_endpoints;
        ++_proxy->_stats.writes;
    }
    virtual ~abstract_write_response_handler() {
        --_proxy->_stats.writes;
        if (_cl_achieved) {
            if (_throttled) {
                _ready.set_value();
            } else {
                _proxy->_stats.background_writes--;
                _proxy->_stats.background_write_bytes -= _mutation_holder->size();
                _proxy->unthrottle();
            }
        } else if (_timedout) {
            _ready.set_exception(mutation_write_timeout_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _total_block_for, _type));
        }
    };
    bool is_counter() const {
        return _type == db::write_type::COUNTER;
    }
    void unthrottle() {
        _proxy->_stats.background_writes++;
        _proxy->_stats.background_write_bytes += _mutation_holder->size();
        _throttled = false;
        _ready.set_value();
    }
    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= _total_block_for) {
             _cl_achieved = true;
             if (_proxy->need_throttle_writes()) {
                 _throttled = true;
                 _proxy->_throttled_writes.push_back(_id);
                 ++_proxy->_stats.throttled_writes;
             } else {
                 unthrottle();
             }
         }
    }
    void on_timeout() {
        if (_cl_achieved) {
            slogger.trace("Write is not acknowledged by {} replicas after achieving CL", get_targets());
        }
        _timedout = true;
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        signal(from);
        auto it = _targets.find(from);
        assert(it != _targets.end());
        _targets.erase(it);
        return _targets.size() == 0;
    }
    future<> wait() {
        return _ready.get_future();
    }
    const std::unordered_set<gms::inet_address>& get_targets() const {
        return _targets;
    }
    const std::vector<gms::inet_address>& get_dead_endpoints() const {
        return _dead_endpoints;
    }
    lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) {
        return _mutation_holder->get_mutation_for(ep);
    }
    const schema_ptr& get_schema() const {
        return _mutation_holder->schema();
    }
    storage_proxy::response_id_type id() const {
      return _id;
    }
    bool read_repair_write() {
        return !_mutation_holder->is_shared();
    }
    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state;
    }
    friend storage_proxy;
};

class datacenter_write_response_handler : public abstract_write_response_handler {
    void signal(gms::inet_address from) override {
        if (is_me(from) || db::is_local(from)) {
            abstract_write_response_handler::signal();
        }
    }
public:
    datacenter_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), db::count_local_endpoints(pending_endpoints), std::move(dead_endpoints)) {}
};

class write_response_handler : public abstract_write_response_handler {
public:
    write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), pending_endpoints.size(), std::move(dead_endpoints)) {}
};

class datacenter_sync_write_response_handler : public abstract_write_response_handler {
    std::unordered_map<sstring, size_t> _dc_responses;
    void signal(gms::inet_address from) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        sstring data_center = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(data_center);

        if (dc_resp->second > 0) {
            --dc_resp->second;
            abstract_write_response_handler::signal();
        }
    }
public:
    datacenter_sync_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets, const std::vector<gms::inet_address>& pending_endpoints,
            std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state) :
        abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh), targets, std::move(tr_state), 0, dead_endpoints) {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        for (auto& target : targets) {
            auto dc = snitch_ptr->get_datacenter(target);

            if (_dc_responses.find(dc) == _dc_responses.end()) {
                auto pending_for_dc = boost::range::count_if(pending_endpoints, [&snitch_ptr, &dc] (const gms::inet_address& ep){
                    return snitch_ptr->get_datacenter(ep) == dc;
                });
                _dc_responses.emplace(dc, db::local_quorum_for(ks, dc) + pending_for_dc);
                _pending_endpoints += pending_for_dc;
            }
        }
    }
};

bool storage_proxy::need_throttle_writes() const {
    return _stats.background_write_bytes > memory::stats().total_memory() / 10 || _stats.queued_write_bytes > 6*1024*1024;
}

void storage_proxy::unthrottle() {
   while(!need_throttle_writes() && !_throttled_writes.empty()) {
       auto id = _throttled_writes.front();
       _throttled_writes.pop_front();
       auto it = _response_handlers.find(id);
       if (it != _response_handlers.end()) {
           it->second.handler->unthrottle();
       }
   }
}

storage_proxy::response_id_type storage_proxy::register_response_handler(shared_ptr<abstract_write_response_handler>&& h) {
    auto id = h->id();
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), [this, id] {
        auto& e = _response_handlers.find(id)->second;
        if (e.handler->_cl_achieved || e.handler->_cl == db::consistency_level::ANY) {
            // we are here because either cl was achieved, but targets left in the handler are not
            // responding, so a hint should be written for them, or cl == any in which case
            // hints are counted towards consistency, so we need to write hints and count how much was written
            auto hints = hint_to_dead_endpoints(e.handler->_mutation_holder, e.handler->get_targets());
            e.handler->signal(hints);
            if (e.handler->_cl == db::consistency_level::ANY && hints) {
                slogger.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            }
        }

        e.handler->on_timeout();
        remove_response_handler(id);
    }));
    assert(e.second);
    return id;
}

void storage_proxy::remove_response_handler(storage_proxy::response_id_type id) {
    _response_handlers.erase(id);
}

void storage_proxy::got_response(storage_proxy::response_id_type id, gms::inet_address from) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second.handler->get_trace_state(), "Got a response from /{}", from);
        if (it->second.handler->response(from)) {
            remove_response_handler(id); // last one, remove entry. Will cancel expiration timer too.
        }
    }
}

future<> storage_proxy::response_wait(storage_proxy::response_id_type id, clock_type::time_point timeout) {
    auto& e = _response_handlers.find(id)->second;

    e.expire_timer.arm(timeout);

    return e.handler->wait();
}

::shared_ptr<abstract_write_response_handler>& storage_proxy::get_write_response_handler(storage_proxy::response_id_type id) {
        return _response_handlers.find(id)->second.handler;
}

storage_proxy::response_id_type storage_proxy::create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m,
                             std::unordered_set<gms::inet_address> targets, const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state)
{
    shared_ptr<abstract_write_response_handler> h;
    auto& rs = ks.get_replication_strategy();

    if (db::is_datacenter_local(cl)) {
        h = ::make_shared<datacenter_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state));
    } else if (cl == db::consistency_level::EACH_QUORUM && rs.get_type() == locator::replication_strategy_type::network_topology){
        h = ::make_shared<datacenter_sync_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state));
    } else {
        h = ::make_shared<write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state));
    }
    return register_response_handler(std::move(h));
}

seastar::metrics::label storage_proxy::split_stats::datacenter_label("datacenter");
seastar::metrics::label storage_proxy::split_stats::op_type_label("op_type");

storage_proxy::split_stats::split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type)
        : _short_description_prefix(short_description_prefix)
        , _long_description_prefix(long_description_prefix)
        , _category(category)
        , _op_type(op_type) {
    // register a local Node counter to begin with...
    namespace sm = seastar::metrics;

    _metrics.add_group(_category, {
        sm::make_derive(_short_description_prefix + sstring("_local_node"), [this] { return _local.val; },
                       sm::description(_long_description_prefix + "on a local Node"), {op_type_label(_op_type)})
    });
}

storage_proxy::stats::stats()
        : writes_attempts(COORDINATOR_STATS_CATEGORY, "total_write_attempts", "total number of write requests", "mutation_data")
        , writes_errors(COORDINATOR_STATS_CATEGORY, "write_errors", "number of write requests that failed", "mutation_data")
        , read_repair_write_attempts(COORDINATOR_STATS_CATEGORY, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data")
        , data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of data read requests", "data")
        , data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of data read requests that completed", "data")
        , data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of data read requests that failed", "data")
        , digest_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of digest read requests", "digest")
        , digest_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of digest read requests that completed", "digest")
        , digest_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of digest read requests that failed", "digest")
        , mutation_data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of mutation data read requests", "mutation_data")
        , mutation_data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of mutation data read requests that completed", "mutation_data")
        , mutation_data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of mutation data read requests that failed", "mutation_data") {}

inline uint64_t& storage_proxy::split_stats::get_ep_stat(gms::inet_address ep) {
    if (is_me(ep)) {
        return _local.val;
    }

    sstring dc = get_dc(ep);

    // if this is the first time we see an endpoint from this DC - add a
    // corresponding collectd metric
    if (_dc_stats.find(dc) == _dc_stats.end()) {
        namespace sm = seastar::metrics;

        _metrics.add_group(_category, {
            sm::make_derive(_short_description_prefix + sstring("_remote_node"), [this, dc] { return _dc_stats[dc].val; },
                            sm::description(seastar::format("{} when communicating with external Nodes in DC {}", _long_description_prefix, dc)), {datacenter_label(dc), op_type_label(_op_type)})
        });
    }
    return _dc_stats[dc].val;
}

storage_proxy::~storage_proxy() {}
storage_proxy::storage_proxy(distributed<database>& db) : _db(db) {
    namespace sm = seastar::metrics;
    _metrics.add_group(COORDINATOR_STATS_CATEGORY, {
        sm::make_histogram("read_latency", sm::description("The general read latency histogram"), [this]{ return _stats.estimated_read.get_histogram(16, 20);}),
        sm::make_histogram("write_latency", sm::description("The general write latency histogram"), [this]{return _stats.estimated_write.get_histogram(16, 20);}),
        sm::make_queue_length("foreground_writes", [this] { return _stats.writes - _stats.background_writes; },
                       sm::description("number of currently pending foreground write requests")),

        sm::make_queue_length("background_writes", [this] { return _stats.background_writes; },
                       sm::description("number of currently pending background write requests")),

        sm::make_queue_length("current_throttled_writes", [this] { return _throttled_writes.size(); },
                       sm::description("number of currently throttled write requests")),

        sm::make_total_operations("throttled_writes", [this] { return _stats.throttled_writes; },
                       sm::description("number of throttled write requests")),

        sm::make_current_bytes("queued_write_bytes", [this] { return _stats.queued_write_bytes; },
                       sm::description("number of bytes in pending write requests")),

        sm::make_current_bytes("background_write_bytes", [this] { return _stats.background_write_bytes; },
                       sm::description("number of bytes in pending background write requests")),

        sm::make_queue_length("foreground_reads", [this] { return _stats.reads - _stats.background_reads; },
                       sm::description("number of currently pending foreground read requests")),

        sm::make_queue_length("background_reads", [this] { return _stats.background_reads; },
                       sm::description("number of currently pending background read requests")),

        sm::make_total_operations("read_retries", [this] { return _stats.read_retries; },
                       sm::description("number of read retry attempts")),

        sm::make_total_operations("canceled_read_repairs", [this] { return _stats.global_read_repairs_canceled_due_to_concurrent_write; },
                       sm::description("number of global read repairs canceled due to a concurrent write")),

        sm::make_total_operations("foreground_read_repair", [this] { return _stats.read_repair_repaired_blocking; },
                      sm::description("number of foreground read repairs")),

        sm::make_total_operations("background_read_repairs", [this] { return _stats.read_repair_repaired_background; },
                       sm::description("number of background read repairs")),

        sm::make_total_operations("write_timeouts", [this] { return _stats.write_timeouts._count; },
                       sm::description("number of write request failed due to a timeout")),

        sm::make_total_operations("write_unavailable", [this] { return _stats.write_unavailables._count; },
                       sm::description("number write requests failed due to an \"unavailable\" error")),

        sm::make_total_operations("read_timeouts", [this] { return _stats.read_timeouts._count; },
                       sm::description("number of read request failed due to a timeout")),

        sm::make_total_operations("read_unavailable", [this] { return _stats.read_unavailables._count; },
                       sm::description("number read requests failed due to an \"unavailable\" error")),

        sm::make_total_operations("range_timeouts", [this] { return _stats.range_slice_timeouts._count; },
                       sm::description("number of range read operations failed due to a timeout")),

        sm::make_total_operations("range_unavailable", [this] { return _stats.range_slice_unavailables._count; },
                       sm::description("number of range read operations failed due to an \"unavailable\" error")),
    });

    _metrics.add_group(REPLICA_STATS_CATEGORY, {
        sm::make_total_operations("received_counter_updates", _stats.received_counter_updates,
                       sm::description("number of counter updates received by this node acting as an update leader")),

        sm::make_total_operations("received_mutations", _stats.received_mutations,
                       sm::description("number of mutations received by a replica Node")),

        sm::make_total_operations("forwarded_mutations", _stats.forwarded_mutations,
                       sm::description("number of mutations forwarded to other replica Nodes")),

        sm::make_total_operations("forwarding_errors", _stats.forwarding_errors,
                       sm::description("number of errors during forwarding mutations to other replica Nodes")),

        sm::make_total_operations("reads", _stats.replica_data_reads,
                       sm::description("number of remote data read requests this Node received"), {storage_proxy::split_stats::op_type_label("data")}),

        sm::make_total_operations("reads", _stats.replica_mutation_data_reads,
                       sm::description("number of remote mutation data read requests this Node received"), {storage_proxy::split_stats::op_type_label("mutation_data")}),

        sm::make_total_operations("reads", _stats.replica_digest_reads,
                       sm::description("number of remote digest read requests this Node received"), {storage_proxy::split_stats::op_type_label("digest")}),

    });
}

storage_proxy::rh_entry::rh_entry(shared_ptr<abstract_write_response_handler>&& h, std::function<void()>&& cb) : handler(std::move(h)), expire_timer(std::move(cb)) {}

storage_proxy::unique_response_handler::unique_response_handler(storage_proxy& p_, response_id_type id_) : id(id_), p(p_) {}
storage_proxy::unique_response_handler::unique_response_handler(unique_response_handler&& x) : id(x.id), p(x.p) { x.id = 0; };
storage_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
storage_proxy::response_id_type storage_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

#if 0
    static
    {
        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the COUNTER_MUTATION stage
         * but on the latter case, the verb handler already run on the COUNTER_MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                counterWriteTask(mutation, targets, responseHandler, localDataCenter).run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                StageManager.getStage(Stage.COUNTER_MUTATION)
                            .execute(counterWriteTask(mutation, targets, responseHandler, localDataCenter));
            }
        };
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName the keyspace for the CAS
     * @param cfName the column family for the CAS
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static ColumnFamily cas(String keyspaceName,
                                   String cfName,
                                   ByteBuffer key,
                                   CASRequest request,
                                   ConsistencyLevel consistencyForPaxos,
                                   ConsistencyLevel consistencyForCommit,
                                   ClientState state)
    throws UnavailableException, IsBootstrappingException, ReadTimeoutException, WriteTimeoutException, InvalidRequestException
    {
        final long start = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
            while (System.nanoTime() - start < timeout)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                Pair<List<InetAddress>, Integer> p = getPaxosParticipants(keyspaceName, key, consistencyForPaxos);
                List<InetAddress> liveEndpoints = p.left;
                int requiredParticipants = p.right;

                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.left;
                contentions += pair.right;
                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");
                long timestamp = System.currentTimeMillis();
                ReadCommand readCommand = ReadCommand.create(keyspaceName, key, cfName, timestamp, request.readFilter());
                List<Row> rows = read(Arrays.asList(readCommand), consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM);
                ColumnFamily current = rows.get(0).cf;
                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    // We should not return null as this means success
                    casWriteMetrics.conditionNotMet.inc();
                    return current == null ? ArrayBackedSortedColumns.factory.create(metadata) : current;
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                ColumnFamily updates = request.makeUpdates(current);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(key, updates);

                Commit proposal = Commit.newProposal(key, ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos))
                {
                    commitPaxos(proposal, consistencyForCommit);
                    Tracing.trace("CAS successful");
                    return null;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
        }
        catch (WriteTimeoutException|ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            throw e;
        }
        catch(UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            throw e;
        }
        finally
        {
            if(contentions > 0)
                casWriteMetrics.contention.update(contentions);
            casWriteMetrics.addNano(System.nanoTime() - start);
        }
    }

    private static Predicate<InetAddress> sameDCPredicateFor(final String dc)
    {
        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress host)
            {
                return dc.equals(snitch.getDatacenter(host));
            }
        };
    }

    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(String keyspaceName, ByteBuffer key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            Predicate<InetAddress> isLocalDc = sameDCPredicateFor(localDc);
            naturalEndpoints = ImmutableList.copyOf(Iterables.filter(naturalEndpoints, isLocalDc));
            pendingEndpoints = ImmutableList.copyOf(Iterables.filter(pendingEndpoints, isLocalDc));
        }
        int participants = pendingEndpoints.size() + naturalEndpoints.size();
        int requiredParticipants = participants + 1  / 2; // See CASSANDRA-833
        List<InetAddress> liveEndpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), IAsyncCallback.isAlive));
        if (liveEndpoints.size() < requiredParticipants)
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, liveEndpoints.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (pendingEndpoints.size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", pendingEndpoints.size()),
                                           consistencyForPaxos,
                                           participants + 1,
                                           liveEndpoints.size());

        return Pair.create(liveEndpoints, requiredParticipants);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static Pair<UUID, Integer> beginAndRepairPaxos(long start,
                                                           ByteBuffer key,
                                                           CFMetaData metadata,
                                                           List<InetAddress> liveEndpoints,
                                                           int requiredParticipants,
                                                           ConsistencyLevel consistencyForPaxos,
                                                           ConsistencyLevel consistencyForCommit,
                                                           final boolean isWrite,
                                                           ClientState state)
    throws WriteTimeoutException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - start < timeout)
        {
            // We don't want to use a timestamp that is older than the last one assigned by the ClientState or operations
            // may appear out-of-order (#7801). But note that state.getTimestamp() is in microseconds while the ballot
            // timestamp is only in milliseconds
            long currentTime = (state.getTimestamp() / 1000) + 1;
            long ballotMillis = summary == null
                              ? currentTime
                              : Math.max(currentTime, 1 + UUIDGen.unixTimestamp(summary.mostRecentInProgressCommit.ballot));
            UUID ballot = UUIDGen.getTimeUUID(ballotMillis);

            // prepare
            Tracing.trace("Preparing {}", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos);
            if (!summary.promised)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                continue;
            }

            Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            Commit mostRecent = summary.mostRecentCommit;

            // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
            // needs to be completed, so do it.
            if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                if(isWrite)
                    casWriteMetrics.unfinishedCommit.inc();
                else
                    casReadMetrics.unfinishedCommit.inc();
                Commit refreshedInProgress = Commit.newProposal(inProgress.key, ballot, inProgress.update);
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos))
                {
                    commitPaxos(refreshedInProgress, consistencyForCommit);
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit();
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            // We might commit this ballot and we want to ensure operations starting after this CAS succeed will be assigned
            // a timestamp greater that the one of this ballot, so operation order is preserved (#7801)
            state.updateLastTimestamp(ballotMillis * 1000);

            return Pair.create(ballot, contentions);
        }

        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.ksName)));
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(Commit commit, Iterable<InetAddress> replicas)
    {
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, commit, Commit.serializer);
        for (InetAddress target : replicas)
            MessagingService.instance().sendOneWay(message, target);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos)
    throws WriteTimeoutException
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.key, toPrepare.update.metadata(), requiredParticipants, consistencyForPaxos);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);
        callback.await();
        return callback;
    }

    private static boolean proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants, boolean timeoutIfPartial, ConsistencyLevel consistencyLevel)
    throws WriteTimeoutException
    {
        ProposeCallback callback = new ProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);

        return false;
    }

    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

        Token tk = StorageService.getPartitioner().getToken(proposal.key);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

        AbstractWriteResponseHandler responseHandler = null;
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
        }

        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                    MessagingService.instance().sendRR(message, destination, responseHandler);
                else
                    MessagingService.instance().sendOneWay(message, destination);
            }
        }

        if (shouldBlock)
            responseHandler.get();
    }
#endif


future<>
storage_proxy::mutate_locally(const mutation& m, clock_type::time_point timeout) {
    auto shard = _db.local().shard_of(m);
    return _db.invoke_on(shard, [s = global_schema_ptr(m.schema()), m = freeze(m), timeout] (database& db) -> future<> {
        return db.apply(s, m, timeout);
    });
}

future<>
storage_proxy::mutate_locally(const schema_ptr& s, const frozen_mutation& m, clock_type::time_point timeout) {
    auto shard = _db.local().shard_of(m);
    return _db.invoke_on(shard, [&m, gs = global_schema_ptr(s), timeout] (database& db) -> future<> {
        return db.apply(gs, m, timeout);
    });
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutations, clock_type::time_point timeout) {
    return do_with(std::move(mutations), [this, timeout] (std::vector<mutation>& pmut){
        return parallel_for_each(pmut.begin(), pmut.end(), [this, timeout] (const mutation& m) {
            return mutate_locally(m, timeout);
        });
    });
}

future<>
storage_proxy::mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                         tracing::trace_state_ptr trace_state) {
    _stats.received_counter_updates += mutations.size();
    return do_with(std::move(mutations), [this, cl, timeout, trace_state = std::move(trace_state)] (std::vector<frozen_mutation_and_schema>& update_ms) mutable {
        return parallel_for_each(update_ms, [this, cl, timeout, trace_state] (frozen_mutation_and_schema& fm_a_s) {
            return mutate_counter_on_leader_and_replicate(fm_a_s.s, std::move(fm_a_s.fm), cl, timeout, trace_state);
        });
    });
}

future<>
storage_proxy::mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation fm, db::consistency_level cl, clock_type::time_point timeout,
                                                      tracing::trace_state_ptr trace_state) {
    auto shard = _db.local().shard_of(fm);
    return _db.invoke_on(shard, [gs = global_schema_ptr(s), fm = std::move(fm), cl, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) {
        auto trace_state = gt.get();
        return db.apply_counter_update(gs, fm, timeout, trace_state).then([cl, timeout, trace_state] (mutation m) mutable {
            return service::get_local_storage_proxy().replicate_counter_from_leader(std::move(m), cl, std::move(trace_state), timeout);
        });
    });
}

future<>
storage_proxy::mutate_streaming_mutation(const schema_ptr& s, utils::UUID plan_id, const frozen_mutation& m, bool fragmented) {
    auto shard = _db.local().shard_of(m);
    return _db.invoke_on(shard, [&m, plan_id, fragmented, gs = global_schema_ptr(s)] (database& db) mutable -> future<> {
        return db.apply_streaming_mutation(gs, plan_id, m, fragmented);
    });
}


/**
 * Helper for create_write_response_handler, shared across mutate/mutate_atomically.
 * Both methods do roughly the same thing, with the latter intermixing batch log ops
 * in the logic.
 * Since ordering is (maybe?) significant, we need to carry some info across from here
 * to the hint method below (dead nodes).
 */
storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const mutation& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    auto keyspace_name = m.schema()->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> natural_endpoints = rs.get_natural_endpoints(m.token());
    std::vector<gms::inet_address> pending_endpoints =
        get_local_storage_service().get_token_metadata().pending_endpoints_for(m.token(), keyspace_name);

    slogger.trace("creating write handler for token: {} natural: {} pending: {}", m.token(), natural_endpoints, pending_endpoints);
    tracing::trace(tr_state, "Creating write handler for token: {} natural: {} pending: {}", m.token(), natural_endpoints ,pending_endpoints);

    // filter out naturale_endpoints from pending_endpoint if later is not yet updated during node join
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints] (gms::inet_address& p) {
        return boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    auto all = boost::range::join(natural_endpoints, pending_endpoints);

    if (std::find_if(all.begin(), all.end(), std::bind1st(std::mem_fn(&storage_proxy::cannot_hint), this)) != all.end()) {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        throw overloaded_exception(_total_hints_in_progress);
    }

    // filter live endpoints from dead ones
    std::unordered_set<gms::inet_address> live_endpoints;
    std::vector<gms::inet_address> dead_endpoints;
    live_endpoints.reserve(all.size());
    dead_endpoints.reserve(all.size());
    std::partition_copy(all.begin(), all.end(), std::inserter(live_endpoints, live_endpoints.begin()), std::back_inserter(dead_endpoints),
            std::bind1st(std::mem_fn(&gms::failure_detector::is_alive), &gms::get_local_failure_detector()));

    slogger.trace("creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);
    tracing::trace(tr_state, "Creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);

    db::assure_sufficient_live_nodes(cl, ks, live_endpoints, pending_endpoints);

    return create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), std::move(live_endpoints), pending_endpoints, std::move(dead_endpoints), std::move(tr_state));
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    std::unordered_set<gms::inet_address> endpoints(m.size());
    boost::copy(m | boost::adaptors::map_keys, std::inserter(endpoints, endpoints.begin()));
    auto mh = std::make_unique<per_destination_mutation>(m);

    slogger.trace("creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);
    tracing::trace(tr_state, "Creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);

    auto keyspace_name = mh->schema()->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);

    return create_write_response_handler(ks, cl, type, std::move(mh), std::move(endpoints), std::vector<gms::inet_address>(), std::vector<gms::inet_address>(), std::move(tr_state));
}

void
storage_proxy::hint_to_dead_endpoints(response_id_type id, db::consistency_level cl) {
    auto& h = *get_write_response_handler(id);

    size_t hints = hint_to_dead_endpoints(h._mutation_holder, h.get_dead_endpoints());

    if (cl == db::consistency_level::ANY) {
        // for cl==ANY hints are counted towards consistency
        h.signal(hints);
    }
}

template<typename Range, typename CreateWriteHandler>
future<std::vector<storage_proxy::unique_response_handler>> storage_proxy::mutate_prepare(const Range& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler create_handler) {
    // apply is used to convert exceptions to exceptional future
    return futurize<std::vector<storage_proxy::unique_response_handler>>::apply([this] (const Range& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler create_handler) {
        std::vector<unique_response_handler> ids;
        ids.reserve(std::distance(std::begin(mutations), std::end(mutations)));
        for (auto& m : mutations) {
            ids.emplace_back(*this, create_handler(m, cl, type));
        }
        return make_ready_future<std::vector<unique_response_handler>>(std::move(ids));
    }, mutations, cl, type, std::move(create_handler));
}

template<typename Range>
future<std::vector<storage_proxy::unique_response_handler>> storage_proxy::mutate_prepare(const Range& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    return mutate_prepare<>(mutations, cl, type, [this, tr_state = std::move(tr_state)] (const typename Range::value_type& m, db::consistency_level cl, db::write_type type) mutable {
        return create_write_response_handler(m, cl, type, tr_state);
    });
}

future<> storage_proxy::mutate_begin(std::vector<unique_response_handler> ids, db::consistency_level cl,
                                     stdx::optional<clock_type::time_point> timeout_opt) {
    return parallel_for_each(ids, [this, cl, timeout_opt] (unique_response_handler& protected_response) {
        auto response_id = protected_response.id;
        // it is better to send first and hint afterwards to reduce latency
        // but request may complete before hint_to_dead_endpoints() is called and
        // response_id handler will be removed, so we will have to do hint with separate
        // frozen_mutation copy, or manage handler live time differently.
        hint_to_dead_endpoints(response_id, cl);

        auto timeout = timeout_opt.value_or(clock_type::now() + std::chrono::milliseconds(_db.local().get_config().write_request_timeout_in_ms()));
        // call before send_to_live_endpoints() for the same reason as above
        auto f = response_wait(response_id, timeout);
        send_to_live_endpoints(protected_response.release(), timeout); // response is now running and it will either complete or timeout
        return std::move(f);
    });
}

// this function should be called with a future that holds result of mutation attempt (usually
// future returned by mutate_begin()). The future should be ready when function is called.
future<> storage_proxy::mutate_end(future<> mutate_result, utils::latency_counter lc, tracing::trace_state_ptr trace_state) {
    assert(mutate_result.available());
    _stats.write.mark(lc.stop().latency());
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency(), _stats.write.hist.count);
    }
    try {
        mutate_result.get();
        tracing::trace(trace_state, "Mutation successfully completed");
        return make_ready_future<>();
    } catch (no_such_keyspace& ex) {
        tracing::trace(trace_state, "Mutation failed: write to non existing keyspace: {}", ex.what());
        slogger.trace("Write to non existing keyspace: {}", ex.what());
        return make_exception_future<>(std::current_exception());
    } catch(mutation_write_timeout_exception& ex) {
        // timeout
        tracing::trace(trace_state, "Mutation failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
        slogger.debug("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
        _stats.write_timeouts.mark();
        return make_exception_future<>(std::current_exception());
    } catch (exceptions::unavailable_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: unavailable");
        _stats.write_unavailables.mark();
        slogger.trace("Unavailable");
        return make_exception_future<>(std::current_exception());
    }  catch(overloaded_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: overloaded");
        _stats.write_unavailables.mark();
        slogger.trace("Overloaded");
        return make_exception_future<>(std::current_exception());
    } catch (...) {
        tracing::trace(trace_state, "Mutation failed: unknown reason");
        throw;
    }
}

gms::inet_address storage_proxy::find_leader_for_counter_update(const mutation& m, db::consistency_level cl) {
    auto& ks = _db.local().find_keyspace(m.schema()->ks_name());
    auto live_endpoints = get_live_endpoints(ks, m.token());

    if (live_endpoints.empty()) {
        throw exceptions::unavailable_exception(cl, block_for(ks, cl), 0);
    }

    auto local_endpoints = boost::copy_range<std::vector<gms::inet_address>>(live_endpoints | boost::adaptors::filtered([&] (auto&& ep) {
        return db::is_local(ep);
    }));
    if (local_endpoints.empty()) {
        // FIXME: O(n log n) to get maximum
        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        snitch->sort_by_proximity(utils::fb_utilities::get_broadcast_address(), live_endpoints);
        return live_endpoints[0];
    } else {
        // FIXME: favour ourselves to avoid additional hop?
        static thread_local std::random_device rd;
        static thread_local std::default_random_engine re(rd());
        std::uniform_int_distribution<> dist(0, local_endpoints.size() - 1);
        return local_endpoints[dist(re)];
    }
}

template<typename Range>
future<> storage_proxy::mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate_counters cl={}", cl);
    mlogger.trace("counter mutations={}", mutations);


    // Choose a leader for each mutation
    std::unordered_map<gms::inet_address, std::vector<frozen_mutation_and_schema>> leaders;
    for (auto& m : mutations) {
        auto leader = find_leader_for_counter_update(m, cl);
        leaders[leader].emplace_back(frozen_mutation_and_schema { freeze(m), m.schema() });
        // FIXME: check if CL can be reached
    }

    // Forward mutations to the leaders chosen for them
    auto timeout = clock_type::now() + std::chrono::milliseconds(_db.local().get_config().counter_write_request_timeout_in_ms());
    auto my_address = utils::fb_utilities::get_broadcast_address();
    return parallel_for_each(leaders, [this, cl, timeout, tr_state = std::move(tr_state), my_address] (auto& endpoint_and_mutations) {
        auto endpoint = endpoint_and_mutations.first;

        // The leader receives a vector of mutations and processes them together,
        // so if there is a timeout we don't really know which one is to "blame"
        // and what to put in ks and cf fields of write timeout exception.
        // Let's just use the schema of the first mutation in a vector.
        auto handle_error = [this, sp = this->shared_from_this(), s = endpoint_and_mutations.second[0].s, cl] (std::exception_ptr exp) {
            auto& ks = _db.local().find_keyspace(s->ks_name());
            try {
                std::rethrow_exception(std::move(exp));
            } catch (rpc::timeout_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            } catch (timed_out_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            }
        };

        auto f = make_ready_future<>();
        if (endpoint == my_address) {
            f = this->mutate_counters_on_leader(std::move(endpoint_and_mutations.second), cl, timeout, tr_state);
        } else {
            auto& mutations = endpoint_and_mutations.second;
            auto fms = boost::copy_range<std::vector<frozen_mutation>>(mutations | boost::adaptors::transformed([] (auto& m) {
                return std::move(m.fm);
            }));

            auto& ms = netw::get_local_messaging_service();
            auto msg_addr = netw::messaging_service::msg_addr{ endpoint_and_mutations.first, 0 };
            tracing::trace(tr_state, "Enqueuing counter update to {}", msg_addr);
            f = ms.send_counter_mutation(msg_addr, timeout, std::move(fms), cl, tracing::make_trace_info(tr_state));
        }
        return f.handle_exception(std::move(handle_error));
    });
}

struct mutate_executor {
    static auto get() { return &storage_proxy::do_mutate; }
};
static thread_local auto mutate_stage = seastar::make_execution_stage("storage_proxy_mutate", mutate_executor::get());

/**
 * Use this method to have these Mutations applied
 * across all replicas. This method will take care
 * of the possibility of a replica being down and hint
 * the data across to some other replica.
 *
 * @param mutations the mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 * @param tr_state trace state handle
 */
future<> storage_proxy::mutate(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, bool raw_counters) {
    return mutate_stage(this, std::move(mutations), cl, std::move(tr_state), raw_counters);
}

future<> storage_proxy::do_mutate(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, bool raw_counters) {
    auto mid = raw_counters ? mutations.begin() : boost::range::partition(mutations, [] (auto&& m) {
        return m.schema()->is_counter();
    });
    return seastar::when_all_succeed(
        mutate_counters(boost::make_iterator_range(mutations.begin(), mid), cl, tr_state),
        mutate_internal(boost::make_iterator_range(mid, mutations.end()), cl, false, tr_state)
    );
}

future<> storage_proxy::replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                                      clock_type::time_point timeout) {
    // FIXME: do not send the mutation to itself, it has already been applied (it is not incorrect to do so, though)
    return mutate_internal(std::array<mutation, 1>{std::move(m)}, cl, true, std::move(tr_state), timeout);
}

/*
 * Range template parameter can either be range of 'mutation' or a range of 'std::unordered_map<gms::inet_address, mutation>'.
 * create_write_response_handler() has specialization for both types. The one for the former uses keyspace to figure out
 * endpoints to send mutation to, the one for the late uses enpoints that are used as keys for the map.
 */
template<typename Range>
future<>
storage_proxy::mutate_internal(Range mutations, db::consistency_level cl, bool counters, tracing::trace_state_ptr tr_state,
                               stdx::optional<clock_type::time_point> timeout_opt) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate cl={}", cl);
    mlogger.trace("mutations={}", mutations);

    // If counters is set it means that we are replicating counter shards. There
    // is no need for special handling anymore, since the leader has already
    // done its job, but we need to return correct db::write_type in case of
    // a timeout so that client doesn't attempt to retry the request.
    auto type = counters ? db::write_type::COUNTER
                         : (std::next(std::begin(mutations)) == std::end(mutations) ? db::write_type::SIMPLE : db::write_type::UNLOGGED_BATCH);
    utils::latency_counter lc;
    lc.start();

    return mutate_prepare(mutations, cl, type, tr_state).then([this, cl, timeout_opt] (std::vector<storage_proxy::unique_response_handler> ids) {
        return mutate_begin(std::move(ids), cl, timeout_opt);
    }).then_wrapped([p = shared_from_this(), lc, tr_state] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, std::move(tr_state));
    });
}

future<>
storage_proxy::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
    bool should_mutate_atomically, tracing::trace_state_ptr tr_state, bool raw_counters) {
    warn(unimplemented::cause::TRIGGERS);
#if 0
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);
        if (augmented != null) {
            return mutate_atomically(augmented, consistencyLevel);
        } else {
#endif
    if (should_mutate_atomically) {
        assert(!raw_counters);
        return mutate_atomically(std::move(mutations), cl, std::move(tr_state));
    }
    return mutate(std::move(mutations), cl, std::move(tr_state), raw_counters);
#if 0
    }
#endif
}

/**
 * See mutate. Adds additional steps before and after writing a batch.
 * Before writing the batch (but after doing availability check against the FD for the row replicas):
 *      write the entire batch to a batchlog elsewhere in the cluster.
 * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
 *
 * @param mutations the Mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 */
future<>
storage_proxy::mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state) {

    utils::latency_counter lc;
    lc.start();

    class context {
        storage_proxy& _p;
        std::vector<mutation> _mutations;
        db::consistency_level _cl;
        tracing::trace_state_ptr _trace_state;

        const utils::UUID _batch_uuid;
        const std::unordered_set<gms::inet_address> _batchlog_endpoints;

    public:
        context(storage_proxy & p, std::vector<mutation>&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state)
                : _p(p)
                , _mutations(std::move(mutations))
                , _cl(cl)
                , _trace_state(std::move(tr_state))
                , _batch_uuid(utils::UUID_gen::get_time_UUID())
                , _batchlog_endpoints(
                        [this]() -> std::unordered_set<gms::inet_address> {
                            auto local_addr = utils::fb_utilities::get_broadcast_address();
                            auto topology = service::get_storage_service().local().get_token_metadata().get_topology();
                            auto local_endpoints = topology.get_datacenter_racks().at(get_local_dc()); // note: origin copies, so do that here too...
                            auto local_rack = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(local_addr);
                            auto chosen_endpoints = db::get_batchlog_manager().local().endpoint_filter(local_rack, local_endpoints);

                            if (chosen_endpoints.empty()) {
                                if (_cl == db::consistency_level::ANY) {
                                    return {local_addr};
                                }
                                throw exceptions::unavailable_exception(db::consistency_level::ONE, 1, 0);
                            }
                            return chosen_endpoints;
                        }()) {
                tracing::trace(_trace_state, "Created a batch context");
                tracing::set_batchlog_endpoints(_trace_state, _batchlog_endpoints);
        }

        future<> send_batchlog_mutation(mutation m, db::consistency_level cl = db::consistency_level::ONE) {
            return _p.mutate_prepare<>(std::array<mutation, 1>{std::move(m)}, cl, db::write_type::BATCH_LOG, [this] (const mutation& m, db::consistency_level cl, db::write_type type) {
                auto& ks = _p._db.local().find_keyspace(m.schema()->ks_name());
                return _p.create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), _batchlog_endpoints, {}, {}, _trace_state);
            }).then([this, cl] (std::vector<unique_response_handler> ids) {
                return _p.mutate_begin(std::move(ids), cl);
            });
        }
        future<> sync_write_to_batchlog() {
            auto m = db::get_batchlog_manager().local().get_batch_log_mutation_for(_mutations, _batch_uuid, netw::messaging_service::current_version);
            tracing::trace(_trace_state, "Sending a batchlog write mutation");
            return send_batchlog_mutation(std::move(m));
        };
        future<> async_remove_from_batchlog() {
            // delete batch
            auto schema = _p._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
            auto key = partition_key::from_exploded(*schema, {uuid_type->decompose(_batch_uuid)});
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            mutation m(key, schema);
            m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));

            tracing::trace(_trace_state, "Sending a batchlog remove mutation");
            return send_batchlog_mutation(std::move(m), db::consistency_level::ANY).handle_exception([] (std::exception_ptr eptr) {
                slogger.error("Failed to remove mutations from batchlog: {}", eptr);
            });
        };

        future<> run() {
            return _p.mutate_prepare(_mutations, _cl, db::write_type::BATCH, _trace_state).then([this] (std::vector<unique_response_handler> ids) {
                return sync_write_to_batchlog().then([this, ids = std::move(ids)] () mutable {
                    tracing::trace(_trace_state, "Sending batch mutations");
                    return _p.mutate_begin(std::move(ids), _cl);
                }).then(std::bind(&context::async_remove_from_batchlog, this));
            });
        }
    };

    auto mk_ctxt = [this, tr_state] (std::vector<mutation> mutations, db::consistency_level cl) mutable {
      try {
          return make_ready_future<lw_shared_ptr<context>>(make_lw_shared<context>(*this, std::move(mutations), cl, std::move(tr_state)));
      } catch(...) {
          return make_exception_future<lw_shared_ptr<context>>(std::current_exception());
      }
    };

    return mk_ctxt(std::move(mutations), cl).then([this] (lw_shared_ptr<context> ctxt) {
        return ctxt->run().finally([ctxt]{});
    }).then_wrapped([p = shared_from_this(), lc, tr_state = std::move(tr_state)] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, std::move(tr_state));
    });
}

bool storage_proxy::cannot_hint(gms::inet_address target) {
    return _total_hints_in_progress > _max_hints_in_progress
            && (get_hints_in_progress_for(target) > 0 && should_hint(target));
}

future<> storage_proxy::send_to_endpoint(mutation m, gms::inet_address target, db::write_type type) {
    utils::latency_counter lc;
    lc.start();

    return mutate_prepare(std::array<mutation, 1>{std::move(m)}, db::consistency_level::ONE, type,
        [this, target] (const mutation& m, db::consistency_level cl, db::write_type type) {
            auto& ks = _db.local().find_keyspace(m.schema()->ks_name());
            return create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), {target}, {}, {}, nullptr);
        }).then([this] (std::vector<unique_response_handler> ids) {
            return mutate_begin(std::move(ids), db::consistency_level::ONE);
        }).then_wrapped([p = shared_from_this(), lc] (future<>&& f) {
            return p->mutate_end(std::move(f), lc, nullptr);
        });
}

/**
 * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
 * is not available.
 *
 * Note about hints:
 *
 * | Hinted Handoff | Consist. Level |
 * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
 * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
 * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 *
 * @throws OverloadedException if the hints cannot be written/enqueued
 */
 // returned future is ready when sent is complete, not when mutation is executed on all (or any) targets!
void storage_proxy::send_to_live_endpoints(storage_proxy::response_id_type response_id, clock_type::time_point timeout)
{
    // extra-datacenter replicas, grouped by dc
    std::unordered_map<sstring, std::vector<gms::inet_address>> dc_groups;
    std::vector<std::pair<const sstring, std::vector<gms::inet_address>>> local;
    local.reserve(3);

    auto handler_ptr = get_write_response_handler(response_id);
    auto& handler = *handler_ptr;

    for(auto dest: handler.get_targets()) {
        sstring dc = get_dc(dest);
        // read repair writes do not go through coordinator since mutations are per destination
        if (handler.read_repair_write() || dc == get_local_dc()) {
            local.emplace_back("", std::vector<gms::inet_address>({dest}));
        } else {
            dc_groups[dc].push_back(dest);
        }
    }

    auto all = boost::range::join(local, dc_groups);
    auto my_address = utils::fb_utilities::get_broadcast_address();

    // lambda for applying mutation locally
    auto lmutate = [handler_ptr, response_id, this, my_address, timeout] (lw_shared_ptr<const frozen_mutation> m) mutable {
        tracing::trace(handler_ptr->get_trace_state(), "Executing a mutation locally");
        auto s = handler_ptr->get_schema();
        return mutate_locally(std::move(s), *m, timeout).then([response_id, this, my_address, m, h = std::move(handler_ptr), p = shared_from_this()] {
            // make mutation alive until it is processed locally, otherwise it
            // may disappear if write timeouts before this future is ready
            got_response(response_id, my_address);
        });
    };

    // lambda for applying mutation remotely
    auto rmutate = [this, handler_ptr, timeout, response_id, my_address] (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward, const frozen_mutation& m) {
        auto& ms = netw::get_local_messaging_service();
        auto msize = m.representation().size();
        _stats.queued_write_bytes += msize;

        auto& tr_state = handler_ptr->get_trace_state();
        tracing::trace(tr_state, "Sending a mutation to /{}", coordinator);

        return ms.send_mutation(netw::messaging_service::msg_addr{coordinator, 0}, timeout, m,
                std::move(forward), my_address, engine().cpu_id(), response_id, tracing::make_trace_info(tr_state)).finally([this, p = shared_from_this(), h = std::move(handler_ptr), msize] {
            _stats.queued_write_bytes -= msize;
            unthrottle();
        });
    };

    // OK, now send and/or apply locally
    for (typename decltype(dc_groups)::value_type& dc_targets : all) {
        auto& forward = dc_targets.second;
        // last one in forward list is a coordinator
        auto coordinator = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();


        lw_shared_ptr<const frozen_mutation> m = handler.get_mutation_for(coordinator);

        if (!m || (handler.is_counter() && coordinator == my_address)) {
            got_response(response_id, coordinator);
        } else {
            if (!handler.read_repair_write()) {
                ++_stats.writes_attempts.get_ep_stat(coordinator);
            } else {
                ++_stats.read_repair_write_attempts.get_ep_stat(coordinator);
            }

            if (coordinator == my_address) {
                f = futurize<void>::apply(lmutate, std::move(m));
            } else {
                f = futurize<void>::apply(rmutate, coordinator, std::move(forward), *m);
            }
        }

        f.handle_exception([coordinator, p = shared_from_this()] (std::exception_ptr eptr) {
            ++p->_stats.writes_errors.get_ep_stat(coordinator);
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(timed_out_error&) {
                // from lmutate(). Ignore so that logs are not flooded
                // database total_writes_timedout counter was incremented.
            } catch(...) {
                slogger.error("exception during mutation write to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

// returns number of hints stored
template<typename Range>
size_t storage_proxy::hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets) noexcept
{
    return boost::count_if(targets | boost::adaptors::filtered(std::bind1st(std::mem_fn(&storage_proxy::should_hint), this)),
            std::bind(std::mem_fn(&storage_proxy::submit_hint), this, std::ref(mh), std::placeholders::_1));
}

size_t storage_proxy::get_hints_in_progress_for(gms::inet_address target) {
    auto it = _hints_in_progress.find(target);

    if (it == _hints_in_progress.end()) {
        return 0;
    }

    return it->second;
}

bool storage_proxy::submit_hint(std::unique_ptr<mutation_holder>& mh, gms::inet_address target)
{
    warn(unimplemented::cause::HINT);
    // local write that time out should be handled by LocalMutationRunnable
    assert(is_me(target));
    return false;
#if 0
    HintRunnable runnable = new HintRunnable(target)
    {
        public void runMayThrow()
        {
            int ttl = HintedHandOffManager.calculateHintTTL(mutation);
            if (ttl > 0)
            {
                slogger.debug("Adding hint for {}", target);
                writeHintForMutation(mutation, System.currentTimeMillis(), ttl, target);
                // Notify the handler only for CL == ANY
                if (responseHandler != null && responseHandler.consistencyLevel == ConsistencyLevel.ANY)
                    responseHandler.response(null);
            } else
            {
                slogger.debug("Skipped writing hint for {} (ttl {})", target, ttl);
            }
        }
    };

    return submitHint(runnable);
#endif
}

#if 0
    private static Future<Void> submitHint(HintRunnable runnable)
    {
        StorageMetrics.totalHintsInProgress.inc();
        getHintsInProgressFor(runnable.target).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    /**
     * @param now current time in milliseconds - relevant for hint replay handling of truncated CFs
     */
    public static void writeHintForMutation(Mutation mutation, long now, int ttl, InetAddress target)
    {
        assert ttl > 0;
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(target);
        assert hostId != null : "Missing host ID for " + target.getHostAddress();
        HintedHandOffManager.instance.hintFor(mutation, now, ttl, hostId).apply();
        StorageMetrics.totalHints.inc();
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, OverloadedException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getKeyspaceName(), cm.key(), localDataCenter, cm.consistency());

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String keyspaceName = cm.getKeyspaceName();
            AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();
            Token tk = StorageService.getPartitioner().getToken(cm.key());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

            rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, cm.consistency(), null, WriteType.COUNTER).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.COUNTER);

            Tracing.trace("Enqueuing counter update to {}", endpoint);
            MessagingService.instance().sendRR(cm.makeMutationMessage(), endpoint, responseHandler, false);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(String keyspaceName, ByteBuffer key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, key);
        if (endpoints.isEmpty())
            // TODO have a way to compute the consistency level
            throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

        List<InetAddress> localEndpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : endpoints)
        {
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);
        }
        if (localEndpoints.isEmpty())
        {
            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(ThreadLocalRandom.current().nextInt(localEndpoints.size()));
        }
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, callback, WriteType.COUNTER);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, null, WriteType.COUNTER);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Iterable<InetAddress> targets,
                                             final AbstractWriteResponseHandler responseHandler,
                                             final String localDataCenter)
    {
        return new DroppableRunnable(MessagingService.Verb.COUNTER_MUTATION)
        {
            @Override
            public void runMayThrow() throws OverloadedException, WriteTimeoutException
            {
                IMutation processed = SinkManager.processWriteRequest(mutation);
                if (processed == null)
                    return;

                assert processed instanceof CounterMutation;
                CounterMutation cm = (CounterMutation) processed;

                Mutation result = cm.apply();
                responseHandler.response(null);

                Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets),
                            ImmutableSet.of(FBUtilities.getBroadcastAddress()));
                if (!remotes.isEmpty())
                    sendToHintedEndpoints(result, remotes, responseHandler, localDataCenter);
            }
        };
    }

    private static boolean systemKeyspaceQuery(List<ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!cmd.ksName.equals(SystemKeyspace.NAME))
                return false;
        return true;
    }
#endif

future<> storage_proxy::schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state) {
    if (diffs.empty()) {
        return make_ready_future<>();
    }
    return mutate_internal(diffs | boost::adaptors::map_values, cl, false, std::move(trace_state));
}

class abstract_read_resolver {
protected:
    db::consistency_level _cl;
    size_t _targets_count;
    promise<> _done_promise; // all target responded
    bool _timedout = false; // will be true if request timeouts
    timer<storage_proxy::clock_type> _timeout;
    size_t _responses = 0;
    schema_ptr _schema;

    virtual void on_timeout() {}
    virtual size_t response_count() const = 0;
public:
    abstract_read_resolver(schema_ptr schema, db::consistency_level cl, size_t target_count, storage_proxy::clock_type::time_point timeout)
        : _cl(cl)
        , _targets_count(target_count)
        , _schema(std::move(schema))
    {
        _timeout.set_callback([this] {
            _timedout = true;
            _done_promise.set_exception(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), _targets_count, _responses != 0));
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    future<> done() {
        return _done_promise.get_future();
    }
    virtual void error(gms::inet_address ep, std::exception_ptr eptr) {
        sstring why;
        try {
            std::rethrow_exception(eptr);
        } catch (rpc::closed_error&) {
            return; // do not report connection closed exception, gossiper does that
        } catch (rpc::timeout_error&) {
            return; // do not report timeouts, the whole operation will timeout and be reported
        } catch(std::exception& e) {
            why = e.what();
        } catch(...) {
            why = "Unknown exception";
        }

        // do nothing other than log for now, request will timeout eventually
        slogger.error("Exception when communicating with {}: {}", ep, why);
    }
};

class digest_read_resolver : public abstract_read_resolver {
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<foreign_ptr<lw_shared_ptr<query::result>>, bool> _cl_promise; // cl is reached
    bool _cl_reported = false;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    std::vector<query::result_digest> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;

    virtual void on_timeout() override {
        if (!_cl_reported) {
            _cl_promise.set_exception(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _block_for, _data_result));
        }
        // we will not need them any more
        _data_result = foreign_ptr<lw_shared_ptr<query::result>>();
        _digest_results.clear();
    }
    virtual size_t response_count() const override {
        return _digest_results.size();
    }
public:
    digest_read_resolver(schema_ptr schema, db::consistency_level cl, size_t block_for, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, 0, timeout), _block_for(block_for) {}
    void add_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<query::result>> result) {
        if (!_timedout) {
            // if only one target was queried digest_check() will be skipped so we can also skip digest calculation
            _digest_results.emplace_back(_targets_count == 1 ? query::result_digest() : *result->digest());
            _last_modified = std::max(_last_modified, result->last_modified());
            if (!_data_result) {
                _data_result = std::move(result);
            }
            got_response(from);
        }
    }
    void add_digest(gms::inet_address from, query::result_digest digest, api::timestamp_type last_modified) {
        if (!_timedout) {
            _digest_results.emplace_back(std::move(digest));
            _last_modified = std::max(_last_modified, last_modified);
            got_response(from);
        }
    }
    bool digests_match() const {
        assert(response_count());
        if (response_count() == 1) {
            return true;
        }
        auto& first = *_digest_results.begin();
        return std::find_if(_digest_results.begin() + 1, _digest_results.end(), [&first] (query::result_digest digest) { return digest != first; }) == _digest_results.end();
    }
    bool waiting_for(gms::inet_address ep) {
        return db::is_datacenter_local(_cl) ? is_me(ep) || db::is_local(ep) : true;
    }
    void got_response(gms::inet_address ep) {
        if (!_cl_reported) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses >= _block_for && _data_result) {
                _cl_reported = true;
                _cl_promise.set_value(std::move(_data_result), digests_match());
            }
        }
        if (is_completed()) {
            _timeout.cancel();
            _done_promise.set_value();
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>, bool> has_cl() {
        return _cl_promise.get_future();
    }
    bool has_data() {
        return _data_result;
    }
    void add_wait_targets(size_t targets_count) {
        _targets_count += targets_count;
    }
    bool is_completed() {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }
};

class data_read_resolver : public abstract_read_resolver {
    struct reply {
        gms::inet_address from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        bool reached_end = false;
        reply(gms::inet_address from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        gms::inet_address from;
        stdx::optional<partition> par;
        bool reached_end;
        bool reached_partition_end;
        version(gms::inet_address from_, stdx::optional<partition> par_, bool reached_end, bool reached_partition_end)
                : from(std::move(from_)), par(std::move(par_)), reached_end(reached_end), reached_partition_end(reached_partition_end) {}
    };
    struct mutation_and_live_row_count {
        mutation mut;
        size_t live_row_count;
    };

    struct primary_key {
        dht::decorated_key partition;
        stdx::optional<clustering_key> clustering;

        class less_compare_clustering {
            bool _is_reversed;
            clustering_key::less_compare _ck_cmp;
        public:
            less_compare_clustering(const schema s, bool is_reversed)
                : _is_reversed(is_reversed), _ck_cmp(s) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                if (!b.clustering) {
                    return false;
                }
                if (!a.clustering) {
                    return true;
                }
                if (_is_reversed) {
                    return _ck_cmp(*b.clustering, *a.clustering);
                } else {
                    return _ck_cmp(*a.clustering, *b.clustering);
                }
            }
        };

        class less_compare {
            const schema& _schema;
            less_compare_clustering _ck_cmp;
        public:
            less_compare(const schema& s, bool is_reversed)
                : _schema(s), _ck_cmp(s, is_reversed) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                auto pk_result = a.partition.tri_compare(_schema, b.partition);
                if (pk_result) {
                    return pk_result < 0;
                }
                return _ck_cmp(a, b);
            }
        };
    };

    size_t _total_live_count = 0;
    uint32_t _max_live_count = 0;
    uint32_t _short_read_diff = 0;
    uint32_t _max_per_partition_live_count = 0;
    uint32_t _partition_count = 0;
    uint32_t _live_partition_count = 0;
    bool _increase_per_partition_limit = false;
    bool _all_reached_end = true;
    query::short_read _is_short_read;
    std::vector<reply> _data_results;
    std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>> _diffs;
private:
    virtual void on_timeout() override {
        // we will not need them any more
        _data_results.clear();
    }
    virtual size_t response_count() const override {
        return _data_results.size();
    }

    void register_live_count(const std::vector<version>& replica_versions, uint32_t reconciled_live_rows, uint32_t limit) {
        bool any_not_at_end = boost::algorithm::any_of(replica_versions, [] (const version& v) {
            return !v.reached_partition_end;
        });
        if (any_not_at_end && reconciled_live_rows < limit && limit - reconciled_live_rows > _short_read_diff) {
            _short_read_diff = limit - reconciled_live_rows;
            _max_per_partition_live_count = reconciled_live_rows;
        }
    }
    void find_short_partitions(const std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions,
                               uint32_t per_partition_limit, uint32_t row_limit, uint32_t partition_limit) {
        // Go through the partitions that weren't limited by the total row limit
        // and check whether we got enough rows to satisfy per-partition row
        // limit.
        auto partitions_left = partition_limit;
        auto rows_left = row_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                register_live_count(*pv, row_count, per_partition_limit);
            } else {
                break;
            }
            ++pv;
        }
    }

    static primary_key get_last_row(const schema& s, const partition& p, bool is_reversed) {
        class last_clustering_key final : public mutation_partition_visitor {
            stdx::optional<clustering_key> _last_ck;
            bool _is_reversed;
        public:
            explicit last_clustering_key(bool is_reversed) : _is_reversed(is_reversed) { }

            virtual void accept_partition_tombstone(tombstone) override { }
            virtual void accept_static_cell(column_id, atomic_cell_view) override { }
            virtual void accept_static_cell(column_id, collection_mutation_view) override { }
            virtual void accept_row_tombstone(const range_tombstone&) override { }
            virtual void accept_row(position_in_partition_view pos, const row_tombstone&, const row_marker&, is_dummy dummy, is_continuous) override {
                assert(!dummy);
                if (!_is_reversed || !_last_ck) {
                    _last_ck = pos.key();
                }
            }
            virtual void accept_row_cell(column_id id, atomic_cell_view) override { }
            virtual void accept_row_cell(column_id id, collection_mutation_view) override { }

            stdx::optional<clustering_key>&& release() {
                return std::move(_last_ck);
            }
        };

        last_clustering_key lck(is_reversed);
        p.mut().partition().accept(s, lck);
        return {p.mut().decorated_key(s), lck.release()};
    }

    // Returns the highest row sent by the specified replica, according to the schema and the direction of
    // the query.
    // versions is a table where rows are partitions in descending order and the columns identify the partition
    // sent by a particular replica.
    static primary_key get_last_row(const schema& s, bool is_reversed, const std::vector<std::vector<version>>& versions, uint32_t replica) {
        const partition* last_partition = nullptr;
        // Versions are in the reversed order.
        for (auto&& pv : versions) {
            const stdx::optional<partition>& p = pv[replica].par;
            if (p) {
                last_partition = &p.value();
                break;
            }
        }
        assert(last_partition);
        return get_last_row(s, *last_partition, is_reversed);
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, const query::read_command& cmd, uint32_t limit, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        auto mp = m.partition();
        auto&& ranges = cmd.slice.row_ranges(s, m.key());
        mp.compact_for_query(s, cmd.timestamp, ranges, is_reversed, limit);

        stdx::optional<clustering_key> ck;
        if (!mp.clustered_rows().empty()) {
            if (is_reversed) {
                ck = mp.clustered_rows().begin()->key();
            } else {
                ck = mp.clustered_rows().rbegin()->key();
            }
        }
        return primary_key { m.decorated_key(), ck };
    }

    static bool got_incomplete_information_in_partition(const schema& s, const primary_key& last_reconciled_row, const std::vector<version>& versions, bool is_reversed) {
        primary_key::less_compare_clustering ck_cmp(s, is_reversed);
        for (auto&& v : versions) {
            if (!v.par || v.reached_partition_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, *v.par, is_reversed);
            if (ck_cmp(replica_last_row, last_reconciled_row)) {
                return true;
            }
        }
        return false;
    }

    bool got_incomplete_information_across_partitions(const schema& s, const query::read_command& cmd,
                                                      const primary_key& last_reconciled_row, std::vector<mutation_and_live_row_count>& rp,
                                                      const std::vector<std::vector<version>>& versions, bool is_reversed) {
        bool short_reads_allowed = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        primary_key::less_compare cmp(s, is_reversed);
        stdx::optional<primary_key> shortest_read;
        auto num_replicas = versions[0].size();
        for (uint32_t i = 0; i < num_replicas; ++i) {
            if (versions.front()[i].reached_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, is_reversed, versions, i);
            if (cmp(replica_last_row, last_reconciled_row)) {
                if (short_reads_allowed) {
                    if (!shortest_read || cmp(replica_last_row, *shortest_read)) {
                        shortest_read = std::move(replica_last_row);
                    }
                } else {
                    return true;
                }
            }
        }

        // Short reads are allowed, trim the reconciled result.
        if (shortest_read) {
            _is_short_read = query::short_read::yes;

            // Prepare to remove all partitions past shortest_read
            auto it = rp.begin();
            for (; it != rp.end() && shortest_read->partition.less_compare(s, it->mut.decorated_key()); ++it) { }

            // Remove all clustering rows past shortest_read
            if (it != rp.end() && it->mut.decorated_key().equal(s, shortest_read->partition)) {
                if (!shortest_read->clustering) {
                    ++it;
                } else {
                    std::vector<query::clustering_range> ranges;
                    ranges.emplace_back(is_reversed ? query::clustering_range::make_starting_with(std::move(*shortest_read->clustering))
                                                    : query::clustering_range::make_ending_with(std::move(*shortest_read->clustering)));
                    it->live_row_count = it->mut.partition().compact_for_query(s, cmd.timestamp, ranges, is_reversed, query::max_rows);
                }
            }

            // Actually remove all partitions past shortest_read
            rp.erase(rp.begin(), it);

            // Update total live count and live partition count
            _live_partition_count = 0;
            _total_live_count = boost::accumulate(rp, uint32_t(0), [this] (uint32_t lc, const mutation_and_live_row_count& m_a_rc) {
                _live_partition_count += !!m_a_rc.live_row_count;
                return lc + m_a_rc.live_row_count;
            });
        }

        return false;
    }

    bool got_incomplete_information(const schema& s, const query::read_command& cmd, uint32_t original_row_limit, uint32_t original_per_partition_limit,
                            uint32_t original_partition_limit, std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions) {
        // We need to check whether the reconciled result contains all information from all available
        // replicas. It is possible that some of the nodes have returned less rows (because the limit
        // was set and they had some tombstones missing) than the others. In such cases we cannot just
        // merge all results and return that to the client as the replicas that returned less row
        // may have newer data for the rows they did not send than any other node in the cluster.
        //
        // This function is responsible for detecting whether such problem may happen. We get partition
        // and clustering keys of the last row that is going to be returned to the client and check if
        // it is in range of rows returned by each replicas that returned as many rows as they were
        // asked for (if a replica returned less rows it means it returned everything it has).
        auto is_reversed = cmd.slice.options.contains(query::partition_slice::option::reversed);

        auto rows_left = original_row_limit;
        auto partitions_left = original_partition_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left > !!row_count) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                if (original_per_partition_limit != query::max_rows) {
                    auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, original_per_partition_limit, is_reversed);
                    if (got_incomplete_information_in_partition(s, last_row, *pv, is_reversed)) {
                        _increase_per_partition_limit = true;
                        return true;
                    }
                }
            } else {
                auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, rows_left, is_reversed);
                return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
            }
            ++pv;
        }
        return false;
    }
public:
    data_read_resolver(schema_ptr schema, db::consistency_level cl, size_t targets_count, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, targets_count, timeout) {
        _data_results.reserve(targets_count);
    }
    void add_mutate_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (!_timedout) {
            _max_live_count = std::max(result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _timeout.cancel();
                _done_promise.set_value();
            }
        }
    }
    uint32_t max_live_count() const {
        return _max_live_count;
    }
    bool any_partition_short_read() const {
        return _short_read_diff > 0;
    }
    bool increase_per_partition_limit() const {
        return _increase_per_partition_limit;
    }
    uint32_t max_per_partition_live_count() const {
        return _max_per_partition_live_count;
    }
    uint32_t partition_count() const {
        return _partition_count;
    }
    uint32_t live_partition_count() const {
        return _live_partition_count;
    }
    bool all_reached_end() const {
        return _all_reached_end;
    }
    stdx::optional<reconcilable_result> resolve(schema_ptr schema, const query::read_command& cmd, uint32_t original_row_limit, uint32_t original_per_partition_limit,
            uint32_t original_partition_limit) {
        assert(_data_results.size());

        if (_data_results.size() == 1) {
            // if there is a result only from one node there is nothing to reconcile
            // should happen only for range reads since single key reads will not
            // try to reconcile for CL=ONE
            auto& p = _data_results[0].result;
            return reconcilable_result(p->row_count(), p->partitions(), p->is_short_read());
        }

        const auto& s = *schema;

        // return true if lh > rh
        auto cmp = [&s](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key(s);
                auto rhk = rh.result->partitions().back().mut().key(s);
                return lhk.ring_order_tri_compare(s, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        std::vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        for (auto& r : _data_results) {
            _is_short_read = _is_short_read || r.result->is_short_read();
            r.reached_end = !r.result->is_short_read() && r.result->row_count() < cmd.row_limit
                            && (cmd.partition_limit == query::max_partitions
                                || boost::range::count_if(r.result->partitions(), [] (const partition& p) {
                                    return p.row_count();
                                }) < cmd.partition_limit);
            _all_reached_end = _all_reached_end && r.reached_end;
        }

        do {
            // after this sort reply with largest key is at the beginning
            boost::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key(s);
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_targets_count);
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key(s).legacy_equal(s, max_key)) {
                    bool reached_partition_end = pit->row_count() < cmd.slice.partition_row_limit();
                    v.emplace_back(r.from, std::move(*pit), r.reached_end, reached_partition_end);
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, stdx::optional<partition>(), r.reached_end, true);
                }
            }

            boost::sort(v, [] (const version& x, const version& y) {
                return x.from < y.from;
            });
        } while(true);

        std::vector<mutation_and_live_row_count> reconciled_partitions;
        reconciled_partitions.reserve(versions.size());

        // reconcile all versions
        boost::range::transform(boost::make_iterator_range(versions.begin(), versions.end()), std::back_inserter(reconciled_partitions),
                                [this, schema, original_per_partition_limit] (std::vector<version>& v) {
            auto it = boost::range::find_if(v, [] (auto&& ver) {
                    return bool(ver.par);
            });
            auto m = boost::accumulate(v, mutation(it->par->mut().key(*schema), schema), [this, schema] (mutation& m, const version& ver) {
                if (ver.par) {
                    m.partition().apply(*schema, ver.par->mut().partition(), *schema);
                }
                return std::move(m);
            });
            auto live_row_count = m.live_row_count();
            _total_live_count += live_row_count;
            _live_partition_count += !!live_row_count;
            return mutation_and_live_row_count { std::move(m), live_row_count };
        });
        _partition_count = reconciled_partitions.size();

        bool has_diff = false;

        // calculate differences
        for (auto z : boost::combine(versions, reconciled_partitions)) {
            const mutation& m = z.get<1>().mut;
            for (const version& v : z.get<0>()) {
                auto diff = v.par
                          ? m.partition().difference(schema, v.par->mut().unfreeze(schema).partition())
                          : m.partition();
                auto it = _diffs[m.token()].find(v.from);
                std::experimental::optional<mutation> mdiff;
                if (!diff.empty()) {
                    has_diff = true;
                    mdiff = mutation(schema, m.decorated_key(), std::move(diff));
                }
                if (it == _diffs[m.token()].end()) {
                    _diffs[m.token()].emplace(v.from, std::move(mdiff));
                } else {
                    // should not really happen, but lets try to deal with it
                    if (mdiff) {
                        if (it->second) {
                            it->second.value().apply(std::move(mdiff.value()));
                        } else {
                            it->second = std::move(mdiff);
                        }
                    }
                }
            }
        }

        if (has_diff) {
            if (got_incomplete_information(*schema, cmd, original_row_limit, original_per_partition_limit,
                                           original_partition_limit, reconciled_partitions, versions)) {
                return {};
            }
            // filter out partitions with empty diffs
            for (auto it = _diffs.begin(); it != _diffs.end();) {
                if (boost::algorithm::none_of(it->second | boost::adaptors::map_values, std::mem_fn(&std::experimental::optional<mutation>::operator bool))) {
                    it = _diffs.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            _diffs.clear();
        }

        find_short_partitions(reconciled_partitions, versions, original_per_partition_limit, original_row_limit, original_partition_limit);

        bool allow_short_reads = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        if (allow_short_reads && _max_live_count >= original_row_limit && _total_live_count < original_row_limit && _total_live_count) {
            // We ended up with less rows than the client asked for (but at least one),
            // avoid retry and mark as short read instead.
            _is_short_read = query::short_read::yes;
        }

        // build reconcilable_result from reconciled data
        // traverse backwards since large keys are at the start
        std::vector<partition> vec;
        auto r = boost::accumulate(reconciled_partitions | boost::adaptors::reversed, std::ref(vec), [] (std::vector<partition>& a, const mutation_and_live_row_count& m_a_rc) {
            a.emplace_back(partition(m_a_rc.live_row_count, freeze(m_a_rc.mut)));
            return std::ref(a);
        });

        return reconcilable_result(_total_live_count, std::move(r.get()), _is_short_read);
    }
    auto total_live_count() const {
        return _total_live_count;
    }
    auto get_diffs_for_repair() {
        return std::move(_diffs);
    }
};

class abstract_read_executor : public enable_shared_from_this<abstract_read_executor> {
protected:
    using targets_iterator = std::vector<gms::inet_address>::iterator;
    using digest_resolver_ptr = ::shared_ptr<digest_read_resolver>;
    using data_resolver_ptr = ::shared_ptr<data_read_resolver>;
    using clock_type = storage_proxy::clock_type;

    schema_ptr _schema;
    shared_ptr<storage_proxy> _proxy;
    lw_shared_ptr<query::read_command> _cmd;
    lw_shared_ptr<query::read_command> _retry_cmd;
    dht::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    std::vector<gms::inet_address> _targets;
    promise<foreign_ptr<lw_shared_ptr<query::result>>> _result_promise;
    tracing::trace_state_ptr _trace_state;
    lw_shared_ptr<column_family> _cf;

public:
    abstract_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, size_t block_for,
            std::vector<gms::inet_address> targets, tracing::trace_state_ptr trace_state) :
                           _schema(std::move(s)), _proxy(std::move(proxy)), _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)), _trace_state(std::move(trace_state)),
                           _cf(std::move(cf)) {
        _proxy->_stats.reads++;
    }
    virtual ~abstract_read_executor() {
        _proxy->_stats.reads--;
    };

protected:
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> make_mutation_data_request(lw_shared_ptr<query::read_command> cmd, gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->_stats.mutation_data_read_attempts.get_ep_stat(ep);
        if (is_me(ep)) {
            tracing::trace(_trace_state, "read_mutation_data: querying locally");
            return _proxy->query_mutations_locally(_schema, cmd, _partition_range, _trace_state);
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_mutation_data: sending a message to /{}", ep);
            return ms.send_read_mutation_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *cmd, _partition_range).then([this, ep](reconcilable_result&& result, rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_mutation_data: got response from /{}", ep);
                return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(make_foreign(::make_lw_shared<reconcilable_result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> make_data_request(gms::inet_address ep, clock_type::time_point timeout, bool want_digest) {
        ++_proxy->_stats.data_read_attempts.get_ep_stat(ep);
        if (is_me(ep)) {
            tracing::trace(_trace_state, "read_data: querying locally");
            auto qrr = want_digest ? query::result_request::result_and_digest : query::result_request::only_result;
            return _proxy->query_result_local(_schema, _cmd, _partition_range, qrr, _trace_state);
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_data: sending a message to /{}", ep);
            auto da = want_digest ? query::digest_algorithm::MD5 : query::digest_algorithm::none;
            return ms.send_read_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd, _partition_range, da).then([this, ep](query::result&& result, rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_data: got response from /{}", ep);
                return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(make_foreign(::make_lw_shared<query::result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<query::result_digest, api::timestamp_type, cache_temperature> make_digest_request(gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->_stats.digest_read_attempts.get_ep_stat(ep);
        if (is_me(ep)) {
            tracing::trace(_trace_state, "read_digest: querying locally");
            return _proxy->query_result_local_digest(_schema, _cmd, _partition_range, _trace_state);
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_digest: sending a message to /{}", ep);
            return ms.send_read_digest(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd, _partition_range).then([this, ep] (query::result_digest d, rpc::optional<api::timestamp_type> t,
                    rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_digest: got response from /{}", ep);
                return make_ready_future<query::result_digest, api::timestamp_type, cache_temperature>(d, t ? t.value() : api::missing_timestamp, hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<> make_mutation_data_requests(lw_shared_ptr<query::read_command> cmd, data_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        return parallel_for_each(begin, end, [this, &cmd, resolver = std::move(resolver), timeout] (gms::inet_address ep) {
            return make_mutation_data_request(cmd, ep, timeout).then_wrapped([this, resolver, ep] (future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_mutate_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->_stats.mutation_data_read_completed.get_ep_stat(ep);
                } catch(...) {
                    ++_proxy->_stats.mutation_data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_data_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout, bool want_digest) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver), timeout, want_digest] (gms::inet_address ep) {
            return make_data_request(ep, timeout, want_digest).then_wrapped([this, resolver, ep] (future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->_stats.data_read_completed.get_ep_stat(ep);
                } catch(...) {
                    ++_proxy->_stats.data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_digest_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver), timeout] (gms::inet_address ep) {
            return make_digest_request(ep, timeout).then_wrapped([this, resolver, ep] (future<query::result_digest, api::timestamp_type, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<2>(v));
                    resolver->add_digest(ep, std::get<0>(v), std::get<1>(v));
                    ++_proxy->_stats.digest_read_completed.get_ep_stat(ep);
                } catch(...) {
                    ++_proxy->_stats.digest_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    virtual future<> make_requests(digest_resolver_ptr resolver, clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        auto want_digest = _targets.size() > 1;
        auto f_data = futurize_apply([&] { return make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest); });
        auto f_digest = futurize_apply([&] { return make_digest_requests(resolver, _targets.begin() + 1, _targets.end(), timeout); });
        return when_all_succeed(std::move(f_data), std::move(f_digest)).handle_exception([] (auto&&) { });
    }
    virtual void got_cl() {}
    uint32_t original_row_limit() const {
        return _cmd->row_limit;
    }
    uint32_t original_per_partition_row_limit() const {
        return _cmd->slice.partition_row_limit();
    }
    uint32_t original_partition_limit() const {
        return _cmd->partition_limit;
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout, lw_shared_ptr<query::read_command> cmd) {
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(_schema, cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        make_mutation_data_requests(cmd, data_resolver, _targets.begin(), _targets.end(), timeout).finally([exec]{});

        data_resolver->done().then_wrapped([this, exec, data_resolver, cmd = std::move(cmd), cl, timeout] (future<> f) {
            try {
                f.get();
                auto rr_opt = data_resolver->resolve(_schema, *cmd, original_row_limit(), original_per_partition_row_limit(), original_partition_limit()); // reconciliation happens here

                // We generate a retry if at least one node reply with count live columns but after merge we have less
                // than the total number of column we are interested in (which may be < count on a retry).
                // So in particular, if no host returned count live columns, we know it's not a short read.
                bool can_send_short_read = rr_opt && rr_opt->is_short_read() && rr_opt->row_count() > 0;
                if (rr_opt && (can_send_short_read || data_resolver->all_reached_end() || rr_opt->row_count() >= original_row_limit()
                               || data_resolver->live_partition_count() >= original_partition_limit())
                        && !data_resolver->any_partition_short_read()) {
                    auto result = ::make_foreign(::make_lw_shared(
                            to_data_query_result(std::move(*rr_opt), _schema, _cmd->slice, _cmd->row_limit, cmd->partition_limit)));
                    // wait for write to complete before returning result to prevent multiple concurrent read requests to
                    // trigger repair multiple times and to prevent quorum read to return an old value, even after a quorum
                    // another read had returned a newer value (but the newer value had not yet been sent to the other replicas)
                    _proxy->schedule_repair(data_resolver->get_diffs_for_repair(), _cl, _trace_state).then([this, result = std::move(result)] () mutable {
                        _result_promise.set_value(std::move(result));
                    }).handle_exception([this, exec] (std::exception_ptr eptr) {
                        try {
                            std::rethrow_exception(eptr);
                        } catch (mutation_write_timeout_exception&) {
                            // convert write error to read error
                            _result_promise.set_exception(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _block_for - 1, _block_for, true));
                        } catch (...) {
                            _result_promise.set_exception(std::current_exception());
                        }
                    });
                } else {
                    _proxy->_stats.read_retries++;
                    _retry_cmd = make_lw_shared<query::read_command>(*cmd);
                    // We asked t (= cmd->row_limit) live columns and got l (=data_resolver->total_live_count) ones.
                    // From that, we can estimate that on this row, for x requested
                    // columns, only l/t end up live after reconciliation. So for next
                    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
                    auto x = [](uint64_t t, uint64_t l) -> uint32_t {
                        auto ret = std::min(static_cast<uint64_t>(query::max_rows), l == 0 ? t + 1 : ((t * t) / l) + 1);
                        return static_cast<uint32_t>(ret);
                    };
                    if (data_resolver->any_partition_short_read() || data_resolver->increase_per_partition_limit()) {
                        // The number of live rows was bounded by the per partition limit.
                        auto new_limit = x(cmd->slice.partition_row_limit(), data_resolver->max_per_partition_live_count());
                        _retry_cmd->slice.set_partition_row_limit(new_limit);
                        _retry_cmd->row_limit = std::max(cmd->row_limit, data_resolver->partition_count() * new_limit);
                    } else {
                        // The number of live rows was bounded by the total row limit or partition limit.
                        if (cmd->partition_limit != query::max_partitions) {
                            _retry_cmd->partition_limit = x(cmd->partition_limit, data_resolver->live_partition_count());
                        }
                        if (cmd->row_limit != query::max_rows) {
                            _retry_cmd->row_limit = x(cmd->row_limit, data_resolver->total_live_count());
                        }
                    }

                    // We may be unable to send a single live row because of replicas bailing out too early.
                    // If that is the case disallow short reads so that we can make progress.
                    if (!data_resolver->total_live_count()) {
                        _retry_cmd->slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }

                    slogger.trace("Retrying query with command {} (previous is {})", *_retry_cmd, *cmd);
                    reconcile(cl, timeout, _retry_cmd);
                }
            } catch (...) {
                _result_promise.set_exception(std::current_exception());
            }
        });
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout) {
        reconcile(cl, timeout, _cmd);
    }

public:
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(storage_proxy::clock_type::time_point timeout) {
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_schema, _cl, _block_for, timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver, timeout).finally([exec]() {
            // hold on to executor until all queries are complete
        });

        digest_resolver->has_cl().then_wrapped([exec, digest_resolver, timeout] (future<foreign_ptr<lw_shared_ptr<query::result>>, bool> f) mutable {
            bool background_repair_check = false;
            try {
                exec->got_cl();

                foreign_ptr<lw_shared_ptr<query::result>> result;
                bool digests_match;
                std::tie(result, digests_match) = f.get(); // can throw

                if (digests_match) {
                    exec->_result_promise.set_value(std::move(result));
                    if (exec->_block_for < exec->_targets.size()) { // if there are more targets then needed for cl, check digest in background
                        background_repair_check = true;
                    }
                } else { // digest mismatch
                    if (is_datacenter_local(exec->_cl)) {
                        auto write_timeout = exec->_proxy->_db.local().get_config().write_request_timeout_in_ms() * 1000;
                        auto delta = int64_t(digest_resolver->last_modified()) - int64_t(exec->_cmd->read_timestamp);
                        if (std::abs(delta) <= write_timeout) {
                            exec->_proxy->_stats.global_read_repairs_canceled_due_to_concurrent_write++;
                            // if CL is local and non matching data is modified less then write_timeout ms ago do only local repair
                            auto i = boost::range::remove_if(exec->_targets, std::not1(std::cref(db::is_local)));
                            exec->_targets.erase(i, exec->_targets.end());
                        }
                    }
                    exec->reconcile(exec->_cl, timeout);
                    exec->_proxy->_stats.read_repair_repaired_blocking++;
                }
            } catch (...) {
                exec->_result_promise.set_exception(std::current_exception());
            }

            exec->_proxy->_stats.background_reads++;
            digest_resolver->done().then([exec, digest_resolver, timeout, background_repair_check] () mutable {
                if (background_repair_check && !digest_resolver->digests_match()) {
                    exec->_proxy->_stats.read_repair_repaired_background++;
                    exec->_result_promise = promise<foreign_ptr<lw_shared_ptr<query::result>>>();
                    exec->reconcile(exec->_cl, timeout);
                    return exec->_result_promise.get_future().discard_result();
                } else {
                    return make_ready_future<>();
                }
            }).handle_exception([] (std::exception_ptr eptr) {
                // ignore any failures during background repair
            }).then([exec] {
                exec->_proxy->_stats.background_reads--;
            });
        });

        return _result_promise.get_future();
    }

    lw_shared_ptr<column_family>& get_cf() {
        return _cf;
    }
};

class never_speculating_read_executor : public abstract_read_executor {
public:
    never_speculating_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, std::vector<gms::inet_address> targets, tracing::trace_state_ptr trace_state) :
                                        abstract_read_executor(std::move(s), std::move(cf), std::move(proxy), std::move(cmd), std::move(pr), cl, 0, std::move(targets), std::move(trace_state)) {
        _block_for = _targets.size();
    }
};

// this executor always asks for one additional data reply
class always_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest),
                        make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout)).discard_result();
    }
};

// this executor sends request to an additional replica after some time below timeout
class speculating_read_executor : public abstract_read_executor {
    timer<storage_proxy::clock_type> _speculate_timer;
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        _speculate_timer.set_callback([this, resolver, timeout] {
            if (!resolver->is_completed()) { // at the time the callback runs request may be completed already
                resolver->add_wait_targets(1); // we send one more request so wait for it too
                // FIXME: consider disabling for CL=*ONE
                bool want_digest = true;
                future<> f = resolver->has_data() ?
                        make_digest_requests(resolver, _targets.end() - 1, _targets.end(), timeout) :
                        make_data_requests(resolver, _targets.end() - 1, _targets.end(), timeout, want_digest);
                f.finally([exec = shared_from_this()]{});
            }
        });
        auto& sr = _schema->speculative_retry();
        auto t = (sr.get_type() == speculative_retry::type::PERCENTILE) ?
            std::min(_cf->get_coordinator_read_latency_percentile(sr.get_value()), std::chrono::milliseconds(_proxy->get_db().local().get_config().read_request_timeout_in_ms()/2)) :
            std::chrono::milliseconds(unsigned(sr.get_value()));
        _speculate_timer.arm(t);

        // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
        // that the last replica in our list is "extra."
        resolver->add_wait_targets(_targets.size() - 1);
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        if (_block_for < _targets.size() - 1) {
            // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a replica more
            // likely to reply; better to let RR fail than the entire query.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest),
                            make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout)).discard_result();
        } else {
            // not doing read repair; all replies are important, so it doesn't matter which nodes we
            // perform data reads against vs digest.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest),
                            make_digest_requests(resolver, _targets.begin() + 1, _targets.end() - 1, timeout)).discard_result();
        }
    }
    virtual void got_cl() override {
        _speculate_timer.cancel();
    }
};

class range_slice_read_executor : public never_speculating_read_executor {
public:
    using never_speculating_read_executor::never_speculating_read_executor;
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(storage_proxy::clock_type::time_point timeout) override {
        if (!service::get_local_storage_service().cluster_supports_digest_multipartition_reads()) {
            reconcile(_cl, timeout);
            return _result_promise.get_future();
        }
        return never_speculating_read_executor::execute(timeout);
    }
};

db::read_repair_decision storage_proxy::new_read_repair_decision(const schema& s) {
    double chance = _read_repair_chance(_urandom);
    if (s.read_repair_chance() > chance) {
        return db::read_repair_decision::GLOBAL;
    }

    if (s.dc_local_read_repair_chance() > chance) {
        return db::read_repair_decision::DC_LOCAL;
    }

    return db::read_repair_decision::NONE;
}

::shared_ptr<abstract_read_executor> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, tracing::trace_state_ptr trace_state) {
    const dht::token& token = pr.start()->value().token();
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    speculative_retry::type retry_type = schema->speculative_retry().get_type();
    gms::inet_address extra_replica;

    std::vector<gms::inet_address> all_replicas = get_live_sorted_endpoints(ks, token);
    db::read_repair_decision repair_decision = new_read_repair_decision(*schema);
    auto cf = _db.local().find_column_family(schema).shared_from_this();
    std::vector<gms::inet_address> target_replicas = db::filter_for_query(cl, ks, all_replicas, repair_decision,
            retry_type == speculative_retry::type::NONE ? nullptr : &extra_replica,
            _db.local().get_config().cache_hit_rate_read_balancing() ? &*cf : nullptr);

    slogger.trace("creating read executor for token {} with all: {} targets: {} rp decision: {}", token, all_replicas, target_replicas, repair_decision);
    tracing::trace(trace_state, "Creating read executor for token {} with all: {} targets: {} repair decision: {}", token, all_replicas, target_replicas, repair_decision);

    // Throw UAE early if we don't have enough replicas.
    try {
        db::assure_sufficient_live_nodes(cl, ks, target_replicas);
    } catch (exceptions::unavailable_exception& ex) {
        slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
        _stats.read_unavailables.mark();
        throw;
    }

    if (repair_decision != db::read_repair_decision::NONE) {
        _stats.read_repair_attempts++;
    }

#if 0
    ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);
#endif

    size_t block_for = db::block_for(ks, cl);
    auto p = shared_from_this();
    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || block_for == all_replicas.size()
            || (repair_decision == db::read_repair_decision::DC_LOCAL && is_datacenter_local(cl) && block_for == target_replicas.size())) {
        return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state));
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    }

    // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
    if (target_replicas.size() == block_for) { // If RRD.DC_LOCAL extra replica may already be present
        if (is_datacenter_local(cl) && !db::is_local(extra_replica)) {
            slogger.trace("read executor no extra target to speculate");
            return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state));
        } else {
            target_replicas.push_back(extra_replica);
            slogger.trace("creating read executor with extra target {}", extra_replica);
        }
    }

    if (retry_type == speculative_retry::type::ALWAYS) {
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    } else {// PERCENTILE or CUSTOM.
        return ::make_shared<speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    }
}

future<query::result_digest, api::timestamp_type, cache_temperature>
storage_proxy::query_result_local_digest(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, tracing::trace_state_ptr trace_state, uint64_t max_size) {
    return query_result_local(std::move(s), std::move(cmd), pr, query::result_request::only_digest, std::move(trace_state), max_size).then([] (foreign_ptr<lw_shared_ptr<query::result>> result, cache_temperature hit_rate) {
        return make_ready_future<query::result_digest, api::timestamp_type, cache_temperature>(*result->digest(), result->last_modified(), hit_rate);
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>
storage_proxy::query_result_local(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, query::result_request request, tracing::trace_state_ptr trace_state, uint64_t max_size) {
    if (pr.is_singular()) {
        unsigned shard = _db.local().shard_of(pr.start()->value().token());
        return _db.invoke_on(shard, [max_size, gs = global_schema_ptr(s), prv = dht::partition_range_vector({pr}) /* FIXME: pr is copied */, cmd, request, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
            tracing::trace(gt, "Start querying the token range that starts with {}", seastar::value_of([&prv] { return prv.begin()->start()->value().token(); }));
            return db.query(gs, *cmd, request, prv, gt, max_size).then([trace_state = gt.get()](auto&& f, cache_temperature ht) {
                tracing::trace(trace_state, "Querying is done");
                return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(make_foreign(std::move(f)), ht);
            });
        });
    } else {
        return query_nonsingular_mutations_locally(s, cmd, {pr}, std::move(trace_state), max_size).then([s, cmd, request] (foreign_ptr<lw_shared_ptr<reconcilable_result>>&& r, cache_temperature&& ht) {
            return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(
                    ::make_foreign(::make_lw_shared(to_data_query_result(*r, s, cmd->slice,  cmd->row_limit, cmd->partition_limit, request))), ht);
        });
    }
}

void storage_proxy::handle_read_error(std::exception_ptr eptr, bool range) {
    try {
        std::rethrow_exception(eptr);
    } catch (read_timeout_exception& ex) {
        slogger.debug("Read timeout: received {} of {} required replies, data {}present", ex.received, ex.block_for, ex.data_present ? "" : "not ");
        if (range) {
            _stats.range_slice_timeouts.mark();
        } else {
            _stats.read_timeouts.mark();
        }
    } catch (...) {
        slogger.debug("Error during read query {}", eptr);
    }
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query_singular(lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges, db::consistency_level cl, tracing::trace_state_ptr trace_state) {
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    exec.reserve(partition_ranges.size());
    auto timeout = storage_proxy::clock_type::now() + std::chrono::milliseconds(_db.local().get_config().read_request_timeout_in_ms());

    for (auto&& pr: partition_ranges) {
        if (!pr.is_singular()) {
            throw std::runtime_error("mixed singular and non singular range are not supported");
        }
        exec.push_back(get_read_executor(cmd, std::move(pr), cl, trace_state));
    }

    query::result_merger merger(cmd->row_limit, cmd->partition_limit);
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        utils::latency_counter lc;
        lc.start();
        return rex->execute(timeout).finally([lc, rex] () mutable {
            if (lc.is_start()) {
                rex->get_cf()->add_coordinator_read_latency(lc.stop().latency());
            }
        });
    }, std::move(merger));

    return f.handle_exception([exec = std::move(exec), p = shared_from_this()] (std::exception_ptr eptr) {
        // hold onto exec until read is complete
        p->handle_read_error(eptr, false);
        return make_exception_future<foreign_ptr<lw_shared_ptr<query::result>>>(eptr);
    });
}

future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>
storage_proxy::query_partition_key_range_concurrent(storage_proxy::clock_type::time_point timeout, std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
        lw_shared_ptr<query::read_command> cmd, db::consistency_level cl, dht::partition_range_vector::iterator&& i,
        dht::partition_range_vector&& ranges, int concurrency_factor, tracing::trace_state_ptr trace_state,
        uint32_t remaining_row_count, uint32_t remaining_partition_count) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    auto concurrent_fetch_starting_index = i;
    auto p = shared_from_this();
    auto& cf= _db.local().find_column_family(schema);
    auto pcf = _db.local().get_config().cache_hit_rate_read_balancing() ? &cf : nullptr;


    while (i != ranges.end() && std::distance(concurrent_fetch_starting_index, i) < concurrency_factor) {
        dht::partition_range& range = *i;
        std::vector<gms::inet_address> live_endpoints = get_live_sorted_endpoints(ks, end_token(range));
        std::vector<gms::inet_address> filtered_endpoints = filter_for_query(cl, ks, live_endpoints, pcf);
        ++i;

        // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
        // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
        // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
        while (i != ranges.end())
        {
            dht::partition_range& next_range = *i;
            std::vector<gms::inet_address> next_endpoints = get_live_sorted_endpoints(ks, end_token(next_range));
            std::vector<gms::inet_address> next_filtered_endpoints = filter_for_query(cl, ks, next_endpoints, pcf);

            // Origin has this to say here:
            // *  If the current range right is the min token, we should stop merging because CFS.getRangeSlice
            // *  don't know how to deal with a wrapping range.
            // *  Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
            // *  the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
            // *  wire compatibility, so It's likely easier not to bother;
            // It obviously not apply for us(?), but lets follow origin for now
            if (end_token(range) == dht::maximum_token()) {
                break;
            }

            std::vector<gms::inet_address> merged = intersection(live_endpoints, next_endpoints);

            // Check if there is enough endpoint for the merge to be possible.
            if (!is_sufficient_live_nodes(cl, ks, merged)) {
                break;
            }

            std::vector<gms::inet_address> filtered_merged = filter_for_query(cl, ks, merged, pcf);

            // Estimate whether merging will be a win or not
            if (!locator::i_endpoint_snitch::get_local_snitch_ptr()->is_worth_merging_for_range_query(filtered_merged, filtered_endpoints, next_filtered_endpoints)) {
                break;
            } else if (pcf) {
                // check that merged set hit rate is not to low
                auto find_min = [pcf] (const std::vector<gms::inet_address>& range) {
                    struct {
                        column_family* cf = nullptr;
                        float operator()(const gms::inet_address& ep) const {
                            return float(cf->get_hit_rate(ep).rate);
                        }
                    } ep_to_hr{pcf};
                    return *boost::range::min_element(range | boost::adaptors::transformed(ep_to_hr));
                };
                auto merged = find_min(filtered_merged) * 1.2; // give merged set 20% boost
                if (merged < find_min(filtered_endpoints) && merged < find_min(next_filtered_endpoints)) {
                    // if lowest cache hits rate of a merged set is smaller than lowest cache hit
                    // rate of un-merged sets then do not merge. The idea is that we better issue
                    // two different range reads with highest chance of hitting a cache then one read that
                    // will cause more IO on contacted nodes
                    break;
                }
            }

            // If we get there, merge this range and the next one
            range = dht::partition_range(range.start(), next_range.end());
            live_endpoints = std::move(merged);
            filtered_endpoints = std::move(filtered_merged);
            ++i;
        }
        slogger.trace("creating range read executor with targets {}", filtered_endpoints);
        try {
            db::assure_sufficient_live_nodes(cl, ks, filtered_endpoints);
        } catch(exceptions::unavailable_exception& ex) {
            slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
            _stats.range_slice_unavailables.mark();
            throw;
        }

        exec.push_back(::make_shared<range_slice_read_executor>(schema, cf.shared_from_this(), p, cmd, std::move(range), cl, std::move(filtered_endpoints), trace_state));
    }

    query::result_merger merger(cmd->row_limit, cmd->partition_limit);
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.then([p, exec = std::move(exec), results = std::move(results), i = std::move(i), ranges = std::move(ranges),
                   cl, cmd, concurrency_factor, timeout, remaining_row_count, remaining_partition_count, trace_state = std::move(trace_state)]
                   (foreign_ptr<lw_shared_ptr<query::result>>&& result) mutable {
        result->ensure_counts();
        remaining_row_count -= result->row_count().value();
        remaining_partition_count -= result->partition_count().value();
        results.emplace_back(std::move(result));
        if (i == ranges.end() || !remaining_row_count || !remaining_partition_count) {
            return make_ready_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>(std::move(results));
        } else {
            cmd->row_limit = remaining_row_count;
            cmd->partition_limit = remaining_partition_count;
            return p->query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, std::move(i),
                    std::move(ranges), concurrency_factor * 2, std::move(trace_state), remaining_row_count, remaining_partition_count);
        }
    }).handle_exception([p] (std::exception_ptr eptr) {
        p->handle_read_error(eptr, true);
        return make_exception_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>(eptr);
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query_partition_key_range(lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, db::consistency_level cl, tracing::trace_state_ptr trace_state) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    dht::partition_range_vector ranges;
    auto timeout = storage_proxy::clock_type::now() + std::chrono::milliseconds(_db.local().get_config().read_request_timeout_in_ms());

    // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
    // expensive in clusters with vnodes)
    if (ks.get_replication_strategy().get_type() == locator::replication_strategy_type::local) {
        ranges = std::move(partition_ranges);
    } else {
        for (auto&& r : partition_ranges) {
            auto restricted_ranges = get_restricted_ranges(*schema, std::move(r));
            std::move(restricted_ranges.begin(), restricted_ranges.end(), std::back_inserter(ranges));
        }
    }

    // estimate_result_rows_per_range() is currently broken, and this is not needed
    // when paging is available in any case
#if 0
    // our estimate of how many result rows there will be per-range
    float result_rows_per_range = estimate_result_rows_per_range(cmd, ks);
    // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
    // fetch enough rows in the first round
    result_rows_per_range -= result_rows_per_range * CONCURRENT_SUBREQUESTS_MARGIN;
    int concurrency_factor = result_rows_per_range == 0.0 ? 1 : std::max(1, std::min(int(ranges.size()), int(std::ceil(cmd->row_limit / result_rows_per_range))));
#else
    int result_rows_per_range = 0;
    int concurrency_factor = 1;
#endif

    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;
    results.reserve(ranges.size()/concurrency_factor + 1);
    slogger.debug("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
            result_rows_per_range, cmd->row_limit, ranges.size(), concurrency_factor);

    return query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, ranges.begin(), std::move(ranges), concurrency_factor,
                                                std::move(trace_state), cmd->row_limit, cmd->partition_limit)
            .then([row_limit = cmd->row_limit, partition_limit = cmd->partition_limit](std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results) {
        query::result_merger merger(row_limit, partition_limit);
        merger.reserve(results.size());

        for (auto&& r: results) {
            merger(std::move(r));
        }

        return merger.get();
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl, tracing::trace_state_ptr trace_state)
{
    if (slogger.is_enabled(logging::log_level::trace) || qlogger.is_enabled(logging::log_level::trace)) {
        static thread_local int next_id = 0;
        auto query_id = next_id++;

        slogger.trace("query {}.{} cmd={}, ranges={}, id={}", s->ks_name(), s->cf_name(), *cmd, partition_ranges, query_id);
        return do_query(s, cmd, std::move(partition_ranges), cl, std::move(trace_state)).then([query_id, cmd, s] (foreign_ptr<lw_shared_ptr<query::result>>&& res) {
            if (res->buf().is_linearized()) {
                res->ensure_counts();
                slogger.trace("query_result id={}, size={}, rows={}, partitions={}", query_id, res->buf().size(), *res->row_count(), *res->partition_count());
            } else {
                slogger.trace("query_result id={}, size={}", query_id, res->buf().size());
            }
            qlogger.trace("id={}, {}", query_id, res->pretty_printer(s, cmd->slice));
            return std::move(res);
        });
    }

    return do_query(s, cmd, std::move(partition_ranges), cl, std::move(trace_state));
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::do_query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    tracing::trace_state_ptr trace_state)
{
    static auto make_empty = [] {
        return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>>(make_foreign(make_lw_shared<query::result>()));
    };

    auto& slice = cmd->slice;
    if (partition_ranges.empty() ||
            (slice.default_row_ranges().empty() && !slice.get_specific_ranges())) {
        return make_empty();
    }
    utils::latency_counter lc;
    lc.start();
    auto p = shared_from_this();

    if (query::is_single_partition(partition_ranges[0])) { // do not support mixed partitions (yet?)
        try {
            return query_singular(cmd, std::move(partition_ranges), cl, std::move(trace_state)).finally([lc, p] () mutable {
                    p->_stats.read.mark(lc.stop().latency());
                    if (lc.is_start()) {
                        p->_stats.estimated_read.add(lc.latency(), p->_stats.read.hist.count);
                    }
            });
        } catch (const no_such_column_family&) {
            _stats.read.mark(lc.stop().latency());
            return make_empty();
        }
    }

    return query_partition_key_range(cmd, std::move(partition_ranges), cl, std::move(trace_state)).finally([lc, p] () mutable {
        p->_stats.range.mark(lc.stop().latency());
        if (lc.is_start()) {
            p->_stats.estimated_range.add(lc.latency(), p->_stats.range.hist.count);
        }
    });
}

#if 0
    private static List<Row> readWithPaxos(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, ClientState state)
    throws InvalidRequestException, UnavailableException, ReadTimeoutException
    {
        assert state != null;

        long start = System.nanoTime();
        List<Row> rows = null;

        try
        {
            // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
            if (commands.size() > 1)
                throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one row at a time");
            ReadCommand command = commands.get(0);

            CFMetaData metadata = Schema.instance.getCFMetaData(command.ksName, command.cfName);
            Pair<List<InetAddress>, Integer> p = getPaxosParticipants(command.ksName, command.key, consistencyLevel);
            List<InetAddress> liveEndpoints = p.left;
            int requiredParticipants = p.right;

            // does the work of applying in-progress writes; throws UAE or timeout if it can't
            final ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                                                   ? ConsistencyLevel.LOCAL_QUORUM
                                                                                   : ConsistencyLevel.QUORUM;
            try
            {
                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, command.key, metadata, liveEndpoints, requiredParticipants, consistencyLevel, consistencyForCommitOrFetch, false, state);
                if (pair.right > 0)
                    casReadMetrics.contention.update(pair.right);
            }
            catch (WriteTimeoutException e)
            {
                throw new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(command.ksName)), false);
            }

            rows = fetchRows(commands, consistencyForCommitOrFetch);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            casReadMetrics.unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            ClientRequestMetrics.readTimeouts.inc();
            casReadMetrics.timeouts.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : commands)
                Keyspace.open(command.ksName).getColumnFamilyStore(command.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }

        return rows;
    }
#endif

std::vector<gms::inet_address> storage_proxy::get_live_endpoints(keyspace& ks, const dht::token& token) {
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> eps = rs.get_natural_endpoints(token);
    auto itend = boost::range::remove_if(eps, std::not1(std::bind1st(std::mem_fn(&gms::failure_detector::is_alive), &gms::get_local_failure_detector())));
    eps.erase(itend, eps.end());
    return std::move(eps);
}

std::vector<gms::inet_address> storage_proxy::get_live_sorted_endpoints(keyspace& ks, const dht::token& token) {
    auto eps = get_live_endpoints(ks, token);
    locator::i_endpoint_snitch::get_local_snitch_ptr()->sort_by_proximity(utils::fb_utilities::get_broadcast_address(), eps);
    // FIXME: before dynamic snitch is implement put local address (if present) at the beginning
    auto it = boost::range::find(eps, utils::fb_utilities::get_broadcast_address());
    if (it != eps.end() && it != eps.begin()) {
        std::iter_swap(it, eps.begin());
    }
    return eps;
}

std::vector<gms::inet_address> storage_proxy::intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2) {
    std::vector<gms::inet_address> inter;
    inter.reserve(l1.size());
    std::remove_copy_if(l1.begin(), l1.end(), std::back_inserter(inter), [&l2] (const gms::inet_address& a) {
        return std::find(l2.begin(), l2.end(), a) == l2.end();
    });
    return inter;
}

/**
 * Estimate the number of result rows (either cql3 rows or storage rows, as called for by the command) per
 * range in the ring based on our local data.  This assumes that ranges are uniformly distributed across the cluster
 * and that the queried data is also uniformly distributed.
 */
float storage_proxy::estimate_result_rows_per_range(lw_shared_ptr<query::read_command> cmd, keyspace& ks)
{
    return 1.0;
#if 0
    ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.columnFamily);
    float resultRowsPerRange = Float.POSITIVE_INFINITY;
    if (command.rowFilter != null && !command.rowFilter.isEmpty())
    {
        List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersForQuery(command.rowFilter);
        if (searchers.isEmpty())
        {
            resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
        }
        else
        {
            // Secondary index query (cql3 or otherwise).  Estimate result rows based on most selective 2ary index.
            for (SecondaryIndexSearcher searcher : searchers)
            {
                // use our own mean column count as our estimate for how many matching rows each node will have
                SecondaryIndex highestSelectivityIndex = searcher.highestSelectivityIndex(command.rowFilter);
                resultRowsPerRange = Math.min(resultRowsPerRange, highestSelectivityIndex.estimateResultRows());
            }
        }
    }
    else if (!command.countCQL3Rows())
    {
        // non-cql3 query
        resultRowsPerRange = cfs.estimateKeys();
    }
    else
    {
        resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
    }

    // adjust resultRowsPerRange by the number of tokens this node has and the replication factor for this ks
    return (resultRowsPerRange / DatabaseDescriptor.getNumTokens()) / keyspace.getReplicationStrategy().getReplicationFactor();
#endif
}

#if 0
    private static float calculateResultRowsUsingEstimatedKeys(ColumnFamilyStore cfs)
    {
        if (cfs.metadata.comparator.isDense())
        {
            // one storage row per result row, so use key estimate directly
            return cfs.estimateKeys();
        }
        else
        {
            float resultRowsPerStorageRow = ((float) cfs.getMeanColumns()) / cfs.metadata.regularColumns().size();
            return resultRowsPerStorageRow * (cfs.estimateKeys());
        }
    }

    private static List<Row> trim(AbstractRangeCommand command, List<Row> rows)
    {
        // When maxIsColumns, we let the caller trim the result.
        if (command.countCQL3Rows())
            return rows;
        else
            return rows.size() > command.limit() ? rows.subList(0, command.limit()) : rows;
    }
#endif

/**
 * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
 * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
 */
dht::partition_range_vector
storage_proxy::get_restricted_ranges(const schema& s, dht::partition_range range) {
    locator::token_metadata& tm = get_local_storage_service().get_token_metadata();
    return service::get_restricted_ranges(tm, s, std::move(range));
}

dht::partition_range_vector
get_restricted_ranges(locator::token_metadata& tm, const schema& s, dht::partition_range range) {
    dht::ring_position_comparator cmp(s);

    // special case for bounds containing exactly 1 token
    if (start_token(range) == end_token(range)) {
        if (start_token(range).is_minimum()) {
            return {};
        }
        return dht::partition_range_vector({std::move(range)});
    }

    dht::partition_range_vector ranges;

    auto add_range = [&ranges, &cmp] (dht::partition_range&& r) {
        ranges.emplace_back(std::move(r));
    };

    // divide the queryRange into pieces delimited by the ring
    auto ring_iter = tm.ring_range(range.start(), false);
    dht::partition_range remainder = std::move(range);
    for (const dht::token& upper_bound_token : ring_iter)
    {
        /*
         * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
         *   - if remainder is tokens, then we'll just split using the provided token.
         *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
         *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
         *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
         *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
         *     keys having 15 as token and B include none of those (since that is what our node owns).
         * asSplitValue() abstracts that choice.
         */

        dht::ring_position split_point(upper_bound_token, dht::ring_position::token_bound::end);
        if (!remainder.contains(split_point, cmp)) {
            break; // no more splits
        }

        {
            // We shouldn't attempt to split on upper bound, because it may result in
            // an ambiguous range of the form (x; x]
            if (end_token(remainder) == upper_bound_token) {
                break;
            }

            std::pair<dht::partition_range, dht::partition_range> splits =
                remainder.split(split_point, cmp);

            add_range(std::move(splits.first));
            remainder = std::move(splits.second);
        }
    }
    add_range(std::move(remainder));

    return ranges;
}

bool storage_proxy::should_hint(gms::inet_address ep) noexcept {
    if (is_me(ep)) { // do not hint to local address
        return false;
    }

    return false;
#if 0
    if (DatabaseDescriptor.shouldHintByDC())
    {
        final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
        //Disable DC specific hints
        if(!DatabaseDescriptor.hintedHandoffEnabled(dc))
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            return false;
        }
    }
    else if (!DatabaseDescriptor.hintedHandoffEnabled())
    {
        HintedHandOffManager.instance.metrics.incrPastWindow(ep);
        return false;
    }

    boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
    if (hintWindowExpired)
    {
        HintedHandOffManager.instance.metrics.incrPastWindow(ep);
        Tracing.trace("Not hinting {} which has been down {}ms", ep, Gossiper.instance.getEndpointDowntime(ep));
    }
    return !hintWindowExpired;
#endif
}

future<> storage_proxy::truncate_blocking(sstring keyspace, sstring cfname) {
    slogger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);

    auto& gossiper = gms::get_local_gossiper();

    if (!gossiper.get_unreachable_token_owners().empty()) {
        slogger.info("Cannot perform truncate, some hosts are down");
        // Since the truncate operation is so aggressive and is typically only
        // invoked by an admin, for simplicity we require that all nodes are up
        // to perform the operation.
        auto live_members = gossiper.get_live_members().size();

        throw exceptions::unavailable_exception(db::consistency_level::ALL,
                live_members + gossiper.get_unreachable_members().size(),
                live_members);
    }

    auto all_endpoints = gossiper.get_live_token_owners();
    auto& ms = netw::get_local_messaging_service();
    auto timeout = std::chrono::milliseconds(_db.local().get_config().truncate_request_timeout_in_ms());

    slogger.trace("Enqueuing truncate messages to hosts {}", all_endpoints);

    return parallel_for_each(all_endpoints, [keyspace, cfname, &ms, timeout](auto ep) {
        return ms.send_truncate(netw::messaging_service::msg_addr{ep, 0}, timeout, keyspace, cfname);
    }).handle_exception([cfname](auto ep) {
       try {
           std::rethrow_exception(ep);
       } catch (rpc::timeout_error& e) {
           slogger.trace("Truncation of {} timed out: {}", cfname, e.what());
       } catch (...) {
           throw;
       }
    });
}

#if 0
    public interface WritePerformer
    {
        public void apply(IMutation mutation,
                          Iterable<InetAddress> targets,
                          AbstractWriteResponseHandler responseHandler,
                          String localDataCenter,
                          ConsistencyLevel consistencyLevel) throws OverloadedException;
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.nanoTime();
        private final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {

            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - constructionTime) > DatabaseDescriptor.getTimeout(verb))
            {
                MessagingService.instance().incrementDroppedMessages(verb);
                return;
            }
            try
            {
                runMayThrow();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(MessagingService.Verb.MUTATION))
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.MUTATION);
                HintRunnable runnable = new HintRunnable(FBUtilities.getBroadcastAddress())
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final InetAddress target;

        protected HintRunnable(InetAddress target)
        {
            this.target = target;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                StorageMetrics.totalHintsInProgress.dec();
                getHintsInProgressFor(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.count();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.count();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            slogger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }
    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }


    public long getReadRepairAttempted() {
        return ReadRepairMetrics.attempted.count();
    }

    public long getReadRepairRepairedBlocking() {
        return ReadRepairMetrics.repairedBlocking.count();
    }

    public long getReadRepairRepairedBackground() {
        return ReadRepairMetrics.repairedBackground.count();
    }
#endif

void storage_proxy::init_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    ms.register_counter_mutation([] (const rpc::client_info& cinfo, rpc::opt_time_point t, std::vector<frozen_mutation> fms, db::consistency_level cl, stdx::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);

        tracing::trace_state_ptr trace_state_ptr;
        if (trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        return do_with(std::vector<frozen_mutation_and_schema>(),
                       [cl, src_addr, timeout = *t, fms = std::move(fms), trace_state_ptr = std::move(trace_state_ptr)] (std::vector<frozen_mutation_and_schema>& mutations) mutable {
            return parallel_for_each(std::move(fms), [&mutations, src_addr] (frozen_mutation& fm) {
                // FIXME: optimise for cases when all fms are in the same schema
                auto schema_version = fm.schema_version();
                return get_schema_for_write(schema_version, std::move(src_addr)).then([&mutations, fm = std::move(fm)] (schema_ptr s) mutable {
                    mutations.emplace_back(frozen_mutation_and_schema { std::move(fm), std::move(s) });
                });
            }).then([trace_state_ptr = std::move(trace_state_ptr), &mutations, cl, timeout] {
                auto sp = get_local_shared_storage_proxy();
                return sp->mutate_counters_on_leader(std::move(mutations), cl, timeout, std::move(trace_state_ptr));
            });
        });
    });
    ms.register_mutation([] (const rpc::client_info& cinfo, rpc::opt_time_point t, frozen_mutation in, std::vector<gms::inet_address> forward, gms::inet_address reply_to, unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<std::experimental::optional<tracing::trace_info>> trace_info) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);

        if (trace_info && *trace_info) {
            tracing::trace_info& tr_info = **trace_info;
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(tr_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        storage_proxy::clock_type::time_point timeout;
        if (!t) {
            auto timeout_in_ms = get_local_shared_storage_proxy()->_db.local().get_config().write_request_timeout_in_ms();
            timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        } else {
            timeout = *t;
        }

        return do_with(std::move(in), get_local_shared_storage_proxy(), [src_addr = std::move(src_addr), &cinfo, forward = std::move(forward), reply_to, shard, response_id, trace_state_ptr, timeout] (const frozen_mutation& m, shared_ptr<storage_proxy>& p) mutable {
            ++p->_stats.received_mutations;
            p->_stats.forwarded_mutations += forward.size();
            return when_all(
                // mutate_locally() may throw, putting it into apply() converts exception to a future.
                futurize<void>::apply([timeout, &p, &m, reply_to, src_addr = std::move(src_addr)] () mutable {
                    // FIXME: get_schema_for_write() doesn't timeout
                    return get_schema_for_write(m.schema_version(), std::move(src_addr)).then([&m, &p, timeout] (schema_ptr s) {
                        return p->mutate_locally(std::move(s), m, timeout);
                    });
                }).then([reply_to, shard, response_id, trace_state_ptr] () {
                    auto& ms = netw::get_local_messaging_service();
                    // We wait for send_mutation_done to complete, otherwise, if reply_to is busy, we will accumulate
                    // lots of unsent responses, which can OOM our shard.
                    //
                    // Usually we will return immediately, since this work only involves appending data to the connection
                    // send buffer.
                    tracing::trace(trace_state_ptr, "Sending mutation_done to /{}", reply_to);
                    return ms.send_mutation_done(netw::messaging_service::msg_addr{reply_to, shard}, shard, response_id).then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, &p] (std::exception_ptr eptr) {
                    seastar::log_level l = seastar::log_level::warn;
                    try {
                        std::rethrow_exception(eptr);
                    } catch (timed_out_error&) {
                        // ignore timeouts so that logs are not flooded.
                        // database total_writes_timedout counter was incremented.
                        l = seastar::log_level::debug;
                    } catch (...) {
                        // ignore
                    }
                    slogger.log(l, "Failed to apply mutation from {}#{}: {}", reply_to, shard, eptr);
                }),
                parallel_for_each(forward.begin(), forward.end(), [reply_to, shard, response_id, &m, &p, trace_state_ptr, timeout] (gms::inet_address forward) {
                    auto& ms = netw::get_local_messaging_service();
                    tracing::trace(trace_state_ptr, "Forwarding a mutation to /{}", forward);
                    return ms.send_mutation(netw::messaging_service::msg_addr{forward, 0}, timeout, m, {}, reply_to, shard, response_id, tracing::make_trace_info(trace_state_ptr)).then_wrapped([&p] (future<> f) {
                        if (f.failed()) {
                            ++p->_stats.forwarding_errors;
                        };
                        f.ignore_ready_future();
                    });
                })
            ).then_wrapped([trace_state_ptr] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore ressult, since we'll be returning them via MUTATION_DONE verbs
                tracing::trace(trace_state_ptr, "Mutation handling is done");
                return netw::messaging_service::no_wait();
            });
        });
    });
    ms.register_mutation_done([] (const rpc::client_info& cinfo, unsigned shard, storage_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return get_storage_proxy().invoke_on(shard, [from, response_id] (storage_proxy& sp) {
            sp.got_response(response_id, from);
            return netw::messaging_service::no_wait();
        });
    });
    ms.register_read_data([] (const rpc::client_info& cinfo, query::read_command cmd, compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_data: message received from /{}", src_addr.addr);
        }
        auto da = oda.value_or(query::digest_algorithm::MD5);
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr), get_local_shared_storage_proxy(), std::move(trace_state_ptr), [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), da, max_size] (compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
            p->_stats.replica_data_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, da, &pr, &p, &trace_state_ptr, max_size] (schema_ptr s) {
                auto pr2 = compat::unwrap(std::move(pr), *s);
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DATA called with wrapping range");
                }
                query::result_request qrr;
                switch (da) {
                case query::digest_algorithm::none:
                    qrr = query::result_request::only_result;
                    break;
                case query::digest_algorithm::MD5:
                    qrr = query::result_request::result_and_digest;
                    break;
                }
                return p->query_result_local(std::move(s), cmd, std::move(pr2.first), qrr, trace_state_ptr, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_data handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_read_mutation_data([] (const rpc::client_info& cinfo, query::read_command cmd, compat::wrapping_partition_range pr) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_mutation_data: message received from /{}", src_addr.addr);
        }
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr),
                       get_local_shared_storage_proxy(),
                       std::move(trace_state_ptr),
                       compat::one_or_two_partition_ranges({}),
                       [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), max_size] (
                               compat::wrapping_partition_range& pr,
                               shared_ptr<storage_proxy>& p,
                               tracing::trace_state_ptr& trace_state_ptr,
                               compat::one_or_two_partition_ranges& unwrapped) mutable {
            p->_stats.replica_mutation_data_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, &pr, &p, &trace_state_ptr, max_size, &unwrapped] (schema_ptr s) mutable {
                unwrapped = compat::unwrap(std::move(pr), *s);
                return p->query_mutations_locally(std::move(s), std::move(cmd), unwrapped, trace_state_ptr, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_mutation_data handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_read_digest([] (const rpc::client_info& cinfo, query::read_command cmd, compat::wrapping_partition_range pr) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_digest: message received from /{}", src_addr.addr);
        }
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr), get_local_shared_storage_proxy(), std::move(trace_state_ptr), [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), max_size] (compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
            p->_stats.replica_digest_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, &pr, &p, &trace_state_ptr, max_size] (schema_ptr s) {
                auto pr2 = compat::unwrap(std::move(pr), *s);
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DIGEST called with wrapping range");
                }
                return p->query_result_local_digest(std::move(s), cmd, std::move(pr2.first), trace_state_ptr, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_digest handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_truncate([](sstring ksname, sstring cfname) {
        return do_with(utils::make_joinpoint([] { return db_clock::now();}),
                        [ksname, cfname](auto& tsf) {
            return get_storage_proxy().invoke_on_all([ksname, cfname, &tsf](storage_proxy& sp) {
                return sp._db.local().truncate(ksname, cfname, [&tsf] { return tsf.value(); });
            });
        });
    });

    ms.register_get_schema_version([] (unsigned shard, table_schema_version v) {
        return get_storage_proxy().invoke_on(shard, [v] (auto&& sp) {
            slogger.debug("Schema version request for {}", v);
            return local_schema_registry().get_frozen(v);
        });
    });
}

void storage_proxy::uninit_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    ms.unregister_mutation();
    ms.unregister_mutation_done();
    ms.unregister_read_data();
    ms.unregister_read_mutation_data();
    ms.unregister_read_digest();
    ms.unregister_truncate();
}

// Merges reconcilable_result:s from different shards into one
// Drops partitions which exceed the limit.
class mutation_result_merger {
    schema_ptr _schema;
    lw_shared_ptr<const query::read_command> _cmd;
    unsigned _row_count = 0;
    unsigned _partition_count = 0;
    bool _short_read_allowed;
    // we get a batch of partitions each time, each with a key
    // partition batches should be maintained in key order
    // batches that share a key should be merged and sorted in decorated_key
    // order
    struct partitions_batch {
        std::vector<partition> partitions;
        query::short_read short_read;
    };
    std::multimap<unsigned, partitions_batch> _partitions;
    query::result_memory_accounter _memory_accounter;
    stdx::optional<unsigned> _stop_after_key;
public:
    explicit mutation_result_merger(schema_ptr schema, lw_shared_ptr<const query::read_command> cmd)
            : _schema(std::move(schema))
            , _cmd(std::move(cmd))
            , _short_read_allowed(_cmd->slice.options.contains(query::partition_slice::option::allow_short_read)) {
    }
    query::result_memory_accounter& memory() {
        return _memory_accounter;
    }
    const query::result_memory_accounter& memory() const {
        return _memory_accounter;
    }
    void add_result(unsigned key, foreign_ptr<lw_shared_ptr<reconcilable_result>> partial_result) {
        if (_stop_after_key && key > *_stop_after_key) {
            // A short result was added that goes before this one.
            return;
        }
        std::vector<partition> partitions;
        partitions.reserve(partial_result->partitions().size());
        // Following three lines to simplify patch; can remove later
        for (const partition& p : partial_result->partitions()) {
            partitions.push_back(p);
            _row_count += p._row_count;
            _partition_count += p._row_count > 0;
        }
        _memory_accounter.update(partial_result->memory_usage());
        if (partial_result->is_short_read()) {
            _stop_after_key = key;
        }
        _partitions.emplace(key, partitions_batch { std::move(partitions), partial_result->is_short_read() });
    }
    reconcilable_result get() && {
        auto unsorted = std::unordered_set<unsigned>();
        struct partitions_and_last_key {
            std::vector<partition> partitions;
            stdx::optional<dht::decorated_key> last; // set if we had a short read
        };
        auto merged = std::map<unsigned, partitions_and_last_key>();
        auto short_read = query::short_read(this->short_read());
        // merge batches with equal keys, and note if we need to sort afterwards
        for (auto&& key_value : _partitions) {
            auto&& key = key_value.first;
            if (_stop_after_key && key > *_stop_after_key) {
                break;
            }
            auto&& batch = key_value.second;
            auto&& dest = merged[key];
            if (dest.partitions.empty()) {
                dest.partitions = std::move(batch.partitions);
            } else {
                unsorted.insert(key);
                std::move(batch.partitions.begin(), batch.partitions.end(), std::back_inserter(dest.partitions));
            }
            // In case of a short read we need to remove all partitions from the
            // batch that come after the last partition of the short read
            // result.
            if (batch.short_read) {
                // Nobody sends a short read with no data.
                const auto& last = dest.partitions.back().mut().decorated_key(*_schema);
                if (!dest.last || last.less_compare(*_schema, *dest.last)) {
                    dest.last = last;
                }
                short_read = query::short_read::yes;
            }
        }

        // Sort batches that arrived with the same keys
        for (auto key : unsorted) {
            struct comparator {
                const schema& s;
                dht::decorated_key::less_comparator dkcmp;

                bool operator()(const partition& a, const partition& b) const {
                    return dkcmp(a.mut().decorated_key(s), b.mut().decorated_key(s));
                }
                bool operator()(const dht::decorated_key& a, const partition& b) const {
                    return dkcmp(a, b.mut().decorated_key(s));
                }
                bool operator()(const partition& a, const dht::decorated_key& b) const {
                    return dkcmp(a.mut().decorated_key(s), b);
                }
            };
            auto cmp = comparator { *_schema, dht::decorated_key::less_comparator(_schema) };

            auto&& batch = merged[key];
            boost::sort(batch.partitions, cmp);
            if (batch.last) {
                // This batch was built from a result that was a short read.
                // We need to remove all partitions that are after that short
                // read.
                auto it = boost::range::upper_bound(batch.partitions, std::move(*batch.last), cmp);
                batch.partitions.erase(it, batch.partitions.end());
            }
        }

        auto final = std::vector<partition>();
        final.reserve(_partition_count);
        for (auto&& batch : merged | boost::adaptors::map_values) {
            std::move(batch.partitions.begin(), batch.partitions.end(), std::back_inserter(final));
        }

        if (short_read) {
            // Short read row and partition counts may be incorrect, recalculate.
            _row_count = 0;
            _partition_count = 0;
            for (const auto& p : final) {
                _row_count += p.row_count();
                _partition_count += p.row_count() > 0;
            }

            if (_row_count >= _cmd->row_limit || _partition_count > _cmd->partition_limit) {
                // Even though there was a short read contributing to the final
                // result we got limited by total row limit or partition limit.
                // Note that we cannot with trivial check make unset short read flag
                // in case _partition_count == _cmd->partition_limit since the short
                // read may have caused the last partition to contain less rows
                // than asked for.
                short_read = query::short_read::no;
            }
        }

        // Trim back partition count and row count in case we overshot.
        // Should be rare for dense tables.
        while ((_partition_count > _cmd->partition_limit)
                || (_partition_count && (_row_count - final.back().row_count() >= _cmd->row_limit))) {
            _row_count -= final.back().row_count();
            _partition_count -= final.back().row_count() > 0;
            final.pop_back();
        }
        if (_row_count > _cmd->row_limit) {
            auto mut = final.back().mut().unfreeze(_schema);
            static const auto all = std::vector<query::clustering_range>({query::clustering_range::make_open_ended_both_sides()});
            auto is_reversed = _cmd->slice.options.contains(query::partition_slice::option::reversed);
            auto final_rows = _cmd->row_limit - (_row_count - final.back().row_count());
            _row_count -= final.back().row_count();
            auto rc = mut.partition().compact_for_query(*_schema, _cmd->timestamp, all, is_reversed, final_rows);
            final.back() = partition(rc, freeze(mut));
            _row_count += rc;
        }

        return reconcilable_result(_row_count, std::move(final), short_read, std::move(_memory_accounter).done());
    }
    bool short_read() const {
        return bool(_stop_after_key) || (_short_read_allowed && _row_count > 0 && _memory_accounter.check());
    }
    unsigned partition_count() const {
        return _partition_count;
    }
    unsigned row_count() const {
        return _row_count;
    }
};

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                       tracing::trace_state_ptr trace_state, uint64_t max_size) {
    if (pr.is_singular()) {
        unsigned shard = _db.local().shard_of(pr.start()->value().token());
        return _db.invoke_on(shard, [max_size, cmd, &pr, gs=global_schema_ptr(s), gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
          return db.get_result_memory_limiter().new_mutation_read(max_size).then([&] (query::result_memory_accounter ma) {
            return db.query_mutations(gs, *cmd, pr, std::move(ma), gt).then([] (reconcilable_result&& result, cache_temperature ht) {
                return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(make_foreign(make_lw_shared(std::move(result))), ht);
            });
          });
        });
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), {pr}, std::move(trace_state), max_size);
    }
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const compat::one_or_two_partition_ranges& pr,
                                       tracing::trace_state_ptr trace_state, uint64_t max_size) {
    if (!pr.second) {
        return query_mutations_locally(std::move(s), std::move(cmd), pr.first, std::move(trace_state), max_size);
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), pr, std::move(trace_state), max_size);
    }
}

}

namespace {

struct element_and_shard {
    unsigned element;   // element in a partition range vector
    unsigned shard;
};

bool operator==(element_and_shard a, element_and_shard b) {
    return a.element == b.element && a.shard == b.shard;
}

}

namespace std {

template <>
struct hash<element_and_shard> {
    size_t operator()(element_and_shard es) const {
        return es.element * 31 + es.shard;
    }
};

}

namespace service {

struct partition_range_and_sort_key {
    query::partition_range pr;
    unsigned sort_key_shard_order;   // for the same source partition range, we sort in shard order
};

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_nonsingular_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& prs,
                                                   tracing::trace_state_ptr trace_state, uint64_t max_size) {
    // no one permitted us to modify *cmd, so make a copy
    auto shard_cmd = make_lw_shared<query::read_command>(*cmd);
    auto range_count = static_cast<unsigned>(prs.size());
    return do_with(cmd,
            shard_cmd,
            0u,
            false,
            range_count,
            std::unordered_map<element_and_shard, partition_range_and_sort_key>{},
            mutation_result_merger{s, cmd},
            dht::ring_position_exponential_vector_sharder{std::move(prs)},
            global_schema_ptr(s),
            tracing::global_trace_state_ptr(std::move(trace_state)),
            cache_temperature(0.0f),
            [this, s, max_size] (lw_shared_ptr<query::read_command>& cmd,
                    lw_shared_ptr<query::read_command>& shard_cmd,
                    unsigned& mutation_result_merger_key,
                    bool& no_more_ranges,
                    unsigned& partition_range_count,
                    std::unordered_map<element_and_shard, partition_range_and_sort_key>& shards_for_this_iteration,
                    mutation_result_merger& mrm,
                    dht::ring_position_exponential_vector_sharder& rpevs,
                    global_schema_ptr& gs,
                    tracing::global_trace_state_ptr& gt,
                    cache_temperature& hit_rate) {
      return _db.local().get_result_memory_limiter().new_mutation_read(max_size).then([&, s] (query::result_memory_accounter ma) {
        mrm.memory() = std::move(ma);
        return repeat_until_value([&, s] () -> future<stdx::optional<std::pair<reconcilable_result, cache_temperature>>> {
            // We don't want to query a sparsely populated table sequentially, because the latency
            // will go through the roof.  We don't want to query a densely populated table in parallel,
            // because we'll throw away most of the results.  So we'll exponentially increase
            // concurrency starting at 1, so we won't waste on dense tables and at most
            // `log(nr_shards) + ignore_msb_bits` latency multiplier for near-empty tables.
            //
            // We use the ring_position_exponential_vector_sharder to give us subranges that follow
            // this scheme.
            shards_for_this_iteration.clear();
            // If we're reading from less than smp::count shards, then we can just append
            // each shard in order without sorting.  If we're reading from more, then
            // we'll read from some shards at least twice, so the partitions within will be
            // out-of-order wrt. other shards
            auto this_iteration_subranges = rpevs.next(*s);
            auto retain_shard_order = true;
            no_more_ranges = true;
            if (this_iteration_subranges) {
                no_more_ranges = false;
                retain_shard_order = this_iteration_subranges->inorder;
                auto sort_key = 0u;
                for (auto&& now : this_iteration_subranges->per_shard_ranges) {
                    shards_for_this_iteration.emplace(element_and_shard{this_iteration_subranges->element, now.shard}, partition_range_and_sort_key{now.ring_range, sort_key++});
                }
            }

            auto key_base = mutation_result_merger_key;

            // prepare for next iteration
            // Each iteration uses a merger key that is either i in the loop above (so in the range [0, shards_in_parallel),
            // or, the element index in prs (so in the range [0, partition_range_count).  Make room for sufficient keys.
            mutation_result_merger_key += std::max(smp::count, partition_range_count);

            shard_cmd->partition_limit = cmd->partition_limit - mrm.partition_count();
            shard_cmd->row_limit = cmd->row_limit - mrm.row_count();

            return parallel_for_each(shards_for_this_iteration, [&, key_base, retain_shard_order] (const std::pair<const element_and_shard, partition_range_and_sort_key>& elem_shard_range) {
                auto&& elem = elem_shard_range.first.element;
                auto&& shard = elem_shard_range.first.shard;
                auto&& range = elem_shard_range.second.pr;
                auto sort_key_shard_order = elem_shard_range.second.sort_key_shard_order;
                return _db.invoke_on(shard, [&, range, gt, fstate = mrm.memory().state_for_another_shard()] (database& db) {
                    query::result_memory_accounter accounter(db.get_result_memory_limiter(), std::move(fstate));
                    return db.query_mutations(gs, *shard_cmd, range, std::move(accounter), std::move(gt)).then([&hit_rate] (reconcilable_result&& rr, cache_temperature ht) {
                        hit_rate = ht;
                        return make_foreign(make_lw_shared(std::move(rr)));
                    });
                }).then([&, key_base, retain_shard_order, elem, sort_key_shard_order] (foreign_ptr<lw_shared_ptr<reconcilable_result>> partial_result) {
                    // Each outer (sequential) iteration is in result order, so we pick increasing keys.
                    // Within the inner (parallel) iteration, the results can be in order (if retain_shard_order), or not (if !retain_shard_order).
                    // If the results are unordered, we still have to order them according to which element of prs they originated from.
                    auto key = key_base; // for outer loop
                    if (retain_shard_order) {
                        key += sort_key_shard_order;  // inner loop is ordered
                    } else {
                        key += elem;  // inner loop ordered only by position within prs
                    }
                    mrm.add_result(key, std::move(partial_result));
                });
            }).then([&] () -> stdx::optional<std::pair<reconcilable_result, cache_temperature>> {
                if (mrm.short_read() || mrm.partition_count() >= cmd->partition_limit || mrm.row_count() >= cmd->row_limit || no_more_ranges) {
                    return stdx::make_optional(std::make_pair(std::move(mrm).get(), hit_rate));
                }
                return stdx::nullopt;
            });
        });
      });
    }).then([] (std::pair<reconcilable_result, cache_temperature>&& result) {
        return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(make_foreign(make_lw_shared(std::move(result.first))), result.second);
    });
}

future<>
storage_proxy::stop() {
    uninit_messaging_service();
    return make_ready_future<>();
}

}
