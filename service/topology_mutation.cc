/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "db/system_keyspace.hh"
#include "topology_mutation.hh"
#include "types/tuple.hh"
#include "types/types.hh"
#include "types/set.hh"
#include "types/map.hh"

namespace db {
    extern thread_local data_type cdc_generation_ts_id_type;
}

namespace service {

topology_mutation_builder::topology_mutation_builder(api::timestamp_type ts) :
        _s(db::system_keyspace::topology()),
        _m(_s, partition_key::from_singular(*_s, db::system_keyspace::TOPOLOGY)),
        _ts(ts) {
}

topology_node_mutation_builder::topology_node_mutation_builder(topology_mutation_builder& builder, raft::server_id id) :
        _builder(builder),
        _r(_builder._m.partition().clustered_row(*_builder._s, clustering_key::from_singular(*_builder._s, id.uuid()))) {
    _r.apply(row_marker(_builder._ts));
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::apply_atomic(const char* cell, const data_value& value) {
    const column_definition* cdef = self().schema().get_column_definition(cell);
    SCYLLA_ASSERT(cdef);
    self().row().apply(*cdef, atomic_cell::make_live(*cdef->type, self().timestamp(), cdef->type->decompose(value), self().ttl()));
    return self();
}

template<typename Builder>
template<std::ranges::range C>
requires std::convertible_to<std::ranges::range_value_t<C>, data_value>
Builder& topology_mutation_builder_base<Builder>::apply_set(const char* cell, collection_apply_mode apply_mode, const C& c) {
    const column_definition* cdef = self().schema().get_column_definition(cell);
    SCYLLA_ASSERT(cdef);
    auto vtype = static_pointer_cast<const set_type_impl>(cdef->type)->get_elements_type();

    std::set<bytes, serialized_compare> cset(vtype->as_less_comparator());
    for (const auto& v : c) {
        cset.insert(vtype->decompose(data_value(v)));
    }

    collection_mutation_description cm;
    cm.cells.reserve(cset.size());
    for (const bytes& raw : cset) {
        cm.cells.emplace_back(raw, atomic_cell::make_live(*bytes_type, self().timestamp(), bytes_view(), self().ttl()));
    }

    if (apply_mode == collection_apply_mode::overwrite) {
        cm.tomb = tombstone(self().timestamp() - 1, gc_clock::now());
    }

    self().row().apply(*cdef, cm.serialize(*cdef->type));
    return self();
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::del(const char* cell) {
    auto cdef = self().schema().get_column_definition(cell);
    SCYLLA_ASSERT(cdef);
    if (!cdef->type->is_multi_cell()) {
        self().row().apply(*cdef, atomic_cell::make_dead(self().timestamp(), gc_clock::now()));
    } else {
        collection_mutation_description cm;
        cm.tomb = tombstone{self().timestamp(), gc_clock::now()};
        self().row().apply(*cdef, cm.serialize(*cdef->type));
    }
    return self();
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, node_state value) {
    return apply_atomic(cell, sstring{::format("{}", value)});
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, topology_request value) {
    return apply_atomic(cell, sstring{::format("{}", value)});
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const sstring& value) {
    return apply_atomic(cell, value);
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const raft::server_id& value) {
    return apply_atomic(cell, value.uuid());
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const uint32_t& value) {
    return apply_atomic(cell, int32_t(value));
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, cleanup_status value) {
    return apply_atomic(cell, sstring{::format("{}", value)});
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const utils::UUID& value) {
    return apply_atomic(cell, value);
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, bool value) {
    return apply_atomic(cell, value);
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const char* value) {
    return apply_atomic(cell, value);
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::set(const char* cell, const db_clock::time_point& value) {
    return apply_atomic(cell, value);
}

row& topology_node_mutation_builder::row() {
    return _r.cells();
}

api::timestamp_type topology_node_mutation_builder::timestamp() const {
    return _builder._ts;
}

const schema& topology_node_mutation_builder::schema() const {
    return *_builder._s;
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const std::unordered_set<raft::server_id>& nodes_ids) {
    return apply_set(cell, collection_apply_mode::overwrite, nodes_ids | boost::adaptors::transformed([] (const auto& node_id) { return node_id.id; }));
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const std::unordered_set<dht::token>& tokens) {
    return apply_set(cell, collection_apply_mode::overwrite, tokens | boost::adaptors::transformed([] (const auto& t) { return t.to_sstring(); }));
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const std::set<sstring>& features) {
    return apply_set(cell, collection_apply_mode::overwrite, features | boost::adaptors::transformed([] (const auto& f) { return sstring(f); }));
}

canonical_mutation topology_node_mutation_builder::build() {
    return canonical_mutation{std::move(_builder._m)};
}

row& topology_mutation_builder::row() {
    return _m.partition().static_row().maybe_create();
}

api::timestamp_type topology_mutation_builder::timestamp() const {
    return _ts;
}

const schema& topology_mutation_builder::schema() const {
    return *_s;
}

topology_mutation_builder& topology_mutation_builder::set_transition_state(topology::transition_state value) {
    return apply_atomic("transition_state", ::format("{}", value));
}

topology_mutation_builder& topology_mutation_builder::set_version(topology::version_t value) {
    _m.set_static_cell("version", value, _ts);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::set_fence_version(topology::version_t value) {
    _m.set_static_cell("fence_version", value, _ts);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::set_session(session_id value) {
    _m.set_static_cell("session", value.uuid(), _ts);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::set_tablet_balancing_enabled(bool value) {
    _m.set_static_cell("tablet_balancing_enabled", value, _ts);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::del_transition_state() {
    return del("transition_state");
}

topology_mutation_builder& topology_mutation_builder::del_session() {
    return del("session");
}

topology_mutation_builder& topology_mutation_builder::set_new_cdc_generation_data_uuid(
        const utils::UUID& value) {
    return apply_atomic("new_cdc_generation_data_uuid", value);
}

topology_mutation_builder& topology_mutation_builder::set_committed_cdc_generations(const std::vector<cdc::generation_id_v2>& values) {
    auto dv = values | boost::adaptors::transformed([&] (const auto& v) {
        return make_tuple_value(db::cdc_generation_ts_id_type, tuple_type_impl::native_type({v.ts, timeuuid_native_type{v.id}}));
    });
    return apply_set("committed_cdc_generations", collection_apply_mode::overwrite, std::move(dv));
}

topology_mutation_builder& topology_mutation_builder::set_new_keyspace_rf_change_data(
        const sstring& ks_name, const std::map<sstring, sstring>& rf_per_dc) {
    apply_atomic("new_keyspace_rf_change_ks_name", ks_name);
    apply_atomic("new_keyspace_rf_change_data",
                 make_map_value(schema().get_column_definition("new_keyspace_rf_change_data")->type,
                                map_type_impl::native_type(rf_per_dc.begin(), rf_per_dc.end())));
    return *this;
}

topology_mutation_builder& topology_mutation_builder::set_unpublished_cdc_generations(const std::vector<cdc::generation_id_v2>& values) {
    auto dv = values | boost::adaptors::transformed([&] (const auto& v) {
        return make_tuple_value(db::cdc_generation_ts_id_type, tuple_type_impl::native_type({v.ts, timeuuid_native_type{v.id}}));
    });
    return apply_set("unpublished_cdc_generations", collection_apply_mode::overwrite, std::move(dv));
}

topology_mutation_builder& topology_mutation_builder::set_global_topology_request(global_topology_request value) {
    return apply_atomic("global_topology_request", ::format("{}", value));
}

topology_mutation_builder& topology_mutation_builder::set_global_topology_request_id(const utils::UUID& value) {
    return apply_atomic("global_topology_request_id", value);
}

topology_mutation_builder& topology_mutation_builder::set_upgrade_state(topology::upgrade_state_type value) {
    return apply_atomic("upgrade_state", ::format("{}", value));
}

topology_mutation_builder& topology_mutation_builder::add_enabled_features(const std::set<sstring>& features) {
    return apply_set("enabled_features", collection_apply_mode::update, features | boost::adaptors::transformed([] (const auto& f) { return sstring(f); }));
}

topology_mutation_builder& topology_mutation_builder::add_new_committed_cdc_generation(const cdc::generation_id_v2& value) {
    auto dv = make_tuple_value(db::cdc_generation_ts_id_type, tuple_type_impl::native_type({value.ts, timeuuid_native_type{value.id}}));
    apply_set("committed_cdc_generations", collection_apply_mode::update, std::vector<data_value>{dv});
    apply_set("unpublished_cdc_generations", collection_apply_mode::update, std::vector<data_value>{std::move(dv)});
    return *this;
}

topology_mutation_builder& topology_mutation_builder::add_ignored_nodes(const std::unordered_set<raft::server_id>& value) {
    return apply_set("ignore_nodes", collection_apply_mode::update, value | boost::adaptors::transformed([] (const auto& id) { return id.uuid(); }));
}

topology_mutation_builder& topology_mutation_builder::set_ignored_nodes(const std::unordered_set<raft::server_id>& value) {
    return apply_set("ignore_nodes", collection_apply_mode::overwrite, value | boost::adaptors::transformed([] (const auto& id) { return id.uuid(); }));
}

topology_mutation_builder& topology_mutation_builder::del_global_topology_request() {
    return del("global_topology_request");
}

topology_mutation_builder& topology_mutation_builder::del_global_topology_request_id() {
    return del("global_topology_request_id");
}

topology_node_mutation_builder& topology_mutation_builder::with_node(raft::server_id n) {
    _node_builder.emplace(*this, n);
    return *_node_builder;
}

topology_request_tracking_mutation_builder::topology_request_tracking_mutation_builder(utils::UUID id, bool set_type) :
        _s(db::system_keyspace::topology_requests()),
        _m(_s, partition_key::from_singular(*_s, id)),
        _ts(utils::UUID_gen::micros_timestamp(id)),
        _r(_m.partition().clustered_row(*_s, clustering_key::make_empty())),
        _set_type(set_type) {
    _r.apply(row_marker(_ts, *ttl(), gc_clock::now() + *ttl()));
}

ttl_opt topology_request_tracking_mutation_builder::ttl() const {
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::microseconds(_ts)) + std::chrono::months(1)
        - std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch());
}

const schema& topology_request_tracking_mutation_builder::schema() const {
    return *_s;
}

row& topology_request_tracking_mutation_builder::row() {
    return _r.cells();
}

api::timestamp_type topology_request_tracking_mutation_builder::timestamp() const {
    return _ts;
}

topology_request_tracking_mutation_builder& topology_request_tracking_mutation_builder::set(const char* cell, topology_request value) {
    return _set_type ? builder_base::set(cell, value) : *this;
}

topology_request_tracking_mutation_builder& topology_request_tracking_mutation_builder::done(std::optional<sstring> error) {
    set("end_time", db_clock::now());
    if (error) {
        set("error", *error);
    }
    return set("done", true);
}

template class topology_mutation_builder_base<topology_mutation_builder>;
template class topology_mutation_builder_base<topology_node_mutation_builder>;
template class topology_mutation_builder_base<topology_request_tracking_mutation_builder>;

} // namespace service
