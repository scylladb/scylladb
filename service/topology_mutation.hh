/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <cstdint>
#include <set>
#include <unordered_set>

#include <seastar/core/sstring.hh>

#include "dht/token.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "raft/server.hh"
#include "schema/schema.hh"
#include "service/topology_state_machine.hh"
#include "timestamp.hh"
#include "utils/UUID.hh"

namespace service {

template<typename Builder>
class topology_mutation_builder_base {
private:
    Builder& self() {
        return *static_cast<Builder*>(this);
    }

protected:
    enum class collection_apply_mode {
        overwrite,
        update,
    };

    using builder_base = topology_mutation_builder_base<Builder>;

    Builder& apply_atomic(const char* cell, const data_value& value);
    template<std::ranges::range C>
    requires std::convertible_to<std::ranges::range_value_t<C>, data_value>
    Builder& apply_set(const char* cell, collection_apply_mode apply_mode, const C& c);
    Builder& set(const char* cell, node_state value);
    Builder& set(const char* cell, topology_request value);
    Builder& set(const char* cell, const sstring& value);
    Builder& set(const char* cell, const raft::server_id& value);
    Builder& set(const char* cell, const uint32_t& value);
    Builder& set(const char* cell, cleanup_status value);
    Builder& set(const char* cell, const utils::UUID& value);
    Builder& set(const char* cell, bool value);
    Builder& set(const char* cell, const char* value);
    Builder& set(const char* cell, const db_clock::time_point& value);

    Builder& del(const char* cell);
};

class topology_mutation_builder;

class topology_node_mutation_builder
        : public topology_mutation_builder_base<topology_node_mutation_builder> {

    friend builder_base;

    topology_mutation_builder& _builder;
    deletable_row& _r;

private:
    row& row();
    api::timestamp_type timestamp() const;
    const schema& schema() const;
    ttl_opt ttl() const { return std::nullopt; }

public:
    topology_node_mutation_builder(topology_mutation_builder&, raft::server_id);

    using builder_base::set;
    using builder_base::del;
    topology_node_mutation_builder& set(const char* cell, const std::unordered_set<raft::server_id>& nodes_ids);
    topology_node_mutation_builder& set(const char* cell, const std::unordered_set<dht::token>& value);
    topology_node_mutation_builder& set(const char* cell, const std::set<sstring>& value);

    canonical_mutation build();
};

class topology_mutation_builder
        : public topology_mutation_builder_base<topology_mutation_builder> {

    friend builder_base;
    friend class topology_node_mutation_builder;

    schema_ptr _s;
    mutation _m;
    api::timestamp_type _ts;

    std::optional<topology_node_mutation_builder> _node_builder;

private:
    row& row();
    api::timestamp_type timestamp() const;
    const schema& schema() const;
    ttl_opt ttl() const { return std::nullopt; }

public:
    topology_mutation_builder(api::timestamp_type ts);
    topology_mutation_builder& set_transition_state(topology::transition_state);
    topology_mutation_builder& set_version(topology::version_t);
    topology_mutation_builder& set_fence_version(topology::version_t);
    topology_mutation_builder& set_session(session_id);
    topology_mutation_builder& set_tablet_balancing_enabled(bool);
    topology_mutation_builder& set_new_cdc_generation_data_uuid(const utils::UUID& value);
    topology_mutation_builder& set_committed_cdc_generations(const std::vector<cdc::generation_id_v2>& values);
    topology_mutation_builder& set_new_keyspace_rf_change_data(const sstring &ks_name, const std::map<sstring, sstring> &rf_per_dc);
    topology_mutation_builder& set_unpublished_cdc_generations(const std::vector<cdc::generation_id_v2>& values);
    topology_mutation_builder& set_global_topology_request(global_topology_request);
    topology_mutation_builder& set_global_topology_request_id(const utils::UUID&);
    topology_mutation_builder& set_upgrade_state(topology::upgrade_state_type);
    topology_mutation_builder& add_enabled_features(const std::set<sstring>& value);
    topology_mutation_builder& add_ignored_nodes(const std::unordered_set<raft::server_id>& value);
    topology_mutation_builder& set_ignored_nodes(const std::unordered_set<raft::server_id>& value);
    topology_mutation_builder& add_new_committed_cdc_generation(const cdc::generation_id_v2& value);
    topology_mutation_builder& del_transition_state();
    topology_mutation_builder& del_session();
    topology_mutation_builder& del_global_topology_request();
    topology_mutation_builder& del_global_topology_request_id();
    topology_node_mutation_builder& with_node(raft::server_id);
    canonical_mutation build() { return canonical_mutation{std::move(_m)}; }
};

class topology_request_tracking_mutation_builder :
            public topology_mutation_builder_base<topology_request_tracking_mutation_builder> {

    schema_ptr _s;
    mutation _m;
    api::timestamp_type _ts;
    deletable_row& _r;
    bool _set_type;

public:
    row& row();
    const schema& schema() const;
    api::timestamp_type timestamp() const;
    ttl_opt ttl() const;

    topology_request_tracking_mutation_builder(utils::UUID id, bool set_type = false);
    using builder_base::set;
    using builder_base::del;
    topology_request_tracking_mutation_builder& set(const char* cell, topology_request value);
    topology_request_tracking_mutation_builder& done(std::optional<sstring> error = std::nullopt);
    canonical_mutation build() { return canonical_mutation{std::move(_m)}; }
};

extern template class topology_mutation_builder_base<topology_mutation_builder>;
extern template class topology_mutation_builder_base<topology_node_mutation_builder>;
extern template class topology_mutation_builder_base<topology_request_tracking_mutation_builder>;

} // namespace service
