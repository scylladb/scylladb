/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <bit>
#include <ranges>
#include <utility>
#include <fmt/std.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/lazy.hh>

#include <seastar/core/shard_id.hh>
#include "utils/log.hh"
#include "locator/topology.hh"
#include "locator/production_snitch_base.hh"
#include "utils/assert.hh"
#include "utils/stall_free.hh"
#include "utils/to_string.hh"

struct node_printer {
    const locator::node* v;
    node_printer(const locator::node* n) noexcept : v(n) {}
};

template <>
struct fmt::formatter<node_printer> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const node_printer& np, fmt::format_context& ctx) const {
        const locator::node* node = np.v;
        auto out = fmt::format_to(ctx.out(), "node={}", fmt::ptr(node));
        if (node) {
            out = fmt::format_to(out, " {:v}", *node);
        }
        return out;
    }
};

static auto lazy_backtrace() {
    return seastar::value_of([] { return current_backtrace(); });
}

namespace locator {

static logging::logger tlogger("topology");

thread_local const endpoint_dc_rack endpoint_dc_rack::default_location = {
    .dc = locator::production_snitch_base::default_dc,
    .rack = locator::production_snitch_base::default_rack,
};

node::node(const locator::topology* topology, locator::host_id id, endpoint_dc_rack dc_rack, state state, shard_id shard_count, this_node is_this_node, node::idx_type idx)
    : _topology(topology)
    , _host_id(id)
    , _dc_rack(std::move(dc_rack))
    , _state(state)
    , _shard_count(std::move(shard_count))
    , _is_this_node(is_this_node)
    , _idx(idx)
{}

node_holder node::make(const locator::topology* topology, locator::host_id id, endpoint_dc_rack dc_rack, state state, shard_id shard_count, node::this_node is_this_node, node::idx_type idx) {
    return std::make_unique<node>(topology, std::move(id), std::move(dc_rack), std::move(state), shard_count, is_this_node, idx);
}

node_holder node::clone() const {
    return make(nullptr, host_id(), dc_rack(), get_state(), get_shard_count(), is_this_node());
}

std::string node::to_string(node::state s) {
    switch (s) {
    case state::none:           return "none";
    case state::bootstrapping:  return "bootstrapping";
    case state::replacing:      return "replacing";
    case state::normal:         return "normal";
    case state::being_decommissioned: return "being_decommissioned";
    case state::being_removed:        return "being_removed";
    case state::being_replaced:       return "being_replaced";
    case state::left:           return "left";
    }
    __builtin_unreachable();
}

future<> topology::clear_gently() noexcept {
    _this_node = nullptr;
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _dc_rack_nodes.clear();
    _dc_nodes.clear();
    _nodes_by_host_id.clear();
    co_await utils::clear_gently(_nodes);
}

topology::topology(shallow_copy, config cfg)
        : _shard(this_shard_id())
        , _cfg(cfg)
        , _sort_by_proximity(true)
{
    // constructor for shallow copying of token_metadata_impl
}

topology::topology(config cfg)
        : _shard(this_shard_id())
        , _cfg(cfg)
        , _sort_by_proximity(!cfg.disable_proximity_sorting)
        , _random_engine(std::random_device{}())
{
    tlogger.trace("topology[{}]: constructing using config: endpoint={} id={} dc={} rack={}", fmt::ptr(this),
            cfg.this_endpoint, cfg.this_host_id, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
    add_node(cfg.this_host_id, cfg.local_dc_rack, node::state::none);
}

topology::topology(topology&& o) noexcept
    : _shard(o._shard)
    , _cfg(std::move(o._cfg))
    , _this_node(std::exchange(o._this_node, nullptr))
    , _nodes(std::move(o._nodes))
    , _nodes_by_host_id(std::move(o._nodes_by_host_id))
    , _dc_nodes(std::move(o._dc_nodes))
    , _dc_rack_nodes(std::move(o._dc_rack_nodes))
    , _dc_endpoints(std::move(o._dc_endpoints))
    , _dc_racks(std::move(o._dc_racks))
    , _sort_by_proximity(o._sort_by_proximity)
    , _datacenters(std::move(o._datacenters))
    , _random_engine(std::move(o._random_engine))
{
    SCYLLA_ASSERT(_shard == this_shard_id());
    tlogger.trace("topology[{}]: move from [{}]", fmt::ptr(this), fmt::ptr(&o));

    for (auto& n : _nodes) {
        if (n) {
            n->set_topology(this);
        }
    }
}

topology& topology::operator=(topology&& o) noexcept {
    if (this != &o) {
        this->~topology();
        new (this) topology(std::move(o));
    }
    return *this;
}

void topology::set_host_id_cfg(host_id this_host_id) {
    if (_cfg.this_host_id) {
        on_internal_error(tlogger, fmt::format("topology[{}] set_host_id_cfg can be caller only once current id {} new id {}",  fmt::ptr(this), _cfg.this_host_id, this_host_id));
    }
    if (_nodes.size() != 1) {
        on_internal_error(tlogger, fmt::format("topology[{}] set_host_id_cfg called while nodes size is greater than 1",  fmt::ptr(this)));
    }
    if (!_this_node) {
        on_internal_error(tlogger, fmt::format("topology[{}] set_host_id_cfg called while _this_nodes is null",  fmt::ptr(this)));
    }
    if (_this_node->host_id()) {
        on_internal_error(tlogger, fmt::format("topology[{}] set_host_id_cfg called while _this_nodes has non null id {}",  fmt::ptr(this), _this_node->host_id()));
    }

    remove_node(*_this_node);

    tlogger.trace("topology[{}]: set host id to {}", fmt::ptr(this), this_host_id);

    _cfg.this_host_id = this_host_id;
    add_or_update_endpoint(this_host_id);
}

future<topology> topology::clone_gently() const {
    topology ret(topology::shallow_copy{}, _cfg);
    tlogger.debug("topology[{}]: clone_gently to {} from shard {}", fmt::ptr(this), fmt::ptr(&ret), _shard);
    for (const auto& nptr : _nodes) {
        if (nptr) {
            ret.add_node(nptr->clone());
        }
        co_await coroutine::maybe_yield();
    }
    ret._sort_by_proximity = _sort_by_proximity;
    ret._random_engine = _random_engine;
    co_return ret;
}

const node& topology::add_node(host_id id, const endpoint_dc_rack& dr, node::state state, shard_id shard_count) {
    if (dr.dc.empty() || dr.rack.empty()) {
        on_internal_error(tlogger, "Node must have valid dc and rack");
    }
    return add_node(node::make(this, id, dr, state, shard_count));
}

bool topology::is_configured_this_node(const node& n) const {
    return _cfg.this_host_id == n.host_id();
}

const node& topology::add_node(node_holder nptr) {
    const node* node = nptr.get();

    if (nptr->topology() != this) {
        if (nptr->topology()) {
            on_fatal_internal_error(tlogger, seastar::format("topology[{}]: {} belongs to different topology={}", fmt::ptr(this), node_printer(node), fmt::ptr(node->topology())));
        }
        nptr->set_topology(this);
    }

    if (node->idx() > 0) {
        on_internal_error(tlogger, seastar::format("topology[{}]: {}: has assigned idx", fmt::ptr(this), node_printer(nptr.get())));
    }

    // Note that _nodes contains also the this_node()
    nptr->set_idx(_nodes.size());
    _nodes.emplace_back(std::move(nptr));

    try {
        if (is_configured_this_node(*node)) {
            if (_this_node) {
                on_internal_error(tlogger, seastar::format("topology[{}]: {}: local node already mapped to {}", fmt::ptr(this), node_printer(node), node_printer(this_node())));
            }
            locator::node& n = *_nodes.back();
            n._is_this_node = node::this_node::yes;
            if (n._dc_rack == endpoint_dc_rack::default_location) {
                n._dc_rack = _cfg.local_dc_rack;
            }
        }

        tlogger.debug("topology[{}]: add_node: {}, at {}", fmt::ptr(this), node_printer(nptr.get()), lazy_backtrace());

        index_node(*node);
    } catch (...) {
        pop_node(*node);
        throw;
    }
    return *node;
}

void topology::update_node(node& node, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> opt_shard_count) {
    tlogger.debug("topology[{}]: update_node: {}: to: host_id={} dc={} rack={} state={} shard_count={}, at {}", fmt::ptr(this), node_printer(&node),
        opt_id ? format("{}", *opt_id) : "unchanged",
        opt_dr ? format("{}", opt_dr->dc) : "unchanged",
        opt_dr ? format("{}", opt_dr->rack) : "unchanged",
        opt_st ? format("{}", *opt_st) : "unchanged",
        opt_shard_count ? format("{}", *opt_shard_count) : "unchanged",
        lazy_backtrace());

    bool changed = false;
    if (opt_id) {
        if (*opt_id != node.host_id()) {
            if (!*opt_id) {
                on_internal_error(tlogger, seastar::format("Updating node host_id to null is disallowed: {}: new host_id={}", node_printer(&node), *opt_id));
            }
            if (node.is_this_node() && node.host_id()) {
                on_internal_error(tlogger, seastar::format("This node host_id is already set: {}: new host_id={}", node_printer(&node), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, seastar::format("Cannot update node host_id: {}: new host_id already exists: {}", node_printer(&node), node_printer(find_node(*opt_id))));
            }
            changed = true;
        } else {
            opt_id.reset();
        }
    }
    if (opt_dr) {
        if (opt_dr->dc.empty() || opt_dr->dc == production_snitch_base::default_dc) {
            opt_dr->dc = node.dc_rack().dc;
        }
        if (opt_dr->rack.empty() || opt_dr->rack == production_snitch_base::default_rack) {
            opt_dr->rack = node.dc_rack().rack;
        }
        if (*opt_dr != node.dc_rack()) {
            changed = true;
        } else {
            opt_dr.reset();
        }
    }
    if (opt_st) {
        changed |= node.get_state() != *opt_st;
    }
    if (opt_shard_count) {
        changed |= node.get_shard_count() != *opt_shard_count;
    }

    if (!changed) {
        return;
    }

    unindex_node(node);
    // The following block must not throw
    try {
        if (opt_id) {
            node._host_id = *opt_id;
        }
        if (opt_dr) {
            node._dc_rack = std::move(*opt_dr);
        }
        if (opt_st) {
            node.set_state(*opt_st);
        }
        if (opt_shard_count) {
            node.set_shard_count(*opt_shard_count);
        }
    } catch (...) {
        std::terminate();
    }
    index_node(node);
}

bool topology::remove_node(host_id id) {
    auto node = find_node(id);
    tlogger.debug("topology[{}]: remove_node: host_id={}: {}", fmt::ptr(this), id, node_printer(node));
    if (node) {
        remove_node(*node);
        return true;
    }
    return false;
}

void topology::remove_node(const node& node) {
    pop_node(node);
}

void topology::index_node(const node& node) {
    tlogger.trace("topology[{}]: index_node: {}, at {}", fmt::ptr(this), node_printer(&node), lazy_backtrace());

    if (node.idx() < 0) {
        on_internal_error(tlogger, seastar::format("topology[{}]: {}: must already have a valid idx", fmt::ptr(this), node_printer(&node)));
    }

    // When a node is initialized before host id is known the topology is constructed with a single
    // node with zero id
    if (!node.host_id() && _nodes.size() != 1) {
        on_internal_error(tlogger, fmt::format("topology[{}] try to index node with zero id while this is not a single node in the topology", fmt::ptr(this)));
    }
    auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node.host_id(), std::cref(node));
    if (!inserted_host_id) {
        on_internal_error(tlogger, seastar::format("topology[{}]: {}: node already exists", fmt::ptr(this), node_printer(&node)));
    }

    // We keep location of left nodes because they may still appear in tablet replica sets
    // and algorithms expect to know which dc they belonged to. View replica pairing needs stable
    // replica indexes.
    // But we don't consider those nodes as members of the cluster so don't update dc registry.
    if (!node.left() && !node.is_none()) {
        const auto& dc = node.dc_rack().dc;
        const auto& rack = node.dc_rack().rack;
        const auto& endpoint = node.host_id();
        _dc_nodes[dc].emplace(std::cref(node));
        _dc_rack_nodes[dc][rack].emplace(std::cref(node));
        _dc_endpoints[dc].insert(endpoint);
        _dc_racks[dc][rack].insert(endpoint);
        _datacenters.insert(dc);
    }

    if (node.is_this_node()) {
        _this_node = &node;
    }
}

void topology::unindex_node(const node& node) {
    tlogger.trace("topology[{}]: unindex_node: {}, at {}", fmt::ptr(this), node_printer(&node), lazy_backtrace());

    const auto& dc = node.dc_rack().dc;
    const auto& rack = node.dc_rack().rack;
    if (_dc_nodes.contains(dc)) {
        bool found = _dc_nodes.at(dc).erase(std::cref(node));
        if (found) {
            if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
                const auto& ep = node.host_id();
                auto& eps = dit->second;
                eps.erase(ep);
                if (eps.empty()) {
                    _dc_rack_nodes.erase(dc);
                    _dc_racks.erase(dc);
                    _dc_endpoints.erase(dit);
                    _datacenters.erase(dc);
                } else {
                    _dc_rack_nodes[dc][rack].erase(std::cref(node));
                    auto& racks = _dc_racks[dc];
                    if (auto rit = racks.find(rack); rit != racks.end()) {
                        auto& rack_eps = rit->second;
                        rack_eps.erase(ep);
                        if (rack_eps.empty()) {
                            racks.erase(rit);
                        }
                    }
                }
            }
        }
    }
    auto host_it = _nodes_by_host_id.find(node.host_id());
    if (host_it != _nodes_by_host_id.end() && host_it->second == node) {
        _nodes_by_host_id.erase(host_it);
    }
    if (_this_node == &node) {
        _this_node = nullptr;
    }
}

node_holder topology::pop_node(const node& node) {
    tlogger.trace("topology[{}]: pop_node: {}, at {}", fmt::ptr(this), node_printer(&node), lazy_backtrace());

    unindex_node(node);

    auto nh = std::exchange(_nodes[node.idx()], {});

    // shrink _nodes if the last node is popped
    // like when failing to index a newly added node
    if (std::cmp_equal(node.idx(), _nodes.size() - 1)) {
        _nodes.resize(node.idx());
    }

    return nh;
}

// Finds a node by its host_id
// Returns nullptr if not found
const node* topology::find_node(host_id id) const noexcept {
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        return &it->second.get();
    }
    tlogger.trace("topology[{}]: find_node did not find {}", fmt::ptr(this), id);
    return nullptr;
}

// Finds a node by its host_id
// Returns nullptr if not found
node* topology::find_node(host_id id) noexcept {
    return make_mutable(const_cast<const topology*>(this)->find_node(id));
}

// Finds a node by its index
// Returns nullptr if not found
const node* topology::find_node(node::idx_type idx) const noexcept {
    if (std::cmp_greater_equal(idx, _nodes.size())) {
        return nullptr;
    }
    return _nodes.at(idx).get();
}

const node& topology::add_or_update_endpoint(host_id id, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> shard_count)
{
    tlogger.trace("topology[{}]: add_or_update_endpoint: host_id={} dc={} rack={} state={} shards={}, at {}", fmt::ptr(this),
        id, opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack, opt_st.value_or(node::state::none), shard_count,
        lazy_backtrace());

    auto* n = find_node(id);
    if (n) {
        update_node(*n, std::nullopt, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
        return *n;
    }

    return add_node(id,
                    opt_dr.value_or(endpoint_dc_rack::default_location),
                    opt_st.value_or(node::state::none),
                    shard_count.value_or(0));
}

bool topology::remove_endpoint(locator::host_id host_id)
{
    auto node = find_node(host_id);
    tlogger.debug("topology[{}]: remove_endpoint: host_id={}: {}", fmt::ptr(this), host_id, node_printer(node));
    // Do not allow removing yourself from the topology
    // The entry is needed for local writes
    if (node && node != _this_node) {
        remove_node(*node);
        return true;
    }
    return false;
}

bool topology::has_node(host_id id) const noexcept {
    auto node = find_node(id);
    tlogger.trace("topology[{}]: has_node: host_id={}: {}", fmt::ptr(this), id, node_printer(node));
    return bool(node);
}

const endpoint_dc_rack& topology::get_location_slow(host_id id) const {
    // We should do the following check after lookup in nodes.
    // In tests, there may be no config for local node, so fall back to get_location()
    // only if no mapping is found. Otherwise, get_location() will return empty location
    // from config or random node, neither of which is correct.
    if (id == _cfg.this_host_id) {
        return get_location();
    }
    throw std::runtime_error(format("Requested location for node {} not in topology. backtrace {}", id, lazy_backtrace()));
}

void topology::sort_by_proximity(locator::host_id address, host_id_vector_replica_set& addresses) const {
    if (can_sort_by_proximity()) {
        do_sort_by_proximity(address, addresses);
    }
}

void topology::do_sort_by_proximity(locator::host_id address, host_id_vector_replica_set& addresses) const {
    const auto& loc = get_location(address);
    struct info {
        locator::host_id id;
        int distance;
    };
    auto host_infos = addresses | std::views::transform([&] (locator::host_id id) {
        const auto& loc1 = get_location(id);
        return info{id, distance(address, loc, id, loc1)};
    }) | std::ranges::to<utils::small_vector<info, host_id_vector_replica_set::internal_capacity()>>();
    std::ranges::sort(host_infos, std::ranges::less{}, std::mem_fn(&info::distance));
    auto dst = addresses.begin();
    auto it = host_infos.begin();
    auto prev = it;
    auto shuffler = _random_engine();
    for (++it; it < host_infos.end(); ++it, ++dst) {
        if (prev->distance == it->distance) {
            if (shuffler & 1) {
                std::swap(prev->id, it->id);
            }
            shuffler = std::rotr(shuffler, 1);
        }
        *dst = prev->id;
        prev = it;
    }
    *dst = prev->id;
}

int topology::distance(const locator::host_id& address, const endpoint_dc_rack& loc, const locator::host_id& a1, const endpoint_dc_rack& loc1) noexcept {
    // The farthest nodes from a given node are:
    // 1. Nodes in other DCs then the reference node
    // 2. Nodes in the other RACKs in the same DC as the reference node
    // 3. Other nodes in the same DC/RACk as the reference node
    int same_dc1 = loc1.dc == loc.dc;
    int same_rack1 = same_dc1 & (loc1.rack == loc.rack);
    int same_node1 = a1 == address;
    int d1 = ((same_dc1 << 2) | (same_rack1 << 1) | same_node1) ^ 7;
    return d1;
}

void topology::for_each_node(std::function<void(const node&)> func) const {
    for (const auto& np : _nodes) {
        if (np && !np->left() && !np->is_none()) {
            func(*np);
        }
    }
}

std::unordered_set<std::reference_wrapper<const node>> topology::get_nodes() const {
    std::unordered_set<std::reference_wrapper<const node>> nodes;
    for (const auto& np : _nodes) {
        if (np && !np->left() && !np->is_none()) {
            nodes.insert(std::cref(*np.get()));
        }
    }
    return nodes;
}

std::unordered_set<locator::host_id> topology::get_all_host_ids() const {
    std::unordered_set<locator::host_id> ids;
    for (const auto& np : _nodes) {
        if (np && !np->left() && !np->is_none()) {
            ids.insert(np->host_id());
        }
    }
    return ids;
}

std::unordered_map<sstring, std::unordered_set<host_id>>
topology::get_datacenter_host_ids() const {
    std::unordered_map<sstring, std::unordered_set<host_id>> ret;
    for (auto& [dc, nodes] : _dc_nodes) {
        ret[dc] = nodes | std::views::transform([] (const node& n) { return n.host_id(); }) | std::ranges::to<std::unordered_set>();
    }
    return ret;
}

void topology::seed_random_engine(random_engine_type::result_type value) {
    _random_engine.seed(value);
}

} // namespace locator

namespace std {

std::ostream& operator<<(std::ostream& out, const locator::node& node) {
    fmt::print(out, "{}", node);
    return out;
}

std::ostream& operator<<(std::ostream& out, const locator::node::state& state) {
    fmt::print(out, "{}", state);
    return out;
}

} // namespace std
