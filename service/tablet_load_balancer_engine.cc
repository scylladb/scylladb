/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// The "ranked" and "pressure" tablet balancing engines.
//
// A candidate is one move, or a same-table swap of two tablets. It is graded by the
// exact change it causes in three measures of imbalance over the nodes in scope:
//
//   0 safety     per node, how far its utilization is above u_high
//   1 table      per (table, shard), how far the tablet count is outside [floor, ceil]
//                of the table's fair share; plus, per shard and per node, how far its
//                tablet total is outside [floor, ceil] of its fair share, weighted by
//                tot_weight
//   2 node disk  per node, how far its utilization is outside a band around the mean
//
// The fair share of a shard is its node's share of the scope's capacity, divided evenly
// among the node's shards.
//
// The ranked comparator compares the changes lexicographically. The pressure comparator
// sums them with weights and accepts a candidate which relieves more than gamma per
// move. Bytes moved only break ties. The best candidate is applied as long as one
// improves, up to one batch per round: the largest shard count of a node in scope.
//
// The scope is whatever load_balancer::make_plan() balances: a rack, where a tablet has
// one replica in scope, or a DC, where it has up to RF.
//
//  * Each replica in scope counts as one tablet of its table.
//  * A candidate is one replica on one shard, or a co-located sibling pair, which
//    counts as two tablets and is only swapped with another pair.
//  * Moves between nodes are filtered by the caller's constraint check.
//  * Once a replica of a tablet is moved, no replica of that tablet is a candidate for
//    the rest of the round.
//  * Ties between equal candidates are broken by generation order: tables in table_id
//    order, nodes in host_id order.

#include "service/tablet_load_balancer_engine.hh"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <optional>
#include <ranges>
#include <tuple>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/chunked_vector.hh"
#include "utils/log.hh"

namespace service::sblb {

static logging::logger elogger("tablet_balancer_engine");

namespace {

// One value per measure.
constexpr size_t nr_measures = 3;
using deltas = std::array<double, nr_measures>;

// One leg of a candidate: a candidate (index into the engine's pool) from shard src to
// shard dst. Shards are dense indices ordered by (host_id, shard id).
struct leg {
    uint32_t cand;
    uint32_t table;
    uint32_t src;
    uint32_t dst;
};

using moves = utils::small_vector<leg, 2>;

struct graded {
    deltas key;
    double cost;
    moves mv;
    deltas delta;
};

bool better(const graded& a, const graded& b) {
    return std::tie(a.key, a.cost) < std::tie(b.key, b.cost);
}

struct move_sig {
    std::array<uint32_t, 4> v{};
    uint32_t n = 0;

    explicit move_sig(const moves& mv) {
        for (const auto& m : mv) {
            v[2 * n] = m.cand;
            v[2 * n + 1] = m.dst;
            ++n;
        }
    }

    bool operator==(const move_sig&) const = default;

    template <typename H>
    friend H AbslHashValue(H h, const move_sig& s) {
        return H::combine(std::move(h), s.v, s.n);
    }
};

// Shard capacity weights are quantized to this step.
constexpr double share_step = 0.25;
constexpr double share_eps = 1e-9;

std::pair<int64_t, int64_t> share_bounds(int64_t total, double fraction) {
    double x = double(total) * fraction;
    return {int64_t(std::floor(x + share_eps)), int64_t(std::ceil(x - share_eps))};
}

// A delta on a 1e-9 grid of its measure's scale, so that candidates which change the
// same terms by the same amounts tie exactly, whatever the order of summation.
double clean(double x, double scale) {
    double q = 1e-9 * scale;
    return std::nearbyint(x / q) * q;
}

class engine {
    using node_terms = std::array<double, 2>; // safety, node disk

    struct cand_entry {
        uint32_t handle;
        uint32_t table;
        uint32_t shard;
        uint32_t count;
        int64_t size;
        bool no_intranode_moves = false;
        bool alive = true;
        utils::small_vector<locator::global_tablet_id, 2> tablets;
    };

    struct node_state {
        locator::host_id id;
        int64_t cap = 0;
        int64_t used = 0;
        uint32_t first_shard = 0;
        uint32_t nr_shards = 0;
        int64_t sn = 0;  // sum of the shards' tablet counts
        uint64_t ver = 0; // bumped whenever a pick touches the node
    };

    struct shard_state {
        uint32_t node = 0;
        unsigned id = 0;
        int64_t bytes = 0;
        int64_t count = 0;
        size_t read_load = 0;
        size_t write_load = 0;
        uint32_t group = 0; // capacity share group
        // table -> candidate pool indices, in tablet id order
        std::map<uint32_t, std::vector<uint32_t>> cands;
    };

    struct tcache_entry {
        bool valid = false;
        uint64_t ver = 0;
        node_terms terms{};
    };

    struct dcache_entry {
        std::array<uint64_t, 4> vers{};
        deltas delta{};
    };

    const engine_config _cfg;
    persistent_state& _persistent;
    constraint_check _constraints;

    std::vector<node_state> _nodes;   // by host_id
    std::vector<shard_state> _shards; // by (host_id, shard id)
    std::vector<table_id> _tables;    // by table_id
    std::vector<cand_entry> _pool;
    absl::flat_hash_map<locator::global_tablet_id, utils::small_vector<uint32_t, 4>> _entries_of_tablet;

    // Tablet counts per (table, shard), dense: _c[t * S + s].
    utils::chunked_vector<int32_t> _c;
    std::vector<int64_t> _totals;                          // per table, replicas in scope
    std::vector<double> _group_fraction;                   // per capacity share group
    std::vector<uint32_t> _group_size;                     // shards per group
    std::vector<std::vector<std::pair<int64_t, int64_t>>> _bounds; // [table][group] -> (floor, ceil)
    std::vector<uint32_t> _above;                          // per table, shards above ceil
    std::vector<uint32_t> _below;                          // per table, shards below floor
    std::vector<double> _m;                                // pressure: per table, max(total / S, 1)
    // The scope's tablet total, its [floor, ceil] per share group (for the shards) and
    // per node, and the ideal totals, the pressure comparator's unit for them.
    int64_t _grand = 0;
    std::vector<std::pair<int64_t, int64_t>> _sbnd; // per group
    std::vector<std::pair<int64_t, int64_t>> _nbnd; // per node
    double _ms = 1.0;
    std::vector<double> _mn;                        // per node

    double _umean = 0;
    double _band = 0;
    bool _active = false;
    deltas _p{};
    deltas _scale{};

    std::vector<tcache_entry> _tcache;
    absl::flat_hash_map<move_sig, dcache_entry> _dcache;
    absl::flat_hash_map<std::pair<uint32_t, uint32_t>, bool> _constraint_cache; // (cand, dst node)

    static const std::vector<uint32_t> _no_cands;

public:
    engine(const engine_config& cfg, persistent_state& st, std::vector<node_input> input, constraint_check constraints)
        : _cfg(cfg)
        , _persistent(st)
        , _constraints(std::move(constraints))
    {
        build(std::move(input));
    }

    future<std::vector<planned_move>> plan() {
        std::vector<planned_move> out;
        if (_nodes.empty()) {
            co_return out;
        }
        setup();
        if (_shards.size() < 2) {
            co_return out;
        }
        size_t batch = 0;
        for (const auto& n : _nodes) {
            batch = std::max<size_t>(batch, n.nr_shards);
        }
        elogger.debug("{} engine: {} nodes, {} shards, {} tables, {} candidates, trigger={}, P={}",
                      _cfg.cmp == comparator::ranked ? "ranked" : "pressure",
                      _nodes.size(), _shards.size(), _tables.size(), _pool.size(), _active, _p);
        while (out.size() < batch) {
            co_await coroutine::maybe_yield();
            auto best = find_best();
            if (!best) {
                break;
            }
            commit(*best);
            for (const auto& m : best->mv) {
                out.push_back(planned_move{_pool[m.cand].handle, replica(m.src), replica(m.dst)});
            }
        }
        elogger.debug("engine planned {} moves, P={}", out.size(), _p);
        co_return out;
    }

private:
    locator::tablet_replica replica(uint32_t s) const {
        return locator::tablet_replica{_nodes[_shards[s].node].id, _shards[s].id};
    }

    size_t nr_shards() const {
        return _shards.size();
    }

    int32_t& count_of(uint32_t t, uint32_t s) {
        return _c[size_t(t) * nr_shards() + s];
    }

    int32_t count_of(uint32_t t, uint32_t s) const {
        return _c[size_t(t) * nr_shards() + s];
    }

    int64_t lo(uint32_t t, uint32_t s) const {
        return _bounds[t][_shards[s].group].first;
    }

    int64_t hi(uint32_t t, uint32_t s) const {
        return _bounds[t][_shards[s].group].second;
    }

    const std::vector<uint32_t>& cands(uint32_t s, uint32_t t) const {
        auto& m = _shards[s].cands;
        auto it = m.find(t);
        return it == m.end() ? _no_cands : it->second;
    }

    uint32_t table_index(table_id id) const {
        auto it = std::ranges::lower_bound(_tables, id);
        return uint32_t(it - _tables.begin());
    }

    void build(std::vector<node_input> input) {
        std::ranges::sort(input, std::less<>(), &node_input::id);

        for (const auto& n : input) {
            for (const auto& s : n.shards) {
                for (const auto& [t, c] : s.tablet_count_per_table) {
                    _tables.push_back(t);
                }
                for (const auto& tc : s.candidates) {
                    _tables.push_back(tc.table);
                }
            }
        }
        std::ranges::sort(_tables);
        auto dups = std::ranges::unique(_tables);
        _tables.erase(dups.begin(), dups.end());

        size_t total_shards = 0;
        for (const auto& n : input) {
            total_shards += n.shards.size();
        }
        const size_t nr_tables = _tables.size();
        _c.resize(nr_tables * total_shards);
        _totals.assign(nr_tables, 0);

        for (auto& n : input) {
            const auto nid = uint32_t(_nodes.size());
            node_state ns{.id = n.id, .cap = int64_t(n.capacity), .used = int64_t(n.used),
                          .first_shard = uint32_t(_shards.size()), .nr_shards = uint32_t(n.shards.size())};
            for (unsigned sid = 0; sid < n.shards.size(); ++sid) {
                auto& si = n.shards[sid];
                const auto s = uint32_t(_shards.size());
                shard_state ss{.node = nid, .id = sid, .bytes = int64_t(si.used), .count = int64_t(si.tablet_count),
                               .read_load = si.streaming_read_load, .write_load = si.streaming_write_load};
                ns.sn += ss.count;
                for (const auto& [tid, c] : si.tablet_count_per_table) {
                    auto t = table_index(tid);
                    _c[size_t(t) * total_shards + s] += int32_t(c);
                    _totals[t] += int64_t(c);
                }
                for (auto& tc : si.candidates) {
                    auto t = table_index(tc.table);
                    std::ranges::sort(tc.candidates, std::less<>(), [] (const candidate& c) { return c.tablets.front(); });
                    auto& list = ss.cands[t];
                    for (auto& c : tc.candidates) {
                        const auto idx = uint32_t(_pool.size());
                        for (auto gid : c.tablets) {
                            _entries_of_tablet[gid].push_back(idx);
                        }
                        _pool.push_back(cand_entry{.handle = c.handle, .table = t, .shard = s, .count = c.count,
                                                   .size = int64_t(c.size), .no_intranode_moves = tc.no_intranode_moves,
                                                   .tablets = std::move(c.tablets)});
                        list.push_back(idx);
                    }
                }
                _shards.push_back(std::move(ss));
            }
            _nodes.push_back(std::move(ns));
        }
        _tcache.resize(_nodes.size());
    }

    // ------------------------------------------------------------------ state

    void setup() {
        const size_t S = nr_shards();
        int64_t total_used = 0;
        int64_t total_cap = 0;
        for (const auto& n : _nodes) {
            total_used += n.used;
            total_cap += n.cap;
        }
        _umean = total_cap ? double(total_used) / double(total_cap) : 0.0;

        // Fair share: shard capacities relative to the smallest shard, quantized to
        // quarters, so that capacity jitter does not move the count targets.
        std::vector<double> shard_cap(S);
        double smallest = std::numeric_limits<double>::max();
        for (size_t s = 0; s < S; ++s) {
            const auto& n = _nodes[_shards[s].node];
            shard_cap[s] = double(n.cap) / double(n.nr_shards);
            smallest = std::min(smallest, shard_cap[s]);
        }
        std::map<double, uint32_t> group_of_weight;
        std::vector<double> group_weight;
        double total_weight = 0;
        for (size_t s = 0; s < S; ++s) {
            double w = smallest <= 0 ? 1.0 : std::max(1.0, std::nearbyint(shard_cap[s] / smallest / share_step) * share_step);
            total_weight += w;
            auto [it, inserted] = group_of_weight.emplace(w, uint32_t(group_weight.size()));
            if (inserted) {
                group_weight.push_back(w);
                _group_size.push_back(0);
            }
            _shards[s].group = it->second;
            _group_size[it->second]++;
        }
        for (double w : group_weight) {
            _group_fraction.push_back(w / total_weight);
        }

        const size_t T = _tables.size();
        _bounds.resize(T);
        _above.assign(T, 0);
        _below.assign(T, 0);
        _m.assign(T, 1.0);
        for (uint32_t t = 0; t < T; ++t) {
            for (double f : _group_fraction) {
                _bounds[t].push_back(share_bounds(_totals[t], f));
            }
            for (uint32_t s = 0; s < S; ++s) {
                auto c = count_of(t, s);
                _above[t] += c > hi(t, s);
                _below[t] += c < lo(t, s);
            }
            _m[t] = std::max(double(_totals[t]) / double(S), 1.0);
        }
        for (auto x : _totals) {
            _grand += x;
        }
        for (double f : _group_fraction) {
            _sbnd.push_back(share_bounds(_grand, f));
        }
        _ms = S ? std::max(double(_grand) / double(S), 1.0) : 1.0;
        for (const auto& ns : _nodes) {
            double nfrac = 0;
            for (uint32_t s = ns.first_shard; s < ns.first_shard + ns.nr_shards; ++s) {
                nfrac += _group_fraction[_shards[s].group];
            }
            _nbnd.push_back(share_bounds(_grand, nfrac));
            _mn.push_back(std::max(double(_grand) * nfrac, 1.0));
        }

        // Node-disk band and its Schmitt trigger. The measure always uses the inner band;
        // the trigger only decides whether disk improvements count (see gate()).
        double f = 1.0 - 0.75 * std::min(1.0, std::max(0.0, (_umean - 0.5) / 0.4));
        double dev = 0;
        for (uint32_t n = 0; n < _nodes.size(); ++n) {
            dev = std::max(dev, std::abs(util(n) - _umean));
        }
        _active = _persistent.disk_trigger_active;
        const double disk_low = _cfg.disk_high * _cfg.disk_low_ratio;
        if (_active && dev <= disk_low * f) {
            _active = false;
        } else if (!_active && dev > _cfg.disk_high * f) {
            _active = true;
        }
        _persistent.disk_trigger_active = _active;
        _band = disk_low * f;

        _p = phi();
        for (size_t i = 0; i < nr_measures; ++i) {
            _scale[i] = std::max(1.0, std::abs(_p[i]));
        }
    }

    double util(uint32_t n) const {
        const auto& ns = _nodes[n];
        return ns.cap ? double(ns.used) / double(ns.cap) : 0.0;
    }

    bool pressure() const {
        return _cfg.cmp == comparator::pressure;
    }

    double t_safety(uint32_t n) const {
        double u = util(n);
        if (!pressure()) {
            return std::pow(std::max(0.0, u - _cfg.u_high), 2);
        }
        const double k = _cfg.kappa;
        const double s = _cfg.u_safe;
        const double knee = _cfg.u_knee;
        if (u <= s) {
            return 0.0;
        }
        if (u <= knee) {
            return k * std::pow((u - s) / (1.0 - u), 2);
        }
        // Past the knee: the tangent, so the term keeps a gradient however full.
        double slope = 2.0 * k * (knee - s) * (1.0 - s) / std::pow(1.0 - knee, 3);
        return k * std::pow((knee - s) / (1.0 - knee), 2) + slope * (u - knee);
    }

    double t_node(uint32_t n) const {
        double x = std::max(0.0, std::abs(util(n) - _umean) - _band);
        return pressure() ? std::pow(x / _cfg.node_unit, 2) : std::pow(x, 2);
    }

    double t_excess(uint32_t t, int64_t e) const {
        return pressure() ? double(e) * double(e) / _m[t] : double(e);
    }

    double t_table(uint32_t t, uint32_t s) const {
        int64_t c = count_of(t, s);
        return t_excess(t, std::max<int64_t>(0, c - hi(t, s)) + std::max<int64_t>(0, lo(t, s) - c));
    }

    // The table term of one shard's or node's total from its excess outside [floor, ceil]
    // of the ideal m.
    double t_excess_total(int64_t e, double m) const {
        return pressure() ? _cfg.tot_weight * double(e) * double(e) / m : _cfg.tot_weight * double(e);
    }

    double t_shard_total(uint32_t s) const {
        const int64_t c = _shards[s].count;
        const auto [l, h] = _sbnd[_shards[s].group];
        return t_excess_total(std::max<int64_t>(0, c - h) + std::max<int64_t>(0, l - c), _ms);
    }

    double t_node_total(uint32_t n) const {
        const int64_t c = _nodes[n].sn;
        const auto [l, h] = _nbnd[n];
        return t_excess_total(std::max<int64_t>(0, c - h) + std::max<int64_t>(0, l - c), _mn[n]);
    }

    template <typename Shards, typename Nodes>
    double t_totals(const Shards& shards, const Nodes& nodes) const {
        double a = 0;
        for (auto s : shards) {
            a += t_shard_total(s);
        }
        double b = 0;
        for (auto n : nodes) {
            b += t_node_total(n);
        }
        return a + b;
    }

    node_terms terms(uint32_t n) const {
        return {t_safety(n), t_node(n)};
    }

    const node_terms& current_terms(uint32_t n) {
        auto& e = _tcache[n];
        if (!e.valid || e.ver != _nodes[n].ver) {
            e = tcache_entry{true, _nodes[n].ver, terms(n)};
        }
        return e.terms;
    }

    deltas phi() const {
        deltas p{};
        for (uint32_t n = 0; n < _nodes.size(); ++n) {
            auto x = terms(n);
            p[0] += x[0];
            p[2] += x[1];
        }
        double tab = 0;
        for (uint32_t t = 0; t < _tables.size(); ++t) {
            for (uint32_t s = 0; s < nr_shards(); ++s) {
                tab += t_table(t, s);
            }
        }
        const double tot = t_totals(std::views::iota(uint32_t(0), uint32_t(nr_shards())),
                                    std::views::iota(uint32_t(0), uint32_t(_nodes.size())));
        p[1] = tab + tot;
        return p;
    }

    // ------------------------------------------------------------ evaluation

    void update_count(uint32_t t, uint32_t s, int64_t d) {
        auto& c = count_of(t, s);
        const int64_t old = c;
        const int64_t nc = old + d;
        const int64_t l = lo(t, s);
        const int64_t h = hi(t, s);
        _above[t] += int(nc > h) - int(old > h);
        _below[t] += int(nc < l) - int(old < l);
        c = int32_t(nc);

        auto& sh = _shards[s];
        auto& ns = _nodes[sh.node];
        const int64_t x = sh.count;
        sh.count = x + d;
        ns.sn += d;
    }

    void mutate(const moves& mv, int sign) {
        for (const auto& m : mv) {
            const auto& c = _pool[m.cand];
            const int64_t b = c.size * sign;
            auto& src = _shards[m.src];
            auto& dst = _shards[m.dst];
            src.bytes -= b;
            dst.bytes += b;
            _nodes[src.node].used -= b;
            _nodes[dst.node].used += b;
            update_count(m.table, m.src, -int64_t(c.count) * sign);
            update_count(m.table, m.dst, int64_t(c.count) * sign);
        }
    }

    // Touched nodes, sorted and unique.
    utils::small_vector<uint32_t, 4> touched_nodes(const moves& mv) const {
        utils::small_vector<uint32_t, 4> nids;
        for (const auto& m : mv) {
            nids.push_back(_shards[m.src].node);
            nids.push_back(_shards[m.dst].node);
        }
        std::ranges::sort(nids);
        auto dups = std::ranges::unique(nids);
        nids.erase(dups.begin(), dups.end());
        return nids;
    }

    // Change in each measure if the moves were applied: applies them, measures the
    // touched terms, reverts.
    deltas compute_delta(const moves& mv, const utils::small_vector<uint32_t, 4>& nids) {
        utils::small_vector<std::pair<uint32_t, uint32_t>, 4> tk;
        for (const auto& m : mv) {
            tk.emplace_back(m.table, m.src);
            tk.emplace_back(m.table, m.dst);
        }
        std::ranges::sort(tk);
        auto dups = std::ranges::unique(tk);
        tk.erase(dups.begin(), dups.end());
        utils::small_vector<uint32_t, 4> tks; // the touched shards
        for (const auto& m : mv) {
            tks.push_back(m.src);
            tks.push_back(m.dst);
        }
        std::ranges::sort(tks);
        auto sdups = std::ranges::unique(tks);
        tks.erase(sdups.begin(), sdups.end());

        utils::small_vector<node_terms, 4> before;
        for (auto n : nids) {
            before.push_back(current_terms(n));
        }
        double before_t = 0;
        for (auto [t, s] : tk) {
            before_t += t_table(t, s);
        }
        before_t += t_totals(tks, nids);
        mutate(mv, +1);
        utils::small_vector<node_terms, 4> after;
        for (auto n : nids) {
            after.push_back(terms(n));
        }
        double after_t = 0;
        for (auto [t, s] : tk) {
            after_t += t_table(t, s);
        }
        after_t += t_totals(tks, nids);
        mutate(mv, -1);

        deltas d{};
        for (size_t i = 0; i < nids.size(); ++i) {
            d[0] += after[i][0] - before[i][0];
            d[2] += after[i][1] - before[i][1];
        }
        d[1] = after_t - before_t;
        return d;
    }

    deltas delta(const moves& mv) {
        auto nids = touched_nodes(mv);
        std::array<uint64_t, 4> vers{};
        for (size_t i = 0; i < nids.size(); ++i) {
            vers[i] = _nodes[nids[i]].ver;
        }
        move_sig sig(mv);
        auto it = _dcache.find(sig);
        if (it != _dcache.end() && it->second.vers == vers) {
            return it->second.delta;
        }
        auto d = compute_delta(mv, nids);
        _dcache.insert_or_assign(sig, dcache_entry{vers, d});
        return d;
    }

    double cost(const moves& mv) const {
        double c = 0;
        for (const auto& m : mv) {
            c += double(_pool[m.cand].size) * (_shards[m.src].node != _shards[m.dst].node ? _cfg.inter_factor : 1.0);
        }
        return c;
    }

    // While the node-disk trigger is off, disk improvements do not count, but pushing a
    // node further out of the band is still charged.
    deltas gate(deltas d) const {
        if (!_active && d[2] < 0) {
            d[2] = 0.0;
        }
        return d;
    }

    // (improves?, key); a lower key is better.
    std::pair<bool, deltas> key(const deltas& d, size_t nr_moves) const {
        if (!pressure()) {
            deltas v;
            for (size_t i = 0; i < nr_measures; ++i) {
                v[i] = clean(d[i], _scale[i]);
            }
            auto first = std::ranges::find_if(v, [] (double x) { return x != 0.0; });
            return {first != v.end() && *first < 0, v};
        }
        const auto& w = _cfg.weights;
        double rest_sum = 0;
        double rest_scale = 0;
        for (size_t i = 1; i < nr_measures; ++i) {
            rest_sum += w[i] * d[i];
            rest_scale += _scale[i];
        }
        double rest = clean(rest_sum, rest_scale);
        double total_scale = 0;
        for (size_t i = 0; i < nr_measures; ++i) {
            total_scale += _scale[i];
        }
        double total = clean(w[0] * d[0] + rest, total_scale);
        const double n = double(std::max<size_t>(1, nr_moves));
        return {total < -_cfg.gamma * n, deltas{total, 0, 0}};
    }

    // Hard filters: tablets which cannot change shard within their node, streaming
    // slots, the critical utilization of the destination, and the replication
    // constraints.
    bool ok(const leg& m) {
        const auto& c = _pool[m.cand];
        const size_t w = c.count;
        const auto& src = _shards[m.src];
        const auto& dst = _shards[m.dst];
        if (c.no_intranode_moves && src.node == dst.node) {
            return false;
        }
        if (src.read_load > 0 && src.read_load + w > _cfg.max_read_streaming_load) {
            return false;
        }
        if (dst.write_load > 0 && dst.write_load + w > _cfg.max_write_streaming_load) {
            return false;
        }
        if (src.node == dst.node) {
            return true;
        }
        const auto& dn = _nodes[dst.node];
        if (dn.cap && double(dn.used + c.size) / double(dn.cap) > _cfg.critical) {
            return false;
        }
        return feasible(m.cand, m.dst);
    }

    // The replication constraints alone. They depend only on the tablet and the
    // destination node, and tablet replicas do not change within a round.
    bool feasible(uint32_t cand, uint32_t dst) {
        const auto& c = _pool[cand];
        const auto dst_node = _shards[dst].node;
        if (_shards[c.shard].node == dst_node) {
            return true;
        }
        auto [it, inserted] = _constraint_cache.try_emplace(std::make_pair(cand, dst_node), false);
        if (inserted) {
            it->second = _constraints(c.handle, replica(c.shard), replica(dst));
        }
        return it->second;
    }

    std::optional<graded> grade(const moves& mv) {
        for (const auto& m : mv) {
            if (m.src == m.dst || !ok(m)) {
                return std::nullopt;
            }
        }
        auto d = delta(mv);
        auto [improves, k] = key(gate(d), mv.size());
        if (!improves) {
            return std::nullopt;
        }
        return graded{k, cost(mv), mv, d};
    }

    // ----------------------------------------------------------- candidates

    // The first candidate of the list with the smallest size.
    uint32_t smallest(const std::vector<uint32_t>& ids) const {
        uint32_t best = ids.front();
        for (auto i : ids) {
            if (_pool[i].size < _pool[best].size) {
                best = i;
            }
        }
        return best;
    }

    // The first candidate of the list with the largest size.
    uint32_t largest(const std::vector<uint32_t>& ids) const {
        uint32_t best = ids.front();
        for (auto i : ids) {
            if (_pool[i].size > _pool[best].size) {
                best = i;
            }
        }
        return best;
    }

    // The first candidate of the list whose size minus `offset` is closest to `want`.
    uint32_t closest(const std::vector<uint32_t>& ids, double offset, double want) const {
        uint32_t best = ids.front();
        double best_d = std::abs(double(_pool[best].size) - offset - want);
        for (auto i : ids) {
            double d = std::abs(double(_pool[i].size) - offset - want);
            if (d < best_d) {
                best = i;
                best_d = d;
            }
        }
        return best;
    }

    static moves one(uint32_t cand, uint32_t t, uint32_t src, uint32_t dst) {
        moves mv;
        mv.push_back(leg{cand, t, src, dst});
        return mv;
    }

    // Both pools are ordered best-first. For each table on the source pool, the first
    // k_disk sources above the table's floor and the first k_disk destinations below its
    // ceiling are paired; without such shards, the first k_disk of the pool.
    template <typename Consider>
    void pair_candidates(const std::vector<uint32_t>& src_pool, const std::vector<uint32_t>& dst_pool, double want,
                         Consider&& consider) {
        std::vector<uint32_t> tables;
        for (auto s : src_pool) {
            for (const auto& [t, ids] : _shards[s].cands) {
                if (!ids.empty()) {
                    tables.push_back(t);
                }
            }
        }
        std::ranges::sort(tables);
        auto dups = std::ranges::unique(tables);
        tables.erase(dups.begin(), dups.end());

        const size_t k_disk = _cfg.k_disk;
        std::vector<uint32_t> srcs_above;
        std::vector<uint32_t> srcs_any;
        std::vector<uint32_t> dsts;
        for (auto t : tables) {
            srcs_above.clear();
            srcs_any.clear();
            for (auto s : src_pool) {
                if (srcs_above.size() >= k_disk) {
                    break;
                }
                if (cands(s, t).empty()) {
                    continue;
                }
                if (count_of(t, s) > lo(t, s)) {
                    srcs_above.push_back(s);
                }
                if (srcs_any.size() < k_disk) {
                    srcs_any.push_back(s);
                }
            }
            const auto& srcs = srcs_above.empty() ? srcs_any : srcs_above;

            dsts.clear();
            for (auto s : dst_pool) {
                if (dsts.size() >= k_disk) {
                    break;
                }
                if (count_of(t, s) < hi(t, s)) {
                    dsts.push_back(s);
                }
            }
            if (dsts.empty()) {
                for (size_t i = 0; i < std::min(k_disk, dst_pool.size()); ++i) {
                    dsts.push_back(dst_pool[i]);
                }
            }

            for (auto src : srcs) {
                const auto& ids = cands(src, t);
                const auto small = smallest(ids);
                const auto fit = closest(ids, 0.0, want);
                for (auto dst : dsts) {
                    if (dst == src) {
                        continue;
                    }
                    consider(one(small, t, src, dst));
                    if (fit != small) {
                        consider(one(fit, t, src, dst));
                    }
                }
                // Same-table swap: moves bytes without changing any count, so it passes
                // the table measure where a single move off a balanced table cannot.
                for (auto dst : dsts) {
                    if (dst == src) {
                        continue;
                    }
                    const auto& back = cands(dst, t);
                    if (back.empty()) {
                        continue;
                    }
                    const auto b = smallest(back);
                    const auto a = closest(ids, double(_pool[b].size), want);
                    if (_pool[a].size > _pool[b].size && _pool[a].count == _pool[b].count) {
                        moves mv;
                        mv.push_back(leg{a, t, src, dst});
                        mv.push_back(leg{b, t, dst, src});
                        consider(mv);
                    }
                }
            }
        }
    }

    std::optional<graded> find_best() {
        std::optional<graded> best;
        absl::flat_hash_set<move_sig> seen;
        const size_t S = nr_shards();

        auto offer = [&] (const graded& g) {
            if (!best || better(g, *best)) {
                best = g;
            }
        };
        auto consider = [&] (const moves& mv) {
            if (!seen.insert(move_sig(mv)).second) {
                return;
            }
            if (auto g = grade(mv)) {
                offer(*g);
            }
        };

        std::vector<double> u(_nodes.size());
        for (uint32_t n = 0; n < _nodes.size(); ++n) {
            u[n] = util(n);
        }

        // (a) per table, excess side: the shard most above its ceiling, to the k_dst
        // shards below theirs with the smallest (count - floor, not on src's node,
        // node utilization, bytes, id), plus the best one on src's node.
        using dst_key = std::tuple<int64_t, bool, double, int64_t, uint32_t>;
        std::vector<dst_key> pool;
        for (uint32_t t = 0; t < _tables.size(); ++t) {
            if (!_above[t]) {
                continue;
            }
            std::optional<uint32_t> src;
            int64_t src_excess = 0;
            for (uint32_t s = 0; s < S; ++s) {
                int64_t e = count_of(t, s) - hi(t, s);
                if (e > 0 && (!src || e >= src_excess)) {
                    src = s;
                    src_excess = e;
                }
            }
            if (!src) {
                continue;
            }
            const auto& ids = cands(*src, t);
            if (ids.empty()) {
                continue;
            }
            const auto src_node = _shards[*src].node;
            const auto small = smallest(ids);
            const auto large = largest(ids);
            pool.clear();
            std::optional<dst_key> same;
            for (uint32_t s = 0; s < S; ++s) {
                if (s == *src || count_of(t, s) >= hi(t, s)) {
                    continue;
                }
                // DC-wide scope: skip destinations neither tablet may go to, so that they do
                // not take the shortlist's places. Never true with one replica per tablet.
                if (!feasible(small, s) && !feasible(large, s)) {
                    continue;
                }
                const auto node = _shards[s].node;
                dst_key k{count_of(t, s) - lo(t, s), node != src_node, u[node], _shards[s].bytes, s};
                pool.push_back(k);
                if (node == src_node && (!same || k < *same)) {
                    same = k;
                }
            }
            const size_t k_dst = std::min(_cfg.k_dst, pool.size());
            std::partial_sort(pool.begin(), pool.begin() + k_dst, pool.end());
            utils::small_vector<uint32_t, 8> dsts;
            for (size_t i = 0; i < k_dst; ++i) {
                dsts.push_back(std::get<4>(pool[i]));
            }
            if (same && std::ranges::find(dsts, std::get<4>(*same)) == dsts.end()) {
                dsts.push_back(std::get<4>(*same));
            }
            for (auto dst : dsts) {
                consider(one(small, t, *src, dst));
                if (large != small) {
                    consider(one(large, t, *src, dst));
                }
            }
        }

        // (a') per table, deficit side: shards below floor, which (a) never proposes when
        // no shard of the table is above its ceiling (counts 3,3,3,1 on 4 shards).
        using short_key = std::tuple<int64_t, double, int64_t, uint32_t>;
        std::vector<short_key> short_pool;
        std::vector<uint32_t> donors;
        for (uint32_t t = 0; t < _tables.size(); ++t) {
            if (!_below[t]) {
                continue;
            }
            short_pool.clear();
            donors.clear();
            for (uint32_t s = 0; s < S; ++s) {
                const int64_t c = count_of(t, s);
                const int64_t l = lo(t, s);
                if (c < l) {
                    short_pool.emplace_back(c - l, u[_shards[s].node], _shards[s].bytes, s);
                } else if (c > l) {
                    donors.push_back(s);
                }
            }
            if (short_pool.empty() || donors.empty()) {
                continue;
            }
            const size_t k_dst = std::min(_cfg.k_dst, short_pool.size());
            std::partial_sort(short_pool.begin(), short_pool.begin() + k_dst, short_pool.end());
            for (size_t i = 0; i < k_dst; ++i) {
                const auto dst = std::get<3>(short_pool[i]);
                const auto dst_node = _shards[dst].node;
                // The donor most above its floor, preferring dst's own node, then the
                // emptiest node, then the highest id.
                // DC-wide scope: only donors with a tablet which may go to dst, or with
                // none movable.
                std::optional<uint32_t> src;
                std::tuple<int64_t, bool, double, uint32_t> src_key;
                for (auto s : donors) {
                    const auto& ids = cands(s, t);
                    if (!ids.empty() && !feasible(smallest(ids), dst) && !feasible(largest(ids), dst)) {
                        continue;
                    }
                    const auto node = _shards[s].node;
                    auto k = std::make_tuple(count_of(t, s) - lo(t, s), node == dst_node, -u[node], s);
                    if (!src || k > src_key) {
                        src = s;
                        src_key = k;
                    }
                }
                if (!src) {
                    continue;
                }
                const auto& ids = cands(*src, t);
                if (ids.empty()) {
                    continue;
                }
                consider(one(smallest(ids), t, *src, dst));
                consider(one(largest(ids), t, *src, dst));
            }
        }

        // (d) totals: shards above their total band, and the shards of nodes above theirs,
        // to shards below their band and the shards of nodes below theirs. Per (source,
        // destination) pair two candidates are graded, preferring tables whose own band
        // the move keeps: the smallest movable tablet, and the one closest to the bytes
        // the destination node lacks against the source node. Pools are cut to k_disk
        // each, so a pick grades at most 2 k_disk^2 candidates.
        {
            auto slo = [&] (uint32_t s) { return _sbnd[_shards[s].group].first; };
            auto shi = [&] (uint32_t s) { return _sbnd[_shards[s].group].second; };
            std::vector<uint8_t> in_src(S, 0);
            std::vector<uint8_t> in_dst(S, 0);
            std::vector<uint32_t> srcs;
            std::vector<uint32_t> dsts;
            bool any = false;
            for (uint32_t s = 0; s < S; ++s) {
                if (_shards[s].count > shi(s)) {
                    srcs.push_back(s);
                    in_src[s] = 1;
                }
                if (_shards[s].count < slo(s)) {
                    dsts.push_back(s);
                    in_dst[s] = 1;
                }
            }
            any = !srcs.empty() || !dsts.empty();
            for (uint32_t n = 0; n < _nodes.size(); ++n) {
                const auto& ns = _nodes[n];
                if (ns.sn > _nbnd[n].second) {
                    any = true;
                    for (uint32_t s = ns.first_shard; s < ns.first_shard + ns.nr_shards; ++s) {
                        if (!in_src[s] && _shards[s].count > slo(s)) {
                            srcs.push_back(s);
                            in_src[s] = 1;
                        }
                    }
                }
                if (ns.sn < _nbnd[n].first) {
                    any = true;
                    for (uint32_t s = ns.first_shard; s < ns.first_shard + ns.nr_shards; ++s) {
                        if (!in_dst[s] && _shards[s].count < shi(s)) {
                            dsts.push_back(s);
                            in_dst[s] = 1;
                        }
                    }
                }
            }
            if (any) {
                if (srcs.empty()) { // a deficit with no shard above: the shards nearest
                    for (uint32_t s = 0; s < S; ++s) {
                        if (!in_dst[s] && _shards[s].count > slo(s)) {
                            srcs.push_back(s);
                            in_src[s] = 1;
                        }
                    }
                }
                if (dsts.empty()) {
                    for (uint32_t s = 0; s < S; ++s) {
                        if (!in_src[s] && _shards[s].count < shi(s)) {
                            dsts.push_back(s);
                        }
                    }
                }
                // Sources: most above its ceiling, fullest node, biggest first.
                // Destinations: most below its floor, emptiest node, smallest first.
                const size_t k_disk = _cfg.k_disk;
                const size_t ns_ = std::min(k_disk, srcs.size());
                std::ranges::partial_sort(srcs, srcs.begin() + ns_, std::less<>(), [&] (uint32_t s) {
                    return std::make_tuple(shi(s) - _shards[s].count, -u[_shards[s].node], -_shards[s].bytes, s);
                });
                srcs.resize(ns_);
                const size_t nd_ = std::min(k_disk, dsts.size());
                std::ranges::partial_sort(dsts, dsts.begin() + nd_, std::less<>(), [&] (uint32_t s) {
                    return std::make_tuple(_shards[s].count - slo(s), u[_shards[s].node], _shards[s].bytes, s);
                });
                dsts.resize(nd_);
                std::vector<uint32_t> room;
                std::vector<uint32_t> every;
                for (auto src : srcs) {
                    const auto src_node = _shards[src].node;
                    for (auto dst : dsts) {
                        if (dst == src) {
                            continue;
                        }
                        const auto dst_node = _shards[dst].node;
                        double want = 0.0;
                        if (src_node != dst_node) {
                            want = std::max(0.0, std::min((u[src_node] - _umean) * double(_nodes[src_node].cap),
                                                          (_umean - u[dst_node]) * double(_nodes[dst_node].cap)));
                        }
                        room.clear();
                        every.clear();
                        for (const auto& [t, ids] : _shards[src].cands) {
                            if (ids.empty()) {
                                continue;
                            }
                            every.push_back(t);
                            if (count_of(t, src) > lo(t, src) && count_of(t, dst) < hi(t, dst)) {
                                room.push_back(t);
                            }
                        }
                        const auto& tables = room.empty() ? every : room;
                        if (tables.empty()) {
                            continue;
                        }
                        // The first candidate with the smallest (size, table), and the first
                        // with the smallest (|size - want|, table).
                        std::optional<leg> small;
                        std::optional<leg> fit;
                        std::pair<int64_t, uint32_t> small_key;
                        std::pair<double, uint32_t> fit_key;
                        for (auto t : tables) {
                            for (auto i : cands(src, t)) {
                                const auto sz = _pool[i].size;
                                if (auto k = std::make_pair(sz, t); !small || k < small_key) {
                                    small = leg{i, t, src, dst};
                                    small_key = k;
                                }
                                if (auto k = std::make_pair(std::abs(double(sz) - want), t); !fit || k < fit_key) {
                                    fit = leg{i, t, src, dst};
                                    fit_key = k;
                                }
                            }
                        }
                        consider(one(small->cand, small->table, src, dst));
                        if (want > 0 && fit->cand != small->cand) {
                            consider(one(fit->cand, fit->table, src, dst));
                        }
                    }
                }
            }
        }

        // (b) node disk and safety: from nodes above the mean to nodes below it, fullest
        // and emptiest first.
        if (_p[0] > 0 || (_active && _p[2] > 0)) {
            std::vector<uint32_t> above;
            std::vector<uint32_t> below;
            for (uint32_t n = 0; n < _nodes.size(); ++n) {
                if (u[n] > _umean) {
                    above.push_back(n);
                } else if (u[n] < _umean) {
                    below.push_back(n);
                }
            }
            std::ranges::sort(above, std::less<>(), [&] (uint32_t n) { return std::make_pair(-u[n], n); });
            std::ranges::sort(below, std::less<>(), [&] (uint32_t n) { return std::make_pair(u[n], n); });
            if (!above.empty() && !below.empty()) {
                const auto full = above.front();
                const auto empty = below.front();
                double want = std::min((u[full] - _umean) * double(_nodes[full].cap),
                                       (_umean - u[empty]) * double(_nodes[empty].cap));
                auto shards_of = [&] (const std::vector<uint32_t>& nodes, bool biggest_first) {
                    std::vector<uint32_t> out;
                    for (auto n : nodes) {
                        const auto& ns = _nodes[n];
                        auto first = out.size();
                        for (uint32_t s = ns.first_shard; s < ns.first_shard + ns.nr_shards; ++s) {
                            out.push_back(s);
                        }
                        std::ranges::sort(out.begin() + first, out.end(), std::less<>(), [&] (uint32_t s) {
                            return std::make_pair(biggest_first ? -_shards[s].bytes : _shards[s].bytes, s);
                        });
                    }
                    return out;
                };
                pair_candidates(shards_of(above, true), shards_of(below, false), std::max(want, 0.0), consider);
            }
        }

        return best;
    }

    // Removes every candidate entry holding one of the tablets of `cand`, on every shard,
    // and marks the nodes holding them as touched.
    void erase_candidate(uint32_t cand) {
        for (auto gid : _pool[cand].tablets) {
            auto it = _entries_of_tablet.find(gid);
            if (it == _entries_of_tablet.end()) {
                continue;
            }
            for (auto e : it->second) {
                auto& entry = _pool[e];
                if (!entry.alive) {
                    continue;
                }
                entry.alive = false;
                auto& sh = _shards[entry.shard];
                auto& list = sh.cands[entry.table];
                list.erase(std::ranges::find(list, e));
                _nodes[sh.node].ver++;
            }
        }
    }

    void commit(const graded& g) {
        for (const auto& m : g.mv) {
            const size_t w = _pool[m.cand].count;
            _shards[m.src].read_load += w;
            _shards[m.dst].write_load += w;
            erase_candidate(m.cand);
        }
        mutate(g.mv, +1);
        for (auto n : touched_nodes(g.mv)) {
            _nodes[n].ver++;
        }
        for (size_t i = 0; i < nr_measures; ++i) {
            _p[i] += g.delta[i];
        }
    }
};

const std::vector<uint32_t> engine::_no_cands;

} // anonymous namespace

future<std::vector<planned_move>> make_plan(const engine_config& cfg, persistent_state& state,
        std::vector<node_input> nodes, constraint_check constraints) {
    engine e(cfg, state, std::move(nodes), std::move(constraints));
    co_return co_await e.plan();
}

} // namespace service::sblb
