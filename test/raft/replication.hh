/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <random>
#include <bit>
#include <seastar/core/app-template.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "utils/assert.hh"
#include "utils/xx_hasher.hh"
#include "utils/to_string.hh"
#include "test/raft/helpers.hh"
#include "test/lib/eventually.hh"
#include "test/lib/random_utils.hh"

// Test Raft library with declarative test definitions
//
//  Each test can be defined by (struct test_case):
//      .nodes                       number of nodes
//      .total_values                how many entries to append to leader nodes (default 100)
//      .initial_term                initial term # for setup
//      .initial_leader              what server is leader
//      .initial_states              initial logs of servers
//          .le                      log entries
//      .initial_snapshots           snapshots present at initial state for servers
//      .updates                     updates to execute on these servers
//          entries{x}               add the following x entries to the current leader
//          new_leader{x}            elect x as new leader
//          partition{a,b,c}         Only servers a,b,c are connected
//          partition{a,leader{b},c} Only servers a,b,c are connected, and make b leader
//          set_config{a,b,c}        Change configuration on leader
//          set_config{a,b,c}        Change configuration on leader
//          check_rpc_config{a,cfg}  Check rpc config of a matches
//          check_rpc_config{[],cfg} Check rpc config multiple nodes matches
//
//      run_test
//      - Creates the servers and initializes logs and snapshots
//        with hasher/digest and tickers to advance servers
//      - Processes updates one by one
//      - Appends remaining values
//      - Waits until all servers have logs of size of total_values entries
//      - Verifies hash
//      - Verifies persisted snapshots
//
//      Tests are run also with 20% random packet drops.
//      Two test cases are created for each with the macro
//          RAFT_TEST_CASE(<test name>, <test case>)

using namespace std::chrono_literals;
using namespace std::placeholders;

extern seastar::logger tlogger;

const auto dummy_command = std::numeric_limits<int>::min();

class hasher_int {
    std::variant<uint64_t, xx_hasher> _hasher;
    inline static thread_local bool _commutative{false};
public:
    static void set_commutative(bool commutative) {
        _commutative = commutative;
    }

    hasher_int() {
        if (_commutative) {
            _hasher.emplace<uint64_t>(0);
        } else {
            _hasher.emplace<xx_hasher>();
        }
    }

    void update(int val) noexcept {
        if (auto* h = get_if<xx_hasher>(&_hasher); h != nullptr) {
            h->update(reinterpret_cast<const char *>(&val), sizeof(val));
        } else {
            get<uint64_t>(_hasher) += val;
        }
    }

    uint64_t finalize_uint64() {
        if (auto* h = get_if<xx_hasher>(&_hasher); h != nullptr) {
            return h->finalize_uint64();
        } else {
            return get<uint64_t>(_hasher);
        }
    }

    static hasher_int hash_range(int max) {
        hasher_int h;
        for (int i = 0; i < max; ++i) {
            h.update(i);
        }
        return h;
    }
};

struct snapshot_value {
    hasher_int hasher;
    raft::index_t idx;
};

struct initial_state {
    raft::config_member address{config_member_from_id({})};
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot_descriptor snapshot;
    snapshot_value snp_value;
    raft::server::configuration server_config = raft::server::configuration{.append_request_threshold = 200};
};

// For verbosity in test declaration (i.e. node_id{x})
struct node_id {
    size_t id;
};


std::vector<raft::server_id> to_raft_id_vec(std::vector<node_id> nodes) noexcept;

raft::server_address_set address_set(std::vector<node_id> nodes) noexcept;
raft::config_member_set config_set(std::vector<node_id> nodes) noexcept;

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
struct entries {
    size_t n;
    // If provided, use this server to add entries.
    std::optional<size_t> server;
    // Don't wait for previous requests to finish before issuing a new one.
    bool concurrent;
    entries(size_t n_arg, std::optional<size_t> server_arg = {}, bool concurrent_arg = false)
        :n(n_arg), server(server_arg), concurrent(concurrent_arg) {}
};
struct new_leader {
    size_t id;
};
struct leader {
    size_t id;
};
// Inclusive range
struct range {
    size_t start;
    size_t end;
};
using partition = std::vector<std::variant<leader,range,int>>;


// Disconnect a node from the rest
struct isolate {
    size_t id;
};

// Disconnect 2 servers both ways
struct two_nodes {
    size_t first;
    size_t second;
};
struct disconnect : public two_nodes {};

struct stop {
    size_t id;
};

struct reset {
    size_t id;
    initial_state state;
};

struct wait_log {
    std::vector<size_t> int_ids;
    wait_log(size_t int_id) : int_ids({int_id}) {}
    wait_log(std::initializer_list<size_t> int_ids) : int_ids(int_ids) {}
};

struct set_config_entry {
    size_t node_idx;
    bool can_vote;

    set_config_entry(size_t idx, bool can_vote = true)
        : node_idx(idx), can_vote(can_vote)
    {}
};
using set_config = std::vector<set_config_entry>;

struct config {
    std::vector<node_id> curr;
    std::vector<node_id> prev;
    operator raft::configuration() {
        auto current = config_set(curr);
        auto previous = config_set(prev);
        return raft::configuration{current, previous};
    }
};

using rpc_address_set = std::vector<node_id>;

struct check_rpc_config {
    std::vector<node_id> nodes;
    rpc_address_set addrs;
    check_rpc_config(node_id node, rpc_address_set addrs) : nodes({node}), addrs(addrs) {}
    check_rpc_config(std::vector<node_id> nodes, rpc_address_set addrs) : nodes(nodes), addrs(addrs) {}
};

struct check_rpc_added {
    std::vector<node_id> nodes;
    size_t expected;
    check_rpc_added(node_id node, size_t expected) : nodes({node}), expected(expected) {}
    check_rpc_added(std::vector<node_id> nodes, size_t expected) : nodes(nodes), expected(expected) {}
};

struct check_rpc_removed {
    std::vector<node_id> nodes;
    size_t expected;
    check_rpc_removed(node_id node, size_t expected) : nodes({node}), expected(expected) {}
    check_rpc_removed(std::vector<node_id> nodes, size_t expected) : nodes(nodes), expected(expected) {}
};

using rpc_reset_counters = std::vector<node_id>;

struct tick {
    uint64_t ticks;
};

struct read_value {
    size_t node_idx; // which node should read
    size_t expected_index; // expected read index
};

using update = std::variant<entries, new_leader, partition, isolate, disconnect,
      stop, reset, wait_log, set_config, check_rpc_config, check_rpc_added,
      check_rpc_removed, rpc_reset_counters, tick, read_value>;

struct log_entry {
    unsigned term;
    std::variant<int, raft::configuration> data;
};

struct initial_log {
    std::vector<log_entry> le;
};

struct initial_snapshot {
    raft::snapshot_descriptor snap;
};

struct test_case {
    const size_t nodes;
    const size_t total_values = 100;
    uint64_t initial_term = 1;
    const size_t initial_leader = 0;
    const std::vector<struct initial_log> initial_states;
    const std::vector<struct initial_snapshot> initial_snapshots;
    const std::vector<raft::server::configuration> config;
    const std::vector<update> updates;
    const bool commutative_hash = false;
    const bool verify_persisted_snapshots = true;
    size_t get_first_val();
};

std::mt19937 random_generator() noexcept;

int rand() noexcept;

// Lets assume one snapshot per server
using snapshots = std::unordered_map<raft::server_id, std::unordered_map<raft::snapshot_id, snapshot_value>>;
using persisted_snapshots = std::unordered_map<raft::server_id, std::pair<raft::snapshot_descriptor, snapshot_value>>;

extern seastar::semaphore snapshot_sync;
// application of a snapshot with that id will be delayed until snapshot_sync is signaled
extern raft::snapshot_id delay_apply_snapshot;
// sending of a snapshot with that id will be delayed until snapshot_sync is signaled
extern raft::snapshot_id delay_send_snapshot;

// Test connectivity configuration
struct rpc_config {
    bool drops = false;
    // Network delay. Note implementation expects it to be smaller than tick delay
    std::chrono::milliseconds network_delay = 0ms;   // 0ms means no delays
    // Latency within same server
    std::chrono::milliseconds local_delay = 0ms;
    // How many nodes per server, rounded to closest power of 2 (fast prefix check)
    size_t local_nodes = 32;
    // Delay 0...extra_delay_max us to mimic busy server
    size_t extra_delay_max = 500;
};

template <typename Clock>
class raft_cluster {
    using apply_fn = std::function<size_t(raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher)>;
    class state_machine;
    class persistence;
    class connected;
    class failure_detector;
    class rpc;
    using rpc_net = std::unordered_map<raft::server_id, rpc*>;
    struct test_server {
        std::unique_ptr<raft::server> server;
        state_machine* sm;
        raft_cluster::rpc* rpc;
    };
    std::vector<test_server> _servers;
    std::unique_ptr<connected> _connected;
    std::unique_ptr<snapshots> _snapshots;
    std::unique_ptr<persisted_snapshots> _persisted_snapshots;
    size_t _apply_entries;
    size_t _next_val;
    rpc_config _rpc_config;
    bool _prevote;
    apply_fn _apply;
    std::unordered_set<size_t> _in_configuration;   // Servers in current configuration
    std::vector<seastar::timer<Clock>> _tickers;
    size_t _leader;
    std::vector<initial_state> get_states(test_case test, bool prevote);
    typename Clock::duration _tick_delta;
    bool _verify_persisted_snapshots;
    rpc_net _rpc_net;
    // Tick phase delay for each node, uniformly spread across tick delta
    std::vector<typename Clock::duration> _tick_delays;
public:
    raft_cluster(test_case test,
            apply_fn apply,
            size_t apply_entries, size_t first_val, size_t first_leader,
            bool prevote, typename Clock::duration tick_delta,
            rpc_config rpc_config);
    // No copy
    raft_cluster(const raft_cluster&) = delete;
    raft_cluster(raft_cluster&&) = default;
    raft::server& get_server(size_t id);
    future<> stop_server(size_t id, sstring reason = "");
    future<> reset_server(size_t id, initial_state state); // Reset a stopped server
    size_t size() {
        return _servers.size();
    }
    future<> start_all();
    future<> stop_all();
    future<> wait_all();
    void disconnect(size_t id, std::optional<raft::server_id> except = std::nullopt);
    void connect_all();
    void elapse_elections();
    future<> elect_new_leader(size_t new_leader);
    future<> free_election();
    future<> init_raft_tickers();
    void pause_tickers();
    future<> restart_tickers();
    void cancel_ticker(size_t id);
    void set_ticker_callback(size_t id) noexcept;
    void init_tick_delays(size_t n);
    future<> add_entry(size_t val, std::optional<size_t> server);
    future<> add_entries(size_t n, std::optional<size_t> server = std::nullopt);
    future<> add_entries_concurrent(size_t n, std::optional<size_t> server = std::nullopt);
    future<> add_remaining_entries();
    future<> wait_log(size_t follower);
    future<> wait_log(::wait_log followers);
    future<> wait_log_all();
    future<> change_configuration(::set_config sc);
    future<> check_rpc_config(::check_rpc_config cc);
    void check_rpc_added(::check_rpc_added expected) const;
    void check_rpc_removed(::check_rpc_removed expected) const;
    void rpc_reset_counters(::rpc_reset_counters nodes);
    future<> reconfigure_all();
    future<> partition(::partition p);
    future<> tick(::tick t);
    future<> read(read_value r);
    future<> stop(::stop server);
    future<> reset(::reset server);
    void disconnect(::disconnect nodes);
    future<> isolate(::isolate node);
    void verify();
private:
    test_server create_server(size_t id, initial_state state);
};

template <typename Clock>
class raft_cluster<Clock>::state_machine : public raft::state_machine {
    raft::server_id _id;
    apply_fn _apply;
    size_t _apply_entries;
    size_t _seen = 0;
    promise<> _done;
    snapshots* _snapshots;
public:
    lw_shared_ptr<hasher_int> hasher;
    state_machine(raft::server_id id, apply_fn apply, size_t apply_entries,
            snapshots* snapshots):
        _id(id), _apply(std::move(apply)), _apply_entries(apply_entries), _snapshots(snapshots),
        hasher(make_lw_shared<hasher_int>()) {}
    future<> apply(const std::vector<raft::command_cref> commands) override {
        auto n = _apply(_id, commands, hasher);
        _seen += n;
        if (n && _seen >= _apply_entries) {
            if (_seen > _apply_entries) {
                // Retrying `commit_status_unknown` may lead to this. Ref: #14072
                tlogger.warn("sm::apply[{}]: _seen ({}) overshot _apply_entries ({})", _id, _seen, _apply_entries);
            }
            _done.set_value();
        }
        tlogger.debug("sm::apply[{}] got {}/{} entries", _id, _seen, _apply_entries);
        return make_ready_future<>();
    }

    future<raft::snapshot_id> take_snapshot() override {
        auto snp_id = raft::snapshot_id::create_random_id();
        (*_snapshots)[_id][snp_id].hasher = *hasher;
        tlogger.debug("sm[{}] takes snapshot id {} {} seen {}", _id, (*_snapshots)[_id][snp_id].hasher.finalize_uint64(), snp_id, _seen);
        (*_snapshots)[_id][snp_id].idx = raft::index_t{_seen};
        return make_ready_future<raft::snapshot_id>(snp_id);
    }
    void drop_snapshot(raft::snapshot_id snp_id) override {
        (*_snapshots)[_id].erase(snp_id);
    }
    future<> load_snapshot(raft::snapshot_id snp_id) override {
        hasher = make_lw_shared<hasher_int>((*_snapshots)[_id][snp_id].hasher);
        tlogger.debug("sm[{}] loads snapshot {} idx={}", _id, (*_snapshots)[_id][snp_id].hasher.finalize_uint64(), (*_snapshots)[_id][snp_id].idx);
        _seen = (*_snapshots)[_id][snp_id].idx.value();
        if (_seen >= _apply_entries) {
            _done.set_value();
        }
        if (snp_id == delay_apply_snapshot) {
            snapshot_sync.signal();
            co_await snapshot_sync.wait();
        }
        co_return;
    };
    future<> abort() override { return make_ready_future<>(); }

    future<> done() {
        return _done.get_future();
    }
};

template <typename Clock>
class raft_cluster<Clock>::persistence : public raft::persistence {
    raft::server_id _id;
    initial_state _conf;
    snapshots* _snapshots;
    persisted_snapshots* _persisted_snapshots;
public:
    persistence(raft::server_id id, initial_state conf, snapshots* snapshots,
            persisted_snapshots* persisted_snapshots) : _id(id),
            _conf(std::move(conf)), _snapshots(snapshots),
            _persisted_snapshots(persisted_snapshots) {}
    persistence() {}
    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override { return seastar::sleep(1us); }
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    future<> store_commit_idx(raft::index_t) override {
        co_return;
    }
    future<raft::index_t> load_commit_idx() override {
        co_return raft::index_t{0};
    }
    future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override {
        (*_persisted_snapshots)[_id] = std::make_pair(snap, (*_snapshots)[_id][snap.id]);
        tlogger.debug("sm[{}] persists snapshot {}", _id, (*_snapshots)[_id][snap.id].hasher.finalize_uint64());
        return make_ready_future<>();
    }
    future<raft::snapshot_descriptor> load_snapshot_descriptor() override {
        return make_ready_future<raft::snapshot_descriptor>(_conf.snapshot);
    }
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override { return seastar::sleep(1us); };
    future<raft::log_entries> load_log() override {
        raft::log_entries log;
        for (auto&& e : _conf.log) {
            log.emplace_back(make_lw_shared(std::move(e)));
        }
        return make_ready_future<raft::log_entries>(std::move(log));
    }
    future<> truncate_log(raft::index_t idx) override { return make_ready_future<>(); }
    future<> abort() override { return make_ready_future<>(); }
};

template <typename Clock>
struct raft_cluster<Clock>::connected {
    struct connection {
       raft::server_id from;
       raft::server_id to;
       bool operator==(const connection &o) const {
           return from == o.from && to == o.to;
       }
    };

    struct hash_connection {
        std::size_t operator() (const connection &c) const {
            return std::hash<utils::UUID>()(c.from.id);
        }
    };

    // Map of from->to disconnections
    std::unordered_set<connection, hash_connection> disconnected;
    size_t n;
    connected(size_t n) : n(n) { }
    // Cut connectivity of two servers both ways
    void cut(raft::server_id id1, raft::server_id id2) {
        disconnected.insert({id1, id2});
        disconnected.insert({id2, id1});
    }
    // Isolate a server
    void disconnect(raft::server_id id, std::optional<raft::server_id> except = std::nullopt) {
        for (size_t other = 0; other < n; ++other) {
            auto other_id = to_raft_id(other);
            // Disconnect if not the same, and the other id is not an exception
            // disconnect(0, except=1)
            if (id != other_id && !(except && other_id == *except)) {
                cut(id, other_id);
            }
        }
    }
    // Re-connect a node to all other nodes
    void connect(raft::server_id id) {
        for (auto it = disconnected.begin(); it != disconnected.end(); ) {
            if (id == it->from || id == it->to) {
                it = disconnected.erase(it);
            } else {
                ++it;
            }
        }
    }
    void connect_all() {
        disconnected.clear();
    }
    bool operator()(raft::server_id id1, raft::server_id id2) {
        // It's connected if both ways are not disconnected
        return !disconnected.contains({id1, id2}) && !disconnected.contains({id1, id2});
    }
};

template <typename Clock>
class raft_cluster<Clock>::failure_detector : public raft::failure_detector {
    raft::server_id _id;
    connected* _connected;
public:
    failure_detector(raft::server_id id, connected* connected) : _id(id), _connected(connected) {}
    bool is_alive(raft::server_id server) override {
        return (*_connected)(server, _id);
    }
};

template <typename Clock>
class raft_cluster<Clock>::rpc : public raft::rpc {
    raft::server_id _id;
    connected* _connected;
    snapshots* _snapshots;
    rpc_net& _net;
    rpc_config _rpc_config;
    raft::server_address_set _known_peers;
    uint32_t _servers_added = 0;
    uint32_t _servers_removed = 0;
    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;
    // prefix mask for shards in same node
    uint64_t _same_node_prefix;
    bool _delays;
public:
    rpc(raft::server_id id, connected* connected, snapshots* snapshots,
        rpc_net& net, rpc_config rpc_config)
            : _id(id)
            , _connected(connected)
            , _snapshots(snapshots)
            , _net(net)
            , _rpc_config(rpc_config)
            , _delays(rpc_config.network_delay > 0ms)
    {
        _net[_id] = this;
        // Rounds to next power of 2
        _same_node_prefix = (1 << std::bit_width(_rpc_config.local_nodes)) - 1;
    }
    bool drop_packet() {
        return _rpc_config.drops && !(rand() % 5);
    }
    bool is_local_node(raft::server_id& id) {
        return (to_int_id(_id.id) & _same_node_prefix) == (to_int_id(id.id) & _same_node_prefix);
    }
    typename Clock::duration get_delay(raft::server_id id) {
        if (is_local_node(id)) {
            return  _rpc_config.local_delay;
        } else {
            return _rpc_config.network_delay;
        }
    }
    auto rand_extra_delay() {
        return tests::random::get_int<size_t>(0, _rpc_config.extra_delay_max) * 1us;
    }

    future<raft::snapshot_reply> send_snapshot(raft::server_id id,
        const raft::install_snapshot& snap, seastar::abort_source& as) override {

        if (!_net.count(id)) {
            throw std::runtime_error("trying to send a message to an unknown node");
        }
        if (!(*_connected)(id, _id)) {
            throw std::runtime_error("cannot send snapshot since nodes are disconnected");
        }
        auto s = snap; // snap is not always held alive by a caller
        (*_snapshots)[id][s.snp.id] = (*_snapshots)[_id][s.snp.id];
        if (s.snp.id == delay_send_snapshot) {
            co_await snapshot_sync.wait();
            snapshot_sync.signal();
        }
        co_return co_await _net[id]->_client->apply_snapshot(_id, std::move(s));
    }

    future<> send_append_entries(raft::server_id id, const raft::append_request& append_request) override {
        if (!_net.count(id)) {
            return make_exception_future(std::runtime_error("trying to send a message to an unknown node"));
        }
        if (!(*_connected)(id, _id)) {
            return make_exception_future<>(std::runtime_error("cannot send append since nodes are disconnected"));
        }
        if (!drop_packet()) {
            if (_delays) {
                return with_gate(_gate, [&, this] () mutable -> future<> {
                    return seastar::sleep(get_delay(id) + rand_extra_delay()).then(
                            [this, id = std::move(id), append_request = std::move(append_request)] {
                        if ((*_connected)(id, _id)) {
                            _net[id]->_client->append_entries(_id, append_request);
                        }
                    });
                });
            } else {
                _net[id]->_client->append_entries(_id, append_request);
            }
        }
        return make_ready_future<>();
    }
    void send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*rpc::_connected)(id, rpc::_id)) {
            return;
        }
        if (!drop_packet()) {
            if (_delays) {
                (void)with_gate(_gate, [&, this] () mutable -> future<> {
                    return seastar::sleep(get_delay(id) + rand_extra_delay()).then(
                            [this, id = std::move(id), reply = std::move(reply)] {
                        if ((*_connected)(id, _id)) {
                            _net[id]->_client->append_entries_reply(rpc::_id, std::move(reply));
                        }
                    });
                });
            } else {
                _net[id]->_client->append_entries_reply(rpc::_id, std::move(reply));
            }
        }
    }
    void send_vote_request(raft::server_id id, const raft::vote_request& vote_request) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*rpc::_connected)(id, rpc::_id)) {
            return;
        }
        if (_delays) {
            (void)with_gate(_gate, [&, this] () mutable -> future<> {
                return seastar::sleep(get_delay(id) + rand_extra_delay()).then(
                        [this, id = std::move(id), vote_request = std::move(vote_request)] {
                    if ((*_connected)(id, _id)) {
                        _net[id]->_client->request_vote(rpc::_id, std::move(vote_request));
                    }
                });
            });
        } else {
            _net[id]->_client->request_vote(rpc::_id, std::move(vote_request));
        }
    }
    void send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*rpc::_connected)(id, rpc::_id)) {
            return;
        }
        if (_delays) {
            (void)with_gate(_gate, [&, this] () mutable -> future<> {
                return seastar::sleep(get_delay(id) + rand_extra_delay()).then([=, this] {
                    if ((*_connected)(id, _id)) {
                        _net[id]->_client->request_vote_reply(rpc::_id, vote_reply);
                    }
                });
            });
        } else {
            _net[id]->_client->request_vote_reply(rpc::_id, vote_reply);
        }
    }
    void send_timeout_now(raft::server_id id, const raft::timeout_now& timeout_now) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        _net[id]->_client->timeout_now_request(_id, std::move(timeout_now));
    }
    future<> abort() override {
        tlogger.debug("[{}] rpc aborting", _id);
        return _gate.close();
    }
    void send_read_quorum(raft::server_id id, const raft::read_quorum& read_quorum) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        if (!drop_packet()) {
            _net[id]->_client->read_quorum_request(_id, read_quorum);
        }
    }
    void send_read_quorum_reply(raft::server_id id, const raft::read_quorum_reply& reply) override {
        if (!_net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        if (!drop_packet()) {
            _net[id]->_client->read_quorum_reply(_id, std::move(reply));
        }
    }
    future<raft::read_barrier_reply> execute_read_barrier_on_leader(raft::server_id id) override {
        if (!_net.count(id)) {
            return make_exception_future<raft::read_barrier_reply>(std::runtime_error("trying to send a message to an unknown node"));
        }
        if (!(*_connected)(id, _id)) {
            return make_exception_future<raft::read_barrier_reply>(std::runtime_error("cannot send append since nodes are disconnected"));
        }
        return _net[id]->_client->execute_read_barrier(_id, nullptr);
    }
    void check_known_and_connected(raft::server_id id) {
        if (!_net.count(id)) {
            throw std::runtime_error("trying to send a message to an unknown node");
        }
        if (!(*_connected)(id, _id)) {
            throw std::runtime_error("cannot send since nodes are disconnected");
        }
    }
    future<raft::add_entry_reply> send_add_entry(raft::server_id id, const raft::command& cmd) override {
        check_known_and_connected(id);
        return _net[id]->_client->execute_add_entry(_id, cmd, nullptr);
    }
    future<raft::add_entry_reply> send_modify_config(raft::server_id id,
        const std::vector<raft::config_member>& add,
        const std::vector<raft::server_id>& del) override {
        check_known_and_connected(id);
        return _net[id]->_client->execute_modify_config(_id, add, del, nullptr);
    }

    void on_configuration_change(raft::server_address_set add, raft::server_address_set del) override {
        _known_peers.merge(add);
        _servers_added += add.size();
        for (const auto& addr: del) {
            _known_peers.erase(addr);
        }
        _servers_removed += del.size();
    }

    const raft::server_address_set& known_peers() const {
        return _known_peers;
    }
    void reset_counters() {
        _servers_added = 0;
        _servers_removed = 0;
    }
    uint32_t servers_added() const {
        return _servers_added;
    }
    uint32_t servers_removed() const {
        return _servers_removed;
    }
};

template <typename Clock>
typename raft_cluster<Clock>::test_server raft_cluster<Clock>::create_server(size_t id, initial_state state) {

    auto uuid = to_raft_id(id);
    auto sm = std::make_unique<state_machine>(uuid, _apply, _apply_entries, _snapshots.get());
    auto& rsm = *sm;

    std::unique_ptr<raft_cluster::rpc> mrpc = std::make_unique<raft_cluster::rpc>(uuid, _connected.get(),
            _snapshots.get(), _rpc_net, _rpc_config);
    auto& rpc_ref = *mrpc;

    auto mpersistence = std::make_unique<persistence>(uuid, state,
            _snapshots.get(), _persisted_snapshots.get());
    auto fd = seastar::make_shared<failure_detector>(uuid, _connected.get());

    auto raft = raft::create_server(uuid, std::move(mrpc), std::move(sm), std::move(mpersistence),
        std::move(fd), state.server_config);

    return {
        std::move(raft),
        &rsm,
        &rpc_ref
    };
}

template <typename Clock>
raft_cluster<Clock>::raft_cluster(test_case test,
    apply_fn apply,
    size_t apply_entries, size_t first_val, size_t first_leader,
    bool prevote, typename Clock::duration tick_delta,
    rpc_config rpc_config)
        : _connected(std::make_unique<struct connected>(test.nodes))
        , _snapshots(std::make_unique<snapshots>())
        , _persisted_snapshots(std::make_unique<persisted_snapshots>())
        , _apply_entries(apply_entries)
        , _next_val(first_val)
        , _rpc_config(rpc_config)
        , _prevote(prevote)
        , _apply(apply)
        , _leader(first_leader)
        , _tick_delta(tick_delta)
        , _verify_persisted_snapshots(test.verify_persisted_snapshots) {

    auto states = get_states(test, prevote);
    for (size_t s = 0; s < states.size(); ++s) {
        _in_configuration.insert(s);
    }

    raft::configuration config;

    for (size_t i = 0; i < states.size(); i++) {
        states[i].address = config_member_from_id(to_raft_id(i));
        config.current.emplace(states[i].address);
    }

    if (_rpc_config.network_delay > 0ms) {
        init_tick_delays(test.nodes);
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = states[i].address;
        states[i].snapshot.config = config;
        (*_snapshots)[s.addr.id][states[i].snapshot.id] = states[i].snp_value;
        _servers.emplace_back(create_server(i, states[i]));
    }
}

template <typename Clock>
void raft_cluster<Clock>::init_tick_delays(size_t n) {
    _tick_delays.reserve(n);
    for (size_t s = 0; s < n; s++) {
        auto delay = tests::random::get_int<size_t>(0, _tick_delta.count());
        _tick_delays.push_back(delay * _tick_delta / _tick_delta.count());
    }
}

template <typename Clock>
raft::server& raft_cluster<Clock>::get_server(size_t id) {
    return *_servers[id].server;
}

template <typename Clock>
future<> raft_cluster<Clock>::stop_server(size_t id, sstring reason) {
    cancel_ticker(id);
    co_await _servers[id].server->abort(std::move(reason));
    if (_snapshots->contains(to_raft_id(id))) {
        BOOST_CHECK_LE((*_snapshots)[to_raft_id(id)].size(), 2);
        _snapshots->erase(to_raft_id(id));
    }
    _persisted_snapshots->erase(to_raft_id(id));
}

// Reset previously stopped server
template <typename Clock>
future<> raft_cluster<Clock>::reset_server(size_t id, initial_state state) {
    _servers[id] = create_server(id, state);
    co_await _servers[id].server->start();
    set_ticker_callback(id);
}

template <typename Clock>
future<> raft_cluster<Clock>::start_all() {
    co_await coroutine::parallel_for_each(_servers, [] (auto& r) {
        return r.server->start();
    });
    co_await init_raft_tickers();
    BOOST_TEST_MESSAGE("Electing first leader " << _leader);
    _servers[_leader].server->wait_until_candidate();
    co_await _servers[_leader].server->wait_election_done();
}

template <typename Clock>
future<> raft_cluster<Clock>::stop_all() {
    for (auto s: _in_configuration) {
        co_await stop_server(s);
    };
}

template <typename Clock>
future<> raft_cluster<Clock>::wait_all() {
    for (auto s: _in_configuration) {
        co_await _servers[s].sm->done();
    }
}

template <typename Clock>
void raft_cluster<Clock>::disconnect(size_t id, std::optional<raft::server_id> except) {
    _connected->disconnect(to_raft_id(id), except);
}

template <typename Clock>
void raft_cluster<Clock>::connect_all() {
    _connected->connect_all();
}

// Add consecutive integer entries to a leader
template <typename Clock>
future<> raft_cluster<Clock>::add_entries(size_t n, std::optional<size_t> server) {
    size_t end = _next_val + n;
    while (_next_val != end) {
        co_await add_entry(_next_val, server);
        _next_val++;
    }
}

// Add consecutive integer entries to a leader concurrently
template <typename Clock>
future<> raft_cluster<Clock>::add_entries_concurrent(size_t n, std::optional<size_t> server) {
    const auto start = _next_val;
    _next_val += n;
    return parallel_for_each(boost::irange(start, _next_val), [this, server](size_t v) { return add_entry(v, server); });
}

template <typename Clock>
future<> raft_cluster<Clock>::add_entry(size_t val, std::optional<size_t> server) {
    while (true) {
        try {
            auto& at = _servers[server ? *server : _leader].server;
            co_await at->add_entry(create_command(val), raft::wait_type::committed, nullptr);
            break;
        } catch (raft::commit_status_unknown& e) {
            // FIXME: in some cases when we get `commit_status_unknown` the entry may have been applied.
            // Retrying it could lead to double application which causes hard to debug failures, e.g. #14029.
            // For now we leave a warning so the logs give a hint if such a failure happens and we need
            // to debug it. Ideally we would never have to handle `commit_status_unknown` but some replication
            // tests rely on retrying it during leader changes etc.
            tlogger.warn("replication_test: got `commit_status_unknown` from `add_entry`"
                    ", val: {}, server: {}", val, server);
        } catch (raft::dropped_entry& e) {
            // retry if an entry is dropped because the leader have changed after it was submitted
        }
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::add_remaining_entries() {
    co_await add_entries(_apply_entries - _next_val);
}

template <typename Clock>
future<> raft_cluster<Clock>::init_raft_tickers() {
    _tickers.resize(_servers.size());
    // Only start tickers for servers in configuration
    for (auto s: _in_configuration) {
        _tickers[s].set_callback([&, s] {
            _servers[s].server->tick();
        });
    }
    co_await restart_tickers();
}

template <typename Clock>
void raft_cluster<Clock>::pause_tickers() {
    for (auto s: _in_configuration) {
        _tickers[s].cancel();
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::restart_tickers() {
    if (_tick_delays.size()) {
        co_await coroutine::parallel_for_each(_in_configuration, [&] (size_t s) -> future<> {
            co_await seastar::sleep(_tick_delays[s]);
            _tickers[s].rearm_periodic(_tick_delta);
        });
    } else {
        for (auto s: _in_configuration) {
            _tickers[s].rearm_periodic(_tick_delta);
        }
    }
}

template <typename Clock>
void raft_cluster<Clock>::cancel_ticker(size_t id) {
    _tickers[id].cancel();
}

template <typename Clock>
void raft_cluster<Clock>::set_ticker_callback(size_t id) noexcept {
    _tickers[id].set_callback([&, id] {
        _servers[id].server->tick();
    });
}

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, raft::index_t start_idx);

size_t apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands,
        lw_shared_ptr<hasher_int> hasher);

// Wait for leader log to propagate to follower
template <typename Clock>
future<> raft_cluster<Clock>::wait_log(size_t follower) {
    if ((*_connected)(to_raft_id(_leader), to_raft_id(follower)) &&
           _in_configuration.contains(_leader) && _in_configuration.contains(follower)) {
        auto leader_log_idx_term = _servers[_leader].server->log_last_idx_term();
        co_await _servers[follower].server->wait_log_idx_term(leader_log_idx_term);
    }
}

// Wait for leader log to propagate to specified followers
template <typename Clock>
future<> raft_cluster<Clock>::wait_log(::wait_log followers) {
    auto leader_log_idx_term = _servers[_leader].server->log_last_idx_term();
    for (auto s: followers.int_ids) {
        co_await _servers[s].server->wait_log_idx_term(leader_log_idx_term);
    }
}

// Wait for all connected followers to catch up
template <typename Clock>
future<> raft_cluster<Clock>::wait_log_all() {
    auto leader_log_idx_term = _servers[_leader].server->log_last_idx_term();
    for (size_t s = 0; s < _servers.size(); ++s) {
        if (s != _leader && (*_connected)(to_raft_id(s), to_raft_id(_leader)) &&
                _in_configuration.contains(s)) {
            co_await _servers[s].server->wait_log_idx_term(leader_log_idx_term);
        }
    }
}

template <typename Clock>
void raft_cluster<Clock>::elapse_elections() {
    for (auto s: _in_configuration) {
        _servers[s].server->elapse_election();
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::elect_new_leader(size_t new_leader) {
    BOOST_CHECK_MESSAGE(new_leader < _servers.size(),
            format("Wrong next leader value {}", new_leader));

    if (new_leader == _leader) {
        co_return;
    }

    // Prevote prevents dueling candidate from bumping up term
    // but in corner cases it needs a loop to retry.
    // With prevote we need our candidate to retry bumping term
    // and waiting log on every loop.
    if (_prevote) {
        bool both_connected = (*_connected)(to_raft_id(_leader), to_raft_id(new_leader));
        if (both_connected) {
            co_await wait_log(new_leader);
        }

        pause_tickers();
        // Leader could be already partially disconnected, save current connectivity state
        struct connected prev_disconnected = *_connected;
        // Disconnect current leader from everyone
        _connected->disconnect(to_raft_id(_leader));
        // Make move all nodes past election threshold, also making old leader follower
        elapse_elections();

        do {
            // Consume leader output messages since a stray append might make new leader step down
            co_await yield();                 // yield
            _servers[new_leader].server->wait_until_candidate();

            if (both_connected) {
                // Allow old leader to vote for new candidate while not looking alive to others
                // Re-connect old leader
                _connected->connect(to_raft_id(_leader));
                // Disconnect old leader from all nodes except new leader
                _connected->disconnect(to_raft_id(_leader), to_raft_id(new_leader));
            }
            co_await _servers[new_leader].server->wait_election_done();

            if (both_connected) {
                // Re-disconnect leader for next loop
                _connected->disconnect(to_raft_id(_leader));
            }
        } while (!_servers[new_leader].server->is_leader());

        // Restore connections to the original setting
        *_connected = prev_disconnected;
        co_await restart_tickers();
        co_await wait_log_all();

    } else {  // not prevote

        do {
            if ((*_connected)(to_raft_id(_leader), to_raft_id(new_leader))) {
                co_await wait_log(new_leader);
            }

            pause_tickers();
            // Leader could be already partially disconnected, save current connectivity state
            struct connected prev_disconnected = *_connected;
            // Disconnect current leader from everyone
            _connected->disconnect(to_raft_id(_leader));
            // Make move all nodes past election threshold, also making old leader follower
            elapse_elections();
            // Consume leader output messages since a stray append might make new leader step down
            co_await yield();                 // yield
            _servers[new_leader].server->wait_until_candidate();
            // Re-connect old leader
            _connected->connect(to_raft_id(_leader));
            // Disconnect old leader from all nodes except new leader
            _connected->disconnect(to_raft_id(_leader), to_raft_id(new_leader));
            co_await restart_tickers();
            co_await _servers[new_leader].server->wait_election_done();

            // Restore connections to the original setting
            *_connected = prev_disconnected;
        } while (!_servers[new_leader].server->is_leader());
    }

    tlogger.debug("confirmed leader on {}", to_raft_id(new_leader));
    _leader = new_leader;
}

// Run a free election of nodes in configuration
// NOTE: there should be enough nodes capable of participating
template <typename Clock>
future<> raft_cluster<Clock>::free_election() {
    tlogger.debug("Running free election");
    size_t loops = 0;
    for (;; loops++) {
        co_await seastar::sleep(_tick_delta);   // Wait for election rpc exchanges
        // find if we have a leader
        for (auto s: _in_configuration) {
            if (_servers[s].server->is_leader()) {
                tlogger.debug("New leader {} (in {} loops)", to_raft_id(s), loops);
                _leader = s;
                co_return;
            }
        }
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::change_configuration(set_config sc) {
    BOOST_CHECK_MESSAGE(sc.size() > 0, "Empty configuration change not supported");
    raft::config_member_set set;
    std::unordered_set<size_t> new_config;
    for (auto s: sc) {
        new_config.insert(s.node_idx);
        auto m = to_config_member(s.node_idx);
        m.can_vote = s.can_vote;
        set.insert(std::move(m));
        BOOST_CHECK_MESSAGE(s.node_idx < _servers.size(),
                format("Configuration element {} past node limit {}", s.node_idx, _servers.size() - 1));
    }
    BOOST_CHECK_MESSAGE(new_config.contains(_leader) || sc.size() < (_servers.size()/2 + 1),
            "New configuration without old leader and below quorum size (no election)");

    if (!new_config.contains(_leader)) {
        // Wait log on all nodes in new config before change
        for (auto s: sc) {
            co_await wait_log(s.node_idx);
        }
    }

    // Start nodes in new configuration but not in current configuration (re-added)
    for (auto s: new_config) {
        if (!_in_configuration.contains(s)) {
            tlogger.debug("Starting node being re-added to configuration {}", s);
            co_await reset_server(s, initial_state{.log = {}});

            if (_tick_delays.size()) {
                co_await seastar::sleep(_tick_delays[s]);
            }
            _tickers[s].rearm_periodic(_tick_delta);
        }
    }

    tlogger.debug("Changing configuration on leader {}", _leader);
    co_await _servers[_leader].server->set_configuration(std::move(set), nullptr);

    if (!new_config.contains(_leader)) {
        co_await free_election();
    }

    // Now we know joint configuration was applied
    // Add a dummy entry to confirm new configuration was committed
    try {
        co_await _servers[_leader].server->add_entry(create_command(dummy_command),
                raft::wait_type::committed, nullptr);
    } catch (raft::not_a_leader& e) {
        // leader stepped down, implying config fully changed
    } catch (raft::commit_status_unknown& e) {}

    // Stop nodes no longer in configuration
    for (auto s: _in_configuration) {
        if (!new_config.contains(s)) {
            _tickers[s].cancel();
            co_await stop_server(s);
        }
    }

    _in_configuration = new_config;
}

template <typename Clock>
future<> raft_cluster<Clock>::check_rpc_config(::check_rpc_config cc) {
    auto as = address_set(cc.addrs);
    for (auto& node: cc.nodes) {
        BOOST_CHECK(node.id < _servers.size());
        co_await seastar::async([&] {
            CHECK_EVENTUALLY_EQUAL(_servers[node.id].rpc->known_peers(), as);
        });
    }
}

template <typename Clock>
void raft_cluster<Clock>::check_rpc_added(::check_rpc_added expected) const {
    for (auto node: expected.nodes) {
        BOOST_CHECK_MESSAGE(_servers[node.id].rpc->servers_added() == expected.expected,
                format("RPC added {} does not match expected {}",
                    _servers[node.id].rpc->servers_added(), expected.expected));
    }
}

template <typename Clock>
void raft_cluster<Clock>::check_rpc_removed(::check_rpc_removed expected) const {
    for (auto node: expected.nodes) {
        BOOST_CHECK_MESSAGE(_servers[node.id].rpc->servers_removed() == expected.expected,
                format("RPC removed {} does not match expected {}",
                    _servers[node.id].rpc->servers_removed(), expected.expected));
    }
}

template <typename Clock>
void raft_cluster<Clock>::rpc_reset_counters(::rpc_reset_counters nodes) {
    for (auto node: nodes) {
        _servers[node.id].rpc->reset_counters();
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::reconfigure_all() {
    if (_in_configuration.size() < _servers.size()) {
        set_config sc;
        for (size_t s = 0; s < _servers.size(); ++s) {
            sc.push_back(s);
        }
        co_await change_configuration(std::move(sc));
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::partition(::partition p) {
    tlogger.debug("partitioning");
    std::unordered_set<size_t> partition_servers;
    std::optional<size_t> next_leader;
    for (auto s: p) {
        if (std::holds_alternative<struct leader>(s)) {
            next_leader = std::get<struct leader>(s).id;
            partition_servers.insert(*next_leader);
        } else if (std::holds_alternative<struct range>(s)) {
            auto range = std::get<struct range>(s);
            for (size_t id = range.start; id <= range.end; id++) {
                SCYLLA_ASSERT(id < _servers.size());
                partition_servers.insert(id);
            }
        } else {
            partition_servers.insert(std::get<int>(s));
        }
    }
    if (next_leader) {
        // Wait for log to propagate to next leader, before disconnections
        co_await wait_log(*next_leader);
    } else {
        // No leader specified, wait log for all connected servers, before disconnections
        for (auto s: partition_servers) {
            if (_in_configuration.contains(s)) {
                co_await wait_log(s);
            }
        }
    }
    pause_tickers();
    _connected->connect_all();
    // NOTE: connectivity is independent of configuration so it's for all servers
    for (size_t s = 0; s < _servers.size(); ++s) {
        if (partition_servers.find(s) == partition_servers.end()) {
            // Disconnect servers not in main partition
            _connected->disconnect(to_raft_id(s));
        }
    }
    if (next_leader) {
        // New leader specified, elect it
        co_await elect_new_leader(*next_leader); // restarts tickers
    } else if (partition_servers.find(_leader) == partition_servers.end() && p.size() > 0) {
        // Old leader disconnected and not specified new, free election
        co_await restart_tickers();
        _servers[_leader].server->elapse_election();   // make old leader step down
        co_await free_election();
    } else {
        co_await restart_tickers();
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::tick(::tick t) {
    for (uint64_t i = 0; i < t.ticks; i++) {
        for (auto&& s: _servers) {
            s.server->tick();
        }
        co_await yield();
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::read(read_value r) {
    co_await _servers[r.node_idx].server->read_barrier(nullptr);
    auto val = _servers[r.node_idx].sm->hasher->finalize_uint64();
    auto expected = hasher_int::hash_range(r.expected_index).finalize_uint64();
    BOOST_CHECK_MESSAGE(val == expected,
            format("Read on server {} saw the wrong value {} != {}", r.node_idx, val, expected));
}


template <typename Clock>
future<> raft_cluster<Clock>::stop(::stop server) {
    co_await stop_server(server.id);
}

template <typename Clock>
future<> raft_cluster<Clock>::reset(::reset server) {
    co_await reset_server(server.id, server.state);
}

template <typename Clock>
void raft_cluster<Clock>::disconnect(::disconnect nodes) {
    _connected->cut(to_raft_id(nodes.first), to_raft_id(nodes.second));
}

template <typename Clock>
future<> raft_cluster<Clock>::isolate(::isolate node) {
    tlogger.debug("disconnecting {}", to_raft_id(node.id));
    _connected->disconnect(to_raft_id(node.id));
    if (node.id == _leader) {
        _servers[_leader].server->elapse_election();   // make old leader step down
        co_await free_election();
    }
    co_return;
}

template <typename Clock>
void raft_cluster<Clock>::verify() {
    BOOST_TEST_MESSAGE("Verifying hashes match expected (snapshot and apply calls)");
    auto expected = hasher_int::hash_range(_apply_entries).finalize_uint64();
    for (auto i: _in_configuration) {
        auto digest = _servers[i].sm->hasher->finalize_uint64();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Digest doesn't match for server [{}]: {} != {}", i, digest, expected));
    }

    if (_verify_persisted_snapshots) {
        BOOST_TEST_MESSAGE("Verifying persisted snapshots");
        // TODO: check that snapshot is taken when it should be
        for (auto& s : (*_persisted_snapshots)) {
            auto& [snp, val] = s.second;
            auto digest = val.hasher.finalize_uint64();
            auto expected = hasher_int::hash_range(val.idx.value()).finalize_uint64();
            BOOST_CHECK_MESSAGE(digest == expected,
                                format("Persisted snapshot {} doesn't match {} != {}", snp.id, digest, expected));
        }
    }
}

template <typename Clock>
std::vector<initial_state> raft_cluster<Clock>::get_states(test_case test, bool prevote) {
    std::vector<initial_state> states(test.nodes);       // Server initial states

    size_t leader = test.initial_leader;

    states[leader].term = raft::term_t{test.initial_term};

    // Server initial logs, etc
    for (size_t i = 0; i < states.size(); ++i) {
        raft::index_t start_idx{1};
        if (i < test.initial_snapshots.size()) {
            states[i].snapshot = test.initial_snapshots[i].snap;
            states[i].snp_value.hasher = hasher_int::hash_range(test.initial_snapshots[i].snap.idx.value());
            states[i].snp_value.idx = test.initial_snapshots[i].snap.idx;
            start_idx = states[i].snapshot.idx + raft::index_t{1};
        }
        if (i < test.initial_states.size()) {
            auto state = test.initial_states[i];
            states[i].log = create_log(state.le, start_idx);
        } else {
            states[i].log = {};
        }
        if (i < test.config.size()) {
            states[i].server_config = test.config[i];
        } else {
            states[i].server_config = { .enable_prevoting = prevote };
        }
    }
    return states;
}

template <typename Clock>
struct run_test {
    future<> operator() (test_case test, bool prevote, typename Clock::duration tick_delta,
            rpc_config rpc_config) {

        hasher_int::set_commutative(test.commutative_hash);

        tlogger.debug("starting test with {}",
                rpc_config.network_delay > 0ms? "delays" : "no delays");

        raft_cluster<Clock> rafts(test, ::apply_changes, test.total_values,
                test.get_first_val(), test.initial_leader, prevote,
                tick_delta, rpc_config);
        co_await rafts.start_all();

        BOOST_TEST_MESSAGE("Processing updates");

        // Process all updates in order
        for (auto update: test.updates) {
            co_await std::visit(make_visitor(
            [&rafts] (entries update) -> future<> {
                co_await (update.concurrent
                        ? rafts.add_entries_concurrent(update.n, update.server)
                        : rafts.add_entries(update.n, update.server));
            },
            [&rafts] (new_leader update) -> future<> {
                co_await rafts.elect_new_leader(update.id);
            },
            [&rafts] (disconnect update) -> future<> {
                rafts.disconnect(update);
                co_return;
            },
            [&rafts] (::isolate update) -> future<> {
                co_await rafts.isolate(update);
            },
            [&rafts] (partition update) -> future<> {
                co_await rafts.partition(update);
            },
            [&rafts] (stop update) -> future<> {
                co_await rafts.stop(update);
            },
            [&rafts] (reset update) -> future<> {
                co_await rafts.reset(update);
            },
            [&rafts] (wait_log update) -> future<> {
                co_await rafts.wait_log(update);
            },
            [&rafts] (set_config update) -> future<> {
                co_await rafts.change_configuration(update);
            },
            [&rafts] (check_rpc_config update) -> future<> {
                co_await rafts.check_rpc_config(update);
            },
            [&rafts] (check_rpc_added update) -> future<> {
                rafts.check_rpc_added(update);
                co_return;
            },
            [&rafts] (check_rpc_removed update) -> future<> {
                rafts.check_rpc_removed(update);
                co_return;
            },
            [&rafts] (rpc_reset_counters update) -> future<> {
                rafts.rpc_reset_counters(update);
                co_return;
            },
            [&rafts] (tick update) -> future<> {
                co_await rafts.tick(update);
            },
            [&rafts] (read_value update) -> future<> {
                co_await rafts.read(update);
            }
            ), std::move(update));
        }

        // Reconnect and bring all nodes back into configuration, if needed
        rafts.connect_all();
        co_await rafts.reconfigure_all();

        if (test.total_values > 0) {
            BOOST_TEST_MESSAGE("Appending remaining values");
            co_await rafts.add_remaining_entries();
            co_await rafts.wait_all();
        }

        co_await rafts.stop_all();

        if (test.total_values > 0) {
            rafts.verify();
        }
    }
};

template <typename Clock>
void replication_test(struct test_case test, bool prevote,
        typename Clock::duration tick_delta,
        rpc_config rpc_config = {}) {
    run_test<Clock>{}(std::move(test), prevote, tick_delta, rpc_config).get();
}
