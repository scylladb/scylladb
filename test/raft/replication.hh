/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "xx_hasher.hh"
#include "test/raft/helpers.hh"
#include "test/lib/eventually.hh"

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

static seastar::logger tlogger("test");

const auto dummy_command = std::numeric_limits<int>::min();

class hasher_int : public xx_hasher {
public:
    using xx_hasher::xx_hasher;
    void update(int val) noexcept {
        xx_hasher::update(reinterpret_cast<const char *>(&val), sizeof(val));
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
    raft::server_address address;
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot snapshot;
    snapshot_value snp_value;
    raft::server::configuration server_config = raft::server::configuration{.append_request_threshold = 200};
};

// For verbosity in test declaration (i.e. node_id{x})
struct node_id {
    size_t id;
};


std::vector<raft::server_id> to_raft_id_vec(std::vector<node_id> nodes) {
    std::vector<raft::server_id> ret;
    for (auto node: nodes) {
        ret.push_back(raft::server_id{to_raft_uuid(node.id)});
    }
    return ret;
}

raft::server_address_set address_set(std::vector<node_id> nodes) {
    return address_set(to_raft_id_vec(nodes));
}

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
struct entries {
    size_t n;
};
struct new_leader {
    size_t id;
};
struct leader {
    size_t id;
};
using partition = std::vector<std::variant<leader,int>>;

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
    std::vector<size_t> local_ids;
    wait_log(size_t local_id) : local_ids({local_id}) {}
    wait_log(std::initializer_list<size_t> local_ids) : local_ids(local_ids) {}
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
        auto current = address_set(curr);
        auto previous = address_set(prev);
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

using update = std::variant<entries, new_leader, partition, disconnect, stop, reset, wait_log,
      set_config, check_rpc_config, check_rpc_added, check_rpc_removed, rpc_reset_counters,
      tick>;

struct log_entry {
    unsigned term;
    std::variant<int, raft::configuration> data;
};

struct initial_log {
    std::vector<log_entry> le;
};

struct initial_snapshot {
    raft::snapshot snap;
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
    size_t get_first_val();
};

size_t test_case::get_first_val() {
    // Count existing leader snap index and entries, if present
    size_t first_val = 0;
    if (initial_leader < initial_states.size()) {
        first_val += initial_states[initial_leader].le.size();
    }
    if (initial_leader < initial_snapshots.size()) {
        first_val = initial_snapshots[initial_leader].snap.idx;
    }
    return first_val;
}

std::mt19937 random_generator() {
    auto& gen = seastar::testing::local_random_engine;
    return std::mt19937(gen());
}

int rand() {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

// Lets assume one snapshot per server
using snapshots = std::unordered_map<raft::server_id, snapshot_value>;
using persisted_snapshots = std::unordered_map<raft::server_id, std::pair<raft::snapshot, snapshot_value>>;

seastar::semaphore snapshot_sync(0);
// application of a snaphot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_apply_snapshot{utils::UUID(0, 0xdeadbeaf)};
// sending of a snaphot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_send_snapshot{utils::UUID(0xdeadbeaf, 0)};

template <typename Clock>
class raft_cluster {
    using apply_fn = std::function<size_t(raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher)>;
    class state_machine;
    class persistence;
    class connected;
    class failure_detector;
    class rpc;
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
    bool _packet_drops;
    bool _prevote;
    apply_fn _apply;
    std::unordered_set<size_t> _in_configuration;   // Servers in current configuration
    std::vector<seastar::timer<Clock>> _tickers;
    size_t _leader;
    std::vector<initial_state> get_states(test_case test, bool prevote);
    typename Clock::duration _tick_delta;
public:
    raft_cluster(test_case test,
            apply_fn apply,
            size_t apply_entries, size_t first_val, size_t first_leader,
            bool prevote, bool packet_drops,
            typename Clock::duration tick_delta);
    // No copy
    raft_cluster(const raft_cluster&) = delete;
    raft_cluster(raft_cluster&&) = default;
    future<> stop_server(size_t id);
    future<> reset_server(size_t id, initial_state state); // Reset a stopped server
    size_t size() {
        return _servers.size();
    }
    future<> start_all();
    future<> stop_all();
    future<> wait_all();
    void tick_all();
    void disconnect(size_t id, std::optional<raft::server_id> except = std::nullopt);
    void connect_all();
    void elapse_elections();
    future<> elect_new_leader(size_t new_leader);
    future<> free_election();
    void init_raft_tickers();
    void pause_tickers();
    void restart_tickers();
    void cancel_ticker(size_t id);
    void set_ticker_callback(size_t id) noexcept;
    future<> add_entries(size_t n);
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
    future<> stop(::stop server);
    future<> reset(::reset server);
    void disconnect(::disconnect nodes);
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
        if (n && _seen == _apply_entries) {
            _done.set_value();
        }
        tlogger.debug("sm::apply[{}] got {}/{} entries", _id, _seen, _apply_entries);
        return make_ready_future<>();
    }

    future<raft::snapshot_id> take_snapshot() override {
        (*_snapshots)[_id].hasher = *hasher;
        tlogger.debug("sm[{}] takes snapshot {}", _id, (*_snapshots)[_id].hasher.finalize_uint64());
        (*_snapshots)[_id].idx = raft::index_t{_seen};
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
    }
    void drop_snapshot(raft::snapshot_id id) override {
        (*_snapshots).erase(_id);
    }
    future<> load_snapshot(raft::snapshot_id id) override {
        hasher = make_lw_shared<hasher_int>((*_snapshots)[_id].hasher);
        tlogger.debug("sm[{}] loads snapshot {}", _id, (*_snapshots)[_id].hasher.finalize_uint64());
        _seen = (*_snapshots)[_id].idx;
        if (_seen >= _apply_entries) {
            _done.set_value();
        }
        if (id == delay_apply_snapshot) {
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
    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) { return seastar::sleep(1us); }
    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) {
        (*_persisted_snapshots)[_id] = std::make_pair(snap, (*_snapshots)[_id]);
        tlogger.debug("sm[{}] persists snapshot {}", _id, (*_snapshots)[_id].hasher.finalize_uint64());
        return make_ready_future<>();
    }
    future<raft::snapshot> load_snapshot() override {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { return seastar::sleep(1us); };
    virtual future<raft::log_entries> load_log() {
        raft::log_entries log;
        for (auto&& e : _conf.log) {
            log.emplace_back(make_lw_shared(std::move(e)));
        }
        return make_ready_future<raft::log_entries>(std::move(log));
    }
    virtual future<> truncate_log(raft::index_t idx) { return make_ready_future<>(); }
    virtual future<> abort() { return make_ready_future<>(); }
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
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
    connected* _connected;
    snapshots* _snapshots;
    bool _packet_drops;
    raft::server_address_set _known_peers;
    uint32_t _servers_added = 0;
    uint32_t _servers_removed = 0;
public:
    rpc(raft::server_id id, connected* connected, snapshots* snapshots,
            bool packet_drops) : _id(id), _connected(connected), _snapshots(snapshots),
            _packet_drops(packet_drops) {
        net[_id] = this;
    }
    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id id, const raft::install_snapshot& snap, seastar::abort_source& as) {
        if (!net.count(id)) {
            throw std::runtime_error("trying to send a message to an unknown node");
        }
        if (!(*_connected)(id, _id)) {
            throw std::runtime_error("cannot send snapshot since nodes are disconnected");
        }
        (*_snapshots)[id] = (*_snapshots)[_id];
        auto s = snap; // snap is not always held alive by a caller
        if (s.snp.id == delay_send_snapshot) {
            co_await snapshot_sync.wait();
            snapshot_sync.signal();
        }
        co_return co_await net[id]->_client->apply_snapshot(_id, std::move(s));
    }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request& append_request) {
        if (!net.count(id)) {
            return make_exception_future(std::runtime_error("trying to send a message to an unknown node"));
        }
        if (!(*_connected)(id, _id)) {
            return make_exception_future<>(std::runtime_error("cannot send append since nodes are disconnected"));
        }
        if (!_packet_drops || (rand() % 5)) {
            net[id]->_client->append_entries(_id, append_request);
        }
        return make_ready_future<>();
    }
    virtual void send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
        if (!net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        if (!_packet_drops || (rand() % 5)) {
            net[id]->_client->append_entries_reply(_id, std::move(reply));
        }
    }
    virtual void send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
        if (!net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        net[id]->_client->request_vote(_id, std::move(vote_request));
    }
    virtual void send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
        if (!net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        net[id]->_client->request_vote_reply(_id, std::move(vote_reply));
    }
    virtual void send_timeout_now(raft::server_id id, const raft::timeout_now& timeout_now) {
        if (!net.count(id)) {
            return;
        }
        if (!(*_connected)(id, _id)) {
            return;
        }
        net[id]->_client->timeout_now_request(_id, std::move(timeout_now));
    }
    virtual void add_server(raft::server_id id, bytes node_info) {
        _known_peers.insert(raft::server_address{id});
        ++_servers_added;
    }
    virtual void remove_server(raft::server_id id) {
        _known_peers.erase(raft::server_address{id});
        ++_servers_removed;
    }
    virtual future<> abort() { return make_ready_future<>(); }
    static void reset_network() {
        net.clear();
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
std::unordered_map<raft::server_id, typename raft_cluster<Clock>::rpc*> raft_cluster<Clock>::rpc::net;

template <typename Clock>
typename raft_cluster<Clock>::test_server raft_cluster<Clock>::create_server(size_t id, initial_state state) {

    auto uuid = to_raft_id(id);
    auto sm = std::make_unique<state_machine>(uuid, _apply, _apply_entries, _snapshots.get());
    auto& rsm = *sm;

    auto mrpc = std::make_unique<raft_cluster::rpc>(uuid, _connected.get(),
            _snapshots.get(), _packet_drops);
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
    bool prevote, bool packet_drops,
    typename Clock::duration tick_delta)
        : _connected(std::make_unique<struct connected>(test.nodes))
        , _snapshots(std::make_unique<snapshots>())
        , _persisted_snapshots(std::make_unique<persisted_snapshots>())
        , _apply_entries(apply_entries)
        , _next_val(first_val)
        , _packet_drops(packet_drops)
        , _prevote(prevote)
        , _apply(apply)
        , _leader(first_leader)
        , _tick_delta(tick_delta) {

    rpc::reset_network();

    auto states = get_states(test, prevote);
    for (size_t s = 0; s < states.size(); ++s) {
        _in_configuration.insert(s);
    }

    raft::configuration config;

    for (size_t i = 0; i < states.size(); i++) {
        states[i].address = raft::server_address{to_raft_id(i)};
        config.current.emplace(states[i].address);
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = states[i].address;
        states[i].snapshot.config = config;
        (*_snapshots)[s.id] = states[i].snp_value;
        _servers.emplace_back(create_server(i, states[i]));
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::stop_server(size_t id) {
    cancel_ticker(id);
    co_await _servers[id].server->abort();
    _snapshots->erase(to_raft_id(id));
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
    co_await parallel_for_each(_servers, [] (auto& r) {
        return r.server->start();
    });
    init_raft_tickers();
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
void raft_cluster<Clock>::tick_all() {
    for (auto s: _in_configuration) {
        _servers[s].server->tick();
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
future<> raft_cluster<Clock>::add_entries(size_t n) {
    size_t end = _next_val + n;
    while (_next_val != end) {
        try {
            co_await _servers[_leader].server->add_entry(create_command(_next_val), raft::wait_type::committed);
            _next_val++;
        } catch (raft::not_a_leader& e) {
            // leader stepped down, update with new leader if present
            if (e.leader != raft::server_id{}) {
                _leader = to_local_id(e.leader.id);
            }
        } catch (raft::commit_status_unknown& e) {
        } catch (raft::dropped_entry& e) {
            // retry if an entry is dropped because the leader have changed after it was submitetd
        }
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::add_remaining_entries() {
    co_await add_entries(_apply_entries - _next_val);
}

template <typename Clock>
void raft_cluster<Clock>::init_raft_tickers() {
    _tickers.resize(_servers.size());
    // Only start tickers for servers in configuration
    for (auto s: _in_configuration) {
        _tickers[s].arm_periodic(_tick_delta);
        _tickers[s].set_callback([&, s] {
            _servers[s].server->tick();
        });
    }
}

template <typename Clock>
void raft_cluster<Clock>::pause_tickers() {
    for (auto s: _in_configuration) {
        _tickers[s].cancel();
    }
}

template <typename Clock>
void raft_cluster<Clock>::restart_tickers() {
    for (auto s: _in_configuration) {
        _tickers[s].rearm_periodic(_tick_delta);
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

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, unsigned start_idx) {
    std::vector<raft::log_entry> log;

    unsigned i = start_idx;
    for (auto e : list) {
        if (std::holds_alternative<int>(e.data)) {
            log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++),
                    create_command(std::get<int>(e.data))});
        } else {
            log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++),
                    std::get<raft::configuration>(e.data)});
        }
    }

    return log;
}

size_t apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands,
        lw_shared_ptr<hasher_int> hasher) {
    size_t entries = 0;
    tlogger.debug("sm::apply_changes[{}] got {} entries", id, commands.size());

    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        if (n != dummy_command) {
            entries++;
            hasher->update(n);      // running hash (values and snapshots)
            tlogger.debug("{}: apply_changes {}", id, n);
        }
    }
    return entries;
};

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
    for (auto s: followers.local_ids) {
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
            co_await later();                 // yield
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
        restart_tickers();
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
            co_await later();                 // yield
            _servers[new_leader].server->wait_until_candidate();
            // Re-connect old leader
            _connected->connect(to_raft_id(_leader));
            // Disconnect old leader from all nodes except new leader
            _connected->disconnect(to_raft_id(_leader), to_raft_id(new_leader));
            restart_tickers();
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
    elapse_elections();
    size_t node = 0;
    for (;;) {
        tick_all();
        co_await seastar::sleep(10us);   // Wait for election rpc exchanges
        // find if we have a leader
        for (auto s: _in_configuration) {
            if (_servers[s].server->is_leader()) {
                tlogger.debug("New leader {}", s);
                _leader = s;
                co_return;
            }
        }
    }
}

template <typename Clock>
future<> raft_cluster<Clock>::change_configuration(set_config sc) {
    BOOST_CHECK_MESSAGE(sc.size() > 0, "Empty configuration change not supported");
    raft::server_address_set set;
    std::unordered_set<size_t> new_config;
    for (auto s: sc) {
        new_config.insert(s.node_idx);
        auto addr = to_server_address(s.node_idx);
        addr.can_vote = s.can_vote;
        set.insert(std::move(addr));
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
            _tickers[s].rearm_periodic(_tick_delta);
        }
    }

    tlogger.debug("Changing configuration on leader {}", _leader);
    co_await _servers[_leader].server->set_configuration(std::move(set));

    if (!new_config.contains(_leader)) {
        co_await free_election();
    }

    // Now we know joint configuration was applied
    // Add a dummy entry to confirm new configuration was committed
    try {
        co_await _servers[_leader].server->add_entry(create_command(dummy_command),
                raft::wait_type::committed);
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
    std::unordered_set<size_t> partition_servers;
    std::optional<size_t> next_leader;
    for (auto s: p) {
        size_t id;
        if (std::holds_alternative<struct leader>(s)) {
            next_leader = std::get<struct leader>(s).id;
            id = *next_leader;
        } else {
            id = std::get<int>(s);
        }
        partition_servers.insert(id);
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
        co_await elect_new_leader(*next_leader);
    } else if (partition_servers.find(_leader) == partition_servers.end() && p.size() > 0) {
        // Old leader disconnected and not specified new, free election
        co_await free_election();
    }
    restart_tickers();
}

template <typename Clock>
future<> raft_cluster<Clock>::tick(::tick t) {
    for (uint64_t i = 0; i < t.ticks; i++) {
        for (auto&& s: _servers) {
            s.server->tick();
        }
        co_await later();
    }
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
void raft_cluster<Clock>::verify() {
    BOOST_TEST_MESSAGE("Verifying hashes match expected (snapshot and apply calls)");
    auto expected = hasher_int::hash_range(_apply_entries).finalize_uint64();
    for (auto i: _in_configuration) {
        auto digest = _servers[i].sm->hasher->finalize_uint64();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Digest doesn't match for server [{}]: {} != {}", i, digest, expected));
    }

    BOOST_TEST_MESSAGE("Verifying persisted snapshots");
    // TODO: check that snapshot is taken when it should be
    for (auto& s : (*_persisted_snapshots)) {
        auto& [snp, val] = s.second;
        auto digest = val.hasher.finalize_uint64();
        auto expected = hasher_int::hash_range(val.idx).finalize_uint64();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Persisted snapshot {} doesn't match {} != {}", snp.id, digest, expected));
   }
}

template <typename Clock>
std::vector<initial_state> raft_cluster<Clock>::get_states(test_case test, bool prevote) {
    std::vector<initial_state> states(test.nodes);       // Server initial states

    size_t leader = test.initial_leader;

    states[leader].term = raft::term_t{test.initial_term};

    // Server initial logs, etc
    for (size_t i = 0; i < states.size(); ++i) {
        size_t start_idx = 1;
        if (i < test.initial_snapshots.size()) {
            states[i].snapshot = test.initial_snapshots[i].snap;
            states[i].snp_value.hasher = hasher_int::hash_range(test.initial_snapshots[i].snap.idx);
            states[i].snp_value.idx = test.initial_snapshots[i].snap.idx;
            start_idx = states[i].snapshot.idx + 1;
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
    future<> operator() (test_case test, bool prevote, bool packet_drops,
            typename Clock::duration tick_delta) {

        raft_cluster<Clock> rafts(test, ::apply_changes, test.total_values,
                test.get_first_val(), test.initial_leader, prevote, packet_drops,
                tick_delta);
        co_await rafts.start_all();

        BOOST_TEST_MESSAGE("Processing updates");

        // Process all updates in order
        for (auto update: test.updates) {
            co_await std::visit(make_visitor(
            [&rafts] (entries update) -> future<> {
                co_await rafts.add_entries(update.n);
            },
            [&rafts] (new_leader update) -> future<> {
                co_await rafts.elect_new_leader(update.id);
            },
            [&rafts] (disconnect update) -> future<> {
                rafts.disconnect(update);
                co_return;
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
void replication_test(struct test_case test, bool prevote, bool packet_drops,
        typename Clock::duration tick_delta) {
    run_test<Clock>{}(std::move(test), prevote, packet_drops, tick_delta).get();
}
