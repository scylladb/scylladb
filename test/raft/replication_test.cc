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

#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>
#include <seastar/util/later.hh>
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

lowres_clock::duration tick_delta = 1ms;

auto dummy_command = std::numeric_limits<int>::min();

std::mt19937 random_generator() {
    auto& gen = seastar::testing::local_random_engine;
    return std::mt19937(gen());
}

int rand() {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

// Raft uses UUID 0 as special case.
// Convert local 0-based integer id to raft +1 UUID
utils::UUID to_raft_uuid(size_t local_id) {
    return utils::UUID{0, local_id + 1};
}

raft::server_id to_raft_id(size_t local_id) {
    return raft::server_id{to_raft_uuid(local_id)};
}

// NOTE: can_vote = true
raft::server_address to_server_address(size_t local_id) {
    return raft::server_address{raft::server_id{to_raft_uuid(local_id)}};
}

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

// Lets assume one snapshot per server
using snapshots = std::unordered_map<raft::server_id, snapshot_value>;
using persisted_snapshots = std::unordered_map<raft::server_id, std::pair<raft::snapshot, snapshot_value>>;

seastar::semaphore snapshot_sync(0);
// application of a snaphot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_apply_snapshot{utils::UUID(0, 0xdeadbeaf)};
// sending of a snaphot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_send_snapshot{utils::UUID(0xdeadbeaf, 0)};

class state_machine : public raft::state_machine {
public:
    using apply_fn = std::function<size_t(raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher)>;
private:
    raft::server_id _id;
    apply_fn _apply;
    size_t _apply_entries;
    size_t _seen = 0;
    promise<> _done;
    lw_shared_ptr<snapshots> _snapshots;
public:
    lw_shared_ptr<hasher_int> hasher;
    state_machine(raft::server_id id, apply_fn apply, size_t apply_entries,
            lw_shared_ptr<snapshots> snapshots):
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
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id{utils::make_random_uuid()});
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

struct initial_state {
    raft::server_address address;
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot snapshot;
    snapshot_value snp_value;
    raft::server::configuration server_config = raft::server::configuration{.append_request_threshold = 200};
};

class persistence : public raft::persistence {
    raft::server_id _id;
    initial_state _conf;
    lw_shared_ptr<snapshots> _snapshots;
    lw_shared_ptr<persisted_snapshots> _persisted_snapshots;
public:
    persistence(raft::server_id id, initial_state conf, lw_shared_ptr<snapshots> snapshots,
            lw_shared_ptr<persisted_snapshots> persisted_snapshots) : _id(id),
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

struct connected {
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

class failure_detector : public raft::failure_detector {
    raft::server_id _id;
    lw_shared_ptr<connected> _connected;
public:
    failure_detector(raft::server_id id, lw_shared_ptr<connected> connected) : _id(id), _connected(connected) {}
    bool is_alive(raft::server_id server) override {
        return (*_connected)(server, _id);
    }
};

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
    lw_shared_ptr<connected> _connected;
    lw_shared_ptr<snapshots> _snapshots;
    bool _packet_drops;
    raft::server_address_set _known_peers;
    uint32_t _servers_added = 0;
    uint32_t _servers_removed = 0;
public:
    rpc(raft::server_id id, lw_shared_ptr<connected> connected, lw_shared_ptr<snapshots> snapshots,
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

std::unordered_map<raft::server_id, rpc*> rpc::net;

struct test_server {
    std::unique_ptr<raft::server> server;
    state_machine* sm;
    rpc* rpc;
};

test_server
create_raft_server(raft::server_id uuid, state_machine::apply_fn apply, initial_state state,
        size_t apply_entries, lw_shared_ptr<connected> connected, lw_shared_ptr<snapshots> snapshots,
        lw_shared_ptr<persisted_snapshots> persisted_snapshots, bool packet_drops) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(apply), apply_entries, snapshots);
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid, connected, snapshots, packet_drops);
    auto& rpc_ref = *mrpc;
    auto mpersistence = std::make_unique<persistence>(uuid, state, snapshots, persisted_snapshots);
    auto fd = seastar::make_shared<failure_detector>(uuid, connected);

    auto raft = raft::create_server(uuid, std::move(mrpc), std::move(sm), std::move(mpersistence),
        std::move(fd), state.server_config);

    return {
        std::move(raft),
        &rsm,
        &rpc_ref
    };
}

future<std::vector<test_server>> create_cluster(std::vector<initial_state> states, state_machine::apply_fn apply, size_t apply_entries,
        lw_shared_ptr<connected> connected, lw_shared_ptr<snapshots> snapshots,
        lw_shared_ptr<persisted_snapshots> persisted_snapshots, bool packet_drops) {
    raft::configuration config;
    std::vector<test_server> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        states[i].address = raft::server_address{to_raft_id(i)};
        config.current.emplace(states[i].address);
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = states[i].address;
        states[i].snapshot.config = config;
        (*snapshots)[s.id] = states[i].snp_value;
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, apply, states[i], apply_entries,
                    connected, snapshots, persisted_snapshots, packet_drops)).server;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, unsigned start_idx) {
    std::vector<raft::log_entry> log;

    unsigned i = start_idx;
    for (auto e : list) {
        log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++), create_command(e.value)});
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

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
using entries = unsigned;
using new_leader = int;
struct leader {
    size_t id;
};
using partition = std::vector<std::variant<leader,int>>;

struct set_config_entry {
    size_t node_idx;
    bool can_vote;

    set_config_entry(size_t idx, bool can_vote = true)
        : node_idx(idx), can_vote(can_vote)
    {}
};
using set_config = std::vector<set_config_entry>;

struct tick {
    uint64_t ticks;
};

using update = std::variant<entries, new_leader, partition, set_config, tick>;

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
};

future<> wait_log(std::vector<test_server>& rafts,
        lw_shared_ptr<connected> connected, std::unordered_set<size_t>& in_configuration,
        size_t leader) {
    // Wait for leader log to propagate
    auto leader_log_idx = rafts[leader].server->log_last_idx();
    for (size_t s = 0; s < rafts.size(); ++s) {
        if (s != leader && (*connected)(to_raft_id(s), to_raft_id(leader)) &&
                in_configuration.contains(s)) {
            co_await rafts[s].server->wait_log_idx(leader_log_idx);
        }
    }
}

void elapse_elections(std::vector<test_server>& rafts) {
    for (size_t s = 0; s < rafts.size(); ++s) {
        rafts[s].server->elapse_election();
    }
}

future<size_t> elect_new_leader(std::vector<test_server>& rafts,
        lw_shared_ptr<connected> connected, std::unordered_set<size_t>& in_configuration,
        size_t leader, size_t new_leader) {
    BOOST_CHECK_MESSAGE(new_leader < rafts.size(),
            format("Wrong next leader value {}", new_leader));

    if (new_leader != leader) {
        do {
            // Leader could be already partially disconnected, save current connectivity state
            struct connected prev_disconnected = *connected;
            // Disconnect current leader from everyone
            connected->disconnect(to_raft_id(leader));
            // Make move all nodes past election threshold, also making old leader follower
            elapse_elections(rafts);
            // Consume leader output messages since a stray append might make new leader step down
            co_await later();                 // yield
            rafts[new_leader].server->wait_until_candidate();
            // Re-connect old leader
            connected->connect(to_raft_id(leader));
            // Disconnect old leader from all nodes except new leader
            connected->disconnect(to_raft_id(leader), to_raft_id(new_leader));
            co_await rafts[new_leader].server->wait_election_done();
            // Restore connections to the original setting
            *connected = prev_disconnected;
        } while (!rafts[new_leader].server->is_leader());
        tlogger.debug("confirmed leader on {}", to_raft_id(new_leader));
    }
    co_return new_leader;
}

// Run a free election of nodes in configuration
// NOTE: there should be enough nodes capable of participating
future<size_t> free_election(std::vector<test_server>& rafts) {
    tlogger.debug("Running free election");
    elapse_elections(rafts);
    size_t node = 0;
    for (;;) {
        for (auto& r: rafts) {
            r.server->tick();
        }
        co_await seastar::sleep(10us);   // Wait for election rpc exchanges
        // find if we have a leader
        for (size_t s = 0; s < rafts.size(); ++s) {
            if (rafts[s].server->is_leader()) {
                tlogger.debug("New leader {}", s);
                co_return s;
            }
        }
    }
}

void pause_tickers(std::vector<seastar::timer<lowres_clock>>& tickers) {
    for (auto& ticker: tickers) {
        ticker.cancel();
    }
}

void restart_tickers(std::vector<seastar::timer<lowres_clock>>& tickers) {
    for (auto& ticker: tickers) {
        ticker.rearm_periodic(tick_delta);
    }
}

future<std::unordered_set<size_t>> change_configuration(std::vector<test_server>& rafts,
        size_t total_values, lw_shared_ptr<connected> connected,
        std::unordered_set<size_t>& in_configuration, lw_shared_ptr<snapshots> snapshots,
        lw_shared_ptr<persisted_snapshots> persisted_snapshots, bool packet_drops, set_config sc,
        size_t& leader, std::vector<seastar::timer<lowres_clock>>& tickers, state_machine::apply_fn apply) {

    BOOST_CHECK_MESSAGE(sc.size() > 0, "Empty configuration change not supported");
    raft::server_address_set set;
    std::unordered_set<size_t> new_config;
    for (auto s: sc) {
        new_config.insert(s.node_idx);
        auto addr = to_server_address(s.node_idx);
        addr.can_vote = s.can_vote;
        set.insert(std::move(addr));
        BOOST_CHECK_MESSAGE(s.node_idx < rafts.size(),
                format("Configuration element {} past node limit {}", s.node_idx, rafts.size() - 1));
    }
    BOOST_CHECK_MESSAGE(new_config.contains(leader) || sc.size() < (rafts.size()/2 + 1),
            "New configuration without old leader and below quorum size (no election)");
    tlogger.debug("Changing configuration on leader {}", leader);
    co_await rafts[leader].server->set_configuration(std::move(set));

    if (!new_config.contains(leader)) {
        leader = co_await free_election(rafts);
    }

    // Now we know joint configuration was applied
    // Add a dummy entry to confirm new configuration was committed
    try {
        co_await rafts[leader].server->add_entry(create_command(dummy_command),
                raft::wait_type::committed);
    } catch (raft::not_a_leader& e) {
        // leader stepped down, implying config fully changed
    } catch (raft::commit_status_unknown& e) {}

    // Reset removed nodes
    pause_tickers(tickers);  // stop all tickers
    for (auto s: in_configuration) {
        if (!new_config.contains(s)) {
            tickers[s].cancel();
            co_await rafts[s].server->abort();
            rafts[s] = create_raft_server(to_raft_id(s), apply, initial_state{.log = {}},
                    total_values, connected, snapshots, persisted_snapshots, packet_drops);
            co_await rafts[s].server->start();
            tickers[s].set_callback([&rafts, s] { rafts[s].server->tick(); });
        }
    }
    restart_tickers(tickers); // start all tickers

    co_return new_config;
}

// Add consecutive integer entries to a leader
future<> add_entries(std::vector<test_server>& rafts,
        size_t start, size_t end, size_t& leader) {
    size_t value = start;
    for (size_t value = start; value != end;) {
        try {
            co_await rafts[leader].server->add_entry(create_command(value), raft::wait_type::committed);
            value++;
        } catch (raft::not_a_leader& e) {
            // leader stepped down, update with new leader if present
            if (e.leader != raft::server_id{}) {
                leader = e.leader.id.get_least_significant_bits() - 1;
            }
        } catch (raft::commit_status_unknown& e) {
        } catch (raft::dropped_entry& e) {
            // retry if an entry is dropped because the leader have changed after it was submitetd
        }
    }
}

using raft_ticker_type = seastar::timer<lowres_clock>;

std::vector<raft_ticker_type> init_raft_tickers(std::vector<test_server>& rafts) {
    std::vector<seastar::timer<lowres_clock>> tickers(rafts.size());
    for (size_t s = 0; s < rafts.size(); ++s) {
        tickers[s].arm_periodic(tick_delta);
        tickers[s].set_callback([&rafts, s] {
            rafts[s].server->tick();
        });
    }
    return tickers;
}

// Run test case (name, nodes, leader, initial logs, updates)
future<> run_test(test_case test, bool prevote, bool packet_drops) {
    rpc::reset_network();

    std::vector<initial_state> states(test.nodes);       // Server initial states

    size_t leader = test.initial_leader;

    states[leader].term = raft::term_t{test.initial_term};

    int leader_initial_entries = 0;
    if (leader < test.initial_states.size()) {
        leader_initial_entries += test.initial_states[leader].le.size();  // Count existing leader entries
    }
    int leader_snap_skipped = 0; // Next value to write
    if (leader < test.initial_snapshots.size()) {
        leader_snap_skipped = test.initial_snapshots[leader].snap.idx;  // Count existing leader entries
    }

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

    auto snaps = make_lw_shared<snapshots>();
    auto persisted_snaps = make_lw_shared<persisted_snapshots>();
    auto connected = make_lw_shared<struct connected>(test.nodes);

    auto rafts = co_await create_cluster(states, apply_changes, test.total_values, connected,
            snaps, persisted_snaps, packet_drops);

    // Tickers for servers
    std::vector<raft_ticker_type> tickers = init_raft_tickers(rafts);

    // Keep track of what servers are in the current configuration
    std::unordered_set<size_t> in_configuration;
    for (size_t s = 0; s < test.nodes; ++s) {
        in_configuration.insert(s);
    }

    BOOST_TEST_MESSAGE("Electing first leader " << leader);
    rafts[leader].server->wait_until_candidate();
    co_await rafts[leader].server->wait_election_done();
    BOOST_TEST_MESSAGE("Processing updates");
    // Process all updates in order
    size_t next_val = leader_snap_skipped + leader_initial_entries;
    for (auto update: test.updates) {
        if (std::holds_alternative<entries>(update)) {
            auto n = std::get<entries>(update);
            BOOST_CHECK_MESSAGE(in_configuration.contains(leader),
                    format("Current leader {} is not in configuration", leader));
            co_await add_entries(rafts, next_val, next_val + n, leader);
            next_val += n;
            co_await wait_log(rafts, connected, in_configuration, leader);
        } else if (std::holds_alternative<new_leader>(update)) {
            unsigned next_leader = std::get<new_leader>(update);
            auto leader_log_idx = rafts[leader].server->log_last_idx();
            co_await rafts[next_leader].server->wait_log_idx(leader_log_idx);
            pause_tickers(tickers);
            leader = co_await elect_new_leader(rafts, connected, in_configuration, leader,
                    next_leader);
            restart_tickers(tickers);
        } else if (std::holds_alternative<partition>(update)) {
            co_await wait_log(rafts, connected, in_configuration, leader);
            pause_tickers(tickers);
            auto p = std::get<partition>(update);
            connected->connect_all();
            std::unordered_set<size_t> partition_servers;
            struct leader new_leader;
            bool have_new_leader = false;
            for (auto s: p) {
                size_t id;
                if (std::holds_alternative<struct leader>(s)) {
                    have_new_leader = true;
                    new_leader = std::get<struct leader>(s);
                    id = new_leader.id;
                } else {
                    id = std::get<int>(s);
                }
                partition_servers.insert(id);
            }
            for (size_t s = 0; s < test.nodes; ++s) {
                if (partition_servers.find(s) == partition_servers.end()) {
                    // Disconnect servers not in main partition
                    connected->disconnect(to_raft_id(s));
                }
            }
            if (have_new_leader && new_leader.id != leader) {
                // New leader specified, elect it
                leader = co_await elect_new_leader(rafts, connected, in_configuration, leader,
                        new_leader.id);
            } else if (partition_servers.find(leader) == partition_servers.end() && p.size() > 0) {
                // Old leader disconnected and not specified new, free election
                leader = co_await free_election(rafts);
            }
            restart_tickers(tickers);
        } else if (std::holds_alternative<set_config>(update)) {
            co_await wait_log(rafts, connected, in_configuration, leader);
            auto sc = std::get<set_config>(update);
            in_configuration = co_await change_configuration(rafts, test.total_values, connected,
                    in_configuration, snaps, persisted_snaps, packet_drops, std::move(sc), leader, tickers, apply_changes);
        } else if (std::holds_alternative<tick>(update)) {
            auto t = std::get<tick>(update);
            for (uint64_t i = 0; i < t.ticks; i++) {
                for (auto& r: rafts) {
                    r.server->tick();
                }
                co_await later();
            }
        }
    }

    // Reconnect and bring all nodes back into configuration, if needed
    connected->connect_all();
    if (in_configuration.size() < test.nodes) {
        set_config sc;
        for (size_t s = 0; s < test.nodes; ++s) {
            sc.push_back(s);
        }
        in_configuration = co_await change_configuration(rafts, test.total_values, connected,
                in_configuration, snaps, persisted_snaps, packet_drops, std::move(sc), leader,
                tickers, apply_changes);
    }

    BOOST_TEST_MESSAGE("Appending remaining values");
    if (next_val < test.total_values) {
        co_await add_entries(rafts, next_val, test.total_values, leader);
    }

    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.sm->done();
    }

    for (auto& r: rafts) {
        co_await r.server->abort(); // Stop servers
    }

    BOOST_TEST_MESSAGE("Verifying hashes match expected (snapshot and apply calls)");
    auto expected = hasher_int::hash_range(test.total_values).finalize_uint64();
    for (size_t i = 0; i < rafts.size(); ++i) {
        auto digest = rafts[i].sm->hasher->finalize_uint64();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Digest doesn't match for server [{}]: {} != {}", i, digest, expected));
    }

    BOOST_TEST_MESSAGE("Verifying persisted snapshots");
    // TODO: check that snapshot is taken when it should be
    for (auto& s : (*persisted_snaps)) {
        auto& [snp, val] = s.second;
        auto digest = val.hasher.finalize_uint64();
        auto expected = hasher_int::hash_range(val.idx).finalize_uint64();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Persisted snapshot {} doesn't match {} != {}", snp.id, digest, expected));
   }

    co_return;
}

void replication_test(struct test_case test, bool prevote, bool packet_drops) {
    run_test(std::move(test), prevote, packet_drops).get();
}

#define RAFT_TEST_CASE(test_name, test_body)  \
    SEASTAR_THREAD_TEST_CASE(test_name) { \
        replication_test(test_body, false, false); }  \
    SEASTAR_THREAD_TEST_CASE(test_name ## _drops) { \
        replication_test(test_body, false, true); } \
    SEASTAR_THREAD_TEST_CASE(test_name ## _prevote) { \
        replication_test(test_body, true, false); }  \
    SEASTAR_THREAD_TEST_CASE(test_name ## _prevote_drops) { \
        replication_test(test_body, true, true); }

raft::server_address_set full_cluster_address_set(size_t nodes) {
    raft::server_address_set res;
    for (size_t i = 0; i < nodes; ++i) {
        res.emplace(to_server_address(i));
    }
    return res;
}

using test_func = seastar::noncopyable_function<
    future<>(std::vector<test_server>&, lw_shared_ptr<connected>, std::vector<raft_ticker_type>&,
        size_t, std::unordered_set<size_t>&)>;

size_t dummy_apply_fn(raft::server_id id, const std::vector<raft::command_cref>& commands,
        lw_shared_ptr<hasher_int> hasher) {
    return 0;
}

future<std::unordered_set<size_t>> rpc_test_change_configuration(std::vector<test_server>& rafts,
        lw_shared_ptr<connected> connected, std::unordered_set<size_t>& in_configuration,
        set_config sc, size_t& leader,
        std::vector<seastar::timer<lowres_clock>>& tickers) {
    return change_configuration(rafts, 1, connected, in_configuration,
        make_lw_shared<snapshots>(), make_lw_shared<persisted_snapshots>(),
        false, sc, leader, tickers, dummy_apply_fn);
}

// Wrapper function for running RPC tests that provides a convenient
// automatic initialization and de-initialization of a raft cluster.
future<> rpc_test(size_t nodes, test_func test_case_body) {
    std::vector<initial_state> states(nodes);
    auto conn = make_lw_shared<connected>(nodes);

    rpc::reset_network();
    // Initialize and start the cluster with corresponding tickers
    auto rafts = co_await create_cluster(states, dummy_apply_fn, 1, conn,
        make_lw_shared<snapshots>(), make_lw_shared<persisted_snapshots>(), false);
    auto tickers = init_raft_tickers(rafts);
    // Keep track of what servers are in the current configuration
    std::unordered_set<size_t> in_configuration;
    for (size_t s = 0; s < rafts.size(); ++s) {
        in_configuration.insert(s);
    }
    // Elect first node a leader
    constexpr size_t initial_leader = 0;
    rafts[initial_leader].server->wait_until_candidate();
    co_await rafts[initial_leader].server->wait_election_done();
    co_await wait_log(rafts, conn, in_configuration, initial_leader);
    try {
        // Execute the test
        co_await test_case_body(rafts, conn, tickers, initial_leader, in_configuration);
    } catch (...) {
        BOOST_ERROR(format("RPC test failed unexpectedly with error: {}", std::current_exception()));
    }
    // Stop tickers
    pause_tickers(tickers);
    // Stop raft servers
    for (auto& raft : rafts) {
        co_await raft.server->abort();
    }
}

// 1 nodes, simple replication, empty, no updates
RAFT_TEST_CASE(simple_replication, (test_case{
         .nodes = 1}))

// 2 nodes, 4 existing leader entries, 4 updates
RAFT_TEST_CASE(non_empty_leader_log, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {entries{4}}}));

// 2 nodes, don't add more entries besides existing log
RAFT_TEST_CASE(non_empty_leader_log_no_new_entries, (test_case{
         .nodes = 2, .total_values = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_auto_12, (test_case{
         .nodes = 1,
         .initial_states = {}, .updates = {entries{12}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_expected, (test_case{
         .nodes = 1, .initial_states = {},
         .updates = {entries{4}}}));

// 1 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_1_pre, (test_case{
         .nodes = 1,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 2 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_2_pre, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 3 nodes, 2 leader changes with 4 client entries each
RAFT_TEST_CASE(leader_changes, (test_case{
         .nodes = 3,
         .updates = {entries{4},new_leader{1},entries{4},new_leader{2},entries{4}}}));

//
// NOTE: due to disrupting candidates protection leader doesn't vote for others, and
//       servers with entries vote for themselves, so some tests use 3 servers instead of
//       2 for simplicity and to avoid a stalemate. This behaviour can be disabled.
//

// 3 nodes, 7 leader entries, 12 client entries, change leader, 12 client entries
RAFT_TEST_CASE(simple_3_pre_chg, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12},new_leader{1},entries{12}},}));

// 3 nodes, leader empty, follower has 3 spurious entries
// node 1 was leader but did not propagate entries, node 0 becomes leader in new term
// NOTE: on first leader election term is bumped to 3
RAFT_TEST_CASE(replace_log_leaders_log_empty, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{}, {{{2,10},{2,20},{2,30}}}},
         .updates = {entries{4}}}));

// 3 nodes, 7 leader entries, follower has 9 spurious entries
RAFT_TEST_CASE(simple_3_spurious_1, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14},{2,15},{2,16},{2,17},{2,18}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 3, leader has 9 entries, follower has 5 spurious entries, 4 client entries
RAFT_TEST_CASE(simple_3_spurious_2, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 2, leader has 7 entries, follower has 3 good and 3 spurious entries
RAFT_TEST_CASE(simple_3_follower_4_1, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{1,0},{1,1},{1,2},{2,20},{2,30},{2,40}}}},
         .updates = {entries{4}}}));

// A follower and a leader have matching logs but leader's is shorter
// 3 nodes, term 2, leader has 2 entries, follower has same and 5 more, 12 updates
RAFT_TEST_CASE(simple_3_short_leader, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1}}},
                            {.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}}}));

// A follower and a leader have no common entries
// 3 nodes, term 2, leader has 7 entries, follower has non-matching 6 entries, 12 updates
RAFT_TEST_CASE(follower_not_matching, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{2,10},{2,20},{2,30},{2,40},{2,50},{2,60}}}},
         .updates = {entries{12}},}));

// A follower and a leader have one common entry
// 3 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries, 12 updates
RAFT_TEST_CASE(follower_one_common_1, (test_case{
         .nodes = 3, .initial_term = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2}}},
                            {.le = {{1,0},{2,11},{2,12},{2,13}}}},
         .updates = {entries{12}}}));

// A follower and a leader have 2 common entries in different terms
// 3 nodes, term 2, leader has 4 entries, follower has matching but in different term
RAFT_TEST_CASE(follower_one_common_2, (test_case{
         .nodes = 3, .initial_term = 5,
         .initial_states = {{.le = {{1,0},{2,1},{3,2},{3,3}}},
                            {.le = {{1,0},{2,1},{2,2},{2,13}}}},
         .updates = {entries{4}}}));

// 2 nodes both taking snapshot while simple replication
RAFT_TEST_CASE(take_snapshot, (test_case{
         .nodes = 2,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}, {.snapshot_threshold = 20, .snapshot_trailing = 10}},
         .updates = {entries{100}}}));

// 2 nodes doing simple replication/snapshoting while leader's log size is limited
RAFT_TEST_CASE(backpressure, (test_case{
         .nodes = 2,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5, .max_log_size = 20}, {.snapshot_threshold = 20, .snapshot_trailing = 10}},
         .updates = {entries{100}}}));

// 3 nodes, add entries, drop leader 0, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_01, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{1,2},entries{4}}}));

// 3 nodes, add entries, drop follower 1, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_02, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{0,2},entries{4},partition{2,1}}}));

// 3 nodes, add entries, drop leader 0, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_03, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{leader{1},2},entries{4}}}));

// 4 nodes, add entries, drop follower 1, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_04, (test_case{
         .nodes = 4,
         .updates = {entries{4},partition{0,2,3},entries{4},partition{1,leader{2},3}}}));

// TODO: change to RAFT_TEST_CASE once it's stable for handling packet drops
SEASTAR_THREAD_TEST_CASE(test_take_snapshot_and_stream) {
    replication_test(
        // Snapshot automatic take and load
        {.nodes = 3,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}},
         .updates = {entries{5}, partition{0,1}, entries{10}, partition{0, 2}, entries{20}}}
    , false, false);
}

// Check removing all followers, add entry, bring back one follower and make it leader
RAFT_TEST_CASE(conf_changes_1, (test_case{
         .nodes = 3,
         .updates = {set_config{0}, entries{1}, set_config{0,1}, entries{1},
                     new_leader{1}, entries{1}}}));

// Check removing leader with entries, add entries, remove follower and add back first node
RAFT_TEST_CASE(conf_changes_2, (test_case{
         .nodes = 3,
         .updates = {entries{1}, new_leader{1}, set_config{1,2}, entries{1},
                     set_config{0,1}, entries{1}}}));

// Check removing a node from configuration, adding entries; cycle for all combinations
SEASTAR_THREAD_TEST_CASE(remove_node_cycle) {
    replication_test(
        {.nodes = 4,
         .updates = {set_config{0,1,2}, entries{2}, new_leader{1},
                     set_config{1,2,3}, entries{2}, new_leader{2},
                     set_config{2,3,0}, entries{2}, new_leader{3},
                     // TODO: find out why it breaks in release mode
                     // set_config{3,0,1}, entries{2}, new_leader{0}
                     }}
    , false, false);
}

SEASTAR_THREAD_TEST_CASE(test_leader_change_during_snapshot_transfere) {
    replication_test(
        {.nodes = 3,
         .initial_snapshots  = {{.snap = {.idx = raft::index_t(10),
                                         .term = raft::term_t(1),
                                         .id = delay_send_snapshot}},
                                {.snap = {.idx = raft::index_t(10),
                                         .term = raft::term_t(1),
                                         .id = delay_apply_snapshot}}},
         .updates = {tick{10} /* ticking starts snapshot transfer */, new_leader{1}, entries{10}}}
    , false, false);
}

// verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
// TODO: add pre-vote case
RAFT_TEST_CASE(etcd_test_leader_cycle, (test_case{
         .nodes = 2,
         .updates = {new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0}
                     }}));

///
/// RPC-related tests
///

SEASTAR_TEST_CASE(rpc_load_conf_from_snapshot) {
    // 1 node cluster with an initial configuration from a snapshot.
    // Test that RPC configuration is set up correctly when the raft server
    // instance is started.
    constexpr size_t nodes = 1;

    raft::server_id sid = id();
    initial_state state;
    state.snapshot.config = raft::configuration{sid};

    auto raft = create_raft_server(sid, dummy_apply_fn, state, 1,
        make_lw_shared<connected>(nodes), make_lw_shared<snapshots>(),
        make_lw_shared<persisted_snapshots>(), false);
    co_await raft.server->start();

    BOOST_CHECK(raft.rpc->known_peers() == address_set({sid}));

    co_await raft.server->abort();
}

SEASTAR_TEST_CASE(rpc_load_conf_from_log) {
    // 1 node cluster.
    // Initial configuration is taken from the persisted log.
    constexpr size_t nodes = 1;

    raft::server_id sid = id();
    initial_state state;
    raft::log_entry conf_entry{.idx = raft::index_t{1}, .data = raft::configuration{sid}};
    state.log.emplace_back(std::move(conf_entry));

    auto raft = create_raft_server(sid, dummy_apply_fn, state, 1,
        make_lw_shared<connected>(nodes), make_lw_shared<snapshots>(),
        make_lw_shared<persisted_snapshots>(), false);
    co_await raft.server->start();

    BOOST_CHECK(raft.rpc->known_peers() == address_set({sid}));

    co_await raft.server->abort();
}

SEASTAR_TEST_CASE(rpc_propose_conf_change) {
    // 3 node cluster {A, B, C}.
    // Shrinked later to 2 nodes and then expanded back to 3 nodes.
    // Test that both configuration changes update RPC configuration correspondingly
    // on all nodes.
    return rpc_test(3, [] (std::vector<test_server>& rafts, lw_shared_ptr<connected> connected,
            std::vector<raft_ticker_type>& tickers, size_t leader,
            std::unordered_set<size_t>& in_configuration) -> future<> {
        // Remove node C from the cluster configuration.
        in_configuration = co_await rpc_test_change_configuration(rafts, connected,
            in_configuration, set_config{0, 1}, leader, tickers);

        // Check that RPC config is updated both on leader and on follower nodes,
        // i.e. `rpc::remove_server` is called.
        auto reduced_config = address_set({to_raft_id(0), to_raft_id(1)});
        for (const auto& node : in_configuration) {
            BOOST_CHECK(rafts[node].rpc->known_peers() == reduced_config);
        }

        // Re-add node C to the cluster configuration.
        in_configuration = co_await rpc_test_change_configuration(rafts, connected,
            in_configuration, set_config{0, 1, 2}, leader, tickers);

        // Check that both A (leader) and B (follower) call `rpc::add_server`,
        // also the newly integrated node gets the actual RPC configuration, too.
        const auto initial_cluster_conf = full_cluster_address_set(rafts.size());
        co_await seastar::async([&] {
            for (const auto& r : rafts) {
                CHECK_EVENTUALLY_EQUAL(r.rpc->known_peers(), initial_cluster_conf);
            }
        });
    });
}

SEASTAR_TEST_CASE(rpc_leader_election) {
    // 3 node cluster {A, B, C}.
    // Test that leader elections don't change RPC configuration.
    return rpc_test(3, [] (std::vector<test_server>& rafts, lw_shared_ptr<connected> connected,
            std::vector<raft_ticker_type>& tickers, size_t initial_leader,
            std::unordered_set<size_t>& in_configuration) -> future<> {
        auto all_nodes = full_cluster_address_set(rafts.size());
        for (auto& raft : rafts) {
            BOOST_CHECK(raft.rpc->known_peers() == all_nodes);
            raft.rpc->reset_counters();
        }

        // Elect 2nd node a leader
        constexpr size_t new_leader = 1;
        pause_tickers(tickers);
        co_await elect_new_leader(rafts, connected, in_configuration, initial_leader, new_leader);
        restart_tickers(tickers);

        // Check that no attempts to update RPC were made.
        for (const auto& r : rafts) {
            BOOST_CHECK(!r.rpc->servers_added());
            BOOST_CHECK(!r.rpc->servers_removed());
        }
    });
}

SEASTAR_TEST_CASE(rpc_voter_non_voter_transision) {
    // 3 node cluster {A, B, C}.
    // Test that demoting of node C to learner state and then promoting back
    // to voter doesn't involve any RPC configuration changes.
    return rpc_test(3, [] (std::vector<test_server>& rafts, lw_shared_ptr<connected> connected,
            std::vector<raft_ticker_type>& tickers, size_t leader,
            std::unordered_set<size_t>& in_configuration) -> future<> {
        const auto all_voter_nodes = full_cluster_address_set(rafts.size());
        for (auto& raft : rafts) {
            BOOST_CHECK(raft.rpc->known_peers() == all_voter_nodes);
            raft.rpc->reset_counters();
        }

        // Make C a non-voting member.
        in_configuration = co_await rpc_test_change_configuration(rafts, connected,
            in_configuration, set_config{0, 1, set_config_entry(2, false)}, leader, tickers);

        // Check that RPC configuration didn't change.
        for (const auto& raft : rafts) {
            BOOST_CHECK(!raft.rpc->servers_added());
            BOOST_CHECK(!raft.rpc->servers_removed());
        }

        // Make C a voting member.
        in_configuration = co_await rpc_test_change_configuration(rafts, connected,
            in_configuration, set_config{0, 1, 2}, leader, tickers);

        // RPC configuration shouldn't change.
        for (const auto& raft : rafts) {
            BOOST_CHECK(!raft.rpc->servers_added());
            BOOST_CHECK(!raft.rpc->servers_removed());
        }
    });
}

SEASTAR_TEST_CASE(rpc_configuration_truncate_restore_from_snp) {
    // 3 node cluster {A, B, C}.
    // Issue a configuration change on leader (A): add node D.
    // Fail the node before the entry is committed (disconnect from the
    // rest of the cluster and restart the node).
    //
    // In the meanwhile, elect a new leader within the connected part of the
    // cluster (B). A becomes an isolated follower in this case.
    // A should observe {A, B, C, D} RPC configuration: when in joint
    // consensus, we need to account for servers in both configurations.
    //
    // Heal network partition and observe that A's log is truncated (actually,
    // it's empty since B does not have any entries at all, except for dummies).
    // The RPC configuration on A is restored from initial snapshot configuration,
    // which is {A, B, C}.
    return rpc_test(3, [] (std::vector<test_server>& rafts, lw_shared_ptr<connected> connected,
            std::vector<raft_ticker_type>& tickers, size_t initial_leader,
            std::unordered_set<size_t>& in_configuration) -> future<> {
        const auto all_nodes = full_cluster_address_set(rafts.size());
        pause_tickers(tickers);
        // Disconnect A from B and C.
        connected->disconnect(to_raft_id(0));
        // Emulate a failed configuration change on A (add node D) by
        // restarting A with a modified initial log containing one extraneous
        // configuration entry.
        co_await rafts[initial_leader].server->abort();
        // Restart A with a synthetic initial state representing
        // the same initial snapshot config (A, B, C) as before,
        // but with the following things in mind:
        // * log contains only one entry: joint configuration entry
        //   that is equivalent to that of A's before the crash.
        // * The configuration entry would have term=1 so that it'll
        //   be truncated when A gets in contact with other nodes
        // * This will completely erase all entries on A leaving its
        //   log empty.
        auto extended_conf = address_set({to_raft_id(0), to_raft_id(1), to_raft_id(2), to_raft_id(3)});
        initial_state restart_state{
            .log = {
                raft::log_entry{raft::term_t(1), raft::index_t(1),
                    raft::configuration(
                        extended_conf,
                        all_nodes
                    )
                }
            },
            .snapshot = {.config = all_nodes}
        };
        rafts[initial_leader] = create_raft_server(to_raft_id(initial_leader), dummy_apply_fn, restart_state, 1,
            connected, make_lw_shared<snapshots>(), make_lw_shared<persisted_snapshots>(), false);
        co_await rafts[initial_leader].server->start();
        tickers[initial_leader].set_callback([&rafts, s=initial_leader] { rafts[s].server->tick(); });
        restart_tickers(tickers);

        // A should see {A, B, C, D} as RPC config since
        // the latest configuration entry points to joint
        // configuration {.current = {A, B, C, D}, .previous = {A, B, C}}.
        // RPC configuration is computed as a union of current
        // and previous configurations.
        BOOST_CHECK(rafts[0].rpc->known_peers() == extended_conf);

        // Elect B as leader
        pause_tickers(tickers);
        auto new_leader = co_await elect_new_leader(rafts, connected, in_configuration,
            initial_leader, 1);
        restart_tickers(tickers);

        // Heal network partition.
        connected->connect_all();

        // wait to synchronize logs between current leader (B) and the rest of the cluster
        co_await wait_log(rafts, connected, in_configuration, new_leader);
        // A should have truncated an offending configuration entry and revert its RPC configuration.
        //
        // Since B's log is effectively empty (does not contain any configuration
        // entries), A's configuration view ({A, B, C}) is restored from
        // initial snapshot.
        co_await seastar::async([&] {
            CHECK_EVENTUALLY_EQUAL(rafts[0].rpc->known_peers(), all_nodes);
        });
    });
}

SEASTAR_TEST_CASE(rpc_configuration_truncate_restore_from_log) {
    // 4 node cluster {A, B, C, D}.
    // Change configuration to {A, B, C} from A and wait for it to become
    // committed.
    //
    // Then, issue a configuration change on leader (A): remove node C.
    // Fail the node before the entry is committed (disconnect from the
    // rest of the cluster and restart the node). We emulate this behavior by
    // just terminating the node and restarting it with a pre-defined state
    // that is equivalent to having an uncommitted configuration entry in
    // the log.
    //
    // In the meanwhile, elect a new leader within the connected part of the
    // cluster (B). A becomes an isolated follower in this case.
    //
    // Heal network partition and observe that A's log is truncated and
    // replaced with that of B. RPC configuration should not change between
    // the crash + network partition and synchronization with B, since
    // the effective RPC cfg would be {A, B, C} both for
    // joint cfg = {.current = {A, B}, .previous = {A, B, C}}
    // and the previously commited cfg = {A, B, C}.
    //
    // After that, test for the second case: switch leader back to A and
    // try to expand the cluster back to initial state (re-add
    // node D): {A, B, C, D}.
    //
    // Try to set configuration {A, B, C, D} on leader A, isolate and crash it.
    // Restart with synthetic state containing an uncommitted configuration entry.
    //
    // This time before healing the network we should observe
    // RPC configuration = {A, B, C, D}, accounting for an uncommitted part of the
    // configuration.
    // After healing the network and synchronizing with new leader B, RPC config
    // should be reverted back to committed state {A, B, C}.
    return rpc_test(4, [] (std::vector<test_server>& rafts, lw_shared_ptr<connected> connected,
            std::vector<raft_ticker_type>& tickers, size_t initial_leader,
            std::unordered_set<size_t>& in_configuration) -> future<> {
        const auto all_nodes = full_cluster_address_set(rafts.size());

        // Remove node D from the cluster configuration.
        auto committed_conf = address_set({to_raft_id(0), to_raft_id(1), to_raft_id(2)});
        in_configuration = co_await rpc_test_change_configuration(rafts, connected,
            in_configuration, set_config{0, 1, 2}, initial_leader, tickers);
        // {A, B, C} configuration is committed by now.

        //
        // First case: shrink cluster (remove node C).
        //

        // Disconnect A from the rest of the cluster.
        connected->disconnect(to_raft_id(0));
        // Try to change configuration (remove node C)
        auto uncommitted_conf = address_set({to_raft_id(0), to_raft_id(1)});
        // `set_configuration` call will fail on A because
        // it's cut off the other nodes and it will be waiting for them,
        // but A is terminated before the network is allowed to heal the partition.
        tickers[0].cancel();
        co_await rafts[initial_leader].server->abort();
        // Restart A with a synthetic initial state that contains two entries
        // in the log:
        //   1. {A, B, C} configuration committed before crash + partition.
        //   2. uncommitted joint configuration entry that is equivalent
        //      to that of A's before the crash.
        initial_state restart_state{
            .log = {
                raft::log_entry{raft::term_t(1), raft::index_t(1),
                    raft::configuration(committed_conf)
                },
                raft::log_entry{raft::term_t(2), raft::index_t(2),
                    raft::configuration(
                        uncommitted_conf,
                        committed_conf
                    )
                }
            },
            .snapshot = {.config = all_nodes}
        };
        rafts[initial_leader] = create_raft_server(to_raft_id(initial_leader), dummy_apply_fn, restart_state, 1,
            connected, make_lw_shared<snapshots>(), make_lw_shared<persisted_snapshots>(), false);
        co_await rafts[initial_leader].server->start();
        tickers[initial_leader].set_callback([&rafts, s=initial_leader] { rafts[s].server->tick(); });
        tickers[0].rearm_periodic(tick_delta);

        // A's RPC configuration should stay the same because
        // for both uncommitted joint cfg = {.current = {A, B}, .previous = {A, B, C}}
        // and committed cfg = {A, B, C} the RPC cfg would be equal to {A, B, C}
        BOOST_CHECK(rafts[0].rpc->known_peers() == committed_conf);

        // Elect B as leader
        pause_tickers(tickers);
        auto new_leader = co_await elect_new_leader(rafts, connected, in_configuration,
            initial_leader, 1);
        restart_tickers(tickers);

        // Heal network partition.
        connected->connect_all();

        // wait to synchronize logs between current leader (B) and the rest of the cluster
        co_await wait_log(rafts, connected, in_configuration, new_leader);

        // Again, A's RPC configuration is the same as before despite the
        // real cfg being reverted to the committed state as it is the union
        // between current and previous configurations in case of
        // joint cfg, anyway.
        co_await seastar::async([&] {
            CHECK_EVENTUALLY_EQUAL(rafts[0].rpc->known_peers(), committed_conf);
        });
        BOOST_CHECK(rafts[1].rpc->known_peers() == committed_conf);
        BOOST_CHECK(rafts[2].rpc->known_peers() == committed_conf);

        //
        // Second case: expand cluster (re-add node D).
        //

        // Elect A leader again.
        pause_tickers(tickers);
        co_await elect_new_leader(rafts, connected, in_configuration,
            new_leader, initial_leader);
        restart_tickers(tickers);

        co_await wait_log(rafts, connected, in_configuration, initial_leader);

        // Disconnect A from the rest of the cluster.
        connected->disconnect(to_raft_id(0));

        // Try to add D back.
        tickers[0].cancel();
        co_await rafts[initial_leader].server->abort();
        initial_state restart_state_2{
            .log = {
                raft::log_entry{raft::term_t(1), raft::index_t(1),
                    raft::configuration(committed_conf)
                },
                raft::log_entry{raft::term_t(2), raft::index_t(2),
                    raft::configuration(
                        all_nodes,
                        committed_conf
                    )
                }
            },
            .snapshot = {.config = all_nodes}
        };
        rafts[initial_leader] = create_raft_server(to_raft_id(initial_leader), dummy_apply_fn, restart_state_2, 1,
            connected, make_lw_shared<snapshots>(), make_lw_shared<persisted_snapshots>(), false);
        co_await rafts[initial_leader].server->start();
        tickers[initial_leader].set_callback([&rafts, s=initial_leader] { rafts[s].server->tick(); });
        tickers[0].rearm_periodic(tick_delta);

        // A should observe RPC configuration = {A, B, C, D} since it's the union
        // of an uncommitted joint config components
        // {.current = {A, B, C, D}, .previous = {A, B, C}}.
        BOOST_CHECK(rafts[0].rpc->known_peers() == all_nodes);

        // Elect B as leader
        pause_tickers(tickers);
        new_leader = co_await elect_new_leader(rafts, connected, in_configuration,
            initial_leader, 1);
        restart_tickers(tickers);

        // Heal network partition.
        connected->connect_all();

        // wait to synchronize logs between current leader (B) and the rest of the cluster
        co_await wait_log(rafts, connected, in_configuration, new_leader);
        // A's RPC configuration is reverted to committed configuration {A, B, C}.
        co_await seastar::async([&] {
            CHECK_EVENTUALLY_EQUAL(rafts[0].rpc->known_peers(), committed_conf);
        });
        BOOST_CHECK(rafts[1].rpc->known_peers() == committed_conf);
        BOOST_CHECK(rafts[2].rpc->known_peers() == committed_conf);
    });
}