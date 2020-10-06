#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"


// Test Raft library with declarative test definitions
//
//  For each test defined by
//      (replication_tests)
//      - Name
//      - Number of servers (nodes)
//      - Current term
//      - Initial leader
//      - Initial states for each server (log entries)
//      - Updates to be procesed
//          - append to log (trickles from leader to the rest)
//          - leader change
//          - configuration change
//
//      (run_test)
//      - Create the servers and initialize
//      - Process updates one by one
//      - Wait until all servers have logs of size of total_values entries
//      - Verify hash


using namespace std::chrono_literals;
using namespace std::placeholders;

static seastar::logger tlogger("test");

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

int rand() {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

bool drop_replication = false;

class state_machine : public raft::state_machine {
public:
    using apply_fn = std::function<void(raft::server_id id, const std::vector<raft::command_cref>& commands)>;
private:
    raft::server_id _id;
    apply_fn _apply;
    size_t _apply_entries;
    size_t _seen = 0;
    promise<> _done;
public:
    state_machine(raft::server_id id, apply_fn apply, size_t apply_entries) :
        _id(id), _apply(std::move(apply)), _apply_entries(apply_entries) {}
    future<> apply(const std::vector<raft::command_cref> commands) override {
        _apply(_id, commands);
        _seen += commands.size();
        if (_seen >= _apply_entries) {
            _done.set_value();
        }
        return make_ready_future<>();
    }

    virtual future<raft::snapshot_id> take_snapshot() { return make_ready_future<raft::snapshot_id>(raft::snapshot_id()); }
    virtual void drop_snapshot(raft::snapshot_id id) {}
    virtual future<> load_snapshot(raft::snapshot_id id) { return make_ready_future<>(); };
    virtual future<> abort() { return make_ready_future<>(); }

    future<> done() {
        return _done.get_future();
    }
};

struct initial_state {
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    size_t total_entries = 100;       // Total entries including initial snapshot
    raft::snapshot snapshot;
};

class storage : public raft::storage {
    raft::server_id _id;
    initial_state _conf;
public:
    storage(raft::server_id id, initial_state conf) : _id(id), _conf(std::move(conf)) {}
    storage() {}
    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) { co_return seastar::sleep(1us); }
    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) { return make_ready_future<>(); }
    virtual future<raft::snapshot> load_snapshot() {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { co_return seastar::sleep(1us); };
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

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
public:
    rpc(raft::server_id id) : _id(id) {
        net[_id] = this;
    }
    virtual future<> send_snapshot(raft::server_id id, const raft::install_snapshot& snap) { return make_ready_future<>(); }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request_send& append_request) {
        if (drop_replication && !(rand() % 5)) {
            return make_ready_future<>();
        }
        raft::append_request_recv req;
        req.current_term = append_request.current_term;
        req.leader_id = append_request.leader_id;
        req.prev_log_idx = append_request.prev_log_idx;
        req.prev_log_term = append_request.prev_log_term;
        req.leader_commit_idx = append_request.leader_commit_idx;
        for (auto&& e: append_request.entries) {
            req.entries.push_back(e);
        }
        net[id]->_client->append_entries(_id, std::move(req));
        //co_return seastar::sleep(1us);
        return make_ready_future<>();
    }
    virtual future<> send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
        if (drop_replication && !(rand() % 5)) {
            return make_ready_future<>();
        }
        net[id]->_client->append_entries_reply(_id, std::move(reply));
        return make_ready_future<>();
    }
    virtual future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
        net[id]->_client->request_vote(_id, std::move(vote_request));
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
        net[id]->_client->request_vote_reply(_id, std::move(vote_reply));
        return make_ready_future<>();
    }
    virtual void add_server(raft::server_id id, bytes node_info) {}
    virtual void remove_server(raft::server_id id) {}
    virtual future<> abort() { return make_ready_future<>(); }
};

std::unordered_set<raft::server_id> SERVER_DISCONNECTED;
class failure_detector : public raft::failure_detector {
    bool is_alive(raft::server_id server) override {
        return SERVER_DISCONNECTED.find(server) == SERVER_DISCONNECTED.end();
    }
};

std::unordered_map<raft::server_id, rpc*> rpc::net;

std::pair<std::unique_ptr<raft::server>, state_machine*>
create_raft_server(raft::server_id uuid, state_machine::apply_fn apply, initial_state state,
        size_t apply_entries) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(apply), apply_entries);
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(uuid, state);
    auto fd = seastar::make_shared<failure_detector>();

    auto raft = raft::create_server(uuid, std::move(mrpc), std::move(sm), std::move(mstorage),
        std::move(fd), raft::server::configuration());

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>> create_cluster(std::vector<initial_state> states, state_machine::apply_fn apply, size_t apply_entries) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::UUID(0, i + 1);   // Custom sequential debug id; 0 is invalid
        config.servers.push_back(raft::server_address{uuid});
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = config.servers[i];
        states[i].snapshot.config = config;
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, apply, states[i], apply_entries)).first;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(std::initializer_list<log_entry> list, unsigned start_idx) {
    std::vector<raft::log_entry> log;

    unsigned i = start_idx;
    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e.value);
        log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++), std::move(command)});
    }

    return log;
}

template <typename T>
std::vector<raft::command> create_commands(std::vector<T> list) {
    std::vector<raft::command> commands;
    commands.reserve(list.size());

    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e);
        commands.push_back(std::move(command));
    }

    return commands;
}

std::unordered_map<raft::server_id, int> sums;

void apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands) {
    tlogger.debug("sm::apply_changes[{}] got {} entries", id, commands.size());

    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        auto it = sums.find(id);
        if (it == sums.end()) {
            sums[id] = 0;
        }
        sums[id] += n;
    }
};

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
using entries = unsigned;
using new_leader = int;
// TODO: config change
using update = std::variant<entries, new_leader>;

struct initial_log {
    std::initializer_list<log_entry> le;
};

struct initial_snapshot {
    raft::snapshot snap;
};

struct test_case {
    const std::string name;
    const size_t nodes;
    const size_t total_values = 100;
    uint64_t initial_term = 1;
    const std::optional<uint64_t> initial_leader;
    const std::vector<struct initial_log> initial_states;
    const std::vector<struct initial_snapshot> initial_snapshots;
    const std::vector<update> updates;
};

// Run test case (name, nodes, leader, initial logs, updates)
future<int> run_test(test_case test) {
    std::vector<initial_state> states(test.nodes);       // Server initial states

    tlogger.debug("running test {}:", test.name);
    size_t leader;
    if (test.initial_leader) {
        leader = *test.initial_leader;
    } else {
        leader = 0;
    }

    states[leader].term = raft::term_t{test.initial_term};

    int leader_initial_entries = 0;
    if (leader < test.initial_states.size()) {
        leader_initial_entries += test.initial_states[leader].le.size();  // Count existing leader entries
    }
    int leader_snap_skipped = 0; // Next value to write
    if (leader < test.initial_snapshots.size()) {
        leader_snap_skipped = test.initial_snapshots[leader].snap.idx;  // Count existing leader entries
    }

    unsigned apply_entries = test.total_values - leader_snap_skipped;

    // Server initial logs, etc
    for (size_t i = 0; i < states.size(); ++i) {
        size_t start_idx = 1;
        if (i < test.initial_snapshots.size()) {
            states[i].snapshot = test.initial_snapshots[i].snap;
            start_idx = states[i].snapshot.idx + 1;
        }
        if (i < test.initial_states.size()) {
            auto state = test.initial_states[i];
            states[i].log = create_log(state.le, start_idx);
        } else {
            states[i].log = {};
        }
    }

    auto rafts = co_await create_cluster(states, apply_changes, apply_entries);

    co_await rafts[leader].first->elect_me_leader();
    // Process all updates in order
    size_t next_val = leader_snap_skipped + leader_initial_entries;
    for (auto update: test.updates) {
        if (std::holds_alternative<entries>(update)) {
            auto n = std::get<entries>(update);
            std::vector<int> values(n);
            std::iota(values.begin(), values.end(), next_val);
            std::vector<raft::command> commands = create_commands<int>(values);
            co_await seastar::parallel_for_each(commands, [&] (raft::command cmd) {
                tlogger.debug("Adding command entry on leader {}", leader);
                return rafts[leader].first->add_entry(std::move(cmd), raft::wait_type::committed);
            });
            next_val += n;
        } else if (std::holds_alternative<new_leader>(update)) {
            unsigned next_leader = std::get<new_leader>(update);
            assert(next_leader < rafts.size());
            SERVER_DISCONNECTED.insert(raft::server_id{utils::UUID(0, leader + 1)});
            for (size_t s = 0; s < test.nodes; ++s) {
                if (s != leader) {
                    rafts[s].first->elapse_election();
                }
            }
            co_await rafts[next_leader].first->elect_me_leader();
            SERVER_DISCONNECTED.erase(raft::server_id{utils::UUID(0, leader + 1)});
            tlogger.debug("confirmed leader on {}", next_leader);
            leader = next_leader;
        }
    }

    if (next_val < test.total_values) {
        // Send remaining updates
        std::vector<int> values(test.total_values - next_val);
        std::iota(values.begin(), values.end(), next_val);
        std::vector<raft::command> commands = create_commands<int>(values);
        tlogger.debug("Adding remaining {} entries on leader {}", values.size(), leader);
        co_await seastar::parallel_for_each(commands, [&] (raft::command cmd) {
            return rafts[leader].first->add_entry(std::move(cmd), raft::wait_type::committed);
        });
    }

    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->abort(); // Stop servers
    }

    co_return 0;
}

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;

    seastar::app_template::config cfg;
    seastar::app_template app(cfg);
    app.add_options()
        ("drop-replication", bpo::value<bool>()->default_value(false), "drop replication packets randomly");

    std::vector<test_case> replication_tests = {
        // 1 nodes, simple replication, empty, no updates
        {.name = "simple_replication", .nodes = 1},
        // 2 nodes, 4 existing leader entries, 4 updates
        {.name = "non_empty_leader_log", .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {entries{4}}},
        // 1 nodes, 12 client entries
        {.name = "simple_1_auto_12", .nodes = 1,
         .initial_states = {}, .updates = {entries{12}}},
        // 1 nodes, 12 client entries
        {.name = "simple_1_expected", .nodes = 1,
         .initial_states = {},
         .updates = {entries{4}}},
        // 1 nodes, 7 leader entries, 12 client entries
        {.name = "simple_1_pre", .nodes = 1,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},},
        // 2 nodes, 7 leader entries, 12 client entries
        {.name = "simple_2_pre", .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},},
        // 3 nodes, 2 leader changes with 4 client entries each
        {.name = "leader_changes", .nodes = 3,
         .updates = {entries{4},new_leader{1},entries{4},new_leader{2},entries{4}}},
        //
        // NOTE: due to disrupting candidates protection leader doesn't vote for others, and
        //       servers with entries vote for themselves, so some tests use 3 servers instead of
        //       2 for simplicity and to avoid a stalemate. This behaviour can be disabled.
        //
        // 3 nodes, 7 leader entries, 12 client entries, change leader, 12 client entries
        {.name = "simple_3_pre_chg", .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12},new_leader{1},entries{12}},},
        // 2 nodes, leader empoty, follower has 3 spurious entries
        {.name = "replace_log_leaders_log_empty", .nodes = 3, .initial_term = 2,
         .initial_states = {{}, {{{2,10},{2,20},{2,30}}}},
         .updates = {entries{4}}},
        // 3 nodes, 7 leader entries, follower has 9 spurious entries
        {.name = "simple_3_spurious", .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14},{2,15},{2,16},{2,17},{2,18}}}},
         .updates = {entries{4}},},
        // 3 nodes, term 3, leader has 9 entries, follower has 5 spurious entries, 4 client entries
        {.name = "simple_3_spurious", .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14}}}},
         .updates = {entries{4}},},
        // 3 nodes, term 2, leader has 7 entries, follower has 3 good and 3 spurious entries
        {.name = "simple_3_follower_4_1", .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{1,0},{1,1},{1,2},{2,20},{2,30},{2,40}}}},
         .updates = {entries{4}}},
        // A follower and a leader have matching logs but leader's is shorter
        // 3 nodes, term 2, leader has 2 entries, follower has same and 5 more, 12 updates
        {.name = "simple_3_short_leader", .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1}}},
                            {.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}}},
        // A follower and a leader have no common entries
        // 3 nodes, term 2, leader has 7 entries, follower has non-matching 6 entries, 12 updates
        {.name = "follower_not_matching", .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{2,10},{2,20},{2,30},{2,40},{2,50},{2,60}}}},
         .updates = {entries{12}},},
        // A follower and a leader have one common entry
        // 3 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries, 12 updates
        {.name = "follower_one_common", .nodes = 3, .initial_term = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2}}},
                            {.le = {{1,0},{2,11},{2,12},{2,13}}}},
         .updates = {entries{12}}},
    };

    return app.run(argc, argv, [&replication_tests, &app] () -> future<int> {
        drop_replication = app.configuration()["drop-replication"].as<bool>();

        for (auto test: replication_tests) {
            if (co_await run_test(test) != 0) {
                co_return 1; // Fail
            }
        }
        co_return 0;
    });
}

