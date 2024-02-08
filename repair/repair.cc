/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "repair.hh"
#include "repair/row_level.hh"

#include "locator/network_topology_strategy.hh"
#include "streaming/stream_reason.hh"
#include "gms/inet_address.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "repair/table_check.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "sstables/sstables.hh"
#include "partition_range_compat.hh"
#include "utils/error_injection.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm_ext.hpp>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/sleep.hh>

#include <cfloat>
#include <atomic>

#include "idl/partition_checksum.dist.hh"

using namespace std::chrono_literals;

logging::logger rlogger("repair");

node_ops_info::node_ops_info(node_ops_id ops_uuid_, shared_ptr<abort_source> as_, std::list<gms::inet_address>&& ignore_nodes_) noexcept
    : ops_uuid(ops_uuid_)
    , as(std::move(as_))
    , ignore_nodes(std::move(ignore_nodes_))
{}

void node_ops_info::check_abort() {
    if (as && as->abort_requested()) {
        auto msg = format("Node operation with ops_uuid={} is aborted", ops_uuid);
        rlogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
}

node_ops_metrics::node_ops_metrics(shared_ptr<repair::task_manager_module> module)
    : _module(module)
{
    namespace sm = seastar::metrics;
    auto ops_label_type = sm::label("ops");
    _metrics.add_group("node_ops", {
        sm::make_gauge("finished_percentage", [this] { return bootstrap_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("bootstrap")}),
        sm::make_gauge("finished_percentage", [this] { return replace_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("replace")}),
        sm::make_gauge("finished_percentage", [this] { return rebuild_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("rebuild")}),
        sm::make_gauge("finished_percentage", [this] { return decommission_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("decommission")}),
        sm::make_gauge("finished_percentage", [this] { return removenode_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("removenode")}),
        sm::make_gauge("finished_percentage", [this] { return repair_finished_percentage(); },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("repair")}),
    });
}

float node_ops_metrics::bootstrap_finished_percentage() {
    return bootstrap_total_ranges == 0 ? 1 : float(bootstrap_finished_ranges) / float(bootstrap_total_ranges);
}
float node_ops_metrics::replace_finished_percentage() {
    return replace_total_ranges == 0 ? 1 : float(replace_finished_ranges) / float(replace_total_ranges);
}
float node_ops_metrics::rebuild_finished_percentage() {
    return rebuild_total_ranges == 0 ? 1 : float(rebuild_finished_ranges) / float(rebuild_total_ranges);
}
float node_ops_metrics::decommission_finished_percentage() {
    return decommission_total_ranges == 0 ? 1 : float(decommission_finished_ranges) / float(decommission_total_ranges);
}
float node_ops_metrics::removenode_finished_percentage() {
    return removenode_total_ranges == 0 ? 1 : float(removenode_finished_ranges) / float(removenode_total_ranges);
}

template <typename T1, typename T2>
inline
static std::ostream& operator<<(std::ostream& os, const std::unordered_map<T1, T2>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem.first << "=" << elem.second;
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& out, row_level_diff_detect_algorithm algo) {
    return out << format_as(algo);
}

std::string_view format_as(row_level_diff_detect_algorithm algo) {
    using enum row_level_diff_detect_algorithm;
    switch (algo) {
    case send_full_set:
        return "send_full_set";
    case send_full_set_rpc_stream:
        return "send_full_set_rpc_stream";
    };
    return "unknown";
}

static size_t get_nr_tables(const replica::database& db, const sstring& keyspace) {
    size_t tables = 0;
    db.get_tables_metadata().for_each_table_id([&keyspace, &tables] (const std::pair<sstring, sstring>& kscf, table_id) {
        tables += kscf.first == keyspace;
    });
    return tables;
}

static std::vector<sstring> list_column_families(const replica::database& db, const sstring& keyspace) {
    std::vector<sstring> ret;
    db.get_tables_metadata().for_each_table_id([&] (const std::pair<sstring, sstring>& kscf, table_id) {
        if (kscf.first == keyspace) {
            ret.push_back(kscf.second);
        }
    });
    return ret;
}

static const replica::column_family* find_column_family_if_exists(const replica::database& db, std::string_view ks_name, std::string_view cf_name, bool warn = true) {
    try {
        auto uuid = db.find_uuid(std::move(ks_name), std::move(cf_name));
        return &db.find_column_family(uuid);
    } catch (replica::no_such_column_family&) {
        if (warn) {
            rlogger.warn("{}", std::current_exception());
        }
        return nullptr;
    }
}

static const replica::column_family* find_column_family_if_exists(const replica::database& db, const table_id& uuid, bool warn = true) {
    try {
        return &db.find_column_family(uuid);
    } catch (...) {
        if (warn) {
            rlogger.warn("{}", std::current_exception());
        }
        return nullptr;
    }
}

std::ostream& operator<<(std::ostream& os, const repair_uniq_id& x) {
    return os << format("[id={}, uuid={}]", x.id, x.uuid());
}

// Must run inside a seastar thread
static std::vector<table_id> get_table_ids(const replica::database& db, const sstring& keyspace, const std::vector<sstring>& tables) {
    std::vector<table_id> table_ids;
    table_ids.reserve(tables.size());
    for (auto& table : tables) {
        thread::maybe_yield();
        try {
            table_ids.push_back(db.find_uuid(keyspace, table));
        } catch (replica::no_such_column_family&) {
            rlogger.warn("Column family {} does not exist in keyspace {}", table, keyspace);
        }
    }
    return table_ids;
}

static std::vector<sstring> get_table_names(const replica::database& db, const std::vector<table_id>& table_ids) {
    std::vector<sstring> table_names;
    table_names.reserve(table_ids.size());
    for (auto& table_id : table_ids) {
        auto* cf = find_column_family_if_exists(db, table_id);
        table_names.push_back(cf ? cf->schema()->cf_name() : "");
    }
    return table_names;
}

template<typename Collection, typename T>
void remove_item(Collection& c, T& item) {
    auto it = std::find(c.begin(), c.end(), item);
    if (it != c.end()) {
        c.erase(it);
    }
}

repair_neighbors::repair_neighbors(std::vector<gms::inet_address> nodes, std::vector<shard_id> shards)
    : all(std::move(nodes))
{
    if (all.size() != shards.size()) {
        throw std::runtime_error("The number of shards and nodes do not match");
    }
    for (unsigned i = 0; i < all.size(); i++) {
        shard_map[all[i]] = shards[i];
    }
}

// Return all of the neighbors with whom we share the provided range.
static std::vector<gms::inet_address> get_neighbors(
        const locator::effective_replication_map& erm,
        const sstring& ksname, query::range<dht::token> range,
        const std::vector<sstring>& data_centers,
        const std::vector<sstring>& hosts,
        const std::unordered_set<gms::inet_address>& ignore_nodes, bool small_table_optimization = false) {
    dht::token tok = range.end() ? range.end()->value() : dht::maximum_token();
    auto ret = erm.get_natural_endpoints(tok);
    if (small_table_optimization) {
        auto normal_nodes = erm.get_token_metadata().get_all_ips();
        ret = inet_address_vector_replica_set(normal_nodes.begin(), normal_nodes.end());
    }
    auto my_address = erm.get_topology().my_address();
    remove_item(ret, my_address);

    if (!data_centers.empty()) {
        auto dc_endpoints_map = erm.get_token_metadata().get_topology().get_datacenter_endpoints();
        std::unordered_set<gms::inet_address> dc_endpoints;
        for (const sstring& dc : data_centers) {
            auto it = dc_endpoints_map.find(dc);
            if (it == dc_endpoints_map.end()) {
                std::vector<sstring> dcs;
                for (const auto& e : dc_endpoints_map) {
                    dcs.push_back(e.first);
                }
                throw std::runtime_error(fmt::format("Unknown data center '{}'. "
                        "Known data centers: {}", dc, dcs));
            }
            for (const auto& endpoint : it->second) {
                dc_endpoints.insert(endpoint);
            }
        }
        // We require, like Cassandra does, that the current host must also
        // be part of the repair
        if (!dc_endpoints.contains(my_address)) {
            throw std::runtime_error("The current host must be part of the repair");
        }
        // The resulting list of nodes is the intersection of the nodes in the
        // listed data centers, and the (range-dependent) list of neighbors.
        std::unordered_set<gms::inet_address> neighbor_set(ret.begin(), ret.end());
        ret.clear();
        for (const auto& endpoint : dc_endpoints) {
            if (neighbor_set.contains(endpoint)) {
                ret.push_back(endpoint);
            }
        }
    } else if (!hosts.empty()) {
        bool found_me = false;
        std::unordered_set<gms::inet_address> neighbor_set(ret.begin(), ret.end());
        ret.clear();
        for (const sstring& host : hosts) {
            gms::inet_address endpoint;
            try {
                endpoint = gms::inet_address(host);
            } catch(...) {
                throw std::runtime_error(format("Unknown host specified: {}", host));
            }
            if (endpoint == my_address) {
                found_me = true;
            } else if (neighbor_set.contains(endpoint)) {
                ret.push_back(endpoint);
                // If same host is listed twice, don't add it again later
                neighbor_set.erase(endpoint);
            }
            // Nodes which aren't neighbors for this range are ignored.
            // This allows the user to give a list of "good" nodes, where
            // for each different range, only the subset of nodes actually
            // holding a replica of the given range is used. This,
            // however, means the user is never warned if one of the nodes
            // on the list isn't even part of the cluster.
        }
        // We require, like Cassandra does, that the current host must also
        // be listed on the "-hosts" option - even those we don't want it in
        // the returned list:
        if (!found_me) {
            throw std::runtime_error("The current host must be part of the repair");
        }
        if (ret.size() < 1) {
            auto me = my_address;
            auto others = erm.get_natural_endpoints(tok);
            remove_item(others, me);
            throw std::runtime_error(fmt::format("Repair requires at least two "
                    "endpoints that are neighbors before it can continue, "
                    "the endpoint used for this repair is {}, other "
                    "available neighbors are {} but these neighbors were not "
                    "part of the supplied list of hosts to use during the "
                    "repair ({}).", me, others, hosts));
        }
    } else if (!ignore_nodes.empty()) {
        auto it = std::remove_if(ret.begin(), ret.end(), [&ignore_nodes] (const gms::inet_address& node) {
            return ignore_nodes.contains(node);
        });
        ret.erase(it, ret.end());
    }

    return boost::copy_range<std::vector<gms::inet_address>>(std::move(ret));

#if 0
    // Origin's ActiveRepairService.getNeighbors() also verifies that the
    // requested range fits into a local range
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

#endif
}

static future<std::list<gms::inet_address>> get_hosts_participating_in_repair(
        const locator::effective_replication_map& erm,
        const sstring& ksname,
        const dht::token_range_vector& ranges,
        const std::vector<sstring>& data_centers,
        const std::vector<sstring>& hosts,
        const std::unordered_set<gms::inet_address>& ignore_nodes) {

    std::unordered_set<gms::inet_address> participating_hosts;

    // Repair coordinator must participate in repair, but it is never
    // returned by get_neighbors - add it here
    auto my_address = erm.get_topology().my_address();
    participating_hosts.insert(my_address);

    co_await do_for_each(ranges, [&] (const dht::token_range& range) {
        const auto nbs = get_neighbors(erm, ksname, range, data_centers, hosts, ignore_nodes);
        for (const auto& nb : nbs) {
            participating_hosts.insert(nb);
        }
    });

    co_return std::list<gms::inet_address>(participating_hosts.begin(), participating_hosts.end());
}

float node_ops_metrics::repair_finished_percentage() {
    return _module->report_progress();
}

repair::task_manager_module::task_manager_module(tasks::task_manager& tm, repair_service& rs, size_t max_repair_memory) noexcept
    : tasks::task_manager::module(tm, "repair")
    , _rs(rs)
    , _range_parallelism_semaphore(std::max(size_t(1), size_t(max_repair_memory / max_repair_memory_per_range / 4)),
            named_semaphore_exception_factory{"repair range parallelism"})
{
    auto nr = _range_parallelism_semaphore.available_units();
    rlogger.info("Setting max_repair_memory={}, max_repair_memory_per_range={}, max_repair_ranges_in_parallel={}",
        max_repair_memory, max_repair_memory_per_range, nr);
}

void repair::task_manager_module::start(repair_uniq_id id) {
    _pending_repairs.insert(id.uuid());
    _status[id.id] = repair_status::RUNNING;
}

void repair::task_manager_module::done(repair_uniq_id id, bool succeeded) {
    _pending_repairs.erase(id.uuid());
    _aborted_pending_repairs.erase(id.uuid());
    if (succeeded) {
        _status.erase(id.id);
    } else {
        _status[id.id] = repair_status::FAILED;
    }
    _done_cond.broadcast();
}

repair_status repair::task_manager_module::get(int id) const {
    if (std::cmp_greater(id, _sequence_number)) {
        throw std::runtime_error(format("unknown repair id {}", id));
    }
    auto it = _status.find(id);
    if (it == _status.end()) {
        return repair_status::SUCCESSFUL;
    } else {
        return it->second;
    }
}

future<repair_status> repair::task_manager_module::repair_await_completion(int id, std::chrono::steady_clock::time_point timeout) {
    return seastar::with_gate(async_gate(), [this, id, timeout] {
        if (std::cmp_greater(id, _sequence_number)) {
            return make_exception_future<repair_status>(std::runtime_error(format("unknown repair id {}", id)));
        }
        return repeat_until_value([this, id, timeout] {
            auto it = _status.find(id);
            if (it == _status.end()) {
                return make_ready_future<std::optional<repair_status>>(repair_status::SUCCESSFUL);
            } else {
                if (it->second == repair_status::FAILED) {
                    return make_ready_future<std::optional<repair_status>>(repair_status::FAILED);
                } else {
                    return _done_cond.wait(timeout).then([] {
                        return make_ready_future<std::optional<repair_status>>(std::nullopt);
                    }).handle_exception_type([] (condition_variable_timed_out&) {
                        return make_ready_future<std::optional<repair_status>>(repair_status::RUNNING);
                    });
                }
            }
        });
    });
}

void repair::task_manager_module::check_in_shutdown() {
    abort_source().check();
}

void repair::task_manager_module::add_shard_task_id(int id, tasks::task_id uuid) {
    _repairs.emplace(id, uuid);
}

void repair::task_manager_module::remove_shard_task_id(int id) {
    _repairs.erase(id);
}

tasks::task_manager::task_ptr repair::task_manager_module::get_shard_task_ptr(int id) {
    auto it = _repairs.find(id);
    if (it != _repairs.end()) {
        auto task_it = _tasks.find(it->second);
        if (task_it != _tasks.end()) {
            return task_it->second;
        }
    }
    return {};
}

std::vector<int> repair::task_manager_module::get_active() const {
    std::vector<int> res;
    boost::push_back(res, _status | boost::adaptors::filtered([] (auto& x) {
        return x.second == repair_status::RUNNING;
    }) | boost::adaptors::map_keys);
    return res;
}

size_t repair::task_manager_module::nr_running_repair_jobs() {
    size_t count = 0;
    if (this_shard_id() != 0) {
        return count;
    }
    for (auto& x : _status) {
        auto& status = x.second;
        if (status == repair_status::RUNNING) {
            count++;
        }
    }
    return count;
}

bool repair::task_manager_module::is_aborted(const tasks::task_id& uuid) {
    return _aborted_pending_repairs.contains(uuid);
}

void repair::task_manager_module::abort_all_repairs() {
    _aborted_pending_repairs = _pending_repairs;
    for (auto& x : _repairs) {
        auto it = _tasks.find(x.second);
        if (it != _tasks.end()) {
            auto& impl = dynamic_cast<repair::shard_repair_task_impl&>(*it->second->_impl);
            // If the task is aborted, its state will change to failed. One can wait for this with task_manager::task::done().
            (void)impl.abort();
        }
    }
    rlogger.info0("Started to abort repair jobs={}, nr_jobs={}", _aborted_pending_repairs, _aborted_pending_repairs.size());
}

float repair::task_manager_module::report_progress() {
    uint64_t nr_ranges_finished = 0;
    uint64_t nr_ranges_total = 0;
    for (auto& x : _repairs) {
        auto it = _tasks.find(x.second);
        if (it != _tasks.end()) {
            auto& impl = dynamic_cast<repair::shard_repair_task_impl&>(*it->second->_impl);
            if (impl.reason() == streaming::stream_reason::repair) {
                nr_ranges_total += impl.ranges_size();
                nr_ranges_finished += impl.nr_ranges_finished;
            }
        }
    }
    return nr_ranges_total == 0 ? 1 : float(nr_ranges_finished) / float(nr_ranges_total);
}

named_semaphore& repair::task_manager_module::range_parallelism_semaphore() {
    return _range_parallelism_semaphore;
}

future<> repair::task_manager_module::run(repair_uniq_id id, std::function<void ()> func) {
    return seastar::with_gate(async_gate(), [this, id, func = std::move(func)] () mutable {
        start(id);
        return seastar::async([func = std::move(func)] { func(); }).then([this, id] {
            rlogger.info("repair[{}]: completed successfully", id.uuid());
            done(id, true);
        }).handle_exception([this, id] (std::exception_ptr ep) {
            done(id, false);
            return make_exception_future(std::move(ep));
        });
    });
}

future<uint64_t> estimate_partitions(seastar::sharded<replica::database>& db, const sstring& keyspace,
        const sstring& cf, const dht::token_range& range) {
    return db.map_reduce0(
        [keyspace, cf, range] (auto& db) {
            // FIXME: column_family should have a method to estimate the number of
            // partitions (and of course it should use cardinality estimation bitmaps,
            // not trivial sum). We shouldn't have this ugly code here...
            // FIXME: If sstables are shared, they will be accounted more than
            // once. However, shared sstables should exist for a short-time only.
            auto sstables = db.find_column_family(keyspace, cf).get_sstables();
            return boost::accumulate(*sstables, uint64_t(0),
                [&range] (uint64_t x, auto&& sst) { return x + sst->estimated_keys_for_range(range); });
        },
        uint64_t(0),
        std::plus<uint64_t>()
    );
}

repair::shard_repair_task_impl::shard_repair_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id id,
        const sstring& keyspace,
        repair_service& repair,
        locator::effective_replication_map_ptr erm_,
        const dht::token_range_vector& ranges_,
        std::vector<table_id> table_ids_,
        repair_uniq_id parent_id_,
        const std::vector<sstring>& data_centers_,
        const std::vector<sstring>& hosts_,
        const std::unordered_set<gms::inet_address>& ignore_nodes_,
        streaming::stream_reason reason_,
        bool hints_batchlog_flushed,
        bool small_table_optimization,
        std::optional<int> ranges_parallelism)
    : repair_task_impl(module, id, 0, "shard", keyspace, "", "", parent_id_.uuid(), reason_)
    , rs(repair)
    , db(repair.get_db())
    , messaging(repair.get_messaging().container())
    , mm(repair.get_migration_manager())
    , gossiper(repair.get_gossiper())
    , erm(std::move(erm_))
    , ranges(ranges_)
    , cfs(get_table_names(db.local(), table_ids_))
    , table_ids(std::move(table_ids_))
    , global_repair_id(parent_id_)
    , data_centers(data_centers_)
    , hosts(hosts_)
    , ignore_nodes(ignore_nodes_)
    , total_rf(erm->get_replication_factor())
    , _hints_batchlog_flushed(std::move(hints_batchlog_flushed))
    , _small_table_optimization(small_table_optimization)
    , _user_ranges_parallelism(ranges_parallelism ? std::optional<semaphore>(semaphore(*ranges_parallelism)) : std::nullopt)
{
    rlogger.debug("repair[{}]: Setting user_ranges_parallelism to {}", global_repair_id.uuid(),
            _user_ranges_parallelism ? std::to_string(_user_ranges_parallelism->available_units()) : "unlimited");
}

void repair::shard_repair_task_impl::check_failed_ranges() {
    rlogger.info("repair[{}]: stats: repair_reason={}, keyspace={}, tables={}, ranges_nr={}, {}",
        global_repair_id.uuid(), _reason, _status.keyspace, table_names(), ranges.size(), _stats.get_stats());
    if (nr_failed_ranges || _aborted || _failed_because) {
        sstring failed_because = "N/A";
        if (!_aborted) {
            failed_because = _failed_because ? *_failed_because : "unknown";
        }
        auto msg = format("repair[{}]: {} out of {} ranges failed, keyspace={}, tables={}, repair_reason={}, nodes_down_during_repair={}, aborted_by_user={}, failed_because={}",
                global_repair_id.uuid(), nr_failed_ranges, ranges_size(), _status.keyspace, table_names(), _reason, nodes_down, _aborted, failed_because);
        rlogger.warn("{}", msg);
        throw std::runtime_error(msg);
    } else {
        if (dropped_tables.size()) {
            rlogger.warn("repair[{}]: completed successfully, keyspace={}, ignoring dropped tables={}", global_repair_id.uuid(), _status.keyspace, dropped_tables);
        } else {
            rlogger.info("repair[{}]: completed successfully, keyspace={}", global_repair_id.uuid(), _status.keyspace);
        }
    }
}

void repair::shard_repair_task_impl::check_in_abort_or_shutdown() {
    try {
        _as.check();
    } catch (...) {
        if (!_aborted) {
            _aborted = true;
            rlogger.warn("repair[{}]: Repair job aborted by user, job={}, keyspace={}, tables={}",
                global_repair_id.uuid(), global_repair_id.uuid(), _status.keyspace, table_names());;
        }
        throw;
    }
}

repair_neighbors repair::shard_repair_task_impl::get_repair_neighbors(const dht::token_range& range) {
    return neighbors.empty() ?
        repair_neighbors(get_neighbors(*erm, _status.keyspace, range, data_centers, hosts, ignore_nodes, _small_table_optimization)) :
        neighbors[range];
}

size_t repair::shard_repair_task_impl::ranges_size() const noexcept {
    return ranges.size() * table_ids.size();
}

// Repair a single local range, multiple column families.
// Comparable to RepairSession in Origin
future<> repair::shard_repair_task_impl::repair_range(const dht::token_range& range, table_info table) {
    check_in_abort_or_shutdown();
    ranges_index++;
    repair_neighbors r_neighbors = get_repair_neighbors(range);
    auto neighbors = std::move(r_neighbors.all);
    auto mandatory_neighbors = std::move(r_neighbors.mandatory);
    auto live_neighbors = boost::copy_range<std::vector<gms::inet_address>>(neighbors |
                boost::adaptors::filtered([this] (const gms::inet_address& node) { return gossiper.is_alive(node); }));
    for (auto& node : mandatory_neighbors) {
        auto it = std::find(live_neighbors.begin(), live_neighbors.end(), node);
        if (it == live_neighbors.end()) {
            nr_failed_ranges++;
            nodes_down.insert(node);
            auto status = format("failed: mandatory neighbor={} is not alive", node);
            rlogger.error("repair[{}]: Repair {} out of {} ranges, keyspace={}, table={}, range={}, peers={}, live_peers={}, status={}",
                    global_repair_id.uuid(), ranges_index, ranges_size(), _status.keyspace, table.name, range, neighbors, live_neighbors, status);
            // If the task is aborted, its state will change to failed. One can wait for this with task_manager::task::done().
            (void)abort();
            co_await coroutine::return_exception(std::runtime_error(format("Repair mandatory neighbor={} is not alive, keyspace={}, mandatory_neighbors={}",
                node, _status.keyspace, mandatory_neighbors)));
        }
    }
    if (live_neighbors.size() != neighbors.size()) {
        nr_failed_ranges++;
        std::unordered_set<gms::inet_address> live_neighbors_set(live_neighbors.begin(), live_neighbors.end());
        for (auto& node : neighbors) {
            if (!live_neighbors_set.contains(node)) {
                nodes_down.insert(node);
            }
        }
        auto status = live_neighbors.empty() ? "skipped_no_live_peers" : "partial";
        rlogger.warn("repair[{}]: Repair {} out of {} ranges, keyspace={}, table={}, range={}, peers={}, live_peers={}, status={}",
                global_repair_id.uuid(), ranges_index, ranges_size(), _status.keyspace, table.name, range, neighbors, live_neighbors, status);
        if (live_neighbors.empty()) {
            co_return;
        }
        neighbors.swap(live_neighbors);
    }
    if (neighbors.empty()) {
        auto status = "skipped_no_followers";
        rlogger.warn("repair[{}]: Repair {} out of {} ranges,  keyspace={}, table={}, range={}, peers={}, live_peers={}, status={}",
                global_repair_id.uuid(), ranges_index, ranges_size(), _status.keyspace, table.name, range, neighbors, live_neighbors, status);
        co_return;
    }
    rlogger.debug("repair[{}]: Repair {} out of {} ranges, keyspace={}, table={}, range={}, peers={}, live_peers={}",
        global_repair_id.uuid(), ranges_index, ranges_size(), _status.keyspace, table.name, range, neighbors, live_neighbors);
    co_await mm.sync_schema(db.local(), neighbors);
    sstring cf;
    try {
        cf = db.local().find_column_family(table.id).schema()->cf_name();
    } catch (replica::no_such_column_family&) {
        co_return;
    }
    // Row level repair
    if (dropped_tables.contains(cf)) {
        co_return;
    }
    try {
        auto dropped = co_await with_table_drop_silenced(db.local(), mm, table.id, [&] (const table_id& uuid) {
            return repair_cf_range_row_level(*this, cf, table.id, range, neighbors, _small_table_optimization);
        });
        if (dropped) {
            dropped_tables.insert(cf);
        }
    } catch (...) {
        nr_failed_ranges++;
        throw;
    }
}

void repair_stats::add(const repair_stats& o) {
    round_nr += o.round_nr;
    round_nr_fast_path_already_synced += o.round_nr_fast_path_already_synced;
    round_nr_fast_path_same_combined_hashes += o.round_nr_fast_path_same_combined_hashes;
    round_nr_slow_path += o.round_nr_slow_path;
    rpc_call_nr += o.rpc_call_nr;
    tx_hashes_nr += o.tx_hashes_nr;
    rx_hashes_nr += o.rx_hashes_nr;
    tx_row_nr += o.tx_row_nr;
    rx_row_nr += o.rx_row_nr;
    tx_row_bytes += o.tx_row_bytes;
    rx_row_bytes += o.rx_row_bytes;
    auto add_map = [] (auto& target, auto& src) {
         for (const auto& [k, v] : src) {
             target[k] += v;
         }
    };
    add_map(row_from_disk_bytes, o.row_from_disk_bytes);
    add_map(row_from_disk_nr, o.row_from_disk_nr);
    add_map(tx_row_nr_peer, o.tx_row_nr_peer);
    add_map(rx_row_nr_peer, o.rx_row_nr_peer);
}

sstring repair_stats::get_stats() {
    std::map<gms::inet_address, float> row_from_disk_bytes_per_sec;
    std::map<gms::inet_address, float> row_from_disk_rows_per_sec;
    auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - start_time).count();
    for (auto& x : row_from_disk_bytes) {
        if (std::fabs(duration) > FLT_EPSILON) {
            row_from_disk_bytes_per_sec[x.first] = x.second / duration / 1024 / 1024;
        } else {
            row_from_disk_bytes_per_sec[x.first] = 0;
        }
    }
    for (auto& x : row_from_disk_nr) {
        if (std::fabs(duration) > FLT_EPSILON) {
            row_from_disk_rows_per_sec[x.first] = x.second / duration;
        } else {
            row_from_disk_rows_per_sec[x.first] = 0;
        }
    }
    return format("round_nr={}, round_nr_fast_path_already_synced={}, round_nr_fast_path_same_combined_hashes={}, round_nr_slow_path={}, rpc_call_nr={}, tx_hashes_nr={}, rx_hashes_nr={}, duration={} seconds, tx_row_nr={}, rx_row_nr={}, tx_row_bytes={}, rx_row_bytes={}, row_from_disk_bytes={}, row_from_disk_nr={}, row_from_disk_bytes_per_sec={} MiB/s, row_from_disk_rows_per_sec={} Rows/s, tx_row_nr_peer={}, rx_row_nr_peer={}",
            round_nr,
            round_nr_fast_path_already_synced,
            round_nr_fast_path_same_combined_hashes,
            round_nr_slow_path,
            rpc_call_nr,
            tx_hashes_nr,
            rx_hashes_nr,
            duration,
            tx_row_nr,
            rx_row_nr,
            tx_row_bytes,
            rx_row_bytes,
            row_from_disk_bytes,
            row_from_disk_nr,
            row_from_disk_bytes_per_sec,
            row_from_disk_rows_per_sec,
            tx_row_nr_peer,
            rx_row_nr_peer);
}

struct repair_options {
    // If primary_range is true, we should perform repair only on this node's
    // primary ranges. The default of false means perform repair on all ranges
    // held by the node. primary_range=true is useful if the user plans to
    // repair all nodes.
    bool primary_range = false;
    // If ranges is not empty, it overrides the repair's default heuristics
    // for determining the list of ranges to repair. In particular, "ranges"
    // overrides the setting of "primary_range".
    dht::token_range_vector ranges;
    // If start_token and end_token are set, they define a range which is
    // intersected with the ranges actually held by this node to decide what
    // to repair.
    sstring start_token;
    sstring end_token;
    // column_families is the list of column families to repair in the given
    // keyspace. If this list is empty (the default), all the column families
    // in this keyspace are repaired
    std::vector<sstring> column_families;
    // hosts specifies the list of known good hosts to repair with this host
    // (note that this host is required to also be on this list). For each
    // range repaired, only the relevant subset of the hosts (holding a
    // replica of this range) is used.
    std::vector<sstring> hosts;
    // The ignore_nodes specifies the list of nodes to ignore in this repair,
    // e.g., the user knows a node is down and wants to run repair without this
    // specific node.
    std::vector<sstring> ignore_nodes;
    // data_centers is used to restrict the repair to the local data center.
    // The node starting the repair must be in the data center; Issuing a
    // repair to a data center other than the named one returns an error.
    std::vector<sstring> data_centers;

    int ranges_parallelism = -1;

    bool small_table_optimization = false;

    repair_options(std::unordered_map<sstring, sstring> options) {
        bool_opt(primary_range, options, PRIMARY_RANGE_KEY);
        ranges_opt(ranges, options, RANGES_KEY);
        list_opt(column_families, options, COLUMNFAMILIES_KEY);
        list_opt(hosts, options, HOSTS_KEY);
        list_opt(ignore_nodes, options, IGNORE_NODES_KEY);
        list_opt(data_centers, options, DATACENTERS_KEY);
        // We currently do not support incremental repair. We could probably
        // ignore this option as it is just an optimization, but for now,
        // let's make it an error.
        bool incremental = false;
        bool_opt(incremental, options, INCREMENTAL_KEY);
        if (incremental) {
            throw std::runtime_error("unsupported incremental repair");
        }
        // We do not currently support the distinction between "parallel" and
        // "sequential" repair, and operate the same for both.
        // We don't currently support "dc parallel" parallelism.
        int parallelism = PARALLEL;
        int_opt(parallelism, options, PARALLELISM_KEY);
        if (parallelism != PARALLEL && parallelism != SEQUENTIAL) {
            throw std::runtime_error(format("unsupported repair parallelism: {}", parallelism));
        }
        string_opt(start_token, options, START_TOKEN);
        string_opt(end_token, options, END_TOKEN);

        bool trace = false;
        bool_opt(trace, options, TRACE_KEY);
        if (trace) {
            throw std::runtime_error("unsupported trace");
        }
        // Consume, ignore.
        int job_threads;
        int_opt(job_threads, options, JOB_THREADS_KEY);

        int_opt(ranges_parallelism, options, RANGES_PARALLELISM_KEY);

        bool_opt(small_table_optimization, options, SMALL_TABLE_OPTIMIZATION_KEY);

        // The parsing code above removed from the map options we have parsed.
        // If anything is left there in the end, it's an unsupported option.
        if (!options.empty()) {
            throw std::runtime_error(format("unsupported repair options: {}",
                    options));
        }
    }

    static constexpr const char* PRIMARY_RANGE_KEY = "primaryRange";
    static constexpr const char* PARALLELISM_KEY = "parallelism";
    static constexpr const char* INCREMENTAL_KEY = "incremental";
    static constexpr const char* JOB_THREADS_KEY = "jobThreads";
    static constexpr const char* RANGES_KEY = "ranges";
    static constexpr const char* COLUMNFAMILIES_KEY = "columnFamilies";
    static constexpr const char* DATACENTERS_KEY = "dataCenters";
    static constexpr const char* HOSTS_KEY = "hosts";
    static constexpr const char* IGNORE_NODES_KEY = "ignore_nodes";
    static constexpr const char* TRACE_KEY = "trace";
    static constexpr const char* START_TOKEN = "startToken";
    static constexpr const char* END_TOKEN = "endToken";
    static constexpr const char* RANGES_PARALLELISM_KEY = "ranges_parallelism";
    static constexpr const char* SMALL_TABLE_OPTIMIZATION_KEY = "small_table_optimization";

    // Settings of "parallelism" option. Numbers must match Cassandra's
    // RepairParallelism enum, which is used by the caller.
    enum repair_parallelism {
        SEQUENTIAL=0, PARALLEL=1, DATACENTER_AWARE=2
    };

private:
    static void bool_opt(bool& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            // Same parsing as Boolean.parseBoolean does:
            if (boost::algorithm::iequals(it->second, "true")) {
                var = true;
            } else {
                var = false;
            }
            options.erase(it);
        }
    }

    static void int_opt(int& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            errno = 0;
            var = strtol(it->second.c_str(), nullptr, 10);
            if (errno) {
                throw(std::runtime_error(format("cannot parse integer: '{}'", it->second)));
            }
            options.erase(it);
        }
    }

    static void string_opt(sstring& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            var = it->second;
            options.erase(it);
        }
    }

    // A range is expressed as start_token:end token and multiple ranges can
    // be given as comma separated ranges(e.g. aaa:bbb,ccc:ddd).
    static void ranges_opt(dht::token_range_vector& var,
            std::unordered_map<sstring, sstring>& options,
                        const sstring& key) {
        auto it = options.find(key);
        if (it == options.end()) {
            return;
        }
        std::vector<sstring> range_strings;
        boost::split(range_strings, it->second, boost::algorithm::is_any_of(","));
        for (auto range : range_strings) {
            std::vector<sstring> token_strings;
            boost::split(token_strings, range, boost::algorithm::is_any_of(":"));
            if (token_strings.size() != 2) {
                throw(std::runtime_error("range must have two components "
                        "separated by ':', got '" + range + "'"));
            }
            auto tok_start = dht::token::from_sstring(token_strings[0]);
            auto tok_end = dht::token::from_sstring(token_strings[1]);
            auto rng = wrapping_range<dht::token>(
                    ::range<dht::token>::bound(tok_start, false),
                    ::range<dht::token>::bound(tok_end, true));
            ::compat::unwrap_into(std::move(rng), dht::token_comparator(), [&] (dht::token_range&& x) {
                var.push_back(std::move(x));
            });
        }
        options.erase(it);
    }

    // A comma-separate list of strings
    static void list_opt(std::vector<sstring>& var,
            std::unordered_map<sstring, sstring>& options,
                        const sstring& key) {
        auto it = options.find(key);
        if (it == options.end()) {
            return;
        }
        std::vector<sstring> range_strings;
        boost::split(var, it->second, boost::algorithm::is_any_of(","));
        options.erase(it);
    }
};

void repair::shard_repair_task_impl::release_resources() noexcept {
    erm = {};
    cfs = {};
    data_centers = {};
    hosts = {};
    ignore_nodes = {};
    neighbors = {};
    dropped_tables = {};
    nodes_down = {};
}

future<> repair::shard_repair_task_impl::do_repair_ranges() {
    // Repair tables in the keyspace one after another
    assert(table_names().size() == table_ids.size());
    for (size_t idx = 0; idx < table_ids.size(); idx++) {
        table_info table_info{
            .name = table_names()[idx],
            .id = table_ids[idx],
        };
        // repair all the ranges in limited parallelism
        rlogger.info("repair[{}]: Started to repair {} out of {} tables in keyspace={}, table={}, table_id={}, repair_reason={}",
                global_repair_id.uuid(), idx + 1, table_ids.size(), _status.keyspace, table_info.name, table_info.id, _reason);
        co_await coroutine::parallel_for_each(ranges, [this, table_info] (auto&& range) -> future<> {
            // It is possible that most of the ranges are skipped. In this case
            // this lambda will just log a message and exit. With a lot of
            // ranges, this can result in stalls, as there are no opportunities
            // to yield when ranges are skipped. The yield below is meant to
            // prevent this.
            co_await coroutine::maybe_yield();

            // Get the system range parallelism
            auto permit = co_await seastar::get_units(rs.get_repair_module().range_parallelism_semaphore(), 1);
            // Get the range parallelism specified by user
            auto user_permit = _user_ranges_parallelism ? co_await seastar::get_units(*_user_ranges_parallelism, 1) : semaphore_units<>();
            co_await repair_range(range, table_info);
            ++_ranges_complete;
            if (_reason == streaming::stream_reason::bootstrap) {
                rs.get_metrics().bootstrap_finished_ranges++;
            } else if (_reason == streaming::stream_reason::replace) {
                rs.get_metrics().replace_finished_ranges++;
            } else if (_reason == streaming::stream_reason::rebuild) {
                rs.get_metrics().rebuild_finished_ranges++;
            } else if (_reason == streaming::stream_reason::decommission) {
                rs.get_metrics().decommission_finished_ranges++;
            } else if (_reason == streaming::stream_reason::removenode) {
                rs.get_metrics().removenode_finished_ranges++;
            } else if (_reason == streaming::stream_reason::repair) {
                rs.get_metrics().repair_finished_ranges_sum++;
                nr_ranges_finished++;
            }
            rlogger.debug("repair[{}]: node ops progress bootstrap={}, replace={}, rebuild={}, decommission={}, removenode={}, repair={}",
                global_repair_id.uuid(),
                rs.get_metrics().bootstrap_finished_percentage(),
                rs.get_metrics().replace_finished_percentage(),
                rs.get_metrics().rebuild_finished_percentage(),
                rs.get_metrics().decommission_finished_percentage(),
                rs.get_metrics().removenode_finished_percentage(),
                rs.get_metrics().repair_finished_percentage());

            if (2 * (_ranges_complete + 1) > ranges_size()) {
                co_await utils::get_local_injector().inject_with_handler("repair_shard_repair_task_impl_do_repair_ranges",
                [] (auto& handler) { return handler.wait_for_message(db::timeout_clock::now() + 10s); });
            }
        });

        if (_reason != streaming::stream_reason::repair) {
            try {
                auto& table = db.local().find_column_family(table_info.id);
                rlogger.debug("repair[{}]: Trigger off-strategy compaction for keyspace={}, table={}",
                    global_repair_id.uuid(), table.schema()->ks_name(), table.schema()->cf_name());
                table.trigger_offstrategy_compaction();
            } catch (replica::no_such_column_family&) {
                // Ignore dropped table
            }
        }
    }
    co_return;
}

future<tasks::task_manager::task::progress> repair::shard_repair_task_impl::get_progress() const {
    co_return tasks::task_manager::task::progress{
        .completed = _ranges_complete,
        .total = ranges_size()
    };
}

// Repairs a list of token ranges, each assumed to be a token
// range for which this node holds a replica, and, importantly, each range
// is assumed to be a indivisible in the sense that all the tokens in has the
// same nodes as replicas.
future<> repair::shard_repair_task_impl::run() {
    rs.get_repair_module().add_shard_task_id(global_repair_id.id, _status.id);
    auto remove_shard_task_id = defer([this] {
        rs.get_repair_module().remove_shard_task_id(global_repair_id.id);
    });
    try {
        co_await do_repair_ranges();
    } catch (...) {
        _failed_because.emplace(fmt::to_string(std::current_exception()));
        rlogger.debug("repair[{}]: got error in do_repair_ranges: {}",
            global_repair_id.uuid(), std::current_exception());
    }
    check_failed_ranges();
    co_return;
}

// repair_start() can run on any cpu; It runs on cpu0 the function
// do_repair_start(). The benefit of always running that function on the same
// CPU is that it allows us to keep some state (like a list of ongoing
// repairs). It is fine to always do this on one CPU, because the function
// itself does very little (mainly tell other nodes and CPUs what to do).
future<int> repair_service::do_repair_start(sstring keyspace, std::unordered_map<sstring, sstring> options_map) {
    get_repair_module().check_in_shutdown();
    auto& sharded_db = get_db();
    auto& db = sharded_db.local();

    // Note: Cassandra can, in some cases, decide immediately that there is
    // nothing to repair, and return 0. "nodetool repair" prints in this case
    // that "Nothing to repair for keyspace '...'". We don't have such a case
    // yet. The id field of repair_uniq_ids returned by next_repair_command()
    // will be >= 1.
    auto id = _repair_module->new_repair_uniq_id();
    rlogger.info("repair[{}]: starting user-requested repair for keyspace {}, repair id {}, options {}", id.uuid(), keyspace, id.id, options_map);

    repair_options options(options_map);

    std::vector<sstring> cfs =
        options.column_families.size() ? options.column_families : list_column_families(db, keyspace);
    if (cfs.empty()) {
        rlogger.info("repair[{}]: completed successfully: no tables to repair", id.uuid());
        co_return id.id;
    }

    {
        // Repair tables in table name order. Later we could repair in other
        // orders, e.g., smaller tables first.
        std::sort(cfs.begin(), cfs.end());

        size_t nr_tablet_table = 0;
        size_t nr_vnode_table = 0;
        bool is_tablet = false;
        for (auto& table_name : cfs) {
            auto& t = db.find_column_family(keyspace, table_name);
            if (t.uses_tablets()) {
                nr_tablet_table++;
            } else {
                nr_vnode_table++;
            }
        }

        if (nr_tablet_table != 0) {
            if (nr_vnode_table != 0) {
                throw std::runtime_error("Mixed vnode table and tablet table");
            }
            is_tablet = true;
        }
        if (is_tablet) {
            // Reject unsupported options for tablet repair
            if (!options.ranges.empty()) {
                throw std::runtime_error("The ranges option is not supported for tablet repair");
            }
            if (!options.hosts.empty()) {
                throw std::runtime_error("The hosts option is not supported for tablet repair");
            }
            if (!options.ignore_nodes.empty()) {
                throw std::runtime_error("The ignore_nodes option is not supported for tablet repair");
            }
            if (!options.data_centers.empty()) {
                throw std::runtime_error("The dataCenters option is not supported for tablet repair");
            }
            if (!options.start_token.empty()) {
                throw std::runtime_error("The startToken option is not supported for tablet repair");
            }
            if (!options.end_token.empty()) {
                throw std::runtime_error("The endToken option is not supported for tablet repair");
            }
            if (options.small_table_optimization) {
                throw std::runtime_error("The small_table_optimization option is not supported for tablet repair");
            }

            auto host2ip = [&addr_map = _addr_map] (locator::host_id host) -> future<gms::inet_address> {
                auto ip = addr_map.local().find(raft::server_id(host.uuid()));
                if (!ip) {
                    throw std::runtime_error(format("Could not get ip address for host {} from raft_address_map", host));
                }
                co_return *ip;
            };
            bool primary_replica_only = options.primary_range;
            co_await repair_tablets(id, keyspace, cfs, host2ip, primary_replica_only);
            co_return id.id;
        }
    }

    auto germs = make_lw_shared(co_await locator::make_global_effective_replication_map(sharded_db, keyspace));
    auto& erm = germs->get();
    auto& topology = erm.get_token_metadata().get_topology();
    auto my_address = erm.get_topology().my_address();

    if (erm.get_replication_strategy().get_type() == locator::replication_strategy_type::local) {
        rlogger.info("repair[{}]: completed successfully: nothing to repair for keyspace {} with local replication strategy", id.uuid(), keyspace);
        co_return id.id;
    }

    if (!_gossiper.local().is_normal(my_address)) {
        throw std::runtime_error("Node is not in NORMAL status yet!");
    }

    // If the "ranges" option is not explicitly specified, we repair all the
    // local ranges (the token ranges for which this node holds a replica of).
    // Each of these ranges may have a different set of replicas, so the
    // repair of each range is performed separately with repair_range().
    dht::token_range_vector ranges;
    if (options.ranges.size()) {
        ranges = options.ranges;
    } else if (options.primary_range) {
        rlogger.info("primary-range repair");
        // when "primary_range" option is on, neither data_centers nor hosts
        // may be set, except data_centers may contain only local DC (-local)
        if (options.data_centers.size() == 1 &&
            options.data_centers[0] == topology.get_datacenter()) {
            // get_primary_ranges_within_dc() is similar to get_primary_ranges(),
            // but instead of each range being assigned just one primary owner
            // across the entire cluster, here each range is assigned a primary
            // owner in each of the DCs.
            ranges = erm.get_primary_ranges_within_dc(my_address);
        } else if (options.data_centers.size() > 0 || options.hosts.size() > 0) {
            throw std::runtime_error("You need to run primary range repair on all nodes in the cluster.");
        } else {
            ranges = erm.get_primary_ranges(my_address);
        }
    } else {
        // get keyspace local ranges
        ranges = erm.get_ranges(my_address);
    }

    if (!options.data_centers.empty() && !options.hosts.empty()) {
        throw std::runtime_error("Cannot combine data centers and hosts options.");
    }

    if (!options.ignore_nodes.empty() && !options.hosts.empty()) {
        throw std::runtime_error("Cannot combine ignore_nodes and hosts options.");
    }
    std::unordered_set<gms::inet_address> ignore_nodes;
    for (const auto& n: options.ignore_nodes) {
        try {
            auto node = gms::inet_address(n);
            ignore_nodes.insert(node);
        } catch(...) {
            throw std::runtime_error(format("Failed to parse node={} in ignore_nodes={} specified by user: {}",
                n, options.ignore_nodes, std::current_exception()));
        }
    }

    if (!options.start_token.empty() || !options.end_token.empty()) {
        // Intersect the list of local ranges with the given token range,
        // dropping ranges with no intersection.
        std::optional<::range<dht::token>::bound> tok_start;
        std::optional<::range<dht::token>::bound> tok_end;
        if (!options.start_token.empty()) {
            tok_start = ::range<dht::token>::bound(
                dht::token::from_sstring(options.start_token),
                false);
        }
        if (!options.end_token.empty()) {
            tok_end = ::range<dht::token>::bound(
                dht::token::from_sstring(options.end_token),
                true);
        }
        auto wrange = wrapping_range<dht::token>(tok_start, tok_end);
        dht::token_range_vector given_ranges;
        ::compat::unwrap_into(std::move(wrange), dht::token_comparator(), [&] (dht::token_range&& x) {
            given_ranges.push_back(std::move(x));
        });
        dht::token_range_vector intersections;
        for (const auto& range : ranges) {
            for (const auto& given_range : given_ranges) {
                auto intersection_opt = range.intersection(given_range, dht::token_comparator());
                if (intersection_opt) {
                    intersections.push_back(*intersection_opt);
                }
            }
        }
        ranges = std::move(intersections);
    }

    auto small_table_optimization = options.small_table_optimization;
    if (small_table_optimization) {
        auto range = dht::token_range(dht::token_range::bound(dht::minimum_token(), false), dht::token_range::bound(dht::maximum_token(), false));
        ranges = {range};
        rlogger.info("repair[{}]: Using small table optimization for keyspace={} tables={} range={}", id.uuid(), keyspace, cfs, range);
    }

    auto ranges_parallelism = options.ranges_parallelism == -1 ? std::nullopt : std::optional<int>(options.ranges_parallelism);
    auto task = co_await _repair_module->make_and_start_task<repair::user_requested_repair_task_impl>({}, id, std::move(keyspace), "", germs, std::move(cfs), std::move(ranges), std::move(options.hosts), std::move(options.data_centers), std::move(ignore_nodes), small_table_optimization, ranges_parallelism);
    co_return id.id;
}

future<> repair::user_requested_repair_task_impl::run() {
    auto module = dynamic_pointer_cast<repair::task_manager_module>(_module);
    auto& rs = module->get_repair_service();
    auto& sharded_db = rs.get_db();
    auto& db = sharded_db.local();
    auto id = get_repair_uniq_id();

    return module->run(id, [this, &rs, &db, id, keyspace = _status.keyspace, germs = std::move(_germs),
            &cfs = _cfs, &ranges = _ranges, hosts = std::move(_hosts), data_centers = std::move(_data_centers), ignore_nodes = std::move(_ignore_nodes)] () mutable {
        auto uuid = node_ops_id{id.uuid().uuid()};
        auto start_time = std::chrono::steady_clock::now();

        bool needs_flush_before_repair = false;
        if (db.features().tombstone_gc_options) {
            for (auto& table: cfs) {
                if (const auto* cf = find_column_family_if_exists(db, keyspace, table)) {
                    auto s = cf->schema();
                    const auto& options = s->tombstone_gc_options();
                    if (options.mode() == tombstone_gc_mode::repair) {
                        needs_flush_before_repair = true;
                    }
                }
            }
        }

        bool hints_batchlog_flushed = false;
        std::list<gms::inet_address> participants;
        if (_small_table_optimization) {
            auto normal_nodes = germs->get().get_token_metadata().get_all_ips();
            participants = std::list<gms::inet_address>(normal_nodes.begin(), normal_nodes.end());
        } else {
            participants = get_hosts_participating_in_repair(germs->get(), keyspace, ranges, data_centers, hosts, ignore_nodes).get();
        }
        if (needs_flush_before_repair) {
            auto waiting_nodes = db.get_token_metadata().get_all_ips();
            std::erase_if(waiting_nodes, [&] (const auto& addr) {
                return ignore_nodes.contains(addr);
            });
            auto hints_timeout = std::chrono::seconds(300);
            auto batchlog_timeout = std::chrono::seconds(300);
            repair_flush_hints_batchlog_request req{id.uuid(), participants, hints_timeout, batchlog_timeout};

            try {
                parallel_for_each(waiting_nodes, [&rs, uuid, &req, &participants] (gms::inet_address node) -> future<> {
                    rlogger.info("repair[{}]: Sending repair_flush_hints_batchlog to node={}, participants={}, started",
                            uuid, node, participants);
                    try {
                        auto& ms = rs.get_messaging();
                        auto resp = co_await ser::partition_checksum_rpc_verbs::send_repair_flush_hints_batchlog(&ms, netw::msg_addr(node), req);
                        (void)resp; // nothing to do with response yet
                    } catch (...) {
                        rlogger.warn("repair[{}]: Sending repair_flush_hints_batchlog to node={}, participants={}, failed: {}",
                                uuid, node, participants, std::current_exception());
                        throw;
                    }
                }).get();
                hints_batchlog_flushed = true;
            } catch (...) {
                rlogger.warn("repair[{}]: Sending repair_flush_hints_batchlog to participants={} failed, continue to run repair",
                        uuid, participants);
            }
        } else {
            rlogger.info("repair[{}]: Skipped sending repair_flush_hints_batchlog to nodes={}", uuid, participants);
        }

        std::vector<future<>> repair_results;
        repair_results.reserve(smp::count);
        auto table_ids = get_table_ids(db, keyspace, cfs);
        abort_source as;
        auto off_strategy_updater = seastar::async([&rs, uuid = uuid.uuid(), &table_ids, &participants, &as] {
            auto tables = std::list<table_id>(table_ids.begin(), table_ids.end());
            auto req = node_ops_cmd_request(node_ops_cmd::repair_updater, node_ops_id{uuid}, {}, {}, {}, {}, std::move(tables));
            auto update_interval = std::chrono::seconds(30);
            while (!as.abort_requested()) {
                sleep_abortable(update_interval, as).get();
                parallel_for_each(participants, [&rs, uuid, &req] (gms::inet_address node) {
                    return rs._messaging.send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        rlogger.debug("repair[{}]: Got node_ops_cmd::repair_updater response from node={}", uuid, node);
                    }).handle_exception([uuid, node] (std::exception_ptr ep) {
                        rlogger.warn("repair[{}]: Failed to send node_ops_cmd::repair_updater to node={}", uuid, node);
                    });
                }).get();
            }
        });
        auto stop_off_strategy_updater = defer([uuid, &off_strategy_updater, &as] () mutable noexcept {
            try {
                rlogger.info("repair[{}]: Started to shutdown off-strategy compaction updater", uuid);
                if (!as.abort_requested()) {
                    as.request_abort();
                }
                off_strategy_updater.get();
            } catch (const seastar::sleep_aborted& ignored) {
            } catch (...) {
                rlogger.warn("repair[{}]: Found error in off-strategy compaction updater: {}", uuid, std::current_exception());
            }
            rlogger.info("repair[{}]: Finished to shutdown off-strategy compaction updater", uuid);
        });

        auto cleanup_repair_range_history = defer([&rs, uuid] () mutable {
            try {
                rs.cleanup_history(tasks::task_id{uuid.uuid()}).get();
            } catch (...) {
                rlogger.warn("repair[{}]: Failed to cleanup history: {}", uuid, std::current_exception());
            }
        });

        if (rs.get_repair_module().is_aborted(id.uuid())) {
            throw abort_requested_exception();
        }

        auto ranges_parallelism = _ranges_parallelism;
        bool small_table_optimization = _small_table_optimization;
        for (auto shard : boost::irange(unsigned(0), smp::count)) {
            auto f = rs.container().invoke_on(shard, [keyspace, table_ids, id, ranges, hints_batchlog_flushed, ranges_parallelism, small_table_optimization,
                    data_centers, hosts, ignore_nodes, parent_data = get_repair_uniq_id().task_info, germs] (repair_service& local_repair) mutable -> future<> {
                local_repair.get_metrics().repair_total_ranges_sum += ranges.size();
                auto task = co_await local_repair._repair_module->make_and_start_task<repair::shard_repair_task_impl>(parent_data, tasks::task_id::create_random_id(), keyspace,
                        local_repair, germs->get().shared_from_this(), std::move(ranges), std::move(table_ids),
                        id, std::move(data_centers), std::move(hosts), std::move(ignore_nodes), streaming::stream_reason::repair, hints_batchlog_flushed, small_table_optimization, ranges_parallelism);
                co_await task->done();
            });
            repair_results.push_back(std::move(f));
        }
        when_all(repair_results.begin(), repair_results.end()).then([] (std::vector<future<>> results) mutable {
            std::vector<sstring> errors;
            for (unsigned shard = 0; shard < results.size(); shard++) {
                auto& f = results[shard];
                if (f.failed()) {
                    auto ep = f.get_exception();
                    errors.push_back(format("shard {}: {}", shard, ep));
                }
            }
            if (!errors.empty()) {
                return make_exception_future<>(std::runtime_error(format("{}", errors)));
            }
            return make_ready_future<>();
        }).get();
        auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start_time);
        rlogger.info("repair[{}]: Finished user-requested repair for vnode keyspace={} tables={} repair_id={} duration={}", id.uuid(), keyspace, cfs, id.id, duration);
    }).handle_exception([id, &rs] (std::exception_ptr ep) {
        rlogger.warn("repair[{}]: user-requested repair failed: {}", id.uuid(), ep);
        // If abort was requested, throw abort_requested_exception instead of wrapped exceptions from all shards,
        // so that it could be properly handled by callers.
        rs.get_repair_module().check_in_shutdown();
        return make_exception_future<>(ep);
    });
}

future<std::optional<double>> repair::user_requested_repair_task_impl::expected_total_workload() const {
    co_return _ranges.size() * _cfs.size() * smp::count;
}

std::optional<double> repair::user_requested_repair_task_impl::expected_children_number() const {
    return smp::count;
}

future<int> repair_start(seastar::sharded<repair_service>& repair,
        sstring keyspace, std::unordered_map<sstring, sstring> options) {
    return repair.invoke_on(0, [keyspace = std::move(keyspace), options = std::move(options)] (repair_service& local_repair) {
        return local_repair.do_repair_start(std::move(keyspace), std::move(options));
    });
}

future<std::vector<int>> repair_service::get_active_repairs() {
    return container().invoke_on(0, [] (repair_service& rs) {
        return rs.get_repair_module().get_active();
    });
}

future<repair_status> repair_service::get_status(int id) {
    return container().invoke_on(0, [id] (repair_service& rs) {
        return rs.get_repair_module().get(id);
    });
}

future<repair_status> repair_service::await_completion(int id, std::chrono::steady_clock::time_point timeout) {
    return container().invoke_on(0, [id, timeout] (repair_service& rs) {
        return rs.get_repair_module().repair_await_completion(id, timeout);
    });
}

future<> repair_service::shutdown() {
    co_await remove_repair_meta();
}

future<> repair_service::abort_all() {
    return container().invoke_on_all([] (repair_service& rs) {
        return rs.get_repair_module().abort_all_repairs();
    });
}

future<> repair_service::sync_data_using_repair(
        sstring keyspace,
        locator::effective_replication_map_ptr erm,
        dht::token_range_vector ranges,
        std::unordered_map<dht::token_range, repair_neighbors> neighbors,
        streaming::stream_reason reason,
        shared_ptr<node_ops_info> ops_info) {
    if (ranges.empty()) {
        co_return;
    }

    assert(this_shard_id() == 0);
    auto task = co_await _repair_module->make_and_start_task<repair::data_sync_repair_task_impl>({}, _repair_module->new_repair_uniq_id(), std::move(keyspace), "", std::move(ranges), std::move(neighbors), reason, ops_info);
    co_await task->done();
}

future<> repair::data_sync_repair_task_impl::run() {
    auto module = dynamic_pointer_cast<repair::task_manager_module>(_module);
    auto& rs = module->get_repair_service();
    auto& keyspace = _status.keyspace;
    auto& sharded_db = rs.get_db();
    auto& db = sharded_db.local();
    auto germs = make_lw_shared(co_await locator::make_global_effective_replication_map(sharded_db, keyspace));

    auto id = get_repair_uniq_id();
    rlogger.info("repair[{}]: sync data for keyspace={}, status=started", id.uuid(), keyspace);
    co_await module->run(id, [this, &rs, id, &db, keyspace, germs = std::move(germs), &ranges = _ranges, &neighbors = _neighbors, reason = _reason] () mutable {
        auto cfs = list_column_families(db, keyspace);
        _cfs_size = cfs.size();
        if (cfs.empty()) {
            rlogger.warn("repair[{}]: sync data for keyspace={}, no table in this keyspace", id.uuid(), keyspace);
            return;
        }
        auto table_ids = get_table_ids(db, keyspace, cfs);
        std::vector<future<>> repair_results;
        repair_results.reserve(smp::count);
        if (rs.get_repair_module().is_aborted(id.uuid())) {
            throw abort_requested_exception();
        }
        for (auto shard : boost::irange(unsigned(0), smp::count)) {
            auto f = rs.container().invoke_on(shard, [keyspace, table_ids, id, ranges, neighbors, reason, germs, parent_data = get_repair_uniq_id().task_info] (repair_service& local_repair) mutable -> future<> {
                auto data_centers = std::vector<sstring>();
                auto hosts = std::vector<sstring>();
                auto ignore_nodes = std::unordered_set<gms::inet_address>();
                bool hints_batchlog_flushed = false;
                bool small_table_optimization = false;
                auto ranges_parallelism = std::nullopt;
                auto task_impl_ptr = seastar::make_shared<repair::shard_repair_task_impl>(local_repair._repair_module, tasks::task_id::create_random_id(), keyspace,
                        local_repair, germs->get().shared_from_this(), std::move(ranges), std::move(table_ids),
                        id, std::move(data_centers), std::move(hosts), std::move(ignore_nodes), reason, hints_batchlog_flushed, small_table_optimization, ranges_parallelism);
                task_impl_ptr->neighbors = std::move(neighbors);
                auto task = co_await local_repair._repair_module->make_task(std::move(task_impl_ptr), parent_data);
                task->start();
                co_await task->done();
            });
            repair_results.push_back(std::move(f));
        }
        when_all(repair_results.begin(), repair_results.end()).then([keyspace] (std::vector<future<>> results) mutable {
            std::vector<sstring> errors;
            for (unsigned shard = 0; shard < results.size(); shard++) {
                auto& f = results[shard];
                if (f.failed()) {
                    auto ep = f.get_exception();
                    errors.push_back(format("shard {}: {}", shard, ep));
                }
            }
            if (!errors.empty()) {
                return make_exception_future<>(std::runtime_error(format("{}", errors)));
            }
            return make_ready_future<>();
        }).get();
    }).then([id, keyspace] {
        rlogger.info("repair[{}]: sync data for keyspace={}, status=succeeded", id.uuid(), keyspace);
    }).handle_exception([&db, id, keyspace, &rs] (std::exception_ptr ep) {
        if (!db.has_keyspace(keyspace)) {
            rlogger.warn("repair[{}]: sync data for keyspace={}, status=failed: keyspace does not exist any more, ignoring it, {}", id.uuid(), keyspace, ep);
            return make_ready_future<>();
        }
        rlogger.warn("repair[{}]: sync data for keyspace={}, status=failed: {}", id.uuid(), keyspace,  ep);
        // If abort was requested, throw abort_requested_exception instead of wrapped exceptions from all shards,
        // so that it could be properly handled by callers.
        rs.get_repair_module().check_in_shutdown();
        return make_exception_future<>(ep);
    });
}

future<std::optional<double>> repair::data_sync_repair_task_impl::expected_total_workload() const {
    co_return _cfs_size ? std::make_optional<double>(_ranges.size() * _cfs_size * smp::count) : std::nullopt;
}

std::optional<double> repair::data_sync_repair_task_impl::expected_children_number() const {
    return smp::count;
}

future<> repair_service::bootstrap_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> bootstrap_tokens) {
    assert(this_shard_id() == 0);
    using inet_address = gms::inet_address;
    return seastar::async([this, tmptr = std::move(tmptr), tokens = std::move(bootstrap_tokens)] () mutable {
        auto& db = get_db().local();
        auto ks_erms = db.get_non_local_strategy_keyspaces_erms();
        auto& topology = tmptr->get_topology();
        auto myloc = topology.get_location();
        auto myid = tmptr->get_my_id();
        auto reason = streaming::stream_reason::bootstrap;
        // Calculate number of ranges to sync data
        size_t nr_ranges_total = 0;
        for (const auto& [keyspace_name, erm] : ks_erms) {
            if (!db.has_keyspace(keyspace_name)) {
                continue;
            }
            auto& strat = erm->get_replication_strategy();
            dht::token_range_vector desired_ranges = strat.get_pending_address_ranges(tmptr, tokens, myid, myloc).get();
            seastar::thread::maybe_yield();
            auto nr_tables = get_nr_tables(db, keyspace_name);
            nr_ranges_total += desired_ranges.size() * nr_tables;
        }
        container().invoke_on_all([nr_ranges_total] (repair_service& rs) {
            rs.get_metrics().bootstrap_finished_ranges = 0;
            rs.get_metrics().bootstrap_total_ranges = nr_ranges_total;
        }).get();
        rlogger.info("bootstrap_with_repair: started with keyspaces={}, nr_ranges_total={}", ks_erms | boost::adaptors::map_keys, nr_ranges_total);
        for (const auto& [keyspace_name, erm] : ks_erms) {
            if (!db.has_keyspace(keyspace_name)) {
                rlogger.info("bootstrap_with_repair: keyspace={} does not exist any more, ignoring it", keyspace_name);
                continue;
            }
            auto& strat = erm->get_replication_strategy();
            dht::token_range_vector desired_ranges = strat.get_pending_address_ranges(tmptr, tokens, myid, myloc).get();
            bool find_node_in_local_dc_only = strat.get_type() == locator::replication_strategy_type::network_topology;
            bool everywhere_topology = strat.get_type() == locator::replication_strategy_type::everywhere_topology;
            auto replication_factor = erm->get_replication_factor();

            //Active ranges
            auto metadata_clone = tmptr->clone_only_token_map().get();
            auto range_addresses = strat.get_range_addresses(metadata_clone).get();

            //Pending ranges
            metadata_clone.update_topology(myid, myloc, locator::node::state::bootstrapping);
            metadata_clone.update_normal_tokens(tokens, myid).get();
            auto pending_range_addresses = strat.get_range_addresses(metadata_clone).get();
            metadata_clone.clear_gently().get();

            //Collects the source that will have its range moved to the new node
            std::unordered_map<dht::token_range, repair_neighbors> range_sources;

            auto nr_tables = get_nr_tables(db, keyspace_name);
            rlogger.info("bootstrap_with_repair: started with keyspace={}, nr_ranges={}", keyspace_name, desired_ranges.size() * nr_tables);
            for (auto& desired_range : desired_ranges) {
                for (auto& x : range_addresses) {
                    const range<dht::token>& src_range = x.first;
                    seastar::thread::maybe_yield();
                    if (src_range.contains(desired_range, dht::operator<=>)) {
                        std::vector<inet_address> old_endpoints(x.second.begin(), x.second.end());
                        auto it = pending_range_addresses.find(desired_range);
                        if (it == pending_range_addresses.end()) {
                            throw std::runtime_error(format("Can not find desired_range = {} in pending_range_addresses", desired_range));
                        }

                        std::unordered_set<inet_address> new_endpoints(it->second.begin(), it->second.end());
                        rlogger.debug("bootstrap_with_repair: keyspace={}, range={}, old_endpoints={}, new_endpoints={}",
                                keyspace_name, desired_range, old_endpoints, new_endpoints);
                        // Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                        // So we need to be careful to only be strict when endpoints == RF
                        // That is we can only have RF >= old_endpoints.size().
                        // 1) If RF > old_endpoints.size(), it means no node is
                        // going to lose the ownership of the range, so there
                        // is no need to choose a mandatory neighbor to sync
                        // data from.
                        // 2) If RF = old_endpoints.size(), it means one node is
                        // going to lose the ownership of the range, we need to
                        // choose it as the mandatory neighbor to sync data
                        // from.
                        std::vector<gms::inet_address> mandatory_neighbors;
                        // All neighbors
                        std::vector<inet_address> neighbors;
                        auto get_node_losing_the_ranges = [&, &keyspace_name = keyspace_name] (const std::vector<gms::inet_address>& old_nodes, const std::unordered_set<gms::inet_address>& new_nodes) {
                            // Remove the new nodes from the old nodes list, so
                            // that it contains only the node that will lose
                            // the ownership of the range.
                            auto nodes = boost::copy_range<std::vector<gms::inet_address>>(old_nodes |
                                    boost::adaptors::filtered([&] (const gms::inet_address& node) { return !new_nodes.contains(node); }));
                            if (nodes.size() != 1) {
                                throw std::runtime_error(format("bootstrap_with_repair: keyspace={}, range={}, expected 1 node losing range but found {} nodes={}",
                                        keyspace_name, desired_range, nodes.size(), nodes));
                            }
                            return nodes;
                        };
                        auto get_rf_in_local_dc = [&, &keyspace_name = keyspace_name] () {
                            size_t rf_in_local_dc = replication_factor;
                            if (strat.get_type() == locator::replication_strategy_type::network_topology) {
                                auto nts = dynamic_cast<const locator::network_topology_strategy*>(&strat);
                                if (!nts) {
                                    throw std::runtime_error(format("bootstrap_with_repair: keyspace={}, range={}, failed to cast to network_topology_strategy",
                                            keyspace_name, desired_range));
                                }
                                rf_in_local_dc = nts->get_replication_factor(myloc.dc);
                            }
                            return rf_in_local_dc;
                        };
                        auto get_old_endpoints_in_local_dc = [&] () {
                            return boost::copy_range<std::vector<gms::inet_address>>(old_endpoints |
                                boost::adaptors::filtered([&] (const gms::inet_address& node) {
                                    return topology.get_datacenter(node) == myloc.dc;
                                })
                            );
                        };
                        auto old_endpoints_in_local_dc = get_old_endpoints_in_local_dc();
                        auto rf_in_local_dc = get_rf_in_local_dc();
                        if (everywhere_topology) {
                            neighbors = old_endpoints_in_local_dc.empty() ? old_endpoints : old_endpoints_in_local_dc;
                            rlogger.debug("bootstrap_with_repair: keyspace={}, range={}, old_endpoints={}, new_endpoints={}, old_endpoints_in_local_dc={}, neighbors={}",
                                    keyspace_name, desired_range, old_endpoints, new_endpoints, old_endpoints_in_local_dc, neighbors);
                        } else if (old_endpoints.size() == replication_factor) {
                            // For example, with RF = 3 and 3 nodes n1, n2, n3
                            // in the cluster, n4 is bootstrapped, old_replicas
                            // = {n1, n2, n3}, new_replicas = {n1, n2, n4}, n3
                            // is losing the range. Choose the bootstrapping
                            // node n4 to run repair to sync with the node
                            // losing the range n3
                            mandatory_neighbors = get_node_losing_the_ranges(old_endpoints, new_endpoints);
                            neighbors = mandatory_neighbors;
                        } else if (old_endpoints.size() < replication_factor) {
                          if (!find_node_in_local_dc_only) {
                            neighbors = old_endpoints;
                          } else {
                            if (old_endpoints_in_local_dc.size() == rf_in_local_dc) {
                                // Local DC has enough replica nodes.
                                mandatory_neighbors = get_node_losing_the_ranges(old_endpoints_in_local_dc, new_endpoints);
                                neighbors = mandatory_neighbors;
                            } else if (old_endpoints_in_local_dc.size() == 0) {
                                // Local DC has zero replica node.
                                // Reject the operation
                                throw std::runtime_error(format("bootstrap_with_repair: keyspace={}, range={}, no existing node in local dc",
                                        keyspace_name, desired_range));
                            } else if (old_endpoints_in_local_dc.size() < rf_in_local_dc) {
                                // Local DC does not have enough replica nodes.
                                // For example, with RF = 3, 2 nodes n1, n2 in the
                                // cluster, n3 is bootstrapped, old_replicas={n1, n2},
                                // new_replicas={n1, n2, n3}. No node is losing
                                // ranges. The bootstrapping node n3 has to sync
                                // with n1 and n2, otherwise n3 might get the old
                                // replica and make the total old replica be 2,
                                // which makes the quorum read incorrect.
                                // Choose the bootstrapping node n3 to reun repair
                                // to sync with n1 and n2.
                                size_t nr_nodes_to_sync_with = std::min<size_t>(rf_in_local_dc / 2 + 1, old_endpoints_in_local_dc.size());
                                neighbors = old_endpoints_in_local_dc;
                                neighbors.resize(nr_nodes_to_sync_with);
                            } else {
                                throw std::runtime_error(format("bootstrap_with_repair: keyspace={}, range={}, wrong number of old_endpoints_in_local_dc={}, rf_in_local_dc={}",
                                        keyspace_name, desired_range, old_endpoints_in_local_dc.size(), rf_in_local_dc));
                            }
                          }
                        } else {
                            throw std::runtime_error(format("bootstrap_with_repair: keyspace={}, range={}, wrong number of old_endpoints={}, rf={}",
                                        keyspace_name, desired_range, old_endpoints, replication_factor));
                        }
                        rlogger.debug("bootstrap_with_repair: keyspace={}, range={}, neighbors={}, mandatory_neighbors={}",
                                keyspace_name, desired_range, neighbors, mandatory_neighbors);
                        range_sources[desired_range] = repair_neighbors(std::move(neighbors), std::move(mandatory_neighbors));
                    }
                }
            }
            auto nr_ranges = desired_ranges.size();
            sync_data_using_repair(keyspace_name, erm, std::move(desired_ranges), std::move(range_sources), reason, nullptr).get();
            rlogger.info("bootstrap_with_repair: finished with keyspace={}, nr_ranges={}", keyspace_name, nr_ranges);
        }
        rlogger.info("bootstrap_with_repair: finished with keyspaces={}", ks_erms | boost::adaptors::map_keys);
    });
}

future<> repair_service::do_decommission_removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops) {
    assert(this_shard_id() == 0);
    using inet_address = gms::inet_address;
    return seastar::async([this, tmptr = std::move(tmptr), leaving_node = std::move(leaving_node), ops] () mutable {
        auto& db = get_db().local();
        auto& topology = tmptr->get_topology();
        auto myip = topology.my_address();
        const auto leaving_node_id = tmptr->get_host_id(leaving_node);
        auto ks_erms = db.get_non_local_strategy_keyspaces_erms();
        auto local_dc = topology.get_datacenter();
        bool is_removenode = myip != leaving_node;
        auto op = is_removenode ? "removenode_with_repair" : "decommission_with_repair";
        streaming::stream_reason reason = is_removenode ? streaming::stream_reason::removenode : streaming::stream_reason::decommission;
        size_t nr_ranges_total = 0;
        for (const auto& [keyspace_name, erm] : ks_erms) {
            dht::token_range_vector ranges = erm->get_ranges(leaving_node);
            auto nr_tables = get_nr_tables(db, keyspace_name);
            nr_ranges_total += ranges.size() * nr_tables;
        }
        if (reason == streaming::stream_reason::decommission) {
            container().invoke_on_all([nr_ranges_total] (repair_service& rs) {
                rs.get_metrics().decommission_finished_ranges = 0;
                rs.get_metrics().decommission_total_ranges = nr_ranges_total;
            }).get();
        } else if (reason == streaming::stream_reason::removenode) {
            container().invoke_on_all([nr_ranges_total] (repair_service& rs) {
                rs.get_metrics().removenode_finished_ranges = 0;
                rs.get_metrics().removenode_total_ranges = nr_ranges_total;
            }).get();
        }
        auto get_ignore_nodes = [ops] () -> std::list<gms::inet_address>& {
            static std::list<gms::inet_address> no_ignore_nodes;
            return ops ? ops->ignore_nodes : no_ignore_nodes;
        };
        rlogger.info("{}: started with keyspaces={}, leaving_node={}, ignore_nodes={}", op, ks_erms | boost::adaptors::map_keys, leaving_node, get_ignore_nodes());
        for (const auto& [keyspace_name, erm] : ks_erms) {
            if (!db.has_keyspace(keyspace_name)) {
                rlogger.info("{}: keyspace={} does not exist any more, ignoring it", op, keyspace_name);
                continue;
            }
            auto& strat = erm->get_replication_strategy();
            // First get all ranges the leaving node is responsible for
            dht::token_range_vector ranges = erm->get_ranges(leaving_node);
            auto nr_tables = get_nr_tables(db, keyspace_name);
            rlogger.info("{}: started with keyspace={}, leaving_node={}, nr_ranges={}", op, keyspace_name, leaving_node, ranges.size() * nr_tables);
            size_t nr_ranges_total = ranges.size() * nr_tables;
            size_t nr_ranges_skipped = 0;
            std::unordered_map<dht::token_range, locator::endpoint_set> current_replica_endpoints;
            // Find (for each range) all nodes that store replicas for these ranges as well
            for (auto& r : ranges) {
                auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
                auto eps = strat.calculate_natural_ips(end_token, *tmptr).get();
                current_replica_endpoints.emplace(r, std::move(eps));
                seastar::thread::maybe_yield();
            }
            auto temp = tmptr->clone_after_all_left().get();
            // leaving_node might or might not be 'leaving'. If it was not leaving (that is, removenode
            // command was used), it is still present in temp and must be removed.
            if (temp.is_normal_token_owner(leaving_node_id)) {
                temp.remove_endpoint(leaving_node_id);
            }
            std::unordered_map<dht::token_range, repair_neighbors> range_sources;
            dht::token_range_vector ranges_for_removenode;
            bool find_node_in_local_dc_only = strat.get_type() == locator::replication_strategy_type::network_topology;
            for (auto&r : ranges) {
                seastar::thread::maybe_yield();
                if (ops) {
                    ops->check_abort();
                }
                auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
                const auto new_eps = strat.calculate_natural_ips(end_token, temp).get();
                const auto& current_eps = current_replica_endpoints[r];
                std::unordered_set<inet_address> neighbors_set = new_eps.get_set();
                bool skip_this_range = false;
                auto new_owner = neighbors_set;
                for (const auto& node : current_eps) {
                    new_owner.erase(node);
                }
                if (new_eps.size() == 0) {
                    throw std::runtime_error(format("{}: keyspace={}, range={}, current_replica_endpoints={}, new_replica_endpoints={}, zero replica after the removal",
                            op, keyspace_name, r, current_eps, new_eps));
                }
                auto get_neighbors_set = [&, &keyspace_name = keyspace_name] (const std::vector<inet_address>& nodes) {
                    for (auto& node : nodes) {
                        if (topology.get_datacenter(node) == local_dc) {
                            return std::unordered_set<inet_address>{node};;
                        }
                    }
                    if (find_node_in_local_dc_only) {
                        throw std::runtime_error(format("{}: keyspace={}, range={}, current_replica_endpoints={}, new_replica_endpoints={}, can not find new node in local dc={}",
                                op, keyspace_name, r, current_eps, new_eps, local_dc));
                    } else {
                        return std::unordered_set<inet_address>{nodes.front()};
                    }
                };
                if (new_owner.size() == 1) {
                    // For example, with RF = 3 and 4 nodes n1, n2, n3, n4 in
                    // the cluster, n3 is removed, old_replicas = {n1, n2, n3},
                    // new_replicas = {n1, n2, n4}.
                    if (is_removenode) {
                        // For removenode operation, use new owner node to
                        // repair the data in case there is new owner node
                        // available. Nodes that are not the new owner of a
                        // range will ignore the range. There can be at most
                        // one new owner for each of the ranges. Note: The new
                        // owner is the node that does not own the range before
                        // but owns the range after the remove operation. It
                        // does not have to be the primary replica of the
                        // range.
                        // Choose the new owner node n4 to run repair to sync
                        // with all replicas.
                        skip_this_range = *new_owner.begin() != myip;
                        neighbors_set.insert(current_eps.begin(), current_eps.end());
                    } else {
                        // For decommission operation, the decommission node
                        // will repair all the ranges the leaving node is
                        // responsible for.
                        // Choose the decommission node n3 to run repair to
                        // sync with the new owner node n4.
                        neighbors_set = get_neighbors_set(std::vector<inet_address>{*new_owner.begin()});
                    }
                } else if (new_owner.size() == 0) {
                    // For example, with RF = 3 and 3 nodes n1, n2, n3 in the
                    // cluster, n3 is removed, old_replicas = {n1, n2, n3},
                    // new_replicas = {n1, n2}.
                    if (is_removenode) {
                        // For removenode operation, use the primary replica
                        // node to repair the data in case there is no new
                        // owner node available.
                        // Choose the primary replica node n1 to run repair to
                        // sync with all replicas.
                        skip_this_range = new_eps.front() != myip;
                        neighbors_set.insert(current_eps.begin(), current_eps.end());
                    } else {
                        // For decommission operation, the decommission node
                        // will repair all the ranges the leaving node is
                        // responsible for. We are losing data on n3, we have
                        // to sync with at least one of {n1, n2}, otherwise we
                        // might lose the only new replica on n3.
                        // Choose the decommission node n3 to run repair to
                        // sync with one of the replica nodes, e.g., n1, in the
                        // local DC.
                        neighbors_set = get_neighbors_set(boost::copy_range<std::vector<inet_address>>(new_eps));
                    }
                } else {
                    throw std::runtime_error(format("{}: keyspace={}, range={}, current_replica_endpoints={}, new_replica_endpoints={}, wrong number of new owner node={}",
                            op, keyspace_name, r, current_eps, new_eps, new_owner));
                }
                neighbors_set.erase(myip);
                neighbors_set.erase(leaving_node);
                // Remove nodes in ignore_nodes
                for (const auto& node : get_ignore_nodes()) {
                    neighbors_set.erase(node);
                }
                auto neighbors = boost::copy_range<std::vector<gms::inet_address>>(neighbors_set |
                    boost::adaptors::filtered([&local_dc, &topology] (const gms::inet_address& node) {
                        return topology.get_datacenter(node) == local_dc;
                    })
                );

                if (skip_this_range) {
                    nr_ranges_skipped++;
                    rlogger.debug("{}: keyspace={}, range={}, current_replica_endpoints={}, new_replica_endpoints={}, neighbors={}, skipped",
                        op, keyspace_name, r, current_eps, new_eps, neighbors);
                } else {
                    std::vector<gms::inet_address> mandatory_neighbors = is_removenode ? neighbors : std::vector<gms::inet_address>{};
                    rlogger.info("{}: keyspace={}, range={}, current_replica_endpoints={}, new_replica_endpoints={}, neighbors={}, mandatory_neighbor={}",
                            op, keyspace_name, r, current_eps, new_eps, neighbors, mandatory_neighbors);
                    range_sources[r] = repair_neighbors(std::move(neighbors), std::move(mandatory_neighbors));
                    if (is_removenode) {
                        ranges_for_removenode.push_back(r);
                    }
                }
            }
            temp.clear_gently().get();
            if (reason == streaming::stream_reason::decommission) {
                container().invoke_on_all([nr_ranges_skipped] (repair_service& rs) {
                    rs.get_metrics().decommission_finished_ranges += nr_ranges_skipped;
                }).get();
            } else if (reason == streaming::stream_reason::removenode) {
                container().invoke_on_all([nr_ranges_skipped] (repair_service& rs) {
                    rs.get_metrics().removenode_finished_ranges += nr_ranges_skipped;
                }).get();
            }
            if (is_removenode) {
                ranges.swap(ranges_for_removenode);
            }
            auto nr_ranges_synced = ranges.size();
            sync_data_using_repair(keyspace_name, erm, std::move(ranges), std::move(range_sources), reason, ops).get();
            rlogger.info("{}: finished with keyspace={}, leaving_node={}, nr_ranges={}, nr_ranges_synced={}, nr_ranges_skipped={}",
                op, keyspace_name, leaving_node, nr_ranges_total, nr_ranges_synced, nr_ranges_skipped);
        }
        rlogger.info("{}: finished with keyspaces={}, leaving_node={}", op, ks_erms | boost::adaptors::map_keys, leaving_node);
    });
}

future<> repair_service::decommission_with_repair(locator::token_metadata_ptr tmptr) {
    assert(this_shard_id() == 0);
    auto my_address = tmptr->get_topology().my_address();
    return do_decommission_removenode_with_repair(std::move(tmptr), my_address, {});
}

future<> repair_service::removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops) {
    assert(this_shard_id() == 0);
    return do_decommission_removenode_with_repair(std::move(tmptr), std::move(leaving_node), std::move(ops)).then([this] {
        rlogger.debug("Triggering off-strategy compaction for all non-system tables on removenode completion");
        seastar::sharded<replica::database>& db = get_db();
        return db.invoke_on_all([](replica::database &db) {
            for (auto& table : db.get_non_system_column_families()) {
                table->trigger_offstrategy_compaction();
            }
        });
    });
}

future<> repair_service::do_rebuild_replace_with_repair(locator::token_metadata_ptr tmptr, sstring op, sstring source_dc, streaming::stream_reason reason, std::unordered_set<gms::inet_address> ignore_nodes) {
    assert(this_shard_id() == 0);
    return seastar::async([this, tmptr = std::move(tmptr), source_dc = std::move(source_dc), op = std::move(op), reason, ignore_nodes = std::move(ignore_nodes)] () mutable {
        auto& db = get_db().local();
        auto ks_erms = db.get_non_local_strategy_keyspaces_erms();
        auto myip = tmptr->get_topology().my_address();
        auto myid = tmptr->get_my_id();
        size_t nr_ranges_total = 0;
        for (const auto& [keyspace_name, erm] : ks_erms) {
            if (!db.has_keyspace(keyspace_name)) {
                continue;
            }
            auto& strat = erm->get_replication_strategy();
            // Okay to yield since tm is immutable
            dht::token_range_vector ranges = strat.get_ranges(myid, tmptr).get();
            auto nr_tables = get_nr_tables(db, keyspace_name);
            nr_ranges_total += ranges.size() * nr_tables;

        }
        if (reason == streaming::stream_reason::rebuild) {
            container().invoke_on_all([nr_ranges_total] (repair_service& rs) {
                rs.get_metrics().rebuild_finished_ranges = 0;
                rs.get_metrics().rebuild_total_ranges = nr_ranges_total;
            }).get();
        } else if (reason == streaming::stream_reason::replace) {
            container().invoke_on_all([nr_ranges_total] (repair_service& rs) {
                rs.get_metrics().replace_finished_ranges = 0;
                rs.get_metrics().replace_total_ranges = nr_ranges_total;
            }).get();
        }
        rlogger.info("{}: started with keyspaces={}, source_dc={}, nr_ranges_total={}, ignore_nodes={}", op, ks_erms | boost::adaptors::map_keys, source_dc, nr_ranges_total, ignore_nodes);
        for (const auto& [keyspace_name, erm] : ks_erms) {
            size_t nr_ranges_skipped = 0;
            if (!db.has_keyspace(keyspace_name)) {
                rlogger.info("{}: keyspace={} does not exist any more, ignoring it", op, keyspace_name);
                continue;
            }
            auto& strat = erm->get_replication_strategy();
            dht::token_range_vector ranges = strat.get_ranges(myid, *tmptr).get();
            auto& topology = erm->get_token_metadata().get_topology();
            std::unordered_map<dht::token_range, repair_neighbors> range_sources;
            auto nr_tables = get_nr_tables(db, keyspace_name);
            rlogger.info("{}: started with keyspace={}, source_dc={}, nr_ranges={}, ignore_nodes={}", op, keyspace_name, source_dc, ranges.size() * nr_tables, ignore_nodes);
            for (auto it = ranges.begin(); it != ranges.end();) {
                auto& r = *it;
                seastar::thread::maybe_yield();
                auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
                auto neighbors = boost::copy_range<std::vector<gms::inet_address>>(strat.calculate_natural_ips(end_token, *tmptr).get() |
                    boost::adaptors::filtered([myip, &source_dc, &topology, &ignore_nodes] (const gms::inet_address& node) {
                        if (node == myip) {
                            return false;
                        }
                        if (ignore_nodes.contains(node)) {
                            return false;
                        }
                        return source_dc.empty() ? true : topology.get_datacenter(node) == source_dc;
                    })
                );
                rlogger.debug("{}: keyspace={}, range={}, neighbors={}", op, keyspace_name, r, neighbors);
                if (!neighbors.empty()) {
                    range_sources[r] = repair_neighbors(std::move(neighbors));
                    ++it;
                } else {
                    // Skip the range with zero neighbors
                    it = ranges.erase(it);
                    nr_ranges_skipped++;
                }
            }
            if (reason == streaming::stream_reason::rebuild) {
                container().invoke_on_all([nr_ranges_skipped, nr_tables] (repair_service& rs) {
                    rs.get_metrics().rebuild_finished_ranges += nr_ranges_skipped * nr_tables;
                }).get();
            } else if (reason == streaming::stream_reason::replace) {
                container().invoke_on_all([nr_ranges_skipped, nr_tables] (repair_service& rs) {
                    rs.get_metrics().replace_finished_ranges += nr_ranges_skipped * nr_tables;
                }).get();
            }
            auto nr_ranges = ranges.size();
            sync_data_using_repair(keyspace_name, erm, std::move(ranges), std::move(range_sources), reason, nullptr).get();
            rlogger.info("{}: finished with keyspace={}, source_dc={}, nr_ranges={}", op, keyspace_name, source_dc, nr_ranges);
        }
        rlogger.info("{}: finished with keyspaces={}, source_dc={}", op, ks_erms | boost::adaptors::map_keys, source_dc);
    });
}

future<> repair_service::rebuild_with_repair(locator::token_metadata_ptr tmptr, sstring source_dc) {
    assert(this_shard_id() == 0);
    auto op = sstring("rebuild_with_repair");
    if (source_dc.empty()) {
        auto& topology = tmptr->get_topology();
        source_dc = topology.get_datacenter();
    }
    auto reason = streaming::stream_reason::rebuild;
    co_await do_rebuild_replace_with_repair(std::move(tmptr), std::move(op), std::move(source_dc), reason, {});
    co_await get_db().invoke_on_all([](replica::database& db) {
        for (auto& t : db.get_non_system_column_families()) {
            t->trigger_offstrategy_compaction();
        }
    });
}

future<> repair_service::replace_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> replacing_tokens, std::unordered_set<gms::inet_address> ignore_nodes) {
    assert(this_shard_id() == 0);
    auto cloned_tm = co_await tmptr->clone_async();
    auto op = sstring("replace_with_repair");
    auto& topology = tmptr->get_topology();
    auto myloc = topology.get_location();
    auto reason = streaming::stream_reason::replace;
    // update a cloned version of tmptr
    // no need to set the original version
    auto cloned_tmptr = make_token_metadata_ptr(std::move(cloned_tm));
    cloned_tmptr->update_topology(tmptr->get_my_id(), myloc, locator::node::state::replacing);
    co_await cloned_tmptr->update_normal_tokens(replacing_tokens, tmptr->get_my_id());
    co_return co_await do_rebuild_replace_with_repair(std::move(cloned_tmptr), std::move(op), myloc.dc, reason, std::move(ignore_nodes));
}

// Repair all tablets belong to this node for the given table
future<> repair_service::repair_tablets(repair_uniq_id rid, sstring keyspace_name, std::vector<sstring> table_names, host2ip_t host2ip, bool primary_replica_only) {
    std::vector<tablet_repair_task_meta> task_metas;
    for (auto& table_name : table_names) {
        lw_shared_ptr<replica::table> t;
        try {
            t = _db.local().find_column_family(keyspace_name, table_name).shared_from_this();
        } catch (replica::no_such_column_family& e) {
            rlogger.debug("repair[{}] Table {}.{} does not exist anymore", rid.uuid(), keyspace_name, table_name);
            continue;
        }
        if (!t->uses_tablets()) {
            throw std::runtime_error(format("repair[{}] Table {}.{} is not a tablet table", rid.uuid(), keyspace_name, table_name));
        }
        table_id tid = t->schema()->id();
        // FIXME: we need to wait for current tablet movement and disable future tablet movement
        auto erm = t->get_effective_replication_map();
        auto& tmap = erm->get_token_metadata_ptr()->tablets().get_tablet_map(tid);
        struct repair_tablet_meta {
            locator::tablet_id id;
            dht::token_range range;
            locator::host_id master_host_id;
            shard_id master_shard_id;
            locator::tablet_replica_set replicas;
        };
        std::vector<repair_tablet_meta> metas;
        auto myhostid = erm->get_token_metadata_ptr()->get_my_id();
        auto myip = erm->get_topology().my_address();
        co_await tmap.for_each_tablet([&] (locator::tablet_id id, const locator::tablet_info& info) -> future<> {
            auto range = tmap.get_token_range(id);
            auto& replicas = info.replicas;
            bool found = false;
            shard_id master_shard_id;
            // Repair all tablets belong to this node
            for (auto& r : replicas) {
                if (r.host == myhostid) {
                    master_shard_id = r.shard;
                    found = true;
                    break;
                }
                if (primary_replica_only) {
                    break;
                }
            }
            if (found) {
                metas.push_back(repair_tablet_meta{id, range, myhostid, master_shard_id, replicas});
            }
            return make_ready_future<>();
        });

        size_t nr = 0;
        for (auto& m : metas) {
            nr++;
            rlogger.debug("repair[{}] Collect {} out of {} tablets: table={}.{} tablet_id={} range={} replicas={} primary_replica_only={}",
                rid.uuid(), nr, metas.size(), keyspace_name, table_name, m.id, m.range, m.replicas, primary_replica_only);
            std::vector<gms::inet_address> nodes;
            auto master_shard_id = m.master_shard_id;
            auto range = m.range;
            std::vector<shard_id> shards;
            for (auto& r : m.replicas) {
                auto shard = r.shard;
                auto ip = co_await host2ip(r.host);
                if (r.host != myhostid) {
                    rlogger.debug("repair[{}] Repair get neighbors table={}.{} hostid={} shard={} ip={} myip={} myhostid={}",
                            rid.uuid(), keyspace_name, table_name, r.host, shard, ip, myip, myhostid);
                    shards.push_back(shard);
                    nodes.push_back(ip);
                }
            }
            task_metas.push_back(tablet_repair_task_meta{keyspace_name, table_name, tid, master_shard_id, range, repair_neighbors(nodes, shards), m.replicas});
        }
    }
    auto task = co_await _repair_module->make_and_start_task<repair::tablet_repair_task_impl>({}, rid, keyspace_name, table_names, streaming::stream_reason::repair, std::move(task_metas));
}

future<> repair::tablet_repair_task_impl::run() {
    auto m = dynamic_pointer_cast<repair::task_manager_module>(_module);
    auto& rs = m->get_repair_service();
    auto& sharded_db = rs.get_db();
    auto& db = sharded_db.local();
    auto id = get_repair_uniq_id();
    auto keyspace = _keyspace;
    rlogger.debug("repair[{}]: Repair tablet for keyspace={} tables={} status=started", id.uuid(), _keyspace, _tables);
    co_await m->run(id, [this, &rs, id] () mutable {
        // This runs inside a seastar thread
        auto start_time = std::chrono::steady_clock::now();
        auto parent_data = get_repair_uniq_id().task_info;
        std::atomic<int> idx{1};
        rs.container().invoke_on_all([&idx, id, metas = _metas, parent_data, reason = _reason] (repair_service& rs) -> future<> {
            for (auto& m : metas) {
                if (m.master_shard_id != this_shard_id()) {
                    continue;
                }
                auto nr = idx.fetch_add(1);
                rlogger.info("repair[{}] Repair {} out of {} tablets: table={}.{} range={} replicas={}",
                    id.uuid(), nr, metas.size(), m.keyspace_name, m.table_name, m.range, m.replicas);
                lw_shared_ptr<replica::table> t;
                try {
                    t = rs._db.local().find_column_family(m.tid).shared_from_this();
                } catch (replica::no_such_column_family& e) {
                    rlogger.debug("repair[{}] Table {}.{} does not exist anymore", id.uuid(), m.keyspace_name, m.table_name);
                    continue;
                }
                auto erm = t->get_effective_replication_map();
                if (rs.get_repair_module().is_aborted(id.uuid())) {
                    throw abort_requested_exception();
                }

                std::unordered_map<dht::token_range, repair_neighbors> neighbors;
                neighbors[m.range] = m.neighbors;
                dht::token_range_vector ranges = {m.range};
                std::vector<table_id> table_ids = {m.tid};

                auto data_centers = std::vector<sstring>();
                auto hosts = std::vector<sstring>();
                auto ignore_nodes = std::unordered_set<gms::inet_address>();
                bool hints_batchlog_flushed = false;
                bool small_table_optimization = false;
                auto ranges_parallelism = std::nullopt;

                auto task_impl_ptr = seastar::make_shared<repair::shard_repair_task_impl>(rs._repair_module, tasks::task_id::create_random_id(),
                        m.keyspace_name, rs, erm, std::move(ranges), std::move(table_ids), id, std::move(data_centers), std::move(hosts),
                        std::move(ignore_nodes), reason, hints_batchlog_flushed, small_table_optimization, ranges_parallelism);
                task_impl_ptr->neighbors = std::move(neighbors);
                auto task = co_await rs._repair_module->make_task(std::move(task_impl_ptr), parent_data);
                task->start();
                co_await task->done();
            }
        }).get();
        auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start_time);
        rlogger.info("repair[{}]: Finished user-requested repair for tablet keyspace={} tables={} repair_id={} tablets_repaired={} duration={}",
                id.uuid(), _keyspace, _tables, id.id, _metas.size(), duration);
    }).then([id, keyspace] {
        rlogger.debug("repair[{}]: Repair tablet for keyspace={} status=succeeded", id.uuid(), keyspace);
    }).handle_exception([&db, id, keyspace, &rs] (std::exception_ptr ep) {
        if (!db.has_keyspace(keyspace)) {
            rlogger.warn("repair[{}]: Repair tablet for keyspace={}, status=failed: keyspace does not exist any more, ignoring it, {}", id.uuid(), keyspace, ep);
            return make_ready_future<>();
        }
        rlogger.warn("repair[{}]: Repair tablet for keyspace={} status=failed: {}", id.uuid(), keyspace,  ep);
        rs.get_repair_module().check_in_shutdown();
        return make_exception_future<>(ep);
    });
}

future<std::optional<double>> repair::tablet_repair_task_impl::expected_total_workload() const {
    auto sz = _metas.size();
    co_return sz ? std::make_optional<double>(sz) : std::nullopt;
}

std::optional<double> repair::tablet_repair_task_impl::expected_children_number() const {
    return _metas.size();
}

node_ops_cmd_category categorize_node_ops_cmd(node_ops_cmd cmd) noexcept {
    switch (cmd) {
    case node_ops_cmd::removenode_prepare:
    case node_ops_cmd::replace_prepare:
    case node_ops_cmd::decommission_prepare:
    case node_ops_cmd::bootstrap_prepare:
        return node_ops_cmd_category::prepare;

    case node_ops_cmd::removenode_heartbeat:
    case node_ops_cmd::replace_heartbeat:
    case node_ops_cmd::decommission_heartbeat:
    case node_ops_cmd::bootstrap_heartbeat:
        return node_ops_cmd_category::heartbeat;

    case node_ops_cmd::removenode_sync_data:
        return node_ops_cmd_category::sync_data;

    case node_ops_cmd::removenode_abort:
    case node_ops_cmd::replace_abort:
    case node_ops_cmd::decommission_abort:
    case node_ops_cmd::bootstrap_abort:
        return node_ops_cmd_category::abort;

    case node_ops_cmd::removenode_done:
    case node_ops_cmd::replace_done:
    case node_ops_cmd::decommission_done:
    case node_ops_cmd::bootstrap_done:
        return node_ops_cmd_category::done;

    default:
        return node_ops_cmd_category::other;
    }
}

std::ostream& operator<<(std::ostream& out, node_ops_cmd cmd) {
    switch (cmd) {
        case node_ops_cmd::removenode_prepare:
            return out << "removenode_prepare";
        case node_ops_cmd::removenode_heartbeat:
            return out << "removenode_heartbeat";
        case node_ops_cmd::removenode_sync_data:
            return out << "removenode_sync_data";
        case node_ops_cmd::removenode_abort:
            return out << "removenode_abort";
        case node_ops_cmd::removenode_done:
            return out << "removenode_done";
        case node_ops_cmd::replace_prepare:
            return out << "replace_prepare";
        case node_ops_cmd::replace_prepare_mark_alive:
            return out << "replace_prepare_mark_alive";
        case node_ops_cmd::replace_prepare_pending_ranges:
            return out << "replace_prepare_pending_ranges";
        case node_ops_cmd::replace_heartbeat:
            return out << "replace_heartbeat";
        case node_ops_cmd::replace_abort:
            return out << "replace_abort";
        case node_ops_cmd::replace_done:
            return out << "replace_done";
        case node_ops_cmd::decommission_prepare:
            return out << "decommission_prepare";
        case node_ops_cmd::decommission_heartbeat:
            return out << "decommission_heartbeat";
        case node_ops_cmd::decommission_abort:
            return out << "decommission_abort";
        case node_ops_cmd::decommission_done:
            return out << "decommission_done";
        case node_ops_cmd::bootstrap_prepare:
            return out << "bootstrap_prepare";
        case node_ops_cmd::bootstrap_heartbeat:
            return out << "bootstrap_heartbeat";
        case node_ops_cmd::bootstrap_abort:
            return out << "bootstrap_abort";
        case node_ops_cmd::bootstrap_done:
            return out << "bootstrap_done";
        case node_ops_cmd::query_pending_ops:
            return out << "query_pending_ops";
        case node_ops_cmd::repair_updater:
            return out << "repair_updater";
        default:
            return out << "unknown cmd (" << static_cast<std::underlying_type_t<node_ops_cmd>>(cmd) << ")";
    }
}

std::ostream& operator<<(std::ostream& out, const node_ops_cmd_request& req) {
    return out << fmt::format("{}[{}]: ignore_nodes={}, leaving_nodes={}, replace_nodes={}, bootstrap_nodes={}, repair_tables={}",
            req.cmd, req.ops_uuid, req.ignore_nodes, req.leaving_nodes, req.replace_nodes, req.bootstrap_nodes, req.repair_tables);
}
