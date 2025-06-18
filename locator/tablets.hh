/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "dht/token.hh"
#include "locator/token_metadata_fwd.hh"
#include "utils/small_vector.hh"
#include "locator/host_id.hh"
#include "service/session.hh"
#include "dht/i_partitioner_fwd.hh"
#include "dht/token-sharding.hh"
#include "dht/ring_position.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "utils/hash.hh"
#include "utils/UUID.hh"

#include <ranges>
#include <seastar/core/reactor.hh>
#include <seastar/util/log.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

namespace gms {
class feature_service;
}

namespace locator {

class topology;

extern seastar::logger tablet_logger;

using token = dht::token;

using tablet_task_id = utils::tagged_uuid<struct tablet_task_id_tag>;

// Identifies tablet within the scope of a single tablet_map,
// which has a scope of (table_id, token metadata version).
// Different tablets of different tables can have the same tablet_id.
// Different tablets in subsequent token metadata version can have the same tablet_id.
// When splitting a tablet, one of the new tablets (in the new token metadata version)
// will have the same tablet_id as the old one.
struct tablet_id {
    size_t id;
    explicit tablet_id(size_t id) : id(id) {}
    size_t value() const { return id; }
    explicit operator size_t() const { return id; }
    auto operator<=>(const tablet_id&) const = default;
};

/// Identifies tablet (not be confused with tablet replica) in the scope of the whole cluster.
struct global_tablet_id {
    table_id table;
    tablet_id tablet;

    auto operator<=>(const global_tablet_id&) const = default;
};

struct tablet_replica {
    host_id host;
    shard_id shard;

    auto operator<=>(const tablet_replica&) const = default;
};

using tablet_replica_set = utils::small_vector<tablet_replica, 3>;

}

namespace std {

template<>
struct hash<locator::tablet_id> {
    size_t operator()(const locator::tablet_id& id) const {
        return std::hash<size_t>()(id.value());
    }
};

template<>
struct hash<locator::tablet_replica> {
    size_t operator()(const locator::tablet_replica& r) const {
        return utils::hash_combine(
                std::hash<locator::host_id>()(r.host),
                std::hash<shard_id>()(r.shard));
    }
};

template<>
struct hash<locator::global_tablet_id> {
    size_t operator()(const locator::global_tablet_id& id) const {
        return utils::hash_combine(
                std::hash<table_id>()(id.table),
                std::hash<locator::tablet_id>()(id.tablet));
    }
};

}

namespace locator {

/// Creates a new replica set with old_replica replaced by new_replica.
/// If there is no old_replica, the set is returned unchanged.
inline
tablet_replica_set replace_replica(const tablet_replica_set& rs, tablet_replica old_replica, tablet_replica new_replica) {
    tablet_replica_set result;
    result.reserve(rs.size());
    for (auto&& r : rs) {
        if (r == old_replica) {
            result.push_back(new_replica);
        } else {
            result.push_back(r);
        }
    }
    return result;
}

/// Subtracts 'sub' from 'rs' and returns the result
/// Replicas from 'sub' that are missing in 'rs' are ignored
inline
std::unordered_set<tablet_replica> substract_sets(const tablet_replica_set& rs, const tablet_replica_set& sub) {
    std::unordered_set<tablet_replica> result(rs.begin(), rs.end());
    for (auto&& r : sub) {
        result.erase(r);
    }
    return result;
}

inline
bool contains(const tablet_replica_set& rs, host_id host) {
    for (auto replica : rs) {
        if (replica.host == host) {
            return true;
        }
    }
    return false;
}

inline
bool contains(const tablet_replica_set& rs, const tablet_replica& r) {
    return std::ranges::any_of(rs, [&] (auto&& r_) { return r_ == r; });
}


enum class tablet_task_type {
    none,
    user_repair,
    auto_repair,
    migration,
    intranode_migration,
    split,
    merge
};

sstring tablet_task_type_to_string(tablet_task_type);
tablet_task_type tablet_task_type_from_string(const sstring&);

struct tablet_task_info {
    tablet_task_type request_type = tablet_task_type::none;
    locator::tablet_task_id tablet_task_id;
    db_clock::time_point request_time;
    int64_t sched_nr = 0;
    db_clock::time_point sched_time;
    std::unordered_set<locator::host_id> repair_hosts_filter;
    std::unordered_set<sstring> repair_dcs_filter;
    bool operator==(const tablet_task_info&) const = default;
    bool is_valid() const;
    bool is_user_repair_request() const;
    bool selected_by_filters(const tablet_replica& replica, const topology& topo) const;
    static tablet_task_info make_user_repair_request(std::unordered_set<locator::host_id> hosts_filter = {}, std::unordered_set<sstring> dcs_filter = {});
    static tablet_task_info make_auto_repair_request(std::unordered_set<locator::host_id> hosts_filter = {}, std::unordered_set<sstring> dcs_filter = {});
    static tablet_task_info make_migration_request();
    static tablet_task_info make_intranode_migration_request();
    static tablet_task_info make_split_request();
    static tablet_task_info make_merge_request();
    static sstring serialize_repair_hosts_filter(std::unordered_set<locator::host_id> filter);
    static sstring serialize_repair_dcs_filter(std::unordered_set<sstring> filter);
    static std::unordered_set<locator::host_id> deserialize_repair_hosts_filter(sstring filter);
    static std::unordered_set<sstring> deserialize_repair_dcs_filter(sstring filter);
};

/// Stores information about a single tablet.
struct tablet_info {
    tablet_replica_set replicas;
    db_clock::time_point repair_time;
    locator::tablet_task_info repair_task_info;
    locator::tablet_task_info migration_task_info;

    tablet_info() = default;
    tablet_info(tablet_replica_set, db_clock::time_point, tablet_task_info, tablet_task_info);
    tablet_info(tablet_replica_set);

    bool operator==(const tablet_info&) const = default;
};

// Merges tablet_info b into a, but with following constraints:
//  - they cannot have active repair task, since each task has a different id
//  - their replicas must be all co-located.
// If tablet infos are mergeable, merged info is returned. Otherwise, nullopt.
std::optional<tablet_info> merge_tablet_info(tablet_info a, tablet_info b);

/// Represents states of the tablet migration state machine.
///
/// The stage serves two major purposes:
///
/// Firstly, it determines which action should be taken by the topology change coordinator on behalf
/// of the tablet before it can move to the next step. When stage is advanced, it means that
/// expected invariants about cluster-wide state relevant to the tablet, associated with the new stage, hold.
///
/// Also, stage affects which replicas are used by the coordinator for reads and writes.
/// Replica selectors kept in tablet_transition_info::writes and tablet_transition_info::reads,
/// are directly derived from the stage stored in group0.
///
/// See "Tablet migration" in docs/dev/topology-over-raft.md
enum class tablet_transition_stage {
    allow_write_both_read_old,
    write_both_read_old,
    streaming,
    rebuild_repair,
    write_both_read_new,
    use_new,
    cleanup,
    cleanup_target,
    revert_migration,
    end_migration,
    repair,
    end_repair,
};

enum class tablet_transition_kind {
    // Tablet replica is migrating from one shard to another.
    // The new replica is (tablet_transition_info::next - tablet_info::replicas).
    // The leaving replica is (tablet_info::replicas - tablet_transition_info::next).
    migration,

    // Like migration, but the new pending replica is on the same host as leaving replica.
    intranode_migration,

    // New tablet replica is replacing a dead one.
    // The new replica is (tablet_transition_info::next - tablet_info::replicas).
    // The leaving replica is (tablet_info::replicas - tablet_transition_info::next).
    rebuild,

    // Like rebuild, but instead of streaming data from all tablet replicas,
    // it repairs the tablet and streams data from one replica.
    rebuild_v2,

    // Repair the tablet replicas
    repair,
};

tablet_transition_kind choose_rebuild_transition_kind(const gms::feature_service& features);

sstring tablet_transition_stage_to_string(tablet_transition_stage);
tablet_transition_stage tablet_transition_stage_from_string(const sstring&);
sstring tablet_transition_kind_to_string(tablet_transition_kind);
tablet_transition_kind tablet_transition_kind_from_string(const sstring&);

using write_replica_set_selector = dht::write_replica_set_selector;

enum class read_replica_set_selector {
    previous, next
};

/// Used for storing tablet state transition during topology changes.
/// Describes transition of a single tablet.
struct tablet_transition_info {
    tablet_transition_stage stage;
    tablet_transition_kind transition;
    tablet_replica_set next;
    std::optional<tablet_replica> pending_replica; // Optimization (next - tablet_info::replicas)
    service::session_id session_id;
    write_replica_set_selector writes;
    read_replica_set_selector reads;

    tablet_transition_info(tablet_transition_stage stage,
                           tablet_transition_kind kind,
                           tablet_replica_set next,
                           std::optional<tablet_replica> pending_replica,
                           service::session_id session_id = {});

    bool operator==(const tablet_transition_info&) const = default;
};

// Returns the leaving replica for a given transition.
std::optional<tablet_replica> get_leaving_replica(const tablet_info&, const tablet_transition_info&);

// True if the tablet is transitioning and it's in a stage that follows the stage
// where we clean up the tablet on the given replica.
bool is_post_cleanup(tablet_replica replica, const tablet_info& tinfo, const tablet_transition_info& trinfo);

/// Represents intention to move a single tablet replica from src to dst.
struct tablet_migration_info {
    locator::tablet_transition_kind kind;
    locator::global_tablet_id tablet;
    locator::tablet_replica src;
    locator::tablet_replica dst;
};

/// Returns the replica set which will become the replica set of the tablet after executing a given tablet transition.
tablet_replica_set get_new_replicas(const tablet_info&, const tablet_migration_info&);
tablet_replica_set get_primary_replicas(const tablet_info&, const tablet_transition_info*);
tablet_transition_info migration_to_transition_info(const tablet_info&, const tablet_migration_info&);

/// Describes streaming required for a given tablet transition.
constexpr int tablet_migration_stream_weight_default = 1;
constexpr int tablet_migration_stream_weight_repair = 2;
struct tablet_migration_streaming_info {
    std::unordered_set<tablet_replica> read_from;
    std::unordered_set<tablet_replica> written_to;
    // The stream_weight for repair migration is set to 2, because it requires
    // more work than just moving the tablet around. The stream_weight for all
    // other migrations are set to 1.
    int stream_weight = tablet_migration_stream_weight_default;
};

tablet_migration_streaming_info get_migration_streaming_info(const locator::topology&, const tablet_info&, const tablet_transition_info&);
tablet_migration_streaming_info get_migration_streaming_info(const locator::topology&, const tablet_info&, const tablet_migration_info&);
bool tablet_has_excluded_node(const locator::topology& topo, const tablet_info& tinfo);

// Describes if a given token is located at either left or right side of a tablet's range
enum tablet_range_side {
    left = 0,
    right = 1,
};

// The decision of whether tablets of a given should be split, merged, or none, is made
// by the load balancer. This decision is recorded in the tablet_map and stored in group0.
struct resize_decision {
    struct none {};
    struct split {};
    struct merge {};

    using seq_number_t = int64_t;

    std::variant<none, split, merge> way;
    // The sequence number globally identifies a resize decision.
    // It's monotonically increasing, globally.
    // Needed to distinguish stale decision from latest one, in case coordinator
    // revokes the current decision and signal it again later.
    seq_number_t sequence_number = 0;

    resize_decision() = default;
    resize_decision(sstring decision, uint64_t seq_number);
    bool split_or_merge() const {
        return !std::holds_alternative<resize_decision::none>(way);
    }
    bool operator==(const resize_decision&) const;
    sstring type_name() const;
    seq_number_t next_sequence_number() const;
    // Returns true if this is the initial decision, before split or merge was emitted.
    bool initial_decision() const;
};

struct table_load_stats {
    uint64_t size_in_bytes = 0;
    // Stores the minimum seq number among all replicas, as coordinator wants to know if
    // all replicas have completed splitting, which happens when they all store the
    // seq number of the current split decision.
    resize_decision::seq_number_t split_ready_seq_number = std::numeric_limits<resize_decision::seq_number_t>::max();

    table_load_stats& operator+=(const table_load_stats& s) noexcept;
    friend table_load_stats operator+(table_load_stats a, const table_load_stats& b) {
        return a += b;
    }
};

// Deprecated, use load_stats instead.
struct load_stats_v1 {
    std::unordered_map<table_id, table_load_stats> tables;
};

struct load_stats {
    std::unordered_map<table_id, table_load_stats> tables;

    // Capacity in bytes for data file storage.
    std::unordered_map<host_id, uint64_t> capacity;

    static load_stats from_v1(load_stats_v1&&);

    load_stats& operator+=(const load_stats& s);
    friend load_stats operator+(load_stats a, const load_stats& b) {
        return a += b;
    }
};

using load_stats_v2 = load_stats;

struct repair_scheduler_config {
    bool auto_repair_enabled = false;
    // If the time since last repair is bigger than auto_repair_threshold
    // seconds, the tablet is eligible for auto repair.
    std::chrono::seconds auto_repair_threshold{10 * 24 * 3600};
    bool operator==(const repair_scheduler_config&) const = default;
};

using load_stats_ptr = lw_shared_ptr<const load_stats>;

struct tablet_desc {
    tablet_id tid;
    const tablet_info* info; // cannot be null.
    const tablet_transition_info* transition; // null if there's no transition.
};

class no_such_tablet_map : public std::runtime_error {
public:
    no_such_tablet_map(const table_id& id);
};

/// Stores information about tablets of a single table.
///
/// The map contains a constant number of tablets, tablet_count().
/// Each tablet has an associated tablet_info, and an optional tablet_transition_info.
/// Any given token is owned by exactly one tablet in this map.
///
/// A tablet map describes the whole ring, it cannot contain a partial mapping.
/// This means that the following sequence is always valid:
///
///    tablet_map& tmap = ...;
///    dht::token t = ...;
///    tablet_id id = tmap.get_tablet_id(t);
///    tablet_info& info = tmap.get_tablet_info(id);
///
/// A tablet_id obtained from an instance of tablet_map is valid for that instance only.
class tablet_map {
public:
    using tablet_container = utils::chunked_vector<tablet_info>;
private:
    // The implementation assumes that _tablets.size() is a power of 2:
    //
    //   _tablets.size() == 1 << _log2_tablets
    //
    tablet_container _tablets;
    size_t _log2_tablets; // log_2(_tablets.size())
    std::unordered_map<tablet_id, tablet_transition_info> _transitions;
    resize_decision _resize_decision;
    tablet_task_info _resize_task_info;
    repair_scheduler_config _repair_scheduler_config;

    /// Returns the largest token owned by tablet_id when the tablet_count is `1 << log2_tablets`.
    dht::token get_last_token(tablet_id id, size_t log2_tablets) const;

    /// Returns token_range which contains all tokens owned by the specified tablet
    /// when the tablet_count is `1 << log2_tablets`.
    dht::token_range get_token_range(tablet_id id, size_t log2_tablets) const;
public:
    /// Constructs a tablet map.
    ///
    /// \param tablet_count The desired tablets to allocate. Must be a power of two.
    explicit tablet_map(size_t tablet_count);

    /// Returns tablet_id of a tablet which owns a given token.
    tablet_id get_tablet_id(token) const;

    // Returns tablet_id and also the side of the tablet's range that a given token belongs to.
    std::pair<tablet_id, tablet_range_side> get_tablet_id_and_range_side(token) const;

    /// Returns tablet_info associated with a given tablet.
    /// The given id must belong to this instance.
    const tablet_info& get_tablet_info(tablet_id) const;

    /// Returns a pointer to tablet_transition_info associated with a given tablet.
    /// If there is no transition for a given tablet, returns nullptr.
    /// \throws std::logic_error If the given id does not belong to this instance.
    const tablet_transition_info* get_tablet_transition_info(tablet_id) const;

    /// Returns the largest token owned by a given tablet.
    /// \throws std::logic_error If the given id does not belong to this instance.
    dht::token get_last_token(tablet_id id) const;

    /// Returns the smallest token owned by a given tablet.
    /// \throws std::logic_error If the given id does not belong to this instance.
    dht::token get_first_token(tablet_id id) const;

    /// Returns token_range which contains all tokens owned by a given tablet and only such tokens.
    /// \throws std::logic_error If the given id does not belong to this instance.
    dht::token_range get_token_range(tablet_id id) const;

    /// Returns the primary replica for the tablet
    tablet_replica get_primary_replica(tablet_id id) const;
    tablet_replica get_primary_replica_within_dc(tablet_id id, const topology& topo, sstring dc) const;

    // Returns the replica that matches hosts and dcs filters for tablet_task_info.
    std::optional<tablet_replica> maybe_get_selected_replica(tablet_id id, const topology& topo, const tablet_task_info& tablet_task_info) const;

    /// Returns a vector of sorted last tokens for tablets.
    future<std::vector<token>> get_sorted_tokens() const;

    /// Returns the id of the first tablet.
    tablet_id first_tablet() const {
        return tablet_id(0);
    }

    /// Returns the id of the last tablet.
    tablet_id last_tablet() const {
        return tablet_id(tablet_count() - 1);
    }

    /// Returns the id of a tablet which follows a given tablet in the ring,
    /// or disengaged optional if the given tablet is the last one.
    std::optional<tablet_id> next_tablet(tablet_id t) const {
        if (t == last_tablet()) {
            return std::nullopt;
        }
        return tablet_id(size_t(t) + 1);
    }

    // Returns the pair of sibling tablets for a given tablet id.
    // For example, if id 1 is provided, a pair of 0 and 1 is returned.
    // Returns disengaged optional when sibling pair cannot be found.
    std::optional<std::pair<tablet_id, tablet_id>> sibling_tablets(tablet_id t) const;

    /// Returns true iff tablet has a given replica.
    /// If tablet is in transition, considers both previous and next replica set.
    bool has_replica(tablet_id, tablet_replica) const;

    const tablet_container& tablets() const {
        return _tablets;
    }

    /// Calls a given function for each tablet in the map in token ownership order.
    future<> for_each_tablet(seastar::noncopyable_function<future<>(tablet_id, const tablet_info&)> func) const;

    /// Calls a given function for each sibling tablet in the map in token ownership order.
    /// If tablet count == 1, then there will be only one call and 2nd tablet_desc is disengaged.
    future<> for_each_sibling_tablets(seastar::noncopyable_function<future<>(tablet_desc, std::optional<tablet_desc>)> func) const;

    const auto& transitions() const {
        return _transitions;
    }

    bool has_transitions() const {
        return !_transitions.empty();
    }

    /// Returns an iterable range over tablet_id:s which includes all tablets in token ring order.
    auto tablet_ids() const {
        return std::views::iota(0u, tablet_count()) | std::views::transform([] (size_t i) {
            return tablet_id(i);
        });
    }

    size_t tablet_count() const {
        return _tablets.size();
    }

    /// Returns tablet_info associated with the tablet which owns a given token.
    const tablet_info& get_tablet_info(token t) const {
        return get_tablet_info(get_tablet_id(t));
    }

    size_t external_memory_usage() const;

    bool operator==(const tablet_map&) const = default;

    bool needs_split() const;
    bool needs_merge() const;

    /// Returns the token_range in which the given token will belong to after a tablet split
    dht::token_range get_token_range_after_split(const token& t) const noexcept;

    const locator::resize_decision& resize_decision() const;
    const tablet_task_info& resize_task_info() const;
    const locator::repair_scheduler_config& repair_scheduler_config() const;
public:
    void set_tablet(tablet_id, tablet_info);
    void set_tablet_transition_info(tablet_id, tablet_transition_info);
    void set_resize_decision(locator::resize_decision);
    void set_resize_task_info(tablet_task_info);
    void set_repair_scheduler_config(locator::repair_scheduler_config config);
    void clear_tablet_transition_info(tablet_id);
    void clear_transitions();

    // Destroys gently.
    // The tablet map is not usable after this call and should be destroyed.
    future<> clear_gently();
    friend fmt::formatter<tablet_map>;
private:
    void check_tablet_id(tablet_id) const;
};

/// Holds information about all tablets in the cluster.
///
/// When this instance is obtained via token_metadata_ptr, it is immutable
/// (represents a snapshot) and references obtained through this are guaranteed
/// to remain valid as long as the containing token_metadata_ptr is held.
///
/// Copy constructor can be invoked across shards.
class tablet_metadata {
public:
    // We want both immutability and cheap updates, so we should use
    // hierarchical data structure with shared pointers and copy-on-write.
    //
    // Also, currently the copy constructor is invoked across shards, which precludes
    // using shared pointers. We should change that and use a foreign_ptr<> to
    // hold immutable tablet_metadata which lives on shard 0 only.
    // See storage_service::replicate_to_all_cores().
    using tablet_map_ptr = foreign_ptr<lw_shared_ptr<const tablet_map>>;
    using table_to_tablet_map = std::unordered_map<table_id, tablet_map_ptr>;
private:
    table_to_tablet_map _tablets;

    // When false, tablet load balancer will not try to rebalance tablets.
    bool _balancing_enabled = true;
public:
    bool balancing_enabled() const { return _balancing_enabled; }
    const tablet_map& get_tablet_map(table_id id) const;
    bool has_tablet_map(table_id id) const;
    const table_to_tablet_map& all_tables() const { return _tablets; }
    size_t external_memory_usage() const;
    bool has_replica_on(host_id) const;
public:
    tablet_metadata() = default;
    // No implicit copy, use copy()
    tablet_metadata(tablet_metadata&) = delete;
    tablet_metadata& operator=(tablet_metadata&) = delete;
    future<tablet_metadata> copy() const;
    // Move is supported.
    tablet_metadata(tablet_metadata&&) = default;
    tablet_metadata& operator=(tablet_metadata&&) = default;

    void set_balancing_enabled(bool value) { _balancing_enabled = value; }
    void set_tablet_map(table_id, tablet_map);
    void drop_tablet_map(table_id);

    // Allow mutating a tablet_map
    // Uses the copy-modify-swap idiom.
    // If func throws, no changes are done to the tablet map.
    void mutate_tablet_map(table_id, noncopyable_function<void(tablet_map&)> func);
    future<> mutate_tablet_map_async(table_id, noncopyable_function<future<>(tablet_map&)> func);

    future<> clear_gently();
public:
    bool operator==(const tablet_metadata&) const;
    friend fmt::formatter<tablet_metadata>;
};

// Check that all tablets which have replicas on this host, have a valid replica shard (< smp::count).
future<bool> check_tablet_replica_shards(const tablet_metadata& tm, host_id this_host);

std::optional<tablet_replica> maybe_get_primary_replica(tablet_id id, const tablet_replica_set& replica_set, std::function<bool(const tablet_replica&)> filter);

struct tablet_routing_info {
    tablet_replica_set tablet_replicas;
    std::pair<dht::token, dht::token> token_range;
};

/// Split a list of ranges, such that conceptually each input range is
/// intersected with each tablet range.
/// Tablets are pre-filtered, selecting only tablets that have a replica on the
/// given host.
/// Return the resulting intersections, in order.
/// The ranges are generated lazily (one at a time).
///
/// Note: the caller is expected to pin tablets, by keeping an
/// effective-replication-map alive.
class tablet_range_splitter {
public:
    struct range_split_result {
        shard_id shard; // shard where the tablet owning this range lives
        dht::partition_range range;
    };

private:
    schema_ptr _schema;
    const dht::partition_range_vector& _ranges;
    dht::partition_range_vector::const_iterator _ranges_it;
    std::vector<range_split_result> _tablet_ranges;
    std::vector<range_split_result>::iterator _tablet_ranges_it;

public:
    tablet_range_splitter(schema_ptr schema, const tablet_map& tablets, host_id host, const dht::partition_range_vector& ranges);
    /// Returns nullopt when there are no more ranges.
    std::optional<range_split_result> operator()();
};

struct tablet_metadata_change_hint {
    struct table_hint {
        table_id table_id;
        std::vector<token> tokens;

        bool operator==(const table_hint&) const = default;
    };
    std::unordered_map<table_id, table_hint> tables;

    bool operator==(const tablet_metadata_change_hint&) const = default;
    explicit operator bool() const noexcept { return !tables.empty(); }
};

class abstract_replication_strategy;

/// Verify that the provided keyspace corresponding to the provided replication strategy is RF-rack-valid, i.e.
/// whether it satisfies the following conditions:
/// * does NOT use tablets, OR,
/// * for every DC, the replication factor corresponding to that DC must be an element of the set
///     {0, 1, the number of racks in that DC with at least one normal node}
///   Special case: if the DC is an arbiter DC (i.e. only consists of zero-token nodes), the RF MUST be equal
///   to 0 for that DC.
///
/// Result:
/// * if the keyspace is RF-rack-valid, no side effect,
/// * if the keyspace is RF-rack-invalid, an exception will be thrown. It will contain information about the reason
///   why the keyspace is RF-rack-invalid and will be worded in a way that can be returned directly to the user.
///   There are NO guarantees about the type of the exception.
///
/// Preconditions:
/// * Every DC that takes part in replication according to the passed replication strategy MUST be known
///   by the passed `token_metadata_ptr`.
///
/// Non-requirements:
/// * The keyspace need not exist. We use its name purely for informational reasons (in error messages).
void assert_rf_rack_valid_keyspace(std::string_view ks, const token_metadata_ptr, const abstract_replication_strategy&);

}

template <>
struct fmt::formatter<locator::tablet_transition_stage> : fmt::formatter<string_view> {
    auto format(const locator::tablet_transition_stage&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_transition_kind> : fmt::formatter<string_view> {
    auto format(const locator::tablet_transition_kind&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::global_tablet_id> : fmt::formatter<string_view> {
    auto format(const locator::global_tablet_id&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_id> : fmt::formatter<string_view> {
    auto format(locator::tablet_id  id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.value());
    }
};

template <>
struct fmt::formatter<locator::tablet_replica> : fmt::formatter<string_view> {
    auto format(const locator::tablet_replica& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}:{}", r.host, r.shard);
    }
};

template <>
struct fmt::formatter<locator::tablet_map> : fmt::formatter<string_view> {
    auto format(const locator::tablet_map&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_metadata> : fmt::formatter<string_view> {
    auto format(const locator::tablet_metadata&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_metadata_change_hint> : fmt::formatter<string_view> {
    auto format(const locator::tablet_metadata_change_hint&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::repair_scheduler_config> : fmt::formatter<string_view> {
    auto format(const locator::repair_scheduler_config&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_task_info> : fmt::formatter<string_view> {
    auto format(const locator::tablet_task_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::tablet_task_type> : fmt::formatter<string_view> {
    auto format(const locator::tablet_task_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
