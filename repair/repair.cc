/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "repair.hh"

#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "gms/inet_address.hh"
#include "db/config.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <cryptopp/sha.h>

static logging::logger logger("repair");

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

static std::vector<sstring> list_column_families(const database& db, const sstring& keyspace) {
    std::vector<sstring> ret;
    for (auto &&e : db.get_column_families_mapping()) {
        if (e.first.first == keyspace) {
            ret.push_back(e.first.second);
        }
    }
    return ret;
}

template<typename Collection, typename T>
void remove_item(Collection& c, T& item) {
    auto it = std::find(c.begin(), c.end(), item);
    if (it != c.end()) {
        c.erase(it);
    }
}

// Return all of the neighbors with whom we share the provided range.
static std::vector<gms::inet_address> get_neighbors(database& db,
        const sstring& ksname, query::range<dht::token> range,
        const std::vector<sstring>& data_centers,
        const std::vector<sstring>& hosts) {

    keyspace& ks = db.find_keyspace(ksname);
    auto& rs = ks.get_replication_strategy();

    dht::token tok = range.end() ? range.end()->value() : dht::maximum_token();
    auto ret = rs.get_natural_endpoints(tok);
    remove_item(ret, utils::fb_utilities::get_broadcast_address());

    if (!data_centers.empty()) {
        auto dc_endpoints_map = service::get_local_storage_service().get_token_metadata().get_topology().get_datacenter_endpoints();
        std::unordered_set<gms::inet_address> dc_endpoints;
        for (const sstring& dc : data_centers) {
            auto it = dc_endpoints_map.find(dc);
            if (it == dc_endpoints_map.end()) {
                std::vector<sstring> dcs;
                for (const auto& e : dc_endpoints_map) {
                    dcs.push_back(e.first);
                }
                throw std::runtime_error(sprint("Unknown data center '%s'. "
                        "Known data centers: %s", dc, dcs));
            }
            for (const auto& endpoint : it->second) {
                dc_endpoints.insert(endpoint);
            }
        }
        // We require, like Cassandra does, that the current host must also
        // be part of the repair
        if (!dc_endpoints.count(utils::fb_utilities::get_broadcast_address())) {
            throw std::runtime_error("The current host must be part of the repair");
        }
        // The resulting list of nodes is the intersection of the nodes in the
        // listed data centers, and the (range-dependent) list of neighbors.
        std::unordered_set<gms::inet_address> neighbor_set(ret.begin(), ret.end());
        ret.clear();
        for (const auto& endpoint : dc_endpoints) {
            if (neighbor_set.count(endpoint)) {
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
                throw std::runtime_error(sprint("Unknown host specified: %s", host));
            }
            if (endpoint == utils::fb_utilities::get_broadcast_address()) {
                found_me = true;
            } else if (neighbor_set.count(endpoint)) {
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
            auto me = utils::fb_utilities::get_broadcast_address();
            auto others = rs.get_natural_endpoints(tok);
            remove_item(others, me);
            throw std::runtime_error(sprint("Repair requires at least two "
                    "endpoints that are neighbors before it can continue, "
                    "the endpoint used for this repair is %s, other "
                    "available neighbors are %s but these neighbors were not "
                    "part of the supplied list of hosts to use during the "
                    "repair (%s).", me, others, hosts));
        }
    }

    return ret;

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


// The repair_tracker tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
// This object is not thread safe, and must be used by only one cpu.
static class {
private:
    // Each repair_start() call returns a unique int which the user can later
    // use to follow the status of this repair with repair_status().
    // We can't use the number 0 - if repair_start() returns 0, it means it
    // decide quickly that there is nothing to repair.
    int _next_repair_command = 1;
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id < _next_repair_command
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Used to allow shutting down repairs in progress, and waiting for them.
    seastar::gate _gate;
public:
    void start(int id) {
        _gate.enter();
        _status[id] = repair_status::RUNNING;
    }
    void done(int id, bool succeeded) {
        if (succeeded) {
            _status.erase(id);
        } else {
            _status[id] = repair_status::FAILED;
        }
        _gate.leave();
    }
    repair_status get(int id) {
        if (id >= _next_repair_command) {
            throw std::runtime_error(sprint("unknown repair id %d", id));
        }
        auto it = _status.find(id);
        if (it == _status.end()) {
            return repair_status::SUCCESSFUL;
        } else {
            return it->second;
        }
    }
    int next_repair_command() {
        return _next_repair_command++;
    }
    future<> shutdown() {
        return _gate.close();
    }
    void check_in_shutdown() {
        _gate.check();
    }
} repair_tracker;

static void check_in_shutdown() {
    // Only call this from the single CPU managing the repair - the only CPU
    // which is allowed to use repair_tracker.
    assert(engine().cpu_id() == 0);
    repair_tracker.check_in_shutdown();
}

class sha256_hasher {
    CryptoPP::SHA256 hash{};
public:
    void update(const char* ptr, size_t length) {
        static_assert(sizeof(char) == sizeof(byte), "Assuming lengths will be the same");
        hash.Update(reinterpret_cast<const byte*>(ptr), length * sizeof(byte));
    }

    void finalize(std::array<uint8_t, 32>& digest) {
        static_assert(CryptoPP::SHA256::DIGESTSIZE == std::tuple_size<std::remove_reference_t<decltype(digest)>>::value * sizeof(digest[0]),
                "digest size");
        hash.Final(reinterpret_cast<unsigned char*>(digest.data()));
    }
};


partition_checksum::partition_checksum(const mutation& m) {
    sha256_hasher h;
    feed_hash(h, m);
    h.finalize(_digest);
}

static inline unaligned<uint64_t>& qword(std::array<uint8_t, 32>& b, int n) {
    return *unaligned_cast<uint64_t>(b.data() + 8 * n);
}
static inline const unaligned<uint64_t>& qword(const std::array<uint8_t, 32>& b, int n) {
    return *unaligned_cast<uint64_t>(b.data() + 8 * n);
}

void partition_checksum::add(const partition_checksum& other) {
     static_assert(std::tuple_size<decltype(_digest)>::value == 32, "digest size");
     // Hopefully the following trickery is faster than XOR'ing 32 separate bytes
     qword(_digest, 0) = qword(_digest, 0) ^ qword(other._digest, 0);
     qword(_digest, 1) = qword(_digest, 1) ^ qword(other._digest, 1);
     qword(_digest, 2) = qword(_digest, 2) ^ qword(other._digest, 2);
     qword(_digest, 3) = qword(_digest, 3) ^ qword(other._digest, 3);
}

bool partition_checksum::operator==(const partition_checksum& other) const {
    static_assert(std::tuple_size<decltype(_digest)>::value == 32, "digest size");
    return qword(_digest, 0) == qword(other._digest, 0) &&
           qword(_digest, 1) == qword(other._digest, 1) &&
           qword(_digest, 2) == qword(other._digest, 2) &&
           qword(_digest, 3) == qword(other._digest, 3);
}

const std::array<uint8_t, 32>& partition_checksum::digest() const {
    return _digest;
}

std::ostream& operator<<(std::ostream& out, const partition_checksum& c) {
    auto save_flags = out.flags();
    out << std::hex << std::setfill('0');
    for (auto b : c._digest) {
        out << std::setw(2) << (unsigned int)b;
    }
    out.flags(save_flags);
    return out;
}

// Calculate the checksum of the data held *on this shard* of a column family,
// in the given token range.
// All parameters to this function are constant references, and the caller
// must ensure they live as long as the future returned by this function is
// not resolved.
// FIXME: Both master and slave will typically call this on consecutive ranges
// so it would be useful to have this code cache its stopping point or have
// some object live throughout the operation. Moreover, it makes sense to to
// vary the collection of sstables used throught a long repair.
// FIXME: cf.make_reader() puts all read partitions in the cache. This might
// not be a good idea (see issue #382). Perhaps it is better to read the
// sstables directly (as compaction does) - after flushing memtables first
// (there might be old data in memtables which isn't flushed because no new
// data is coming in).
static future<partition_checksum> checksum_range_shard(database &db,
        const sstring& keyspace_name, const sstring& cf_name,
        const ::range<dht::token>& range) {
    auto& cf = db.find_column_family(keyspace_name, cf_name);
    return do_with(query::to_partition_range(range), [&cf] (const auto& partition_range) {
        return do_with(cf.make_reader(cf.schema(), partition_range, service::get_local_streaming_read_priority()), partition_checksum(),
            [] (auto& reader, auto& checksum) {
            return repeat([&reader, &checksum] () {
                return reader().then([&checksum] (auto mopt) {
                    if (mopt) {
                        checksum.add(partition_checksum(*mopt));
                        return stop_iteration::no;
                    } else {
                        return stop_iteration::yes;
                    }
                });
            }).then([&checksum] {
                return checksum;
            });
        });
    });
}

// Calculate the checksum of the data held on all shards of a column family,
// in the given token range.
// In practice, we only need to consider one or two shards which intersect the
// given "range". This is because the token ring has nodes*vnodes tokens,
// dividing the token space into nodes*vnodes ranges, with "range" being one
// of those. This number is big (vnodes = 256 by default). At the same time,
// sharding divides the token space into relatively few large ranges, one per
// thread.
// Watch out: All parameters to this function are constant references, and the
// caller must ensure they live as line as the future returned by this
// function is not resolved.
future<partition_checksum> checksum_range(seastar::sharded<database> &db,
        const sstring& keyspace, const sstring& cf,
        const ::range<dht::token>& range) {
    unsigned shard_begin = range.start() ?
            dht::shard_of(range.start()->value()) : 0;
    unsigned shard_end = range.end() ?
            dht::shard_of(range.end()->value())+1 : smp::count;
    return do_with(partition_checksum(), [shard_begin, shard_end, &db, &keyspace, &cf, &range] (auto& result) {
        return parallel_for_each(boost::counting_iterator<int>(shard_begin),
                boost::counting_iterator<int>(shard_end),
                [&db, &keyspace, &cf, &range, &result] (unsigned shard) {
            return db.invoke_on(shard, [&keyspace, &cf, &range] (database& db) {
                return checksum_range_shard(db, keyspace, cf, range);
            }).then([&result] (partition_checksum sum) {
                result.add(sum);
            });
        }).then([&result] {
            return make_ready_future<partition_checksum>(result);
        });
    });
}

static future<> sync_range(seastar::sharded<database>& db,
        const sstring& keyspace, const sstring& cf,
        const ::range<dht::token>& range,
        std::vector<gms::inet_address>& neighbors) {
    return do_with(streaming::stream_plan("repair-in"),
                   streaming::stream_plan("repair-out"),
            [&db, &keyspace, &cf, &range, &neighbors]
            (auto& sp_in, auto& sp_out) {
        for (const auto& peer : neighbors) {
            sp_in.request_ranges(peer, keyspace, {range}, {cf});
            sp_out.transfer_ranges(peer, keyspace, {range}, {cf});
        }
        return sp_in.execute().discard_result().then([&sp_out] {
                return sp_out.execute().discard_result();
        }).handle_exception([] (auto ep) {
            logger.error("repair's stream failed: {}", ep);
            return make_exception_future(ep);
        });
    });
}
static void split_and_add(std::vector<::range<dht::token>>& ranges,
        const range<dht::token>& range,
        uint64_t estimated_partitions, uint64_t target_partitions) {
    if (estimated_partitions < target_partitions) {
        // We're done, the range is small enough to not be split further
        ranges.push_back(range);
        return;
    }
    // The use of minimum_token() here twice is not a typo - because wrap-
    // around token ranges are supported by midpoint(), the beyond-maximum
    // token can also be represented by minimum_token().
    auto midpoint = dht::global_partitioner().midpoint(
            range.start() ? range.start()->value() : dht::minimum_token(),
            range.end() ? range.end()->value() : dht::minimum_token());
    auto halves = range.split(midpoint, dht::token_comparator());
    ranges.push_back(halves.first);
    ranges.push_back(halves.second);
}

// Repair a single cf in a single local range.
// Comparable to RepairJob in Origin.
static future<> repair_cf_range(seastar::sharded<database>& db,
        sstring keyspace, sstring cf, ::range<dht::token> range,
        std::vector<gms::inet_address>& neighbors) {
    if (neighbors.empty()) {
        // Nothing to do in this case...
        return make_ready_future<>();
    }

    // The partition iterating code inside checksum_range_shard does not
    // support wrap-around ranges, so we need to break at least wrap-
    // around ranges.
    std::vector<::range<dht::token>> ranges;
    if (range.is_wrap_around(dht::token_comparator())) {
        auto unwrapped = range.unwrap();
        ranges.push_back(unwrapped.first);
        ranges.push_back(unwrapped.second);
    } else {
        ranges.push_back(range);
    }
    // Additionally, we want to break up large ranges so they will have
    // (approximately) a desired number of rows each.
    // FIXME: column_family should have a method to estimate the number of
    // partitions (and of course it should use cardinality estimation bitmaps,
    // not trivial sum). We shouldn't have this ugly code here...
    auto sstables = db.local().find_column_family(keyspace, cf).get_sstables();
    uint64_t estimated_partitions = 0;
    for (auto sst : *sstables) {
        estimated_partitions += sst.second->get_estimated_key_count();
    }
    // This node contains replicas of rf * vnodes ranges like this one, so
    // estimate the number of partitions in just this range:
    estimated_partitions /= db.local().get_config().num_tokens();
    estimated_partitions /= db.local().find_keyspace(keyspace).get_replication_strategy().get_replication_factor();

    // FIXME: we should have an on-the-fly iterator generator here, not
    // fill a vector in advance.
    std::vector<::range<dht::token>> tosplit;
    ranges.swap(tosplit);
    for (const auto& range : tosplit) {
        // FIXME: this "100" needs to be a parameter.
        split_and_add(ranges, range, estimated_partitions, 100);
    }

    // We don't need to wait for one checksum to finish before we start the
    // next, but doing too many of these operations in parallel also doesn't
    // make sense, so we limit the number of concurrent ongoing checksum
    // requests with a semaphore.
    //
    // FIXME: We shouldn't use a magic number here, but rather bind it to
    // some resource. Otherwise we'll be doing too little in some machines,
    // and too much in others.
    constexpr int parallelism = 10;
    return do_with(semaphore(parallelism), true, std::move(keyspace), std::move(cf), std::move(ranges),
        [&db, &neighbors, parallelism] (auto& sem, auto& success, const auto& keyspace, const auto& cf, const auto& ranges) {
        return do_for_each(ranges, [&sem, &success, &db, &neighbors, &keyspace, &cf]
                           (const auto& range) {
            check_in_shutdown();
            return sem.wait(1).then([&sem, &success, &db, &neighbors, &keyspace, &cf, &range] {
                // Ask this node, and all neighbors, to calculate checksums in
                // this range. When all are done, compare the results, and if
                // there are any differences, sync the content of this range.
                std::vector<future<partition_checksum>> checksums;
                checksums.reserve(1 + neighbors.size());
                checksums.push_back(checksum_range(db, keyspace, cf, range));
                for (auto&& neighbor : neighbors) {
                    checksums.push_back(
                            net::get_local_messaging_service().send_repair_checksum_range(
                                    net::msg_addr{neighbor},keyspace, cf, range));
                }
                when_all(checksums.begin(), checksums.end()).then(
                        [&db, &keyspace, &cf, &range, &neighbors, &success]
                        (std::vector<future<partition_checksum>> checksums) {
                    // If only some of the replicas of this range are alive,
                    // we set success=false so repair will fail, but we can
                    // still do our best to repair available replicas.
                    std::vector<gms::inet_address> live_neighbors;
                    for (unsigned i = 0; i < checksums.size(); i++) {
                        if (checksums[i].failed()) {
                            logger.warn(
                                "Checksum of range {} on {} failed: {}",
                                range,
                                (i ? neighbors[i-1] :
                                 utils::fb_utilities::get_broadcast_address()),
                                checksums[i].get_exception());
                            success = false;
                            // Do not break out of the loop here, so we can log
                            // (and discard) all the exceptions.
                        } else if (i > 0) {
                            live_neighbors.push_back(neighbors[i - 1]);
                        }
                    }
                    if (!checksums[0].available() || live_neighbors.empty()) {
                        return make_ready_future<>();
                    }
                    // If one of the available checksums is different, repair
                    // all the neighbors which returned a checksum.
                    auto checksum0 = checksums[0].get();
                    for (unsigned i = 1; i < checksums.size(); i++) {
                        if (checksums[i].available() && checksum0 != checksums[i].get()) {
                            logger.info("Found differing range {} on nodes {}", range, live_neighbors);
                            return do_with(std::move(live_neighbors), [&db, &keyspace, &cf, &range] (auto& live_neighbors) {
                                return sync_range(db, keyspace, cf, range, live_neighbors);
                            });
                        }
                    }
                    return make_ready_future<>();
                }).handle_exception([&success, &range] (std::exception_ptr eptr) {
                    // Something above (e.g., sync_range) failed. We could
                    // stop the repair immediately, or let it continue with
                    // other ranges (at the moment, we do the latter). But in
                    // any case, we need to remember that the repair failed to
                    // tell the caller.
                    success = false;
                    logger.warn("Failed sync of range {}: {}", range, eptr);
                }).finally([&sem] { sem.signal(1); });
            });
        }).finally([&sem, &success, parallelism] {
            return sem.wait(parallelism).then([&success] {
                return success ? make_ready_future<>() :
                        make_exception_future<>(std::runtime_error("Checksum or sync of partial range failed"));
            });
        });
    });
}

// Repair a single local range, multiple column families.
// Comparable to RepairSession in Origin
static future<> repair_range(seastar::sharded<database>& db, sstring keyspace,
        ::range<dht::token> range, std::vector<sstring>& cfs,
        const std::vector<sstring>& data_centers,
        const std::vector<sstring>& hosts) {
    auto id = utils::UUID_gen::get_time_UUID();
    return do_with(get_neighbors(db.local(), keyspace, range, data_centers, hosts), [&db, &cfs, keyspace, id, range] (auto& neighbors) {
        logger.info("[repair #{}] new session: will sync {} on range {} for {}.{}", id, neighbors, range, keyspace, cfs);
        return do_for_each(cfs.begin(), cfs.end(),
                [&db, keyspace, &neighbors, id, range] (auto&& cf) {
            return repair_cf_range(db, keyspace, cf, range, neighbors);
        });
    });
}

static std::vector<query::range<dht::token>> get_ranges_for_endpoint(
        database& db, sstring keyspace, gms::inet_address ep) {
    auto& rs = db.find_keyspace(keyspace).get_replication_strategy();
    return rs.get_ranges(ep);
}

static std::vector<query::range<dht::token>> get_local_ranges(
        database& db, sstring keyspace) {
    return get_ranges_for_endpoint(db, keyspace, utils::fb_utilities::get_broadcast_address());
}

static std::vector<query::range<dht::token>> get_primary_ranges_for_endpoint(
        database& db, sstring keyspace, gms::inet_address ep) {
    auto& rs = db.find_keyspace(keyspace).get_replication_strategy();
    return rs.get_primary_ranges(ep);
}

static std::vector<query::range<dht::token>> get_primary_ranges(
        database& db, sstring keyspace) {
    return get_primary_ranges_for_endpoint(db, keyspace,
            utils::fb_utilities::get_broadcast_address());
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
    std::vector<query::range<dht::token>> ranges;
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
    // data_centers is used to restrict the repair to the local data center.
    // The node starting the repair must be in the data center; Issuing a
    // repair to a data center other than the named one returns an error.
    std::vector<sstring> data_centers;

    repair_options(std::unordered_map<sstring, sstring> options) {
        bool_opt(primary_range, options, PRIMARY_RANGE_KEY);
        ranges_opt(ranges, options, RANGES_KEY);
        list_opt(column_families, options, COLUMNFAMILIES_KEY);
        list_opt(hosts, options, HOSTS_KEY);
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
            throw std::runtime_error(sprint(
                    "unsupported repair parallelism: %d", parallelism));
        }
        string_opt(start_token, options, START_TOKEN);
        string_opt(end_token, options, END_TOKEN);
        // The parsing code above removed from the map options we have parsed.
        // If anything is left there in the end, it's an unsupported option.
        if (!options.empty()) {
            throw std::runtime_error(sprint("unsupported repair options: %s",
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
    static constexpr const char* TRACE_KEY = "trace";
    static constexpr const char* START_TOKEN = "startToken";
    static constexpr const char* END_TOKEN = "endToken";

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
                throw(std::runtime_error(sprint("cannot parse integer: '%s'", it->second)));
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
    static void ranges_opt(std::vector<query::range<dht::token>>& var,
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
            auto tok_start = dht::global_partitioner().from_sstring(token_strings[0]);
            auto tok_end = dht::global_partitioner().from_sstring(token_strings[1]);
            var.emplace_back(
                    ::range<dht::token>::bound(tok_start, false),
                    ::range<dht::token>::bound(tok_end, true));
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

// repair_ranges repairs a list of token ranges, each assumed to be a token
// range for which this node holds a replica, and, importantly, each range
// is assumed to be a indivisible in the sense that all the tokens in has the
// same nodes as replicas.
static future<> repair_ranges(seastar::sharded<database>& db, sstring keyspace,
        std::vector<query::range<dht::token>> ranges,
        std::vector<sstring> cfs, int id,
        std::vector<sstring> data_centers, std::vector<sstring> hosts) {
    return do_with(std::move(ranges), std::move(keyspace), std::move(cfs),
            std::move(data_centers), std::move(hosts),
            [&db, id] (auto& ranges, auto& keyspace, auto& cfs, auto& data_centers, auto& hosts) {
#if 1
        // repair all the ranges in parallel
        return parallel_for_each(ranges.begin(), ranges.end(), [&db, keyspace, &cfs, &data_centers, &hosts, id] (auto&& range) {
#else
        // repair all the ranges in sequence
        return do_for_each(ranges.begin(), ranges.end(), [&db, keyspace, &cfs, &data_centers, &hosts, id] (auto&& range) {
#endif
            check_in_shutdown();
            return repair_range(db, keyspace, range, cfs, data_centers, hosts);
        }).then([id] {
            logger.info("repair {} completed sucessfully", id);
            repair_tracker.done(id, true);
        }).handle_exception([id] (std::exception_ptr eptr) {
            logger.info("repair {} failed - {}", id, eptr);
            repair_tracker.done(id, false);
        });
    });
}

// repair_start() can run on any cpu; It runs on cpu0 the function
// do_repair_start(). The benefit of always running that function on the same
// CPU is that it allows us to keep some state (like a list of ongoing
// repairs). It is fine to always do this on one CPU, because the function
// itself does very little (mainly tell other nodes and CPUs what to do).
static int do_repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options_map) {
    check_in_shutdown();

    repair_options options(options_map);

    // Note: Cassandra can, in some cases, decide immediately that there is
    // nothing to repair, and return 0. "nodetool repair" prints in this case
    // that "Nothing to repair for keyspace '...'". We don't have such a case
    // yet. Real ids returned by next_repair_command() will be >= 1.
    int id = repair_tracker.next_repair_command();
    logger.info("starting user-requested repair for keyspace {}, repair id {}, options {}", keyspace, id, options_map);

    repair_tracker.start(id);

    // If the "ranges" option is not explicitly specified, we repair all the
    // local ranges (the token ranges for which this node holds a replica of).
    // Each of these ranges may have a different set of replicas, so the
    // repair of each range is performed separately with repair_range().
    std::vector<query::range<dht::token>> ranges;
    if (options.ranges.size()) {
        ranges = options.ranges;
    } else if (options.primary_range) {
        logger.info("primary-range repair");
        // when "primary_range" option is on, neither data_centers nor hosts
        // may be set, except data_centers may contain only local DC (-local)
#if 0
        if (options.data_centers.size() == 1 &&
                options.data_centers[0] == DatabaseDescriptor.getLocalDataCenter()) {
            ranges = get_primary_ranges_within_dc(db.local(), keyspace);
        } else
#endif
#if 0
        if (options.data_centers.size() > 0 || options.hosts.size() > 0) {
            throw std::runtime_error("You need to run primary range repair on all nodes in the cluster.");
        } else {
#endif
            ranges = get_primary_ranges(db.local(), keyspace);
#if 0
        }
#endif
    } else {
        ranges = get_local_ranges(db.local(), keyspace);
    }

    if (!options.start_token.empty() || !options.end_token.empty()) {
        // Intersect the list of local ranges with the given token range,
        // dropping ranges with no intersection.
        // We don't have a range::intersect() method, but we can use
        // range::subtract() and subtract the complement range.
        std::experimental::optional<::range<dht::token>::bound> tok_start;
        std::experimental::optional<::range<dht::token>::bound> tok_end;
        if (!options.start_token.empty()) {
            tok_start = ::range<dht::token>::bound(
                dht::global_partitioner().from_sstring(options.start_token),
                true);
        }
        if (!options.end_token.empty()) {
            tok_end = ::range<dht::token>::bound(
                dht::global_partitioner().from_sstring(options.end_token),
                false);
        }
        ::range<dht::token> given_range_complement(tok_end, tok_start);
        std::vector<query::range<dht::token>> intersections;
        for (const auto& range : ranges) {
            auto rs = range.subtract(given_range_complement,
                    dht::token_comparator());
            intersections.insert(intersections.end(), rs.begin(), rs.end());
        }
        ranges = std::move(intersections);
    }

    std::vector<sstring> cfs;
    if (options.column_families.size()) {
        cfs = options.column_families;
        for (auto& cf : cfs) {
            try {
                db.local().find_column_family(keyspace, cf);
            } catch(...) {
                throw std::runtime_error(sprint(
                    "No column family '%s' in keyspace '%s'", cf, keyspace));
            }
        }
    } else {
        cfs = list_column_families(db.local(), keyspace);
    }

    repair_ranges(db, std::move(keyspace), std::move(ranges), std::move(cfs),
            id, options.data_centers, options.hosts);

    return id;
}

future<int> repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options) {
    return db.invoke_on(0, [&db, keyspace = std::move(keyspace), options = std::move(options)] (database& localdb) {
        return do_repair_start(db, std::move(keyspace), std::move(options));
    });
}

future<repair_status> repair_get_status(seastar::sharded<database>& db, int id) {
    return db.invoke_on(0, [id] (database& localdb) {
        return repair_tracker.get(id);
    });
}

future<> repair_shutdown(seastar::sharded<database>& db) {
    logger.info("Starting shutdown of repair");
    return db.invoke_on(0, [] (database& localdb) {
        return repair_tracker.shutdown().then([] {
            logger.info("Completed shutdown of repair");
        });
    });
}
