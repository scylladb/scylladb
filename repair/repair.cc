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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

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
        const sstring& ksname, query::range<dht::token> range
        //Collection<String> dataCenters, Collection<String> hosts)
        ) {
    keyspace& ks = db.find_keyspace(ksname);
    auto& rs = ks.get_replication_strategy();

    dht::token tok = range.end() ? range.end()->value() : dht::maximum_token();
    auto ret = rs.get_natural_endpoints(tok);
    remove_item(ret, utils::fb_utilities::get_broadcast_address());
    return ret;

#if 0
    // Origin's ActiveRepairService.getNeighbors() contains a lot of important
    // stuff we need to do, like verifying the requested range fits a local
    // range, and also taking the "datacenters" and "hosts" options.
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

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Repair requires at least two endpoints that are neighbours before it can continue, the endpoint used for this repair is %s, " +
                             "other available neighbours are %s but these neighbours were not part of the supplied list of hosts to use during the repair (%s).";
                throw new IllegalArgumentException(String.format(msg, specifiedHost, neighbors, hosts));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
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
public:
    void start(int id) {
        _status[id] = repair_status::RUNNING;
    }
    void done(int id, bool succeeded) {
        if (succeeded) {
            _status.erase(id);
        } else {
            _status[id] = repair_status::FAILED;
        }
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
} repair_tracker;

// repair_start() can run on any cpu; It runs on cpu0 the function
// do_repair_start(). The benefit of always running that function on the same
// CPU is that it allows us to keep some state (like a list of ongoing
// repairs). It is fine to always do this on one CPU, because the function
// itself does very little (mainly tell other nodes and CPUs what to do).

// Repair a single range. Comparable to RepairSession in Origin
// In Origin, this is composed of several "repair jobs", each with one cf,
// but our streaming already works for several cfs.
static future<> repair_range(seastar::sharded<database>& db, sstring keyspace,
        query::range<dht::token> range, std::vector<sstring> cfs) {
    auto sp = make_lw_shared<streaming::stream_plan>("repair");
    auto id = utils::UUID_gen::get_time_UUID();

    auto neighbors = get_neighbors(db.local(), keyspace, range);
    logger.info("[repair #{}] new session: will sync {} on range {} for {}.{}", id, neighbors, range, keyspace, cfs);
    for (auto peer : neighbors) {
        // FIXME: obviously, we'll need Merkel trees or another alternative
        // method to decide which parts of the data we need to stream instead
        // of streaming everything like we do now. So this logging is kind of
        // silly, and we never log the corresponding "... is consistent with"
        // message: see SyncTask.run() in Origin for the original messages.
        auto me = utils::fb_utilities::get_broadcast_address();
        for (auto &&cf : cfs) {
            logger.info("[repair #{}] Endpoints {} and {} have {} range(s) out of sync for {}", id, me, peer, 1, cf);
        }

        // FIXME: think: if we have several neighbors, perhaps we need to
        // request ranges from all of them and only later transfer ranges to
        // all of them? Otherwise, we won't necessarily fully repair the
        // other ndoes, just this one? What does Cassandra do here?
        sp->transfer_ranges(peer, peer, keyspace, {range}, cfs);
        sp->request_ranges(peer, peer, keyspace, {range}, cfs);
    }
    return sp->execute().discard_result().then([sp, id] {
        logger.info("repair session #{} successful", id);
    }).handle_exception([id] (auto ep) {
        logger.error("repair session #{} stream failed: {}", id, ep);
        return make_exception_future(std::runtime_error("repair_range failed"));
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
    // column_families is the list of column families to repair in the given
    // keyspace. If this list is empty (the default), all the column families
    // in this keyspace are repaired
    std::vector<sstring> column_families;

    repair_options(std::unordered_map<sstring, sstring> options) {
        bool_opt(primary_range, options, PRIMARY_RANGE_KEY);
        ranges_opt(ranges, options, RANGES_KEY);
        list_opt(column_families, options, COLUMNFAMILIES_KEY);
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
        // The parsing code above removed from the map options we have parsed.
        // If anything is left there in the end, it's an unsupported option.
        if (!options.empty()) {
            throw std::runtime_error(sprint("unsupported repair options: %s",
                    options));
        }
    }

    static constexpr const char* PRIMARY_RANGE_KEY = "primaryRange";
    static constexpr const char* PARALLELISM_KEY = "parallelism"; // TODO
    static constexpr const char* INCREMENTAL_KEY = "incremental"; // TODO
    static constexpr const char* JOB_THREADS_KEY = "jobThreads"; // TODO
    static constexpr const char* RANGES_KEY = "ranges";
    static constexpr const char* COLUMNFAMILIES_KEY = "columnFamilies"; // TODO
    static constexpr const char* DATACENTERS_KEY = "dataCenters"; // TODO
    static constexpr const char* HOSTS_KEY = "hosts"; // TODO
    static constexpr const char* TRACE_KEY = "trace"; // TODO

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

static int do_repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options_map) {

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

    std::vector<sstring> cfs;
    if (options.column_families.size()) {
        cfs = options.column_families;
    } else {
        cfs = list_column_families(db.local(), keyspace);
    }

    do_with(std::move(ranges), [&db, keyspace, cfs, id] (auto& ranges) {
#if 1
        // repair all the ranges in parallel
        return parallel_for_each(ranges.begin(), ranges.end(), [&db, keyspace, cfs, id] (auto&& range) {
#else
        // repair all the ranges in sequence
        return do_for_each(ranges.begin(), ranges.end(), [&db, keyspace, cfs, id] (auto&& range) {
#endif
            return repair_range(db, keyspace, range, cfs);
        }).then([id] {
            logger.info("repair {} completed sucessfully", id);
            repair_tracker.done(id, true);
        }).handle_exception([id] (std::exception_ptr eptr) {
            logger.info("repair {} failed", id);
            repair_tracker.done(id, false);
        });
    });

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
