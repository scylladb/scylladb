/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "repair.hh"

#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "gms/inet_address.hh"

static logging::logger logger("repair");

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

// Each repair_start() call returns a unique integer which the user can later
// use to follow the status of this repair with the repair_status() function.
static std::atomic<int> next_repair_command {0};

// The repair_tracker tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
// This object is not thread safe, and must be used by only one cpu.
static class {
private:
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id < next_repair_command
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> status;
public:
    void start(int id) {
        status[id] = repair_status::RUNNING;
    }
    void done(int id, bool succeeded) {
        if (succeeded) {
            status.erase(id);
        } else {
            status[id] = repair_status::FAILED;
        }
    }
    repair_status get(int id) {
        if (id >= next_repair_command.load(std::memory_order_relaxed)) {
            throw std::runtime_error(sprint("unknown repair id %d", id));
        }
        auto it = status.find(id);
        if (it == status.end()) {
            return repair_status::SUCCESSFUL;
        } else {
            return it->second;
        }
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


static void do_repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options, int id) {

    logger.info("starting user-requested repair for keyspace {}", keyspace);

    repair_tracker.start(id);

    // If the "ranges" option is not explicitly specified, we repair all the
    // local ranges (the token ranges for which this node holds a replica of).
    // Each of these ranges may have a different set of replicas, so the
    // repair of each range is performed separately with repair_range().
    std::vector<query::range<dht::token>> ranges;
    // FIXME: if the "ranges" options exists, use that instead of
    // get_local_ranges() below. Also, translate the following Origin code:

#if 0
         if (option.isPrimaryRange())
         {
             // when repairing only primary range, neither dataCenters nor hosts can be set
             if (option.getDataCenters().isEmpty() && option.getHosts().isEmpty())
                 option.getRanges().addAll(getPrimaryRanges(keyspace));
                 // except dataCenters only contain local DC (i.e. -local)
             else if (option.getDataCenters().size() == 1 && option.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter()))
                 option.getRanges().addAll(getPrimaryRangesWithinDC(keyspace));
             else
                 throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
         }
         else
#endif
    ranges = get_local_ranges(db.local(), keyspace);

    // FIXME: let the cfs be overriden by an option
    std::vector<sstring> cfs = list_column_families(db.local(), keyspace);

#if 1
    // repair all the ranges in parallel
    auto done = make_lw_shared<semaphore>(0);
    auto success = make_lw_shared<bool>(true);
    for (auto range : ranges) {
        repair_range(db, keyspace, range, cfs).
                handle_exception([success] (std::exception_ptr eptr)
                        { *success = false; }).
                finally([done] { done->signal(); });
    }
    done->wait(ranges.size()).then([done, id, success] {
        logger.info("repair {} complete, success={}", id, success);
        repair_tracker.done(id, *success);
    });
#else
    // repair all the ranges in sequence
    do_with(std::move(ranges), [&db, keyspace, cfs, id] (auto& ranges) {
        return do_for_each(ranges.begin(), ranges.end(), [&db, keyspace, cfs, id] (auto&& range) {
            return repair_range(db, keyspace, range, cfs);
        }).then([id] {
            logger.info("repair {} completed sucessfully", id);
            repair_tracker.done(id, true);
        }).handle_exception([id] (std::exception_ptr eptr) {
            logger.info("repair {} failed", id);
            repair_tracker.done(id, false);
        });
    });
#endif
}

int repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options) {
    int i = next_repair_command++;
    db.invoke_on(0, [i, &db, keyspace = std::move(keyspace), options = std::move(options)] (database& localdb) {
        do_repair_start(db, std::move(keyspace), std::move(options), i);
    }); // Note we ignore the value of this future
    return i;
}

future<repair_status> repair_get_status(seastar::sharded<database>& db, int id) {
    return db.invoke_on(0, [id] (database& localdb) {
        return repair_tracker.get(id);
    });
}
