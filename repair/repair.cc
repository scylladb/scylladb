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
// use to follow the status of this repair.
static std::atomic<int> next_repair_command {0};

// repair_start() can run on any cpu; It runs on cpu0 the function
// do_repair_start(). The benefit of always running that function on the same
// CPU is that it allows us to keep some state (like a list of ongoing
// repairs). It is fine to always do this on one CPU, because the function
// itself does very little (mainly tell other nodes and CPUs what to do).

// Repair a single range. Comparable to RepairSession in Origin
// In Origin, this is composed of several "repair jobs", each with one cf,
// but our streaming already works for several cfs.
static void repair_range(seastar::sharded<database>& db, sstring keyspace,
        query::range<dht::token> range, std::vector<sstring> cfs) {
    auto sp = streaming::stream_plan("repair");
    auto id = utils::UUID_gen::get_time_UUID();

    auto neighbors = get_neighbors(db.local(), keyspace, range);
    logger.info("[repair #{}] new session: will sync {} on range {} for {}.{}", id, neighbors, range, keyspace, cfs);
    for (auto peer : neighbors) {
        sp.transfer_ranges(peer, peer, keyspace, {range}, cfs);
        sp.request_ranges(peer, peer, keyspace, {range}, cfs);
        sp.execute(); // FIXME: use future return value, and handle errors
    }
}

static void do_repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options) {

    logger.info("starting user-requested repair for keyspace {}", keyspace);

    // FIXME: ranges and column families should be overriden by options, and these are defaults:
    std::vector<query::range<dht::token>> ranges = {query::range<dht::token>::make_open_ended_both_sides()};
    std::vector<sstring> cfs = list_column_families(db.local(), keyspace);

    for (auto range : ranges) {
        repair_range(db, keyspace, range, cfs);
    }
}

int repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options) {
    int i = next_repair_command++;
    db.invoke_on(0, [&db, keyspace = std::move(keyspace), options = std::move(options)] (database& localdb) {
        do_repair_start(db, std::move(keyspace), std::move(options));
    }); // Note we ignore the value of this future
    return i;
}
