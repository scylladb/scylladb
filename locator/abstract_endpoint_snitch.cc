/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "abstract_endpoint_snitch.hh"

namespace locator {

i_endpoint_snitch::~i_endpoint_snitch() {};

std::vector<inet_address> abstract_endpoint_snitch::get_sorted_list_by_proximity(inet_address address, std::unordered_set<inet_address>& unsorted_address)
{
    std::vector<inet_address> preferred(unsorted_address.begin(), unsorted_address.end());
    sort_by_proximity(address, preferred);
    return preferred;
}

void abstract_endpoint_snitch::sort_by_proximity(inet_address address, std::vector<inet_address>& addresses)
{
    std::sort(addresses.begin(), addresses.end(), [this, &address](inet_address& a1, inet_address& a2) {
       return compare_endpoints(address, a1, a2) < 0;
    });
}

void abstract_endpoint_snitch::gossiper_starting()
{
    // noop by default
}

const bool abstract_endpoint_snitch::is_worth_merging_for_range_query(std::vector<inet_address>& merged, std::vector<inet_address>& l1, std::vector<inet_address>& l2) const
{
    // Querying remote DC is likely to be an order of magnitude slower than
    // querying locally, so 2 queries to local nodes is likely to still be
    // faster than 1 query involving remote ones
    bool merged_has_remote = has_remote_node(merged);
    return merged_has_remote
         ? has_remote_node(l1) || has_remote_node(l2)
         : true;
}

const bool abstract_endpoint_snitch::has_remote_node(std::vector<inet_address>& l) const
{
#if 0
    sstring local_dc = DatabaseDescriptor.getLocalDataCenter();
    for (InetAddress ep : l)
    {
        if (!localDc.equals(getDatacenter(ep)))
            return true;
    }
#endif
    return false;
}

}
