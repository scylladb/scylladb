#include "locator/types.hh"
#include "locator/topology.hh"
#include "utils/fb_utilities.hh"
#include "to_string.hh"

#include <seastar/testing/thread_test_case.hh>

using namespace locator;

SEASTAR_THREAD_TEST_CASE(test_remove_endpoint) {
    using dc_endpoints_t = std::unordered_map<sstring, std::unordered_set<inet_address>>;
    using dc_racks_t = std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>>;
    using dcs_t = std::unordered_set<sstring>;

    const auto ep1 = gms::inet_address("127.0.0.1");
    const auto ep2 = gms::inet_address("127.0.0.2");
    const auto dc_rack1 = endpoint_dc_rack {
        .dc = "dc1",
        .rack = "rack1"
    };
    const auto dc_rack2 = endpoint_dc_rack {
        .dc = "dc1",
        .rack = "rack2"
    };

    utils::fb_utilities::set_broadcast_address(ep1);
    topology::config cfg = {
        .local_dc_rack = dc_rack1
    };

    auto topo = topology(cfg);

    topo.update_endpoint(ep1, dc_rack1);
    topo.update_endpoint(ep2, dc_rack2);

    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {ep1, ep2}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {ep1}}, {"rack2", {ep2}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    topo.remove_endpoint(ep2);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{{"dc1", {ep1}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{{"dc1", {{"rack1", {ep1}}}}}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{"dc1"}));

    topo.remove_endpoint(ep1);
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_endpoints(), (dc_endpoints_t{}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenter_racks(), (dc_racks_t{}));
    BOOST_REQUIRE_EQUAL(topo.get_datacenters(), (dcs_t{}));
}