#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "core/reactor.hh"

SEASTAR_TEST_CASE(test_boot_shutdown){
    return net::get_messaging_service().start(gms::inet_address("127.0.0.1")).then( [] () {
        return gms::get_failure_detector().start().then([] {
            return gms::get_gossiper().start().then([] {
                return gms::get_gossiper().stop().then( [] (){
                    return gms::get_failure_detector().stop().then( [] (){
                        return net::get_messaging_service().stop().then ( [] () {
                            return make_ready_future<>();
                        });
                    });
                });
            });
        });
    });
}
