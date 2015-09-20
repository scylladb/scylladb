
/*
 * Copyright 2015 Cloudius Systems
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
