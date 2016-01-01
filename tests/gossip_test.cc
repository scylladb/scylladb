
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
#include "service/pending_range_calculator_service.hh"
#include "service/storage_service.hh"
#include "core/distributed.hh"
#include "database.hh"

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        service::get_pending_range_calculator_service().start(std::ref(db));
        service::get_storage_service().start(std::ref(db)).get();
        db.start().get();
        net::get_messaging_service().start(gms::inet_address("127.0.0.1")).get();
        gms::get_failure_detector().start().get();

        gms::get_gossiper().start().get();
        gms::get_gossiper().stop().get();
        gms::get_failure_detector().stop().get();
        net::get_messaging_service().stop().get();
        db.stop().get();
        service::get_storage_service().stop().get();
        service::get_pending_range_calculator_service().stop().get();
    });
}
