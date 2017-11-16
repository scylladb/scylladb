
/*
 * Copyright (C) 2015 ScyllaDB
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


#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "core/reactor.hh"
#include "service/storage_service.hh"
#include "core/distributed.hh"
#include "database.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        sharded<auth::service> auth_service;
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
        service::get_storage_service().start(std::ref(db), std::ref(auth_service)).get();
        db.start().get();
        netw::get_messaging_service().start(gms::inet_address("127.0.0.1")).get();
        gms::get_failure_detector().start().get();

        gms::get_gossiper().start().get();
        gms::get_gossiper().stop().get();
        gms::get_failure_detector().stop().get();
        db.stop().get();
        service::get_storage_service().stop().get();
        netw::get_messaging_service().stop().get();
        locator::i_endpoint_snitch::stop_snitch().get();
    });
}
