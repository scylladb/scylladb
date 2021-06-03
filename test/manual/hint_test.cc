/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <stdlib.h>
#include <iostream>
#include <list>
#include <unordered_set>

#include "test/lib/test_services.hh"
#include <seastar/testing/test_case.hh>

#include "test/lib/mutation_source_test.hh"
#include "test/lib/mutation_assertions.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include "utils/UUID_gen.hh"
#include "test/lib/tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/rp_set.hh"
#include "log.hh"
#include "schema.hh"

using namespace db;

static future<> cl_test(commitlog::config cfg, noncopyable_function<future<> (commitlog& log)> f) {
    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    return commitlog::create_commitlog(cfg).then([f = std::move(f)](commitlog log) mutable {
        return do_with(std::move(log), [f = std::move(f)](commitlog& log) {
            return futurize_invoke(f, log).finally([&log] {
                return log.shutdown().then([&log] {
                    return log.clear();
                });
            });
        });
    }).finally([tmp = std::move(tmp)] {
    });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment_custom_prefix){
    commitlog::config cfg;
    cfg.fname_prefix = "HintedLog-0-kaka-";
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        return do_with(rp_set(), [&log](auto& set) {
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([&set]() { return set.size() > 1; }, [&log, &set, uuid]() {
                sstring tmp = "hej bubba cow";
                return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                    dst.write(tmp.data(), tmp.size());
                }).then([&set](rp_handle h) {
                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                    set.put(std::move(h));
                });
            });
        }).then([&log] {
//          std::cout << log.get_active_segment_names() <<std::endl;
            auto n = log.get_active_segment_names().size();
            BOOST_REQUIRE(n > 1);
        });
    });
}
