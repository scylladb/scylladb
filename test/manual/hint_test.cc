/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>

#include <stdlib.h>
#include <iostream>

#include <seastar/testing/test_case.hh>

#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include "utils/UUID_gen.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/rp_set.hh"

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
            auto uuid = table_id(utils::UUID_gen::get_time_UUID());
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
