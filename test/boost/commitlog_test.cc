/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <boost/range/adaptor/map.hpp>

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <deque>

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/closeable.hh>

#include "utils/UUID_gen.hh"
#include "test/lib/tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/rp_set.hh"
#include "log.hh"
#include "service/priority_manager.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/data_model.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/reader_permit.hh"
#include "test/lib/mutation_source_test.hh"

using namespace db;

static future<> cl_test(commitlog::config cfg, noncopyable_function<future<> (commitlog&)> f) {
    // enable as needed.
    // moved from static init because static init fiasco.
#if 0
    logging::logger_registry().set_logger_level("commitlog", logging::log_level::trace);
#endif
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

static future<> cl_test(noncopyable_function<future<> (commitlog&)> f) {
    commitlog::config cfg;
    cfg.metrics_category_name = "commitlog";
    return cl_test(cfg, std::move(f));
}

// just write in-memory...
SEASTAR_TEST_CASE(test_create_commitlog){
    return cl_test([](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.data(), tmp.size());
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    });
        });
}

// check we
SEASTAR_TEST_CASE(test_commitlog_written_to_disk_batch){
    commitlog::config cfg;
    cfg.mode = commitlog::sync_mode::BATCH;
    return cl_test(cfg, [](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.data(), tmp.size());
                    }).then([&log](replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                        auto n = log.get_flush_count();
                        BOOST_REQUIRE(n > 0);
                    });
        });
}

// check that an entry marked as sync is immediately flushed to a storage
SEASTAR_TEST_CASE(test_commitlog_written_to_disk_sync){
    commitlog::config cfg;
    return cl_test(cfg, [](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), db::commitlog::force_sync::yes, [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.data(), tmp.size());
                    }).then([&log](replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                        auto n = log.get_flush_count();
                        BOOST_REQUIRE(n > 0);
                    });
        });
}

// check that an entry marked as sync is immediately flushed to a storage
SEASTAR_TEST_CASE(test_commitlog_written_to_disk_no_sync){
    commitlog::config cfg;
    cfg.commitlog_sync_period_in_ms = 10000000000;
    return cl_test(cfg, [](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.data(), tmp.size());
                    }).then([&log](replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                        auto n = log.get_flush_count();
                        BOOST_REQUIRE(n == 0);
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_written_to_disk_periodic){
    return cl_test([](commitlog& log) {
            auto state = make_lw_shared<bool>(false);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [&log, state, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([&log, state](replay_position rp) {
                                    BOOST_CHECK_NE(rp, db::replay_position());
                                    auto n = log.get_flush_count();
                                    *state = n > 0;
                                });

                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment){
    commitlog::config cfg;
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
            auto n = log.get_active_segment_names().size();
            BOOST_REQUIRE(n > 1);
        });
    });
}

typedef std::vector<sstring> segment_names;

static segment_names segment_diff(commitlog& log, segment_names prev = {}) {
    segment_names now = log.get_active_segment_names();
    segment_names diff;
    // safety fix. We should always get segment names in alphabetical order, but
    // we're not explicitly guaranteed it. Lets sort the sets just to be sure.
    std::sort(now.begin(), now.end());
    std::sort(prev.begin(), prev.end());
    std::set_difference(prev.begin(), prev.end(), now.begin(), now.end(), std::back_inserter(diff));
    return diff;
}

SEASTAR_TEST_CASE(test_commitlog_discard_completed_segments){
    //logging::logger_registry().set_logger_level("commitlog", logging::log_level::trace);
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
            struct state_type {
                std::vector<utils::UUID> uuids;
                std::unordered_map<utils::UUID, db::rp_set> rps;

                mutable size_t index = 0;

                state_type() {
                    for (int i = 0; i < 10; ++i) {
                        uuids.push_back(utils::UUID_gen::get_time_UUID());
                    }
                }
                const utils::UUID & next_uuid() const {
                    return uuids[index++ % uuids.size()];
                }
                bool done() const {
                    return std::any_of(rps.begin(), rps.end(), [](auto& rps) {
                        return rps.second.size() > 1;
                    });
                }
            };

            auto state = make_lw_shared<state_type>();
            return do_until([state]() { return state->done(); },
                    [&log, state]() {
                        sstring tmp = "hej bubba cow";
                        auto uuid = state->next_uuid();
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([state, uuid](db::rp_handle h) {
                                    state->rps[uuid].put(std::move(h));
                                });
                    }).then([&log, state]() {
                        auto names = log.get_active_segment_names();
                        BOOST_REQUIRE(names.size() > 1);
                        // sync all so we have no outstanding async sync ops that
                        // might prevent discard_completed_segments to actually dispose
                        // of clean segments (shared_ptr in task)
                        return log.sync_all_segments().then([&log, state, names] {
                            for (auto & p : state->rps) {
                                log.discard_completed_segments(p.first, p.second);
                            }
                            auto diff = segment_diff(log, names);
                            auto nn = diff.size();
                            auto dn = log.get_num_segments_destroyed();

                            BOOST_REQUIRE(nn > 0);
                            BOOST_REQUIRE(nn <= names.size());
                            BOOST_REQUIRE(dn <= nn);
                        });
                    }).then([&log] {
                        return log.shutdown().then([&log] {
                            return log.list_existing_segments().then([] (auto descs) {
                                BOOST_REQUIRE(descs.empty());
                            });
                        });
                    });
        });
}

SEASTAR_TEST_CASE(test_equal_record_limit){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size();
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    });
        });
}

SEASTAR_TEST_CASE(test_exceed_record_limit){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size() + 1;
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then_wrapped([](future<db::rp_handle> f) {
                        try {
                            f.get();
                        } catch (...) {
                            // ok.
                            return make_ready_future();
                        }
                        throw std::runtime_error("Did not get expected exception from writing too large record");
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_closed) {
    commitlog::config cfg;
    return cl_test(cfg, [](commitlog& log) {
        return log.shutdown().then([&log] {
            sstring tmp = "test321";
            auto uuid = utils::UUID_gen::get_time_UUID();
            return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            }).then_wrapped([] (future<db::rp_handle> f) {
                BOOST_REQUIRE_EXCEPTION(f.get(), gate_closed_exception, exception_predicate::message_equals("gate closed"));
            });
        });
    });
}

SEASTAR_TEST_CASE(test_commitlog_delete_when_over_disk_limit) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 2;
    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 1;
    cfg.commitlog_sync_period_in_ms = 1;
    return cl_test(cfg, [](commitlog& log) {
            auto sem = make_lw_shared<semaphore>(0);
            auto segments = make_lw_shared<segment_names>();

            // add a flush handler that simply says we're done with the range.
            auto r = log.add_flush_handler([&log, sem, segments](cf_id_type id, replay_position pos) {
                auto f = make_ready_future<>();
                // #6195 only get segment list at first callback. We can (not often)
                // be called again, but reading segment list at that point might (will)
                // render same list as in the diff check below. 
                if (segments->empty()) {
                    *segments = log.get_active_segment_names();
                    // Verify #5899 - file size should not exceed the config max. 
                    f = parallel_for_each(*segments, [](sstring filename) {
                        return file_size(filename).then([](uint64_t size) {
                            BOOST_REQUIRE_LE(size, max_size_mb * 1024 * 1024);
                        });
                    });
                }
                return f.then([&log, sem, id] {
                    log.discard_completed_segments(id);
                    sem->signal();
                });
            });

            auto set = make_lw_shared<std::set<segment_id_type>>();
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([set, sem]() {return set->size() > 2 && sem->try_wait();},
                    [&log, set, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([set](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    set->insert(h.release().id);
                                });
                    }).then([&log, segments]() {
                        auto diff = segment_diff(log, *segments);
                        auto nn = diff.size();
                        auto dn = log.get_num_segments_destroyed();

                        BOOST_REQUIRE(nn > 0);
                        BOOST_REQUIRE(nn <= segments->size());
                        BOOST_REQUIRE(dn <= nn);
                    }).finally([r = std::move(r)] {
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_reader){
    static auto count_mutations_in_segment = [] (sstring path) -> future<size_t> {
        auto count = make_lw_shared<size_t>(0);
        return db::commitlog::read_log_file(path, db::commitlog::descriptor::FILENAME_PREFIX, service::get_local_commitlog_priority(), [count](db::commitlog::buffer_and_replay_position buf_rp) {
            auto&& [buf, rp] = buf_rp;
            auto linearization_buffer = bytes_ostream();
            auto in = buf.get_istream();
            auto str = to_sstring_view(in.read_bytes_view(buf.size_bytes(), linearization_buffer));
            BOOST_CHECK_EQUAL(str, "hej bubba cow");
            (*count)++;
            return make_ready_future<>();
        }).then([count] {
            return *count;
        });
    };
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
            auto set = make_lw_shared<rp_set>();
            auto count = make_lw_shared<size_t>(0);
            auto count2 = make_lw_shared<size_t>(0);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([count, set]() {return set->size() > 1;},
                    [&log, uuid, count, set]() {
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([&log, set, count](auto h) {
                                    BOOST_CHECK_NE(db::replay_position(), h.rp());
                                    set->put(std::move(h));
                                    if (set->size() == 1) {
                                        ++(*count);
                                    }
                                });

                    }).then([&log, set, count2]() {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(segments.size() > 1);

                        auto ids = boost::copy_range<std::vector<segment_id_type>>(set->usage() | boost::adaptors::map_keys);
                        std::sort(ids.begin(), ids.end());
                        auto id = ids.front();
                        auto i = std::find_if(segments.begin(), segments.end(), [id](sstring filename) {
                            commitlog::descriptor desc(filename, db::commitlog::descriptor::FILENAME_PREFIX);
                            return desc.id == id;
                        });
                        if (i == segments.end()) {
                            throw std::runtime_error("Did not find expected log file");
                        }
                        return *i;
                    }).then([&log, count] (sstring segment_path) {
                        // Check reading from an unsynced segment
                        return count_mutations_in_segment(segment_path).then([count] (size_t replay_count) {
                            BOOST_CHECK_GE(*count, replay_count);
                        }).then([&log, count, segment_path] {
                            return log.sync_all_segments().then([count, segment_path] {
                                // Check reading from a synced segment
                                return count_mutations_in_segment(segment_path).then([count] (size_t replay_count) {
                                    BOOST_CHECK_EQUAL(*count, replay_count);
                                });
                            });
                        });
                    });
        });
}

static future<> corrupt_segment(sstring seg, uint64_t off, uint32_t value) {
    return open_file_dma(seg, open_flags::rw).then([off, value](file f) {
        size_t size = align_up<size_t>(off, 4096);
        return do_with(std::move(f), [size, off, value](file& f) {
            return f.dma_read_exactly<char>(0, size).then([&f, off, value](auto buf) {
                *unaligned_cast<uint32_t *>(buf.get_write() + off) = value;
                auto dst = buf.get();
                auto size = buf.size();
                return f.dma_write(0, dst, size).then([buf = std::move(buf)](size_t) {});
            }).finally([&f] {
                return f.close();
            });
        });
    });
}

SEASTAR_TEST_CASE(test_commitlog_entry_corruption){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        auto rps = make_lw_shared<std::vector<db::replay_position>>();
        return do_until([rps]() {return rps->size() > 1;},
                    [&log, rps]() {
                        auto uuid = utils::UUID_gen::get_time_UUID();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([&log, rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log, rps] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        return corrupt_segment(seg, rps->at(1).pos + 4, 0x451234ab).then([seg, rps, &log] {
                            return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, service::get_local_commitlog_priority(), [rps](db::commitlog::buffer_and_replay_position buf_rp) {
                                auto&& [buf, rp] = buf_rp;
                                BOOST_CHECK_EQUAL(rp, rps->at(0));
                                return make_ready_future<>();
                            }).then_wrapped([](auto&& f) {
                                try {
                                    f.get();
                                    BOOST_FAIL("Expected exception");
                                } catch (commitlog::segment_data_corruption_error& e) {
                                    // ok.
                                    BOOST_REQUIRE(e.bytes() > 0);
                                }
                            });
                        });
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_chunk_corruption){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        auto rps = make_lw_shared<std::vector<db::replay_position>>();
        return do_until([rps]() {return rps->size() > 1;},
                    [&log, rps]() {
                        auto uuid = utils::UUID_gen::get_time_UUID();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([&log, rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log, rps] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        return corrupt_segment(seg, rps->at(0).pos - 4, 0x451234ab).then([seg, rps, &log] {
                            return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, service::get_local_commitlog_priority(), [rps](db::commitlog::buffer_and_replay_position buf_rp) {
                                BOOST_FAIL("Should not reach");
                                return make_ready_future<>();
                            }).then_wrapped([](auto&& f) {
                                try {
                                    f.get();
                                    BOOST_FAIL("Expected exception");
                                } catch (commitlog::segment_data_corruption_error& e) {
                                    // ok.
                                    BOOST_REQUIRE(e.bytes() > 0);
                                }
                            });
                        });
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_reader_produce_exception){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        auto rps = make_lw_shared<std::vector<db::replay_position>>();
        return do_until([rps]() {return rps->size() > 1;},
                    [&log, rps]() {
                        auto uuid = utils::UUID_gen::get_time_UUID();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([&log, rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, service::get_local_commitlog_priority(), [](db::commitlog::buffer_and_replay_position buf_rp) {
                            return make_exception_future(std::runtime_error("I am in a throwing mode"));
                        }).then_wrapped([](auto&& f) {
                            try {
                                f.get();
                                BOOST_FAIL("Expected exception");
                            } catch (std::runtime_error&) {
                                // Ok
                            } catch (...) {
                                // ok.
                                BOOST_FAIL("Wrong exception");
                            }
                        });
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_counters) {
    auto count_cl_counters = []() -> size_t {
        auto ids = scollectd::get_collectd_ids();
        return std::count_if(ids.begin(), ids.end(), [](const scollectd::type_instance_id& id) {
            return id.plugin() == "commitlog";
        });
    };
    BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    return cl_test([count_cl_counters](commitlog& log) {
        BOOST_CHECK_GT(count_cl_counters(), 0);
        return make_ready_future<>();
    }).finally([count_cl_counters] {
        BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    });
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR

SEASTAR_TEST_CASE(test_allocation_failure){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size() - 1;

            auto junk = make_lw_shared<std::list<std::unique_ptr<char[]>>>();

            // Use us loads of memory so we can OOM at the appropriate place
            try {
                assert(fragmented_temporary_buffer::default_fragment_size < size);
                for (;;) {
                    junk->emplace_back(new char[fragmented_temporary_buffer::default_fragment_size]);
                }
            } catch (std::bad_alloc&) {
            }
            auto last = junk->end();
            junk->erase(--last);
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then_wrapped([junk, size](future<db::rp_handle> f) {
                        std::exception_ptr ep;
                        try {
                            f.get();
                            throw std::runtime_error(format("Adding mutation of size {} succeeded unexpectedly", size));
                        } catch (std::bad_alloc&) {
                            // ok. this is what we expected
                            junk->clear();
                            return make_ready_future();
                        } catch (...) {
                            ep = std::current_exception();
                        }
                        throw std::runtime_error(format("Got an unexpected exception from writing too large record: {}", ep));
                    });
        });
}

#endif

SEASTAR_TEST_CASE(test_commitlog_replay_invalid_key){
    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.execute_cql("create table t (pk text primary key, v text)").get();

        auto& db = env.local_db();
        auto& table = db.find_column_family("ks", "t");
        auto& cl = *table.commitlog();
        auto s = table.schema();
        auto& mt = table.active_memtable();

        auto add_entry = [&db, &cl, s] (bytes key) mutable {
            auto md = tests::data_model::mutation_description({ key });
            md.add_clustered_cell({}, "v", to_bytes("val"));
            auto m = md.build(s);

            auto fm = freeze(m);
            commitlog_entry_writer cew(s, fm, db::commitlog::force_sync::yes);
            cl.add_entry(m.column_family_id(), cew, db::no_timeout).get();
            return db.shard_of(m);
        };

        const auto shard = add_entry(bytes{});
        auto pk1_raw = make_key_for_shard(shard, s);

        add_entry(to_bytes(pk1_raw));

        BOOST_REQUIRE(mt.empty());

        {
            auto paths = cl.get_active_segment_names();
            BOOST_REQUIRE(!paths.empty());
            auto rp = db::commitlog_replayer::create_replayer(env.db()).get0();
            rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX).get();
        }

        {
            auto rd = mt.make_flat_reader(s, tests::make_permit());
            auto close_rd = deferred_close(rd);
            auto mopt = read_mutation_from_flat_mutation_reader(rd, db::no_timeout).get0();
            BOOST_REQUIRE(mopt);

            mopt = {};
            mopt = read_mutation_from_flat_mutation_reader(rd, db::no_timeout).get0();
            BOOST_REQUIRE(!mopt);
        }
    });
}

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_commitlog_add_entries) {
    return cl_test([](commitlog& log) {
        return seastar::async([&] {
            using force_sync = commitlog_entry_writer::force_sync;

            constexpr auto n = 10;
            for (auto fs : { force_sync(false), force_sync(true) }) {
                std::vector<commitlog_entry_writer> writers;
                std::vector<frozen_mutation> mutations;
                std::vector<replay_position> rps;

                writers.reserve(n);
                mutations.reserve(n);
                
                for (auto i = 0; i < n; ++i) {
                    random_mutation_generator gen(random_mutation_generator::generate_counters(false));
                    mutations.emplace_back(gen(1).front());
                    writers.emplace_back(gen.schema(), mutations.back(), fs);
                }

                auto res = log.add_entries(writers, db::timeout_clock::now() + 60s).get0();

                std::set<segment_id_type> ids;
                for (auto& h : res) {
                    ids.emplace(h.rp().id);
                    rps.emplace_back(h.rp());
                }
                BOOST_CHECK_EQUAL(ids.size(), 1);

                log.sync_all_segments().get();
                auto segments = log.get_active_segment_names();
                BOOST_REQUIRE(!segments.empty());

                std::unordered_set<replay_position> result;

                for (auto& seg : segments) {
                    db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, service::get_local_commitlog_priority(), [&](db::commitlog::buffer_and_replay_position buf_rp) {
                        commitlog_entry_reader r(buf_rp.buffer);
                        auto& rp = buf_rp.position;
                        auto i = std::find(rps.begin(), rps.end(), rp);
                        // since we are looping, we can be reading last test cases 
                        // segment (force_sync permutations)
                        if (i != rps.end()) {
                            auto n = std::distance(rps.begin(), i);
                            auto& fm1 = mutations.at(n);
                            auto& fm2 = r.mutation();
                            auto s = writers.at(n).schema();
                            auto m1 = fm1.unfreeze(s);
                            auto m2 = fm2.unfreeze(s);
                            BOOST_CHECK_EQUAL(m1, m2);
                            result.emplace(rp);
                        }
                        return make_ready_future<>();
                    }).get();
                }

                BOOST_CHECK_EQUAL(result.size(), rps.size());
            }
        });
    });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment_odsync){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.use_o_dsync = true;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = utils::UUID_gen::get_time_UUID();
        rp_set set;
        while (set.size() <= 1) {
            sstring tmp = "hej bubba cow";
            rp_handle h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            });
            set.put(std::move(h));
        }

        auto names = log.get_active_segment_names();
        BOOST_REQUIRE(names.size() > 1);

        // check that all the segments are pre-allocated.
        for (auto& name : names) {
            auto f = co_await seastar::open_file_dma(name, seastar::open_flags::ro);
            auto s = co_await f.size();
            co_await f.close();
            BOOST_CHECK_EQUAL(s, 1024u*1024u);
        }
    });
}

// Test for #8363
// try to provoke edge case where we race segment deletion
// and waiting for recycled to be replenished.
SEASTAR_TEST_CASE(test_commitlog_deadlock_in_recycle) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 2;
    cfg.commitlog_segment_size_in_mb = max_size_mb;
    // ensure total size per shard is not multiple of segment size.
    cfg.commitlog_total_space_in_mb = 5 * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.reuse_segments = true;
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    std::deque<rp_set> queue;
    size_t n = 0;

    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = utils::UUID_gen::get_time_UUID();
    auto size = log.max_record_size() / 2;

    timer<> t;
    t.set_callback([&] {
        while (!queue.empty()) {
            auto flush = std::move(queue.front());
            queue.pop_front();
            log.discard_completed_segments(uuid, flush);
            ++n;
        };
    });

    // add a flush handler that delays releasing things until disk threshold is reached.
    auto r = log.add_flush_handler([&](cf_id_type, replay_position pos) {
        auto old = std::exchange(rps, rp_set{});
        queue.emplace_back(std::move(old));
        if (log.disk_footprint() >= log.disk_limit() && !t.armed()) {            
            t.arm(5s);
        }
    });

    bool release = true;

    try {
        while (n < 10) {
            auto now = timeout_clock::now();            
            rp_handle h = co_await with_timeout(now + 30s, log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.fill('1', size);
            }));
            rps.put(std::move(h));
        }
    } catch (timed_out_error&) {
        BOOST_FAIL("log write timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
        release = false;
    }

    if (release) {  
        co_await log.shutdown();
        co_await log.clear();
    }
}

// Test for #8438 - ensure we can shut down (in orderly fashion)
// even if CL write is stuck waiting for segment recycle/alloc
SEASTAR_TEST_CASE(test_commitlog_shutdown_during_wait) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 2;
    cfg.commitlog_segment_size_in_mb = max_size_mb;
    // ensure total size per shard is not multiple of segment size.
    cfg.commitlog_total_space_in_mb = 5 * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.reuse_segments = true;
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    std::deque<rp_set> queue;
    size_t n = 0;

    // uncomment for verbosity
    //logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = utils::UUID_gen::get_time_UUID();
    auto size = log.max_record_size() / 2;

    // add a flush handler that does not.
    auto r = log.add_flush_handler([&](cf_id_type, replay_position pos) {
        auto old = std::exchange(rps, rp_set{});
        queue.emplace_back(std::move(old));
    });

    for (;;) {
        try {
            auto now = timeout_clock::now();
            rp_handle h = co_await with_timeout(now + 10s, log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.fill('1', size);
            }));
            rps.put(std::move(h));
        } catch (timed_out_error&) {
            if (log.disk_footprint() >= log.disk_limit()) {
                // now a segment alloc is waiting.
                break;
            }
        }
    }

    // shut down is assumed to 
    // a.) stop allocating
    // b.) ensure all segments get's free:d
    while (!queue.empty()) {
        auto flush = std::move(queue.front());
        queue.pop_front();
        log.discard_completed_segments(uuid, flush);
    }

    try {
        auto now = timeout_clock::now();
        co_await with_timeout(now + 30s, log.shutdown());
        co_await log.clear();
    } catch (timed_out_error&) {
        BOOST_FAIL("log shutdown timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
    }
}
