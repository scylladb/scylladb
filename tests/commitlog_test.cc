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
#include <boost/range/adaptor/map.hpp>

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <set>

#include "tests/test-utils.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/scollectd_api.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "utils/UUID_gen.hh"
#include "tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/rp_set.hh"
#include "log.hh"
#include "service/priority_manager.hh"

using namespace db;

template<typename Func>
static future<> cl_test(commitlog::config cfg, Func && f) {
    tmpdir tmp;
    cfg.commit_log_location = tmp.path;
    return commitlog::create_commitlog(cfg).then([f = std::forward<Func>(f)](commitlog log) mutable {
        return do_with(std::move(log), [f = std::forward<Func>(f)](commitlog& log) {
            return futurize_apply(f, log).finally([&log] {
                return log.shutdown().then([&log] {
                    return log.clear();
                });
            });
        });
    }).finally([tmp = std::move(tmp)] {
    });
}

template<typename Func>
static future<> cl_test(Func && f) {
    commitlog::config cfg;
    cfg.metrics_category_name = "commitlog";
    return cl_test(cfg, std::forward<Func>(f));
}

#if 0
static int loggo = [] {
        logging::logger_registry().set_logger_level("commitlog", logging::log_level::trace);
        return 0;
}();
#endif

// just write in-memory...
SEASTAR_TEST_CASE(test_create_commitlog){
    return cl_test([](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), [tmp](db::commitlog::output& dst) {
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
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), tmp.size(), [tmp](db::commitlog::output& dst) {
                        dst.write(tmp.data(), tmp.size());
                    }).then([&log](replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                        auto n = log.get_flush_count();
                        BOOST_REQUIRE(n > 0);
                    });
        });
}

SEASTAR_TEST_CASE(test_commitlog_written_to_disk_periodic){
    return cl_test([](commitlog& log) {
            auto state = make_lw_shared(false);
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([state]() {return *state;},
                    [&log, state, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    });
        });
}

SEASTAR_TEST_CASE(test_exceed_record_limit){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size() + 1;
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, [size](db::commitlog::output& dst) {
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

SEASTAR_TEST_CASE(test_commitlog_delete_when_over_disk_limit) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 2;
    cfg.commitlog_total_space_in_mb = 1;
    cfg.commitlog_sync_period_in_ms = 1;
    return cl_test(cfg, [](commitlog& log) {
            auto sem = make_lw_shared<semaphore>(0);
            auto segments = make_lw_shared<segment_names>();

            // add a flush handler that simply says we're done with the range.
            auto r = log.add_flush_handler([&log, sem, segments](cf_id_type id, replay_position pos) {
                *segments = log.get_active_segment_names();
                log.discard_completed_segments(id);
                sem->signal();
            });

            auto set = make_lw_shared<std::set<segment_id_type>>();
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([set, sem]() {return set->size() > 2 && sem->try_wait();},
                    [&log, set, uuid]() {
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
        return db::commitlog::read_log_file(path, service::get_local_commitlog_priority(), [count](temporary_buffer<char> buf, db::replay_position rp) {
            sstring str(buf.get(), buf.size());
            BOOST_CHECK_EQUAL(str, "hej bubba cow");
            (*count)++;
            return make_ready_future<>();
        }).then([](auto s) {
            return do_with(std::move(s), [](auto& s) {
                return s->done();
            });
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
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                            return db::commitlog::read_log_file(seg, service::get_local_commitlog_priority(), [rps](temporary_buffer<char> buf, db::replay_position rp) {
                                BOOST_CHECK_EQUAL(rp, rps->at(0));
                                return make_ready_future<>();
                            }).then([](auto s) {
                                return do_with(std::move(s), [](auto& s) {
                                    return s->done();
                                });
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
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                            return db::commitlog::read_log_file(seg, service::get_local_commitlog_priority(), [rps](temporary_buffer<char> buf, db::replay_position rp) {
                                BOOST_FAIL("Should not reach");
                                return make_ready_future<>();
                            }).then([](auto s) {
                                return do_with(std::move(s), [](auto& s) {
                                    return s->done();
                                });
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
                        return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
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
                        return db::commitlog::read_log_file(seg, service::get_local_commitlog_priority(), [](temporary_buffer<char> buf, db::replay_position rp) {
                            return make_exception_future(std::runtime_error("I am in a throwing mode"));
                        }).then([](auto s) {
                            return do_with(std::move(s), [](auto& s) {
                                return s->done();
                            });
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
                for (;;) {
                    junk->emplace_back(new char[size]);
                }
            } catch (std::bad_alloc&) {
            }
            return log.add_mutation(utils::UUID_gen::get_time_UUID(), size, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then_wrapped([junk](future<db::rp_handle> f) {
                        try {
                            f.get();
                        } catch (std::bad_alloc&) {
                            // ok. this is what we expected
                            junk->clear();
                            return make_ready_future();
                        } catch (...) {
                        }
                        throw std::runtime_error("Did not get expected exception from writing too large record");
                    });
        });
}

#endif
