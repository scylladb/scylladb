/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <deque>

#include <fmt/ranges.h>

#include "test/lib/scylla_test_case.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/closeable.hh>

#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "test/lib/tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "db/commitlog/rp_set.hh"
#include "db/extensions.hh"
#include "readers/combined.hh"
#include "log.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/data_model.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/test_utils.hh"

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

static table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

// just write in-memory...
SEASTAR_TEST_CASE(test_create_commitlog){
    return cl_test([](commitlog& log) {
            sstring tmp = "hej bubba cow";
            return log.add_mutation(make_table_id(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
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
            return log.add_mutation(make_table_id(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
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
            return log.add_mutation(make_table_id(), tmp.size(), db::commitlog::force_sync::yes, [tmp](db::commitlog::output& dst) {
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
            return log.add_mutation(make_table_id(), tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
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
            auto uuid = make_table_id();
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
            auto uuid = make_table_id();
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
                std::vector<table_id> uuids;
                std::unordered_map<table_id, db::rp_set> rps;

                mutable size_t index = 0;

                state_type() {
                    for (int i = 0; i < 10; ++i) {
                        uuids.push_back(make_table_id());
                    }
                }
                const table_id& next_uuid() const {
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
                                BOOST_CHECK_EQUAL(descs, decltype(descs){});
                            });
                        });
                    });
        });
}

SEASTAR_TEST_CASE(test_equal_record_limit){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size();
            return log.add_mutation(make_table_id(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
                        dst.fill(char(1), size);
                    }).then([](db::replay_position rp) {
                        BOOST_CHECK_NE(rp, db::replay_position());
                    });
        });
}

SEASTAR_TEST_CASE(test_exceed_record_limit){
    return cl_test([](commitlog& log) {
            auto size = log.max_record_size() + 1;
            return log.add_mutation(make_table_id(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
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
            auto uuid = make_table_id();
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
            auto segments = make_lw_shared<std::set<sstring>>();

            // add a flush handler that simply says we're done with the range.
            auto r = log.add_flush_handler([&log, sem, segments](cf_id_type id, replay_position pos) {
                auto active_segments = log.get_active_segment_names();
                for (auto&& s : active_segments) {
                    segments->insert(s);
                }

                // Verify #5899 - file size should not exceed the config max.
                return parallel_for_each(active_segments, [](sstring filename) {
                    return file_size(filename).then([](uint64_t size) {
                        BOOST_REQUIRE_LE(size, max_size_mb * 1024 * 1024);
                    });
                }).then([&log, sem, id] {
                    log.discard_completed_segments(id);
                    sem->signal();
                });
            });

            auto set = make_lw_shared<std::set<segment_id_type>>();
            auto uuid = make_table_id();
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
                        segment_names names(segments->begin(), segments->end());
                        auto diff = segment_diff(log, names);
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
        size_t count = 0;
        try {
            co_await db::commitlog::read_log_file(path, db::commitlog::descriptor::FILENAME_PREFIX, [&count](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                auto&& [buf, rp] = buf_rp;
                auto linearization_buffer = bytes_ostream();
                auto in = buf.get_istream();
                auto str = to_sstring_view(in.read_bytes_view(buf.size_bytes(), linearization_buffer));
                BOOST_CHECK_EQUAL(str, "hej bubba cow");
                count++;
                co_return;
            });
        } catch (commitlog::segment_truncation&) {
            // ok. this does not ensure to have fully synced segments
            // before reading them in all code paths. We can end up
            // hitting (premature) eof or even half-written page.
        }
        co_return count;
    };

    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) -> future<> {
        rp_set set;
        size_t count = 0;
        auto uuid = make_table_id();
        sstring tmp = "hej bubba cow";

        while (set.size() < 2) {
            auto h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            });
            BOOST_CHECK_NE(db::replay_position(), h.rp());
            set.put(std::move(h));
            if (set.size() == 1) {
                ++count;
            }
        }

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(segments.size() > 1);

        auto ids = boost::copy_range<std::vector<segment_id_type>>(set.usage() | boost::adaptors::map_keys);
        std::sort(ids.begin(), ids.end());
        auto id = ids.front();
        auto i = std::find_if(segments.begin(), segments.end(), [id](sstring filename) {
            commitlog::descriptor desc(filename, db::commitlog::descriptor::FILENAME_PREFIX);
            return desc.id == id;
        });
        if (i == segments.end()) {
            throw std::runtime_error("Did not find expected log file");
        }
        sstring segment_path = *i;

        // Check reading from an unsynced segment
        auto replay_count = co_await count_mutations_in_segment(segment_path);
        BOOST_CHECK_GE(count, replay_count);

        co_await log.sync_all_segments();
        // Check reading from a synced segment
        auto replay_count2 = co_await count_mutations_in_segment(segment_path);
        BOOST_CHECK_EQUAL(count, replay_count2);
    });
}

static future<> corrupt_segment(sstring seg, uint64_t off, uint32_t value) {
    return open_file_dma(seg, open_flags::rw).then([off, value](file f) {
        size_t size = align_up<size_t>(off, 4096);
        return do_with(std::move(f), [size, off, value](file& f) {
            return f.dma_read_exactly<char>(0, size).then([&f, off, value](auto buf) {
                std::copy_n(reinterpret_cast<const char*>(&value), sizeof(value), buf.get_write() + off);
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
                        auto uuid = make_table_id();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log, rps] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        return corrupt_segment(seg, rps->at(1).pos + 4, 0x451234ab).then([seg, rps] {
                            return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [rps](db::commitlog::buffer_and_replay_position buf_rp) {
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
                        auto uuid = make_table_id();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log, rps] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        auto cpos = rps->at(0).pos - 4;
                        return corrupt_segment(seg, cpos, 0x451234ab).then([seg, rps] {
                            return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [rps](db::commitlog::buffer_and_replay_position buf_rp) {
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


SEASTAR_TEST_CASE(test_commitlog_chunk_corruption2){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        auto rps = make_lw_shared<std::vector<db::replay_position>>();
        // write enough entries to fill more than one chunk
        return do_until([rps]() {return rps->size() > (128*1024/8);},
                    [&log, rps]() {
                        auto uuid = make_table_id();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log, rps] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        auto desc = commitlog::descriptor(seg);
                        auto e = std::find_if(rps->begin(), rps->end(), [desc](auto& rp) {
                            return rp.id != desc.id;
                        });
                        auto idx = std::distance(rps->begin(), e);
                        auto cpos = rps->at(idx/2).pos;

                        return corrupt_segment(seg, cpos, 0x451234ab).then([seg, rps, cpos] {
                            return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [rps, cpos](db::commitlog::buffer_and_replay_position buf_rp) {
                                BOOST_CHECK_NE(buf_rp.position.pos, cpos);
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


SEASTAR_TEST_CASE(test_commitlog_chunk_corruption3){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 2 * smp::count;
    cfg.allow_going_over_size_limit = false;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        sstring tmp = "hej bubba cow";

        std::optional<db::segment_id_type> last;
        db::replay_position last_rp;

        int n = 0;

        for (;;) {
            auto h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            });
            BOOST_CHECK_NE(h.rp(), db::replay_position());

            // release data immediately
            auto rp = h.rp();
            auto id = rp.id;

            if (last && last != id) {
                // this should mean we are in a recycled segment.
                if (++n > 3) {
                    last_rp = h.release(); // prevent removal for now.
                    break;
                }
            }
            last = id;
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        for (auto& seg : segments) {
            auto desc = commitlog::descriptor(seg);
            if (desc.id == last_rp.id) {
                try {
                    co_await db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
                        BOOST_CHECK_LE(buf_rp.position, last_rp);
                        return make_ready_future<>();
                    });
                    BOOST_FAIL("Expected exception");
                } catch (commitlog::segment_truncation& e) {
                    // ok.
                }
                co_return;
            }
        }

        BOOST_FAIL("Did not find segment");
    });
}

SEASTAR_TEST_CASE(test_commitlog_replay_single_large_mutation){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 4;
    cfg.commitlog_total_space_in_mb = 2 * cfg.commitlog_segment_size_in_mb * smp::count;
    cfg.allow_going_over_size_limit = false;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto size = log.max_record_size();

        auto buf = fragmented_temporary_buffer::allocate_to_fit(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<char> dist;

        auto out = buf.get_ostream();
        for (size_t i = 0; i < size; ++i) {
            auto c = dist(gen);
            out.write(&c, 1);
        }

        auto h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
            for (auto& tmp : buf) {
                dst.write(tmp.get(), tmp.size());
            }
        });

        auto rp = h.release();

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        for (auto& seg : segments) {
            auto desc = commitlog::descriptor(seg);
            if (desc.id == rp.id) {
                co_await db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
                    BOOST_CHECK_EQUAL(buf_rp.position, rp);
                    auto& rp_buf = buf_rp.buffer;
                    auto in1 = buf.get_istream();
                    auto in2 = rp_buf.get_istream();
                    for (size_t i = 0; i < size; ++i) {
                        auto c1 = in1.read<char>();
                        auto c2 = in2.read<char>();
                        BOOST_CHECK_EQUAL(c1, c2);
                    }
                    return make_ready_future<>();
                });
                co_return;
            }
        }

        BOOST_FAIL("Did not find segment");
    });
}

/**
 * Checks same thing as above, but will also ensure the seek mechanism in 
 * replayer is working, since we will span multiple chunks.
 */
SEASTAR_TEST_CASE(test_commitlog_replay_large_mutations){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 14;
    cfg.commitlog_total_space_in_mb = 2 * cfg.commitlog_segment_size_in_mb * smp::count;
    cfg.allow_going_over_size_limit = false;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto size = log.max_record_size() / 2;

        std::unordered_map<replay_position, fragmented_temporary_buffer> buffers;

        for (size_t i = 0; i < 4; ++i) {
            auto buf = fragmented_temporary_buffer::allocate_to_fit(size);
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<char> dist;

            auto out = buf.get_ostream();
            for (size_t i = 0; i < size; ++i) {
                auto c = dist(gen);
                out.write(&c, 1);
            }

            auto h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                for (auto& tmp : buf) {
                    dst.write(tmp.get(), tmp.size());
                }
            });

            auto rp = h.release();

            buffers.emplace(rp, std::move(buf));
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t n = 0;

        for (auto& seg : segments) {
            auto desc = commitlog::descriptor(seg);
            if (std::any_of(buffers.begin(), buffers.end(), [&](auto& p) { return p.first.id == desc.id; })) {
                co_await db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
                    auto& buf = buffers.at(buf_rp.position);
                    auto& rp_buf = buf_rp.buffer;
                    auto in1 = buf.get_istream();
                    auto in2 = rp_buf.get_istream();
                    for (size_t i = 0; i < size; ++i) {
                        auto c1 = in1.read<char>();
                        auto c2 = in2.read<char>();
                        BOOST_CHECK_EQUAL(c1, c2);
                    }
                    ++n;
                    return make_ready_future<>();
                });
            }
        }

        BOOST_CHECK_EQUAL(n, buffers.size());
    });
}

// Tests #15269 - skipping past EOF
SEASTAR_TEST_CASE(test_commitlog_chunk_truncation) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        sstring tmp = "hej bubba cow";
        db::replay_position rp, corrupt;

        // we want to corrupt somewhere in second chunk, before 
        // middle of it. I.e. between 128k and 192k
        constexpr auto min_corrupt_pos = 128*1024;
        constexpr auto max_corrupt_pos = 128*1024 + 64 * 1024;

        // fill one full segment.
        for (;;) {
            auto h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            });
            auto nrp = h.release();
            if (rp != db::replay_position() && nrp.base_id() != rp.base_id()) {
                break;
            }
            rp = nrp;
            if (rp.pos > min_corrupt_pos && (rp.pos + tmp.size() + 12) < max_corrupt_pos) {
                corrupt = rp;
            }
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();

        BOOST_REQUIRE(!segments.empty());
        BOOST_CHECK_NE(rp, db::replay_position());
        BOOST_CHECK_NE(corrupt, db::replay_position());

        BOOST_CHECK_EQUAL(rp.base_id(), corrupt.base_id());

        for (auto& seg : segments) {
            commitlog::descriptor d(seg);
            if (d.id == rp.base_id()) {
                // Corrupt the entry so we skip the rest of the chunk.
                co_await corrupt_segment(seg, corrupt.pos + 4, 'bose');
                auto f = co_await open_file_dma(seg, open_flags::rw);
                // then truncate the file so skipping the chunk will
                // cause EOF.
                co_await f.truncate(max_corrupt_pos);
                co_await f.close();

                // Reading this segment will now get corruption at the above position,
                // right before where we have truncated the file. It will try to skip
                // to next chunk, which is past actual EOF. If #15269 is broken, this
                // will SCYLLA_ASSERT and crash in file_data_source_impl. If not, we should 
                // get a corruption exception and no more entries past the corrupt one.
                db::position_type pos = 0;
                try {
                    co_await db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
                        pos = buf_rp.position.pos; // keep track of how far we reach in the segment.
                        return make_ready_future<>();
                    });
                } catch (commitlog::segment_data_corruption_error& e) {
                    // ok.
                    BOOST_CHECK_GT(e.bytes(), 0);
                }
                BOOST_CHECK_GT(pos, min_corrupt_pos);
                BOOST_CHECK_LT(pos, max_corrupt_pos);

                co_return;
            }
        }
        BOOST_FAIL("Should not reach");
    });
}

SEASTAR_TEST_CASE(test_commitlog_reader_produce_exception){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        auto rps = make_lw_shared<std::vector<db::replay_position>>();
        return do_until([rps]() {return rps->size() > 1;},
                    [&log, rps]() {
                        auto uuid = make_table_id();
                        sstring tmp = "hej bubba cow";
                        return log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [tmp](db::commitlog::output& dst) {
                                    dst.write(tmp.data(), tmp.size());
                                }).then([rps](rp_handle h) {
                                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                                    rps->push_back(h.release());
                                });
                    }).then([&log]() {
                        return log.sync_all_segments();
                    }).then([&log] {
                        auto segments = log.get_active_segment_names();
                        BOOST_REQUIRE(!segments.empty());
                        auto seg = segments[0];
                        return db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [](db::commitlog::buffer_and_replay_position buf_rp) {
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
                SCYLLA_ASSERT(fragmented_temporary_buffer::default_fragment_size < size);
                for (;;) {
                    junk->emplace_back(new char[fragmented_temporary_buffer::default_fragment_size]);
                }
            } catch (std::bad_alloc&) {
            }
            auto last = junk->end();
            junk->erase(--last);
            return log.add_mutation(make_table_id(), size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
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
        auto& sharder = table.get_effective_replication_map()->get_sharder(*table.schema());
        auto memtables = active_memtables(table);

        auto add_entry = [&cl, s, &sharder] (const partition_key& key) mutable {
            auto md = tests::data_model::mutation_description(key.explode());
            md.add_clustered_cell({}, "v", to_bytes("val"));
            auto m = md.build(s);

            auto fm = freeze(m);
            commitlog_entry_writer cew(s, fm, db::commitlog::force_sync::yes);
            cl.add_entry(m.column_family_id(), cew, db::no_timeout).get();
            return sharder.shard_for_reads(m.token());
        };

        const auto shard = add_entry(partition_key::make_empty());
        auto dk = tests::generate_partition_key(s, shard);

        add_entry(std::move(dk.key()));

        BOOST_REQUIRE(std::ranges::all_of(memtables, std::mem_fn(&replica::memtable::empty)));

        {
            auto paths = cl.get_active_segment_names();
            BOOST_REQUIRE(!paths.empty());
            auto rp = db::commitlog_replayer::create_replayer(env.db(), env.get_system_keyspace()).get();
            rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX).get();
        }

        {
            std::vector<mutation_reader> readers;
            readers.reserve(memtables.size());
            auto permit = db.get_reader_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {});
            for (auto mt : memtables) {
                readers.push_back(mt->make_flat_reader(s, permit));
            }
            auto rd = make_combined_reader(s, permit, std::move(readers));
            auto close_rd = deferred_close(rd);
            auto mopt = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(mopt);

            mopt = {};
            mopt = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(!mopt);
        }
    });
}

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_commitlog_add_entry) {
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

                std::set<segment_id_type> ids;

                for (auto& w : writers) {
                    auto h = log.add_entry(w.schema()->id(), w, db::timeout_clock::now() + 60s).get();
                    ids.emplace(h.rp().id);
                    rps.emplace_back(h.rp());
                }

                BOOST_CHECK_EQUAL(ids.size(), 1);

                log.sync_all_segments().get();
                auto segments = log.get_active_segment_names();
                BOOST_REQUIRE(!segments.empty());

                std::unordered_set<replay_position> result;

                for (auto& seg : segments) {
                    db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
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

                auto res = log.add_entries(writers, db::timeout_clock::now() + 60s).get();

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
                    db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
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

// #16298 - check entry offsets so that we report the correct file positions both
// when reading and writing CL data.
SEASTAR_TEST_CASE(test_commitlog_entry_offsets) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 2 * smp::count;
    cfg.allow_going_over_size_limit = false;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        sstring tmp = "hej bubba cow";

        auto h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
            dst.write(tmp.data(), tmp.size());
        });

        auto rp = h.release();

        // verify the first rp is at file offset 32 (file header + chunk header)
        BOOST_CHECK_EQUAL(rp.pos, 24 + 8); // TODO: export these sizes for tests. 

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        for (auto& seg : segments) {
            auto desc = commitlog::descriptor(seg);
            if (desc.id == rp.id) {
                bool found = false;
                co_await db::commitlog::read_log_file(seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) {
                    BOOST_CHECK_EQUAL(buf_rp.position, rp);
                    found = true;
                    return make_ready_future<>();
                });
                BOOST_REQUIRE(found);
                co_return;
            }
        }

        BOOST_FAIL("Did not find segment");
    });
}

SEASTAR_TEST_CASE(test_commitlog_max_segment_size) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 2 * smp::count;
    cfg.allow_going_over_size_limit = false;

    return cl_test(cfg, [max_size = cfg.commitlog_segment_size_in_mb](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        sstring tmp = "hej bubba cow";
        db::replay_position last_rp;
        std::optional<db::segment_id_type> last;
        auto max_size_bytes = max_size * 1024 * 1024;

        for (;;) {
            auto h = co_await log.add_mutation(uuid, tmp.size(), db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.write(tmp.data(), tmp.size());
            });

            auto rp = h.release();

            if (last && last != rp.id) {
                break;
            }
            last = rp.id;
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        for (auto& seg : segments) {
            auto desc = commitlog::descriptor(seg);
            if (last == desc.id) {
                auto size = co_await file_size(seg);
                BOOST_REQUIRE_LE(size, max_size_bytes);
                co_return;
            }
        }

        BOOST_FAIL("Did not find segment");
    });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment_odsync){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.use_o_dsync = true;

    return cl_test(cfg, [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
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

    auto uuid = make_table_id();
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

    uint64_t num_active_allocations = 0, num_blocked_on_new_segment = 0;

    // add a flush handler that delays releasing things until disk threshold is reached.
    auto r = log.add_flush_handler([&](cf_id_type, replay_position pos) {
        auto old = std::exchange(rps, rp_set{});
        queue.emplace_back(std::move(old));
        if (log.disk_footprint() >= log.disk_limit()) {
            num_active_allocations += log.get_num_active_allocations();
            num_blocked_on_new_segment += log.get_num_blocked_on_new_segment();
            if (!t.armed()) {
                t.arm(5s);
            }
        }
    });

    try {
        while (n < 10 || !num_active_allocations || !num_blocked_on_new_segment) {
            auto now = timeout_clock::now();            
            rp_handle h = co_await with_timeout(now + 30s, log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.fill('1', size);
            }));
            rps.put(std::move(h));
        }
    } catch (timed_out_error&) {
        BOOST_ERROR("log write timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
        abort();
    }

    co_await log.shutdown();
    co_await log.clear();

    BOOST_REQUIRE_GT(num_active_allocations, 0);
    BOOST_REQUIRE_GT(num_blocked_on_new_segment, 0);
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
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    std::deque<rp_set> queue;

    // uncomment for verbosity
    //logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
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
    // b.) ensure all segments get freed
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
        BOOST_ERROR("log shutdown timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
        abort();
    }
}

SEASTAR_TEST_CASE(test_commitlog_deadlock_with_flush_threshold) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 2 * max_size_mb * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
    auto size = log.max_record_size();

    bool done = false;

    auto r = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        log.discard_completed_segments(id, rps);
        done = true;
    });

    try {
        while (!done) {
            auto now = timeout_clock::now();
            rp_handle h = co_await with_timeout(now + 30s, log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.fill('1', size);
            }));
            rps.put(std::move(h));
        }
    } catch (timed_out_error&) {
        BOOST_ERROR("log write timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
        abort();
    }

    co_await log.shutdown();
    co_await log.clear();
}

static future<> do_test_exception_in_allocate_ex(bool do_file_delete) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 2 * max_size_mb * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.allow_going_over_size_limit = false; // #9348 - now can enforce size limit always
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();

    class myfail : public std::exception {
    public:
        using std::exception::exception;
    };

    struct myext: public db::commitlog_file_extension {
    public:
        bool fail = false;
        bool thrown = false;
        bool do_file_delete;

        myext(bool dd)
            : do_file_delete(dd)
        {}

        seastar::future<seastar::file> wrap_file(const seastar::sstring& filename, seastar::file f, seastar::open_flags flags) override {
            if (fail && !thrown) {
                thrown = true;
                if (do_file_delete) {
                    co_await f.close();
                    co_await seastar::remove_file(filename);
                }
                throw myfail{};
            }
            co_return f;
        }
        seastar::future<> before_delete(const seastar::sstring&) override {
            co_return;
        }
    };

    auto ep = std::make_unique<myext>(do_file_delete);
    auto& mx = *ep;

    db::extensions myexts;
    myexts.add_commitlog_file_extension("hufflepuff", std::move(ep));

    cfg.extensions = &myexts;

    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
    auto size = log.max_record_size();

    auto r = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        log.discard_completed_segments(id, rps);
        mx.fail = true;
    });

    try {
        while (!mx.thrown) {
            rp_handle h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
                dst.fill('1', size);
            });
            rps.put(std::move(h));
        }
    } catch (...) {
        BOOST_ERROR("log write timed out. maybe it is deadlocked... Will not free log. ASAN errors and leaks will follow...");
        abort();
    }

    co_await log.shutdown();
    co_await log.clear();
}

/**
 * Test generating an exception in segment file allocation
 */
SEASTAR_TEST_CASE(test_commitlog_exceptions_in_allocate_ex) {
    co_await do_test_exception_in_allocate_ex(false);
}

/**
 * Test generating an exception in segment file allocation, but also 
 * delete the file, which in turn should cause follow-up exceptions
 * in cleanup delete. Which CL should handle
 */

SEASTAR_TEST_CASE(test_commitlog_exceptions_in_allocate_ex_deleted_file_no_recycle) {
    co_await do_test_exception_in_allocate_ex(true);
}

using namespace std::string_literals;

BOOST_AUTO_TEST_CASE(test_commitlog_segment_descriptor) {
    for (auto& prefix : { "tuta"s, "ninja"s, "Commitlog"s, "Schemalog"s, "bamboo"s }) {
        // create a descriptor without given filename
        commitlog::descriptor d(db::replay_position(), prefix + commitlog::descriptor::SEPARATOR);

        for (auto& add : { ""s, "Recycled-"s }) {
            auto filename = "/some/path/we/dont/open/"s + add + std::string(d.filename());

            // ensure we only allow same prefix
            for (auto& wrong_prefix : { "fisk"s, "notter"s, "blazer"s }) {
                try {
                    commitlog::descriptor d2(filename, wrong_prefix + commitlog::descriptor::SEPARATOR);
                } catch (std::domain_error&) {
                    // ok
                    continue;
                }
                BOOST_FAIL("Should not reach");
            }

            commitlog::descriptor d3(filename, prefix + commitlog::descriptor::SEPARATOR);

            try {
                // check we require id
                commitlog::descriptor d3("/tmp/" + add + prefix + commitlog::descriptor::SEPARATOR + ".log", prefix);
                BOOST_FAIL("Should not reach");
            } catch (std::domain_error&) {
                // ok
            } 
            try {
                // check we require ver
                commitlog::descriptor d3("/tmp/" + add + prefix + commitlog::descriptor::SEPARATOR + "12.log", prefix);
                BOOST_FAIL("Should not reach");
            } catch (std::domain_error&) {
                // ok
            } 
        }
    }
}

// Test for #16207 - files deleted after recycle does not have size updated
// and when deleting does not actual decrease footprint.
SEASTAR_TEST_CASE(test_delete_recycled_segment_removes_size) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 2 * max_size_mb * smp::count;
    cfg.allow_going_over_size_limit = false; // #9348 - now can enforce size limit always
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();

    std::vector<sstring> fakes;

    struct myext: public db::commitlog_file_extension {
    public:
        std::vector<std::string> deleted;

        seastar::future<seastar::file> wrap_file(const seastar::sstring& filename, seastar::file f, seastar::open_flags flags) override {
            co_return f;
        }
        seastar::future<> before_delete(const seastar::sstring& filename) override {
            deleted.emplace_back(filename);
            co_return;
        }
    };

    auto ep = std::make_unique<myext>();
    auto& ex = *ep;

    db::extensions myexts;
    myexts.add_commitlog_file_extension("hufflepuff", std::move(ep));

    cfg.extensions = &myexts;

    auto log = co_await commitlog::create_commitlog(cfg);

    auto max_file_size_bytes = (max_size_mb * 1024 * 1024);
    // we might be one segment over disk threshold. 
    auto max_shard_size_bytes = max_file_size_bytes + (cfg.commitlog_total_space_in_mb * 1024 * 1024) / smp::count;

    // Add a bunch of fake segments (pretending replayed). The total footprint will be much 
    // more than above limit.
    for (int i = 0; i < 20; ++i) {
        fakes.emplace_back(cfg.commit_log_location + "/fake" + std::to_string(i) + ".log");

        auto f = co_await open_file_dma(fakes.back(), open_flags::wo|open_flags::create);
        co_await f.truncate(max_file_size_bytes);
        auto size = co_await f.size();
        co_await f.close();
        BOOST_CHECK_EQUAL(size, max_file_size_bytes);
    }

    // this will add the size of all the files, and if the buf is fixed
    // also ensure actual delete _drops_ footprint.
    co_await log.delete_segments(fakes);

    // Must have deleted.
    BOOST_REQUIRE_GT(ex.deleted.size(), 0);
    // Must have footprint less than effective limit (limit + crossover segment size)
    BOOST_REQUIRE_LE(log.disk_footprint(), max_shard_size_bytes);
    // All our fake files should have been either deleted or recycled, in any case 
    // they should pass through the above extension.
    BOOST_REQUIRE(std::all_of(fakes.begin(), fakes.end(), [&](const std::string& name) {
        return std::find(ex.deleted.begin(), ex.deleted.end(), name) != ex.deleted.end();
    }));

    co_await log.shutdown();
    co_await log.clear();
}

SEASTAR_TEST_CASE(test_wait_for_delete) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 8 * max_size_mb * smp::count;
    cfg.allow_going_over_size_limit = false; // #9348 - now can enforce size limit always
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    // not using cl_test, because we need to be able to abandon
    // the log.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();

    std::vector<sstring> fakes;

    struct myext: public db::commitlog_file_extension {
    public:
        commitlog* log = nullptr;
        std::vector<future<>> waiters;
        bool done = false;

        seastar::future<seastar::file> wrap_file(const seastar::sstring& filename, seastar::file f, seastar::open_flags flags) override {
            co_return f;
        }
        seastar::future<> before_delete(const seastar::sstring& filename) override {
            if (!done) {
                auto f = log->wait_for_pending_deletes();
                BOOST_REQUIRE(!f.available());
                waiters.emplace_back(std::move(f));
            }
            co_return;
        }
    };

    auto ep = std::make_unique<myext>();
    auto& ex = *ep;

    db::extensions myexts;
    myexts.add_commitlog_file_extension("hufflepuff", std::move(ep));

    cfg.extensions = &myexts;

    auto log = co_await commitlog::create_commitlog(cfg);
    ex.log = &log;

    auto r = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        log.discard_completed_segments(id);
    });

    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
    auto size = log.max_record_size();

    while (ex.waiters.size() < 5) {
        rp_handle h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
            dst.fill('1', size);
        });
        h.release();
    }

    ex.done = true; // stop adding futures

    for (auto&& f : ex.waiters) {
        co_await std::move(f);
    }

    co_await log.shutdown();
    co_await log.clear();
}

SEASTAR_TEST_CASE(test_commitlog_max_data_lifetime) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 2 * max_size_mb * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.commitlog_data_max_lifetime_in_seconds = 2;
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
    auto size = log.max_record_size();

    std::unordered_set<cf_id_type> ids;

    condition_variable cond;

    auto r = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        log.discard_completed_segments(id, rps);
        ids.insert(id);
        cond.signal();
    });

    rp_handle h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
        dst.fill('1', size);
    });
    h.release();

    // should not be signaled yet. 
    BOOST_REQUIRE(!ids.contains(uuid));

    int n = 0; 
    while (!ids.contains(uuid) && n++ < 3) {
        // way long, but lets give it some leeway on slow test cluster.
        co_await cond.wait(20s);
    }

    // but now it must
    BOOST_REQUIRE(ids.contains(uuid));

    co_await log.shutdown();
    co_await log.clear();
}

SEASTAR_TEST_CASE(test_commitlog_update_max_data_lifetime) {
    commitlog::config cfg;

    constexpr auto max_size_mb = 1;

    cfg.commitlog_segment_size_in_mb = max_size_mb;
    cfg.commitlog_total_space_in_mb = 2 * max_size_mb * smp::count;
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.commitlog_data_max_lifetime_in_seconds = std::nullopt;
    cfg.allow_going_over_size_limit = false;
    cfg.use_o_dsync = true; // make sure we pre-allocate.

    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(cfg);

    rp_set rps;
    // uncomment for verbosity
    // logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

    auto uuid = make_table_id();
    auto size = log.max_record_size();

    std::unordered_set<cf_id_type> ids;

    condition_variable cond;

    auto r = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        log.discard_completed_segments(id, rps);
        ids.insert(id);
        cond.signal();
    });

    rp_handle h = co_await log.add_mutation(uuid, size, db::commitlog::force_sync::no, [&](db::commitlog::output& dst) {
        dst.fill('1', size);
    });
    h.release();

    try {
        co_await cond.wait(10s);
        BOOST_FAIL("should not reach");
    } catch (condition_variable_timed_out&) {
    }

    // should not be signaled yet. 
    BOOST_REQUIRE(!ids.contains(uuid));

    log.update_max_data_lifetime(2);

    int n = 0; 
    while (!ids.contains(uuid) && n++ < 3) {
        // way long, but lets give it some leeway on slow test cluster.
        co_await cond.wait(10s);
    }

    // but now it must
    BOOST_REQUIRE(ids.contains(uuid));

    co_await log.shutdown();
    co_await log.clear();
}
