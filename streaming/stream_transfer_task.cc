/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "log.hh"
#include "streaming/stream_fwd.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_session.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_reason.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "service/topology_guard.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "message/messaging_service.hh"
#include "interval.hh"
#include "dht/i_partitioner.hh"
#include "dht/sharder.hh"
#include <boost/range/irange.hpp>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_set.hpp>
#include <fmt/ranges.h>
#include "sstables/sstables.hh"
#include "replica/database.hh"
#include "repair/table_check.hh"
#include "gms/feature_service.hh"
#include "utils/error_injection.hh"

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, table_id cf_id, dht::token_range_vector ranges, long total_size)
    : stream_task(session, cf_id)
    , _ranges(std::move(ranges))
    , _total_size(total_size) {
}

stream_transfer_task::~stream_transfer_task() = default;

struct send_info {
    netw::messaging_service& ms;
    streaming::plan_id plan_id;
    table_id cf_id;
    netw::messaging_service::msg_addr id;
    uint32_t dst_cpu_id;
    stream_reason reason;
    service::frozen_topology_guard topo_guard;
    size_t mutations_nr{0};
    semaphore mutations_done{0};
    bool error_logged = false;
    lw_shared_ptr<replica::table> cf;
    dht::token_range_vector ranges;
    dht::partition_range_vector prs;
    mutation_fragment_v1_stream reader;
    noncopyable_function<void(size_t)> update;
    send_info(netw::messaging_service& ms_, streaming::plan_id plan_id_, lw_shared_ptr<replica::table> tbl_, reader_permit permit_,
              dht::token_range_vector ranges_, netw::messaging_service::msg_addr id_,
              uint32_t dst_cpu_id_, stream_reason reason_, service::frozen_topology_guard topo_guard_,
              noncopyable_function<void(size_t)> update_fn)
        : ms(ms_)
        , plan_id(plan_id_)
        , cf_id(tbl_->schema()->id())
        , id(id_)
        , dst_cpu_id(dst_cpu_id_)
        , reason(reason_)
        , topo_guard(topo_guard_)
        , cf(tbl_)
        , ranges(std::move(ranges_))
        , prs(dht::to_partition_ranges(ranges))
        , reader(cf->make_streaming_reader(cf->schema(), std::move(permit_), prs, gc_clock::now()))
        , update(std::move(update_fn))
    {
    }
    future<bool> has_relevant_range_on_this_shard() {
        return do_with(false, ranges.begin(), [this] (bool& found_relevant_range, dht::token_range_vector::iterator& ranges_it) {
            auto stop_cond = [this, &found_relevant_range, &ranges_it] { return ranges_it == ranges.end() || found_relevant_range; };
            return do_until(std::move(stop_cond), [this, &found_relevant_range, &ranges_it] {
                dht::token_range range = *ranges_it++;
                if (!found_relevant_range) {
                    auto& table_sharder = cf->get_effective_replication_map()->get_sharder(*cf->schema());
                    auto sharder = dht::selective_token_range_sharder(table_sharder, std::move(range), this_shard_id());
                    auto range_shard = sharder.next();
                    if (range_shard) {
                        found_relevant_range = true;
                    }
                }
                return make_ready_future<>();
            }).then([&found_relevant_range] {
                return found_relevant_range;
            });
        });
    }
    future<size_t> estimate_partitions() {
        return do_with(cf->get_sstables(), size_t(0), [this] (auto& sstables, size_t& partition_count) {
            return do_for_each(*sstables, [this, &partition_count] (auto& sst) {
                return do_for_each(ranges, [&sst, &partition_count] (auto& range) {
                    partition_count += sst->estimated_keys_for_range(range);
                });
            }).then([&partition_count] {
                return partition_count;
            });
        });
    }
};

future<> send_mutation_fragments(lw_shared_ptr<send_info> si) {
 return si->reader.has_more_fragments().then([si] (bool there_is_more) {
  if (!there_is_more) {
    // The reader contains no data
    sslog.info("[Stream #{}] Skip sending ks={}, cf={}, reader contains no data, with new rpc streaming",
        si->plan_id, si->cf->schema()->ks_name(), si->cf->schema()->cf_name());
    return make_ready_future<>();
  }
  return si->estimate_partitions().then([si] (size_t estimated_partitions) {
    sslog.info("[Stream #{}] Start sending ks={}, cf={}, estimated_partitions={}, with new rpc streaming", si->plan_id, si->cf->schema()->ks_name(), si->cf->schema()->cf_name(), estimated_partitions);
    return si->ms.make_sink_and_source_for_stream_mutation_fragments(si->reader.schema()->version(), si->plan_id, si->cf_id, estimated_partitions, si->reason, si->topo_guard, si->id).then_unpack([si] (rpc::sink<frozen_mutation_fragment, stream_mutation_fragments_cmd> sink, rpc::source<int32_t> source) mutable {
        auto got_error_from_peer = make_lw_shared<bool>(false);
        auto table_is_dropped = make_lw_shared<bool>(false);

        auto source_op = [source, got_error_from_peer, table_is_dropped, si] () mutable -> future<> {
            return repeat([source, got_error_from_peer, table_is_dropped, si] () mutable {
                return source().then([source, got_error_from_peer, table_is_dropped, si] (std::optional<std::tuple<int32_t>> status_opt) mutable {
                    if (status_opt) {
                        auto status = std::get<0>(*status_opt);
                        if (status == -1) {
                            *got_error_from_peer = true;
                        } else if (status == -2) {
                            *got_error_from_peer = true;
                            *table_is_dropped = true;
                        }
                        sslog.debug("Got status code from peer={}, plan_id={}, cf_id={}, status={}", si->id.addr, si->plan_id, si->cf_id, status);
                        // we've got an error from the other side, but we cannot just abandon rpc::source we
                        // need to continue reading until EOS since this will signal that no more work
                        // is left and rpc::source can be destroyed. The sender closes connection immediately
                        // after sending the status, so EOS should arrive shortly.
                        return stop_iteration::no;
                    } else {
                        return stop_iteration::yes;
                    }
                });
            });
        }();

        auto sink_op = [sink, si, got_error_from_peer] () mutable -> future<> {
            mutation_fragment_stream_validator validator(*(si->reader.schema()));
            return do_with(std::move(sink), std::move(validator), [si, got_error_from_peer] (rpc::sink<frozen_mutation_fragment, stream_mutation_fragments_cmd>& sink, mutation_fragment_stream_validator& validator) {
                return repeat([&sink, &validator, si, got_error_from_peer] () mutable {
                    return si->reader().then([&sink, &validator, si, s = si->reader.schema(), got_error_from_peer] (mutation_fragment_opt mf) mutable {
                        if (*got_error_from_peer) {
                            return make_exception_future<stop_iteration>(std::runtime_error("Got status error code from peer"));
                        }
                        if (mf) {
                            if (!validator(mf->mutation_fragment_kind())) {
                                return make_exception_future<stop_iteration>(std::runtime_error(format("Stream reader mutation_fragment validator failed, previous={}, current={}",
                                        validator.previous_mutation_fragment_kind(), mf->mutation_fragment_kind())));
                            }
                            frozen_mutation_fragment fmf = freeze(*s, *mf);
                            auto size = fmf.representation().size();
                            si->update(size);
                            return sink(fmf, stream_mutation_fragments_cmd::mutation_fragment_data).then([] { return stop_iteration::no; });
                        } else {
                            if (!validator.on_end_of_stream()) {
                                return make_exception_future<stop_iteration>(std::runtime_error(format("Stream reader mutation_fragment validator failed on end_of_stream, previous={}, current=end_of_stream",
                                        validator.previous_mutation_fragment_kind())));
                            }
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                    });
                }).then([&sink] () mutable {
                    return sink(frozen_mutation_fragment(bytes_ostream()), stream_mutation_fragments_cmd::end_of_stream);
                }).handle_exception([&sink] (std::exception_ptr ep) mutable {
                    // Notify the receiver the sender has failed
                    return sink(frozen_mutation_fragment(bytes_ostream()), stream_mutation_fragments_cmd::error).then([ep = std::move(ep)] () mutable {
                        return make_exception_future<>(std::move(ep));
                    });
                }).finally([&sink] () mutable {
                    return sink.close();
                });
            });
        }();

        return when_all_succeed(std::move(source_op), std::move(sink_op)).then_unpack([got_error_from_peer, table_is_dropped, si] {
            if (*got_error_from_peer) {
                if (*table_is_dropped) {
                     sslog.info("[Stream #{}] Skipped streaming the dropped table {}.{}", si->plan_id, si->cf->schema()->ks_name(), si->cf->schema()->cf_name());
                } else {
                    throw std::runtime_error(format("Peer failed to process mutation_fragment peer={}, plan_id={}, cf_id={}", si->id.addr, si->plan_id, si->cf_id));
                }
            }
        });
    });
  });
 });
}

future<> stream_transfer_task::execute() {
    auto plan_id = session->plan_id();
    auto cf_id = this->cf_id;
    auto id = netw::messaging_service::msg_addr{session->peer, session->dst_cpu_id};
    auto& sm = session->manager();
    auto table_dropped = co_await repair::with_table_drop_silenced(sm.db(), sm.mm(), cf_id, [this, &sm, cf_id, plan_id, id] (const table_id &) {
        auto dst_cpu_id = session->dst_cpu_id;
        sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}", plan_id, cf_id);
        sort_and_merge_ranges();
        auto reason = session->get_reason();
        auto topo_guard = session->topo_guard();
        return sm.container().invoke_on_all([plan_id, cf_id, id, dst_cpu_id, ranges=this->_ranges, reason, topo_guard] (stream_manager& sm) mutable {
            auto tbl = sm.db().find_column_family(cf_id).shared_from_this();
            return sm.db().obtain_reader_permit(*tbl, "stream-transfer-task", db::no_timeout, {}).then([&sm, tbl, plan_id, cf_id, id, dst_cpu_id, ranges=std::move(ranges), reason, topo_guard] (reader_permit permit) mutable {
                auto si = make_lw_shared<send_info>(sm.ms(), plan_id, tbl, std::move(permit), std::move(ranges), id, dst_cpu_id, reason, topo_guard, [&sm, plan_id, addr = id.addr] (size_t sz) {
                    sm.update_progress(plan_id, addr, streaming::progress_info::direction::OUT, sz);
                });
                return si->has_relevant_range_on_this_shard().then([si, plan_id, cf_id] (bool has_relevant_range_on_this_shard) {
                    if (!has_relevant_range_on_this_shard) {
                        sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}: ignore ranges on shard={}",
                                plan_id, cf_id, this_shard_id());
                        return make_ready_future<>();
                    }
                    return send_mutation_fragments(std::move(si));
                }).finally([si] {
                    return si->reader.close();
                });
            });
        }).then([this, plan_id, cf_id, id, &sm] {
            sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, cf_id={}", plan_id, id, cf_id);
            return sm.ms().send_stream_mutation_done(id, plan_id, _ranges,
                    cf_id, session->dst_cpu_id).handle_exception([plan_id, id] (auto ep) {
                sslog.warn("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", plan_id, id, ep);
                std::rethrow_exception(ep);
            });
        }).then([this, id, plan_id] {
            _mutation_done_sent = true;
            sslog.debug("[Stream #{}] GOT STREAM_MUTATION_DONE Reply from {}", plan_id, id.addr);
        }).handle_exception([plan_id, id, &sm] (std::exception_ptr ep) {
            sslog.warn("[Stream #{}] stream_transfer_task: Fail to send to {}: {}", plan_id, id, ep);
            utils::get_local_injector().inject("stream_mutation_fragments_table_dropped", [&sm] () {
                sm.db().find_column_family(table_id::create_null_id());
            });
            std::rethrow_exception(ep);
        });
    });
    // If the table is dropped during streaming, we can ignore the
    // errors and make the stream successful. This allows user to
    // drop tables during node operations like decommission or
    // bootstrap.
    if (table_dropped) {
        sslog.warn("[Stream #{}] Ignore the table with table_id {} which is dropped during streaming", plan_id, cf_id);
        if (!_mutation_done_sent) {
            co_await session->manager().ms().send_stream_mutation_done(id, plan_id, _ranges, cf_id, session->dst_cpu_id);
        }
    }
}

void stream_transfer_task::append_ranges(const dht::token_range_vector& ranges) {
    _ranges.insert(_ranges.end(), ranges.begin(), ranges.end());
}

void stream_transfer_task::sort_and_merge_ranges() {
    boost::icl::interval_set<dht::token> myset;
    dht::token_range_vector ranges;
    sslog.debug("cf_id = {}, before ranges = {}, size={}", cf_id, _ranges, _ranges.size());
    _ranges.swap(ranges);
    for (auto& range : ranges) {
        // TODO: We should convert range_to_interval and interval_to_range to
        // take interval ranges.
        myset += locator::token_metadata::range_to_interval(std::move(range));
    }
    ranges.clear();
    ranges.shrink_to_fit();
    for (auto& i : myset) {
        auto r = locator::token_metadata::interval_to_range(i);
        _ranges.push_back(dht::token_range(std::move(r)));
    }
    sslog.debug("cf_id = {}, after  ranges = {}, size={}", cf_id, _ranges, _ranges.size());
}

} // namespace streaming
