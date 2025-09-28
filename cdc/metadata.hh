/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>

#include "clocks/db_clock.hh"
#include "mutation/timestamp.hh"
#include "cdc/generation.hh"

namespace dht {
    class token;
}

namespace cdc {

class stream_id;
class topology_description;

/* Represents the node's knowledge about CDC generations used in the cluster.
 * Used during writes to pick streams to which CDC log writes should be sent to
 * (i.e., to pick partition keys for these writes).
 */
class metadata final {
    // Note: we use db_clock (1ms resolution) for generation timestamps
    // (because we need to insert them into tables using columns of timestamp types,
    //  and the native type of our columns' timestamp_type is db_clock::time_point).
    // On the other hand, timestamp_clock (1us resolution) is used for mutation timestamps,
    // and api::timestamp_type represents the number of ticks of a timestamp_clock::time_point since epoch.

    using container_t = std::map<api::timestamp_type, std::optional<topology_description>>;
    container_t _gens;

    using table_streams_ptr = lw_shared_ptr<const table_streams>;
    using tablet_streams_map = std::unordered_map<table_id, table_streams_ptr>;

    tablet_streams_map _tablet_streams;

    /* The timestamp used in the last successful `get_vnode_stream` call. */
    api::timestamp_type _last_stream_timestamp = api::missing_timestamp;

    container_t::const_iterator gen_used_at(api::timestamp_type ts) const;

    const std::vector<stream_id>& get_tablet_stream_set(table_id tid, api::timestamp_type ts) const;

public:
    /* Is a generation with the given timestamp already known or obsolete? It is obsolete if and only if
     * it is older than the generation operating at `now - get_generation_leeway()`.
     */
    bool known_or_obsolete(db_clock::time_point) const;

    /* Are there streams available. I.e. valid for time == now. If this is false, any writes to 
     * CDC logs will fail fast.
     */
    bool streams_available() const;

    /* Return the stream for a vnode-based keyspace for the base partition whose token is `tok` to which a corresponding
     * log write should go according to the generation used at time `ts` (i.e, the latest generation whose timestamp is
     * less or equal to `ts`).
     *
     * If the provided timestamp is too far away "into the future" (where "now" is defined according to our local clock),
     * we reject the get_vnode_stream query. This is because the resulting stream might belong to a generation which we don't
     * yet know about. Similarly, we reject queries to the previous generations if the timestamp is too far away "into
     * the past". The amount of leeway (how much "into the future" or "into the past" we allow `ts` to be) is defined by
     * `get_generation_leeway()`.
     */
    stream_id get_vnode_stream(api::timestamp_type ts, dht::token tok);

    /* Similar to get_vnode_stream but for tablet-based keyspaces.
     * In addition to the base partition token and timestamp, the stream also depends on the table id because each table
     * has its own set of streams.
     */
    stream_id get_tablet_stream(table_id tid, api::timestamp_type ts, dht::token tok);

    /* Insert the generation given by `gen` with timestamp `ts` to be used by the `get_vnode_stream` function,
     * if the generation is not already known or older than the currently known ones.
     *
     * Returns true if the generation was inserted,
     * meaning that `get_vnode_stream` might return a stream from this generation (at some time points).
     */
    bool insert(db_clock::time_point ts, topology_description&& gen);

    /* Prepare for inserting a new generation whose timestamp is `ts`.
     * This method is not required to be called before `insert`, but it's here
     * to increase safety of `get_vnode_stream` calls in some situations. Use it if you:
     * 1. know that there is a new generation, but
     * 2. you didn't yet retrieve the generation's topology_description.
     *
     * After preparing a generation, if `get_vnode_stream` is supposed to return a stream from this generation
     * but we don't yet have the generation's data, it will reject the query to maintain consistency of streams.
     *
     * Returns true iff this generation is not obsolete and wasn't previously prepared nor inserted.
     */
    bool prepare(db_clock::time_point ts);

    void load_tablet_streams_map(table_id tid, table_streams new_table_map);
    void remove_tablet_streams_map(table_id tid);

    const tablet_streams_map& get_all_tablet_streams() const {
        return _tablet_streams;
    }

    std::vector<table_id> get_tables_with_cdc_tablet_streams() const;

    static future<std::vector<stream_id>> construct_next_stream_set(
        const std::vector<cdc::stream_id>& prev_stream_set,
        std::vector<cdc::stream_id> opened,
        const std::vector<cdc::stream_id>& closed);

    static future<cdc_stream_diff> generate_stream_diff(
        const std::vector<stream_id>& before,
        const std::vector<stream_id>& after);

};

} // namespace cdc
