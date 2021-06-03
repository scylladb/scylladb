/*
 * Copyright (C) 2019-present ScyllaDB
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

#pragma once

#include <map>

#include "db_clock.hh"
#include "timestamp.hh"
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
    // Note: we use db_clock (1ms resolution) for generation timestaps
    // (because we need to insert them into tables using columns of timestamp types,
    //  and the native type of our columns' timestamp_type is db_clock::time_point).
    // On the other hand, timestamp_clock (1us resolution) is used for mutation timestamps,
    // and api::timestamp_type represents the number of ticks of a timestamp_clock::time_point since epoch.

    using container_t = std::map<api::timestamp_type, std::optional<topology_description>>;
    container_t _gens;

    /* The timestamp used in the last successful `get_stream` call. */
    api::timestamp_type _last_stream_timestamp = api::missing_timestamp;

    container_t::const_iterator gen_used_at(api::timestamp_type ts) const;
public:
    /* Is a generation with the given timestamp already known or superseded by a newer generation? */
    bool known_or_obsolete(db_clock::time_point) const;

    /* Are there streams available. I.e. valid for time == now. If this is false, any writes to 
     * CDC logs will fail fast.
     */
    bool streams_available() const;
    /* Return the stream for the base partition whose token is `tok` to which a corresponding log write should go
     * according to the generation used at time `ts` (i.e, the latest generation whose timestamp is less or equal to `ts`).
     *
     * If the provided timestamp is too far away "into the future" (where "now" is defined according to our local clock),
     * we reject the get_stream query. This is because the resulting stream might belong to a generation which we don't
     * yet know about. The amount of leeway (how much "into the future" we allow `ts` to be) is defined
     * by the `cdc::generation_leeway` constant.
     */
    stream_id get_stream(api::timestamp_type ts, dht::token tok);

    /* Insert the generation given by `gen` with timestamp `ts` to be used by the `get_stream` function,
     * if the generation is not already known or older than the currently known ones.
     *
     * Returns true if the generation was inserted,
     * meaning that `get_stream` might return a stream from this generation (at some time points).
     */
    bool insert(db_clock::time_point ts, topology_description&& gen);

    /* Prepare for inserting a new generation whose timestamp is `ts`.
     * This method is not required to be called before `insert`, but it's here
     * to increase safety of `get_stream` calls in some situations. Use it if you:
     * 1. know that there is a new generation, but
     * 2. you didn't yet retrieve the generation's topology_description.
     *
     * After preparing a generation, if `get_stream` is supposed to return a stream from this generation
     * but we don't yet have the generation's data, it will reject the query to maintain consistency of streams.
     *
     * Returns true iff this generation is not obsolete and wasn't previously prepared nor inserted.
     */
    bool prepare(db_clock::time_point ts);
};

} // namespace cdc
