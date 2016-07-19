#pragma once
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
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

#include <stdint.h>
#include <assert.h>

#include <memory>
#include <chrono>

#include "UUID.hh"
#include "db_clock.hh"

namespace utils {

/**
 * The goods are here: www.ietf.org/rfc/rfc4122.txt.
 */
class UUID_gen
{
private:
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    static constexpr int64_t START_EPOCH = -12219292800000L;
    static thread_local const int64_t clock_seq_and_node;

    /*
     * The min and max possible lsb for a UUID.
     * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
     * compares the lsb parts as a signed byte array comparison. So the min
     * value is 8 times -128 and the max is 8 times +127.
     *
     * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
     * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
     * I don't think that has any practical consequence and is more robust in
     * case someone provides a UUID with a broken variant.
     */
    static constexpr int64_t MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
    static constexpr int64_t MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    static thread_local const std::unique_ptr<UUID_gen> instance;

    int64_t last_nanos = 0;

    UUID_gen()
    {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        assert(clock_seq_and_node != 0);
    }

public:
    /**
     * Creates a type 1 UUID (time-based UUID).
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID()
    {
        return UUID(instance->create_time_safe(), clock_seq_and_node);
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID(int64_t when)
    {
        return UUID(create_time(from_unix_timestamp(when)), clock_seq_and_node);
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the wall clock time point @param tp.
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID(std::chrono::system_clock::time_point tp)
    {
        using namespace std::chrono;
        // "nanos" needs to be in 100ns intervals since the adoption of the Gregorian calendar in the West.
        uint64_t nanos = duration_cast<nanoseconds>(tp.time_since_epoch()).count() / 100;
        nanos -= (10000ULL * START_EPOCH);
        return UUID(create_time(nanos), clock_seq_and_node);
    }

    static UUID get_time_UUID(int64_t when, int64_t clock_seq_and_node)
    {
        return UUID(create_time(from_unix_timestamp(when)), clock_seq_and_node);
    }

    /** creates uuid from raw bytes. */
    static UUID get_UUID(bytes raw) {
        assert(raw.size() == 16);
        return get_UUID(raw.begin());
    }

    /** creates uuid from raw bytes. src must point to a region of 16 bytes*/
    static UUID get_UUID(int8_t* src) {
        struct tmp { uint64_t msb, lsb; } t;
        std::copy(src, src + 16, reinterpret_cast<char*>(&t));
        return UUID(net::ntoh(t.msb), net::ntoh(t.lsb));
    }

    /**
     * Creates a type 3 (name based) UUID based on the specified byte array.
     */
    static UUID get_name_UUID(bytes_view b);
    static UUID get_name_UUID(sstring_view str);
    static UUID get_name_UUID(const unsigned char* s, size_t len);

    /** decomposes a uuid into raw bytes. */
    static std::array<int8_t, 16> decompose(const UUID& uuid)
    {
        uint64_t most = uuid.get_most_significant_bits();
        uint64_t least = uuid.get_least_significant_bits();
        std::array<int8_t, 16> b;
        for (int i = 0; i < 8; i++)
        {
            b[i] = (char)(most >> ((7-i) * 8));
            b[8+i] = (char)(least >> ((7-i) * 8));
        }
        return b;
    }

    /**
     * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
     * based on the current system time.
     *
     * @return a type 1 UUID represented as a byte[]
     */
    static std::array<int8_t, 16> get_time_UUID_bytes()
    {
        return create_time_UUID_bytes(instance->create_time_safe());
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    static UUID min_time_UUID(int64_t timestamp)
    {
        return UUID(create_time(from_unix_timestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
    }

    /**
     * Returns the biggest possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    static UUID max_time_UUID(int64_t timestamp)
    {
        // unix timestamp are milliseconds precision, uuid timestamp are 100's
        // nanoseconds precision. If we ask for the biggest uuid have unix
        // timestamp 1ms, then we should not extend 100's nanoseconds
        // precision by taking 10000, but rather 19999.
        int64_t uuid_tstamp = from_unix_timestamp(timestamp + 1) - 1;
        return UUID(create_time(uuid_tstamp), MAX_CLOCK_SEQ_AND_NODE);
    }

    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    static int64_t unix_timestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param uuid
     * @return microseconds since Unix epoch
     */
    static int64_t micros_timestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10) + START_EPOCH * 1000;
    }

private:
    /**
     * @param timestamp milliseconds since Unix epoch
     * @return
     */
    static int64_t from_unix_timestamp(int64_t timestamp) {
        return (timestamp - START_EPOCH) * 10000;
    }

public:
    /**
     * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     *
     * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
     * a number 0 <= n < 10000 such that n*100 is the number of nanoseconds within that millisecond.
     *
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @return a type 1 UUID represented as a byte[]
     */
    static std::array<int8_t, 16> get_time_UUID_bytes(int64_t time_millis, int nanos)
    {
#if 0
        if (nanos >= 10000)
            throw new IllegalArgumentException();
#endif
        return create_time_UUID_bytes(instance->create_time_unsafe(time_millis, nanos));
    }

private:
    static std::array<int8_t, 16> create_time_UUID_bytes(uint64_t msb)
    {
        uint64_t lsb = clock_seq_and_node;
        std::array<int8_t, 16> uuid_bytes;

        for (int i = 0; i < 8; i++)
            uuid_bytes[i] = (int8_t) (msb >> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuid_bytes[i] = (int8_t) (lsb >> 8 * (7 - (i - 8)));

        return uuid_bytes;
    }

public:
    /**
     * Returns a milliseconds-since-epoch value for a type-1 UUID.
     *
     * @param uuid a type-1 (time-based) UUID
     * @return the number of milliseconds since the unix epoch
     * @throws IllegalArgumentException if the UUID is not version 1
     */
    static int64_t get_adjusted_timestamp(UUID uuid)
    {
#if 0
        if (uuid.version() != 1)
            throw new IllegalArgumentException("incompatible with uuid version: "+uuid.version());
#endif
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

private:

    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    // NOTE: In the original Java code this function was "synchronized". This isn't
    // needed if we assume our code will run on just one CPU.
    int64_t create_time_safe()
    {
        using namespace std::chrono;
        int64_t millis = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()).count();
        int64_t nanos_since = (millis - START_EPOCH) * 10000;
        if (nanos_since > last_nanos)
            last_nanos = nanos_since;
        else
            nanos_since = ++last_nanos;

        return create_time(nanos_since);
    }

    int64_t create_time_unsafe(int64_t when, int nanos)
    {
        uint64_t nanos_since = ((when - START_EPOCH) * 10000) + nanos;
        return create_time(nanos_since);
    }

    static int64_t create_time(uint64_t nanos_since)
    {
        uint64_t msb = 0L;
        msb |= (0x00000000ffffffffL & nanos_since) << 32;
        msb |= (0x0000ffff00000000UL & nanos_since) >> 16;
        msb |= (0xffff000000000000UL & nanos_since) >> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }
};

// for the curious, here is how I generated START_EPOCH
//        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
//        c.set(Calendar.YEAR, 1582);
//        c.set(Calendar.MONTH, Calendar.OCTOBER);
//        c.set(Calendar.DAY_OF_MONTH, 15);
//        c.set(Calendar.HOUR_OF_DAY, 0);
//        c.set(Calendar.MINUTE, 0);
//        c.set(Calendar.SECOND, 0);
//        c.set(Calendar.MILLISECOND, 0);
//        long START_EPOCH = c.getTimeInMillis();

} // namespace utils
