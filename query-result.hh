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

#pragma once

#include "bytes_ostream.hh"
#include "digest_algorithm.hh"
#include "query-request.hh"
#include <experimental/optional>
#include <seastar/util/bool_class.hh>
#include "seastarx.hh"

namespace query {

// result_memory_limiter, result_memory_accounter and result_memory_tracker
// form an infrastructure for limiting size of query results.
//
// result_memory_limiter is a shard-local object which ensures that all results
// combined do not use more than 10% of the shard memory.
//
// result_memory_accounter is used by result producers, updates the shard-local
// limits as well as keeps track of the individual maximum result size limit
// which is 1 MB.
//
// result_memory_tracker is just an object that makes sure the
// result_memory_limiter is notified when memory is released (but not sooner).

class result_memory_accounter;

class result_memory_limiter {
    const size_t _maximum_total_result_memory;
    semaphore _memory_limiter;
public:
    static constexpr size_t minimum_result_size = 4 * 1024;
    static constexpr size_t maximum_result_size = 1 * 1024 * 1024;
public:
    explicit result_memory_limiter(size_t maximum_total_result_memory)
        : _maximum_total_result_memory(maximum_total_result_memory)
        , _memory_limiter(_maximum_total_result_memory)
    { }

    result_memory_limiter(const result_memory_limiter&) = delete;
    result_memory_limiter(result_memory_limiter&&) = delete;

    ssize_t total_used_memory() const {
        return _maximum_total_result_memory - _memory_limiter.available_units();
    }

    // Reserves minimum_result_size and creates new memory accounter for
    // mutation query. Uses the specified maximum result size and may be
    // stopped before reaching it due to memory pressure on shard.
    future<result_memory_accounter> new_mutation_read(size_t max_result_size);

    // Reserves minimum_result_size and creates new memory accounter for
    // data query. Uses the specified maximum result size, result will *not*
    // be stopped due to on shard memory pressure in order to avoid digest
    // mismatches.
    future<result_memory_accounter> new_data_read(size_t max_result_size);

    // Creates a memory accounter for digest reads. Such accounter doesn't
    // contribute to the shard memory usage, but still stops producing the
    // result after individual limit has been reached.
    future<result_memory_accounter> new_digest_read(size_t max_result_size);

    // Checks whether the result can grow any more, takes into account only
    // the per shard limit.
    stop_iteration check() const {
        return stop_iteration(_memory_limiter.current() <= 0);
    }

    // Consumes n bytes from memory limiter and checks whether the result
    // can grow any more (considering just the per-shard limit).
    stop_iteration update_and_check(size_t n) {
        _memory_limiter.consume(n);
        return check();
    }

    void release(size_t n) noexcept {
        _memory_limiter.signal(n);
    }

    semaphore& sem() noexcept { return _memory_limiter; }
};


class result_memory_tracker {
    semaphore_units<> _units;
    size_t _used_memory;
private:
    static thread_local semaphore _dummy;
public:
    result_memory_tracker() noexcept : _units(_dummy, 0), _used_memory(0) { }
    result_memory_tracker(semaphore& sem, size_t blocked, size_t used) noexcept
        : _units(sem, blocked), _used_memory(used) { }
    size_t used_memory() const { return _used_memory; }
};

class result_memory_accounter {
    result_memory_limiter* _limiter = nullptr;
    size_t _blocked_bytes = 0;
    size_t _used_memory = 0;
    size_t _total_used_memory = 0;
    size_t _maximum_result_size = 0;
    stop_iteration _stop_on_global_limit;
private:
    // Mutation query accounter. Uses provided individual result size limit and
    // will stop when shard memory pressure grows too high.
    struct mutation_query_tag { };
    explicit result_memory_accounter(mutation_query_tag, result_memory_limiter& limiter, size_t max_size) noexcept
        : _limiter(&limiter)
        , _blocked_bytes(result_memory_limiter::minimum_result_size)
        , _maximum_result_size(max_size)
        , _stop_on_global_limit(true)
    { }

    // Data query accounter. Uses provided individual result size limit and
    // will *not* stop even though shard memory pressure grows too high.
    struct data_query_tag { };
    explicit result_memory_accounter(data_query_tag, result_memory_limiter& limiter, size_t max_size) noexcept
        : _limiter(&limiter)
        , _blocked_bytes(result_memory_limiter::minimum_result_size)
        , _maximum_result_size(max_size)
    { }

    // Digest query accounter. Uses provided individual result size limit and
    // will *not* stop even though shard memory pressure grows too high. This
    // accounter does not contribute to the shard memory limits.
    struct digest_query_tag { };
    explicit result_memory_accounter(digest_query_tag, result_memory_limiter&, size_t max_size) noexcept
        : _blocked_bytes(0)
        , _maximum_result_size(max_size)
    { }

    friend class result_memory_limiter;
public:
    // State of a accounter on another shard. Used to pass information about
    // the size of the result so far in range queries.
    class foreign_state {
        size_t _used_memory;
        size_t _max_result_size;
    public:
        foreign_state(size_t used_mem, size_t max_result_size)
                : _used_memory(used_mem), _max_result_size(max_result_size) { }
        size_t used_memory() const { return _used_memory; }
        size_t max_result_size() const { return _max_result_size; }
    };
public:
    result_memory_accounter() = default;

    // This constructor is used in cases when a result is produced on multiple
    // shards (range queries). foreign_accounter is an accounter that, possibly,
    // exist on the other shard and is used for merging the result. This
    // accouter will learn how big the total result alread is and limit the
    // part produced on this shard so that after merging the final result
    // does not exceed the individual limit.
    result_memory_accounter(result_memory_limiter& limiter, foreign_state fstate) noexcept
        : _limiter(&limiter)
        , _total_used_memory(fstate.used_memory())
        , _maximum_result_size(fstate.max_result_size())
    { }

    result_memory_accounter(result_memory_accounter&& other) noexcept
        : _limiter(std::exchange(other._limiter, nullptr))
        , _blocked_bytes(other._blocked_bytes)
        , _used_memory(other._used_memory)
        , _total_used_memory(other._total_used_memory)
        , _maximum_result_size(other._maximum_result_size)
        , _stop_on_global_limit(other._stop_on_global_limit)
    { }

    result_memory_accounter& operator=(result_memory_accounter&& other) noexcept {
        if (this != &other) {
            this->~result_memory_accounter();
            new (this) result_memory_accounter(std::move(other));
        }
        return *this;
    }

    ~result_memory_accounter() {
        if (_limiter) {
            _limiter->release(_blocked_bytes);
        }
    }

    size_t used_memory() const { return _used_memory; }

    foreign_state state_for_another_shard() {
        return foreign_state(_used_memory, _maximum_result_size);
    }

    // Consume n more bytes for the result. Returns stop_iteration::yes if
    // the result cannot grow any more (taking into account both individual
    // and per-shard limits).
    stop_iteration update_and_check(size_t n) {
        _used_memory += n;
        _total_used_memory += n;
        auto stop = stop_iteration(_total_used_memory > _maximum_result_size);
        if (_limiter && _used_memory > _blocked_bytes) {
            auto to_block = std::min(_used_memory - _blocked_bytes, n);
            _blocked_bytes += to_block;
            stop = (_limiter->update_and_check(to_block) && _stop_on_global_limit) || stop;
        }
        return stop;
    }

    // Checks whether the result can grow any more.
    stop_iteration check() const {
        stop_iteration stop { _total_used_memory > result_memory_limiter::maximum_result_size };
        if (!stop && _used_memory >= _blocked_bytes && _limiter) {
            return _limiter->check() && _stop_on_global_limit;
        }
        return stop;
    }

    // Consume n more bytes for the result.
    void update(size_t n) {
        update_and_check(n);
    }

    result_memory_tracker done() && {
        if (!_limiter) {
            return { };
        }
        auto& sem = std::exchange(_limiter, nullptr)->sem();
        return result_memory_tracker(sem, _blocked_bytes, _used_memory);
    }
};

inline future<result_memory_accounter> result_memory_limiter::new_mutation_read(size_t max_size) {
    return _memory_limiter.wait(minimum_result_size).then([this, max_size] {
        return result_memory_accounter(result_memory_accounter::mutation_query_tag(), *this, max_size);
    });
}

inline future<result_memory_accounter> result_memory_limiter::new_data_read(size_t max_size) {
    return _memory_limiter.wait(minimum_result_size).then([this, max_size] {
        return result_memory_accounter(result_memory_accounter::data_query_tag(), *this, max_size);
    });
}

inline future<result_memory_accounter> result_memory_limiter::new_digest_read(size_t max_size) {
    return make_ready_future<result_memory_accounter>(result_memory_accounter(result_memory_accounter::digest_query_tag(), *this, max_size));
}

enum class result_request {
    only_result,
    only_digest,
    result_and_digest,
};

struct result_options {
    result_request request = result_request::only_result;
    digest_algorithm digest_algo = query::digest_algorithm::none;

    static result_options only_result() {
        return result_options{};
    }

    static result_options only_digest(digest_algorithm da) {
        return {result_request::only_digest, da};
    }
};

class result_digest {
public:
    using type = std::array<uint8_t, 16>;
private:
    type _digest;
public:
    result_digest() = default;
    result_digest(type&& digest) : _digest(std::move(digest)) {}
    const type& get() const { return _digest; }
    bool operator==(const result_digest& rh) const {
        return _digest == rh._digest;
    }
    bool operator!=(const result_digest& rh) const {
        return _digest != rh._digest;
    }
};

//
// The query results are stored in a serialized form. This is in order to
// address the following problems, which a structured format has:
//
//   - high level of indirection (vector of vectors of vectors of blobs), which
//     is not CPU cache friendly
//
//   - high allocation rate due to fine-grained object structure
//
// On replica side, the query results are probably going to be serialized in
// the transport layer anyway, so serializing the results up-front doesn't add
// net work. There is no processing of the query results on replica other than
// concatenation in case of range queries and checksum calculation. If query
// results are collected in serialized form from different cores, we can
// concatenate them without copying by simply appending the fragments into the
// packet.
//
// On coordinator side, the query results would have to be parsed from the
// transport layer buffers anyway, so the fact that iterators parse it also
// doesn't add net work, but again saves allocations and copying. The CQL
// server doesn't need complex data structures to process the results, it just
// goes over it linearly consuming it.
//
// The coordinator side could be optimized even further for CQL queries which
// do not need processing (eg. select * from cf where ...). We could make the
// replica send the query results in the format which is expected by the CQL
// binary protocol client. So in the typical case the coordinator would just
// pass the data using zero-copy to the client, prepending a header.
//
// Users which need more complex structure of query results can convert this
// to query::result_set.
//
// Related headers:
//  - query-result-reader.hh
//  - query-result-writer.hh

struct short_read_tag { };
using short_read = bool_class<short_read_tag>;

class result {
    bytes_ostream _w;
    stdx::optional<result_digest> _digest;
    stdx::optional<uint32_t> _row_count;
    api::timestamp_type _last_modified = api::missing_timestamp;
    short_read _short_read;
    query::result_memory_tracker _memory_tracker;
    stdx::optional<uint32_t> _partition_count;
public:
    class builder;
    class partition_writer;
    friend class result_merger;

    result();
    result(bytes_ostream&& w, short_read sr, stdx::optional<uint32_t> c, stdx::optional<uint32_t> pc,
           result_memory_tracker memory_tracker = { })
        : _w(std::move(w))
        , _row_count(c)
        , _short_read(sr)
        , _memory_tracker(std::move(memory_tracker))
        , _partition_count(pc)
    {
        w.reduce_chunk_count();
    }
    result(bytes_ostream&& w, stdx::optional<result_digest> d, api::timestamp_type last_modified,
           short_read sr, stdx::optional<uint32_t> c, stdx::optional<uint32_t> pc, result_memory_tracker memory_tracker = { })
        : _w(std::move(w))
        , _digest(d)
        , _row_count(c)
        , _last_modified(last_modified)
        , _short_read(sr)
        , _memory_tracker(std::move(memory_tracker))
        , _partition_count(pc)
    {
        w.reduce_chunk_count();
    }
    result(result&&) = default;
    result(const result&) = default;
    result& operator=(result&&) = default;
    result& operator=(const result&) = default;

    const bytes_ostream& buf() const {
        return _w;
    }

    const stdx::optional<result_digest>& digest() const {
        return _digest;
    }

    const stdx::optional<uint32_t>& row_count() const {
        return _row_count;
    }

    const api::timestamp_type last_modified() const {
        return _last_modified;
    }

    short_read is_short_read() const {
        return _short_read;
    }

    const stdx::optional<uint32_t>& partition_count() const {
        return _partition_count;
    }

    void ensure_counts();

    struct printer {
        schema_ptr s;
        const query::partition_slice& slice;
        const query::result& res;
    };

    sstring pretty_print(schema_ptr, const query::partition_slice&) const;
    printer pretty_printer(schema_ptr, const query::partition_slice&) const;
};

std::ostream& operator<<(std::ostream& os, const query::result::printer&);
}
