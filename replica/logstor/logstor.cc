/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include "query/query-request.hh"
#include "readers/from_mutations.hh"
#include "readers/empty.hh"
#include "keys/keys.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include <seastar/core/when_all.hh>
#include "utils/managed_bytes.hh"
#include <seastar/util/defer.hh>
#include <openssl/evp.h>
#include <algorithm>
#include <queue>
#include <vector>

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

namespace {

class key_hasher {
    EVP_MD_CTX* _mdctx;

public:
    key_hasher()
        : _mdctx(EVP_MD_CTX_new()) {
        if (!_mdctx) {
            throw std::runtime_error("Failed to allocate OpenSSL digest context for logstor key hash (SHA-256)");
        }
    }

    ~key_hasher() {
        EVP_MD_CTX_free(_mdctx);
    }

    key_hash compute(partition_key_view key) {
        if (1 != EVP_DigestInit_ex(_mdctx, EVP_sha256(), nullptr)) {
            throw std::runtime_error("Failed to initialize SHA-256 digest for logstor key");
        }

        for (bytes_view frag : fragment_range(key.representation())) {
            if (1 != EVP_DigestUpdate(_mdctx, frag.data(), frag.size())) {
                throw std::runtime_error("Failed to update SHA-256 digest for logstor key");
            }
        }

        std::array<uint8_t, EVP_MAX_MD_SIZE> full_digest;
        unsigned int digest_size = 0;
        if (1 != EVP_DigestFinal_ex(_mdctx, full_digest.data(), &digest_size)) {
            throw std::runtime_error("Failed to finalize SHA-256 digest for logstor key");
        }

        key_hash h;
        std::copy_n(full_digest.begin(), h.size(), h.begin());
        return h;
    }
};

key_hash compute_key_hash(partition_key_view key) {
    static thread_local key_hasher hasher;
    return hasher.compute(key);
}

dht::token_range partition_range_to_token_range(const dht::partition_range& pr) {
    using token_range_bound = dht::token_range::bound;

    std::optional<token_range_bound> start;
    std::optional<token_range_bound> end;

    if (pr.start()) {
        const auto& pos = pr.start()->value();
        const bool inclusive = pos.has_key() || pos.bound() == dht::ring_position::token_bound::start;
        start = token_range_bound(pos.token(), inclusive);
    }

    if (pr.end()) {
        const auto& pos = pr.end()->value();
        const bool inclusive = pos.has_key() || pos.bound() == dht::ring_position::token_bound::end;
        end = token_range_bound(pos.token(), inclusive);
    }

    return dht::token_range(std::move(start), std::move(end));
}

} // anonymous namespace

primary_index_key::primary_index_key(const dht::decorated_key& dk)
    : _token(dk.token())
    , _hash(compute_key_hash(dk.key().view())) {
}

static api::timestamp_type extract_logstor_record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

logstor::logstor(logstor_config config, ::cache_tracker& shared_cache_tracker)
    : _segment_manager(config.segment_manager_cfg)
    , _write_buffer(_segment_manager, config.flush_sg)
    , _cache_tracker(shared_cache_tracker) {
}

future<> logstor::do_recovery(replica::database& db) {
    co_await _segment_manager.do_recovery(db);
}

future<> logstor::do_recovery_for_test() {
    co_await _segment_manager.do_recovery_for_test();
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    logstor_logger.info("Stopping logstor");

    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_memory_usage();
}

segment_manager& logstor::get_segment_manager() noexcept {
    return _segment_manager;
}

const segment_manager& logstor::get_segment_manager() const noexcept {
    return _segment_manager;
}

compaction_manager& logstor::get_compaction_manager() noexcept {
    return _segment_manager.get_compaction_manager();
}

const compaction_manager& logstor::get_compaction_manager() const noexcept {
    return _segment_manager.get_compaction_manager();
}

future<> logstor::write(const mutation& m, write_target target, db::timeout_clock::time_point timeout) {
    auto& cg = *target.cg;
    primary_index_key key(m.decorated_key());
    table_id table = m.schema()->id();
    auto& index = cg.logstor_index();

    const auto ts = extract_logstor_record_timestamp(m);

    log_record record {
        .header = {
            .key = key,
            .timestamp = ts,
            .table = table,
        },
        .mut = canonical_mutation(m)
    };

    return _write_buffer.write(std::move(record), timeout, std::move(target)).then_unpack([this, index_ptr = &index, ts, key = std::move(key)]
            (log_location location, seastar::gate::holder op) {
        index_entry new_entry {
            .location = location,
            .timestamp = ts,
        };

        auto [inserted, prev_entry] = index_ptr->insert(key, std::move(new_entry));

        if (!inserted) {
            // A newer entry already exists; free the record we just wrote.
            _segment_manager.free_record(location);
        } else if (prev_entry) {
            // Overwrote an older entry; free it.
            _segment_manager.free_record(prev_entry->location);
        }
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error writing mutation: {}", ep);
        return make_exception_future<>(ep);
    });
}

future<std::optional<mutation>> logstor::read(const schema& s, const primary_index& index, const dht::decorated_key& dk, const query::partition_slice& slice) {
    auto op = index.start_read();

    primary_index_key pk(dk);

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    auto* cache = bypass_cache ? nullptr : index.cache_tracker();

    auto it = index.find(pk);
    if (it == index.end()) {
        co_return std::nullopt;
    }

    // lookup in cache
    if (cache) {
        auto cached_partition = cache->lookup(*it, s.shared_from_this());
        if (cached_partition) {
            co_return mutation(s.shared_from_this(), dk, std::move(*cached_partition));
        }
    }

    // Cache miss (or bypass): read from disk using the entry we already have.
    // copy the entry. we want to remember the original entry that we use for the read. the entry may change while we read.
    const index_entry entry_for_read = it->entry();
    auto record = co_await _segment_manager.read(entry_for_read.location);

    if (record.mut.key() != dk.key()) [[unlikely]] {
        on_internal_error(logstor_logger, format("Key mismatch reading log entry: expected {}, got {}", dk.key(), record.mut.key()));
    }

    mutation m = record.mut.to_mutation(s.shared_from_this());

    // Populate the cache with the freshly deserialized mutation.
    // Skipped when bypass_cache is set.
    // We must re-find the entry because the iterator may have been invalidated
    // across the co_await above.
    if (cache) {
        auto it = index.find(pk);
        if (it != index.end() && it->entry().location == entry_for_read.location) {
            cache->populate(*it, m);
        }
    }

    co_return std::move(m);
}

mutation_reader logstor::make_reader(schema_ptr schema, const primary_index& index, reader_permit permit, const dht::partition_range& pr,
        const query::partition_slice& slice, tracing::trace_state_ptr trace_state) {

    class logstor_single_key_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::decorated_key _dk;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        mutation_reader_opt _current_partition_reader;

    public:
        logstor_single_key_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                logstor* ls, dht::decorated_key dk,
                query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _dk(std::move(dk))
            , _slice(std::move(slice)), _trace_state(std::move(ts)) {
        }

        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                co_return;
            }

            if (_current_partition_reader) {
                co_await _current_partition_reader->fill_buffer();
                _current_partition_reader->move_buffer_content_to(*this);
                if (!_current_partition_reader->is_end_of_stream()) {
                    co_return;
                }
                co_await _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                _end_of_stream = true;
                co_return;
            }

            auto guard = reader_permit::awaits_guard(_permit);
            auto mut = co_await _logstor->read(*_schema, _index, _dk, _slice);
            if (!mut) {
                _end_of_stream = true;
                co_return;
            }

            tracing::trace(_trace_state, "logstor_single_key_reader: fetched key {}", _dk);

            _current_partition_reader = make_mutation_reader_from_mutations(
                _schema, _permit, std::move(*mut),
                _slice, streamed_mutation::forwarding::no
            );
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                return make_ready_future<>();
            }
            _end_of_stream = false;
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            if (!(pr.is_singular() && pr.start()->value().has_key() && pr.start()->value().as_decorated_key().equal(*_schema, _dk))) {
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (_current_partition_reader) {
                clear_buffer();
                return _current_partition_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_current_partition_reader) {
                return _current_partition_reader->close();
            }
            return make_ready_future<>();
        }
    };

    class logstor_range_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::partition_range _pr;
        dht::token_range _tr;
        struct scan_cursor {
            enum class kind { before_start, scanning, exhausted };
            kind state = kind::before_start;
            std::optional<dht::token> last_token;
        } _cursor;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        std::queue<mutation> _pending_mutations;
        mutation_reader_opt _current_partition_reader;

        struct mutation_batch {
            std::vector<log_location> locations;
            dht::token first_token;
            dht::token last_token;
            bool exhausted;
        };

        dht::token_range unread_token_range() const {
            if (!_cursor.last_token) {
                return _tr;
            }
            return dht::token_range(
                    dht::token_range::bound(*_cursor.last_token, false),
                    _tr.end());
        }

        void start_scan_for_current_token_range() {
            _cursor.state = scan_cursor::kind::scanning;
        }

        future<std::vector<mutation>> read_mutations_for_batch(const std::vector<log_location>& locations) {
            auto guard = reader_permit::awaits_guard(_permit);

            std::vector<future<mutation>> reads;
            reads.reserve(locations.size());

            // read directly from disk, bypass cache.
            // logstor::read() requires the partition key because when it reads from the cache it gets
            // a mutation_partition and then constructs the mutation from it using the provided key.
            // however we don't have the partition key here.
            // we could change it by storing the partition key in the cache entry.

            for (const auto location : locations) {
                reads.push_back([this, location] () -> future<mutation> {
                    return _logstor->_segment_manager.read(location).then([this] (log_record record) {
                        return record.mut.to_mutation(_schema);
                    });
                }());
            }

            auto read_mutations = co_await when_all_succeed(reads.begin(), reads.end());
            co_return std::move(read_mutations);
        }

        // Primary-index scan order is by (token, key hash). That means the batch already arrives
        // in token order, but entries that share a token are only ordered by the hash stored in
        // the index. Partition ranges and reader output use ring order instead: first by token,
        // then by the full partition key. We can only restore that order after reading the log
        // records, because only then do we have the full decorated key rather than just its hash.
        // After sorting each same-token run by ring order, filtering against the non-wrapping
        // partition range can only remove a prefix and/or suffix of the batch.
        void sort_and_filter_mutations_for_range(std::vector<mutation>& mutations) const {
            auto cmp = dht::ring_position_comparator(*_schema);
            auto in_range = [&] (const mutation& mut) {
                return _pr.contains(dht::ring_position(mut.decorated_key()), cmp);
            };

            auto run_begin = mutations.begin();
            while (run_begin != mutations.end()) {
                const auto& token = run_begin->decorated_key().token();
                auto run_end = std::ranges::find_if(run_begin, mutations.end(), [&] (const mutation& mut) {
                    return mut.decorated_key().token() != token;
                });
                if (std::distance(run_begin, run_end) > 1) {
                    std::ranges::sort(run_begin, run_end, [&] (const mutation& lhs, const mutation& rhs) {
                        return cmp(lhs.decorated_key(), rhs.decorated_key()) < 0;
                    });
                }
                run_begin = run_end;
            }

            auto first_in_range = std::ranges::find_if(mutations, in_range);
            if (first_in_range == mutations.end()) {
                mutations.clear();
                return;
            }

            auto last_in_range = std::ranges::find_if(mutations.rbegin(), mutations.rend(), in_range).base();
            mutations.erase(last_in_range, mutations.end());
            mutations.erase(mutations.begin(), first_in_range);
        }

        std::optional<mutation_batch> collect_batch(primary_index::token_range_scan scan, size_t max_entries) const {
            auto it = scan.begin();
            if (it == scan.end()) {
                return std::nullopt;
            }

            mutation_batch batch{
                .first_token = (*it).token(),
                .last_token = (*it).token(),
                .exhausted = false,
            };

            size_t entry_count = 0;
            while (it != scan.end()) {
                auto token_entries = *it;
                auto token_entry_count = size_t(std::distance(token_entries.begin(), token_entries.end()));
                if (entry_count != 0 && entry_count + token_entry_count > max_entries) {
                    break;
                }

                batch.last_token = token_entries.token();
                batch.locations.reserve(batch.locations.size() + token_entry_count);
                for (const auto& entry : token_entries) {
                    batch.locations.push_back(entry.entry().location);
                }

                entry_count += token_entry_count;
                ++it;
            }

            batch.exhausted = it == scan.end();
            return batch;
        }

        future<bool> load_next_token_mutations() {
            static constexpr size_t read_ahead_entries = 10;

            while (true) {
                switch (_cursor.state) {
                case scan_cursor::kind::before_start:
                    start_scan_for_current_token_range();
                    break;
                case scan_cursor::kind::scanning:
                    break;
                case scan_cursor::kind::exhausted:
                    co_return false;
                }

                auto op = _index.start_read();
                auto scan = _index.scan(unread_token_range());
                auto batch = collect_batch(scan, read_ahead_entries);
                if (!batch) {
                    _cursor.state = scan_cursor::kind::exhausted;
                    _cursor.last_token.reset();
                    co_return false;
                }

                if (batch->exhausted) {
                    _cursor.state = scan_cursor::kind::exhausted;
                    _cursor.last_token.reset();
                } else {
                    _cursor.state = scan_cursor::kind::scanning;
                    _cursor.last_token = batch->last_token;
                }

                auto mutations = co_await read_mutations_for_batch(batch->locations);
                sort_and_filter_mutations_for_range(mutations);

                tracing::trace(_trace_state,
                        "logstor_range_reader: fetched {} keys for token range [{}, {}]",
                        mutations.size(), batch->first_token, batch->last_token);

                for (auto& m : mutations) {
                    _pending_mutations.push(std::move(m));
                }
                co_return true;
            }
        }

        bool has_pending_mutations_for_current_token() const {
            return !_pending_mutations.empty();
        }

        future<bool> open_next_partition_reader() {
            while (!has_pending_mutations_for_current_token()) {
                if (!co_await load_next_token_mutations()) {
                    co_return false;
                }
            }

            auto mut = std::move(_pending_mutations.front());
            _pending_mutations.pop();
            _current_partition_reader = make_mutation_reader_from_mutations(
                _schema, _permit, std::move(mut),
                _slice, streamed_mutation::forwarding::no
            );
            co_return true;
        }

        future<> reset_current_partition_reader() {
            if (!_current_partition_reader) {
                return make_ready_future<>();
            }

            auto fut = _current_partition_reader->close();
            _current_partition_reader = std::nullopt;
            return fut;
        }

    public:
        logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                    logstor* ls, dht::partition_range pr,
                    query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _pr(std::move(pr))
            , _tr(partition_range_to_token_range(_pr))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
        {}

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !_end_of_stream) {
                // Drain current partition's reader first
                if (_current_partition_reader) {
                    co_await _current_partition_reader->fill_buffer();
                    _current_partition_reader->move_buffer_content_to(*this);
                    if (!_current_partition_reader->is_end_of_stream()) {
                        continue;
                    }
                    co_await reset_current_partition_reader();
                }

                if (!co_await open_next_partition_reader()) {
                    _end_of_stream = true;
                    break;
                }
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                return make_ready_future<>();
            }
            _end_of_stream = false;
            if (_current_partition_reader) {
                return reset_current_partition_reader();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _pr = pr;
            _tr = partition_range_to_token_range(_pr);
            _cursor = {};
            _pending_mutations = {};
            if (_current_partition_reader) {
                return reset_current_partition_reader();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (_current_partition_reader) {
                clear_buffer();
                return _current_partition_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_current_partition_reader) {
                return reset_current_partition_reader();
            }
            return make_ready_future<>();
        }
    };

    if (pr.is_singular() && pr.start()->value().has_key()) {
        return make_mutation_reader<logstor_single_key_reader>(
            std::move(schema), index, std::move(permit), this, pr.start()->value().as_decorated_key(), slice, std::move(trace_state)
        );
    } else {
        return make_mutation_reader<logstor_range_reader>(
            std::move(schema), index, std::move(permit), this, pr, slice, std::move(trace_state)
        );
    }
}

future<> logstor::flush_to_separator() {
    co_await _segment_manager.await_pending_writes();
}

void logstor::set_trigger_compaction_hook(std::function<void()> fn) {
    _segment_manager.set_trigger_compaction_hook(std::move(fn));
}

void logstor::set_trigger_separator_flush_hook(std::function<void(segment_sequence)> fn) {
    _segment_manager.set_trigger_separator_flush_hook(std::move(fn));
}

}
