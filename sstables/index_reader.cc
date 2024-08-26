#include "index_reader.hh"
#include "downsampling.hh"

namespace sstables {

// Provides access to sstable indexes.
//
// Maintains logical cursors to sstable elements (partitions, cells).
// Holds two cursors pointing to the range within sstable (upper cursor may be not set).
// Initially the lower cursor is positioned on the first partition in the sstable.
// Lower cursor can be accessed and advanced from outside.
// Upper cursor can only be advanced along with the lower cursor and not accessed from outside.
//
// If eof() then the lower bound cursor is positioned past all partitions in the sstable.
class index_reader_old : public index_reader {
    shared_sstable _sstable;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    std::unique_ptr<partition_index_cache> _local_index_cache; // Used when caching is disabled
    partition_index_cache& _index_cache;
    logalloc::allocating_section _alloc_section;
    logalloc::region& _region;
    use_caching _use_caching;
    bool _single_page_read;
    abort_source _abort;

    std::unique_ptr<index_consume_entry_context<index_consumer>> make_context(uint64_t begin, uint64_t end, index_consumer& consumer) {
        auto index_file = make_tracked_index_file(*_sstable, _permit, _trace_state, _use_caching);
        auto input = make_file_input_stream(index_file, begin, (_single_page_read ? end : _sstable->index_size()) - begin,
                        get_file_input_stream_options());
        auto trust_pi = trust_promoted_index(_sstable->has_correct_promoted_index_entries());
        auto ck_values_fixed_lengths = _sstable->get_version() >= sstable_version_types::mc
                            ? std::make_optional(get_clustering_values_fixed_lengths(_sstable->get_serialization_header()))
                            : std::optional<column_values_fixed_lengths>{};
        return std::make_unique<index_consume_entry_context<index_consumer>>(*_sstable, _permit, consumer, trust_pi, std::move(input),
                            begin, end - begin, ck_values_fixed_lengths, _abort, _trace_state);
    }

    future<> advance_context(index_bound& bound, uint64_t begin, uint64_t end, int quantity) {
        if (!bound.context) {
            bound.consumer = std::make_unique<index_consumer>(_region, _sstable->get_schema());
            bound.context = make_context(begin, end, *bound.consumer);
            bound.consumer->prepare(quantity);
            return make_ready_future<>();
        }
        bound.consumer->prepare(quantity);
        return bound.context->fast_forward_to(begin, end);
    }

private:
    index_bound _lower_bound;
    // Upper bound may remain uninitialized
    std::optional<index_bound> _upper_bound;

private:
    bool bound_eof(const index_bound& b) const {
        return b.data_file_position == data_file_end();
    }

    static future<> reset_clustered_cursor(index_bound& bound) noexcept {
        if (bound.clustered_cursor) {
            return bound.clustered_cursor->close().then([&bound] {
                bound.clustered_cursor.reset();
            });
        }
        return make_ready_future<>();
    }

    future<> advance_to_end(index_bound& bound) {
        sstlog.trace("index {}: advance_to_end() bound {}", fmt::ptr(this), fmt::ptr(&bound));
        bound.data_file_position = data_file_end();
        bound.element = indexable_element::partition;
        bound.current_list = {};
        bound.end_open_marker.reset();
        return reset_clustered_cursor(bound);
    }

    // Must be called for non-decreasing summary_idx.
    future<> advance_to_page(index_bound& bound, uint64_t summary_idx) {
        sstlog.trace("index {}: advance_to_page({}), bound {}", fmt::ptr(this), summary_idx, fmt::ptr(&bound));
        SCYLLA_ASSERT(!bound.current_list || bound.current_summary_idx <= summary_idx);
        if (bound.current_list && bound.current_summary_idx == summary_idx) {
            sstlog.trace("index {}: same page", fmt::ptr(this));
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        if (summary_idx >= summary.header.size) {
            sstlog.trace("index {}: eof", fmt::ptr(this));
            return advance_to_end(bound);
        }
        auto loader = [this, &bound] (uint64_t summary_idx) -> future<index_list> {
            auto& summary = _sstable->get_summary();
            uint64_t position = summary.entries[summary_idx].position;
            uint64_t quantity = downsampling::get_effective_index_interval_after_index(summary_idx, summary.header.sampling_level,
                summary.header.min_index_interval);

            uint64_t end;
            if (summary_idx + 1 >= summary.header.size) {
                end = _sstable->index_size();
            } else {
                end = summary.entries[summary_idx + 1].position;
            }

            return advance_context(bound, position, end, quantity).then([this, &bound] {
                return bound.context->consume_input().then_wrapped([this, &bound] (future<> f) {
                    std::exception_ptr ex;
                    if (f.failed()) {
                        ex = f.get_exception();
                        sstlog.error("failed reading index for {}: {}", _sstable->get_filename(), ex);
                    }
                    if (ex) {
                        return make_exception_future<index_list>(std::move(ex));
                    }
                    if (_single_page_read) {
                        // if the associated reader is forwarding despite having singular range, we prepare for that
                        _single_page_read = false;
                        auto& ctx = *bound.context;
                        return ctx.close().then([bc = std::move(bound.context), &bound] { return std::move(bound.consumer->indexes); });
                    }
                    return make_ready_future<index_list>(std::move(bound.consumer->indexes));
                });
            });
        };

        return _index_cache.get_or_load(summary_idx, loader).then([this, &bound, summary_idx] (partition_index_cache::entry_ptr ref) {
            bound.current_list = std::move(ref);
            bound.current_summary_idx = summary_idx;
            bound.current_index_idx = 0;
            bound.current_pi_idx = 0;
            if (bound.current_list->empty()) {
                throw malformed_sstable_exception(format("missing index entry for summary index {} (bound {})", summary_idx, fmt::ptr(&bound)), _sstable->index_filename());
            }
            bound.data_file_position = bound.current_list->_entries[0]->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();

            if (sstlog.is_enabled(seastar::log_level::trace)) {
                sstlog.trace("index {} bound {}: page:", fmt::ptr(this), fmt::ptr(&bound));
                logalloc::reclaim_lock rl(_region);
                for (auto&& e : bound.current_list->_entries) {
                    auto dk = dht::decorate_key(*_sstable->_schema,
                        e->get_key().to_partition_key(*_sstable->_schema));
                    sstlog.trace("  {} -> {}", dk, e->position());
                }
            }

            return reset_clustered_cursor(bound);
        });
    }

    future<> advance_lower_to_start(const dht::partition_range &range) {
        if (range.start()) {
            return advance_to(_lower_bound,
                dht::ring_position_view(range.start()->value(),
                    dht::ring_position_view::after_key(!range.start()->is_inclusive())));
        }
        return make_ready_future<>();
    }

    future<> advance_upper_to_end(const dht::partition_range &range) {
        if (!_upper_bound) {
            _upper_bound.emplace();
        }
        if (range.end()) {
            return advance_to(*_upper_bound,
                dht::ring_position_view(range.end()->value(),
                    dht::ring_position_view::after_key(range.end()->is_inclusive())));
        }
        return advance_to_end(*_upper_bound);
    }

    // Tells whether details about current partition can be accessed.
    // If this returns false, you have to call read_partition_data().
    //
    // Calling read_partition_data() may involve doing I/O. The reason
    // why control over this is exposed and not done under the hood is that
    // in some cases it only makes sense to access partition details from index
    // if it is readily available, and if it is not, we're better off obtaining
    // them by continuing reading from sstable.
    bool partition_data_ready(const index_bound& bound) const {
        return static_cast<bool>(bound.current_list);
    }

    // Valid if partition_data_ready(bound)
    index_entry& current_partition_entry(index_bound& bound) {
        SCYLLA_ASSERT(bound.current_list);
        return *bound.current_list->_entries[bound.current_index_idx];
    }

    future<> advance_to_next_partition(index_bound& bound) {
        sstlog.trace("index {} bound {}: advance_to_next_partition()", fmt::ptr(&bound), fmt::ptr(this));
        if (!partition_data_ready(bound)) {
            return advance_to_page(bound, 0).then([this, &bound] {
                return advance_to_next_partition(bound);
            });
        }
        if (bound.current_index_idx + 1 < bound.current_list->size()) {
            ++bound.current_index_idx;
            bound.current_pi_idx = 0;
            bound.data_file_position = bound.current_list->_entries[bound.current_index_idx]->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();
            return reset_clustered_cursor(bound);
        }
        auto& summary = _sstable->get_summary();
        if (bound.current_summary_idx + 1 < summary.header.size) {
            return advance_to_page(bound, bound.current_summary_idx + 1);
        }
        return advance_to_end(bound);
    }

    future<> advance_to(index_bound& bound, dht::ring_position_view pos) {
        sstlog.trace("index {} bound {}: advance_to({}), _previous_summary_idx={}, _current_summary_idx={}",
            fmt::ptr(this), fmt::ptr(&bound), pos, bound.previous_summary_idx, bound.current_summary_idx);

        if (pos.is_min()) {
            sstlog.trace("index {}: first entry", fmt::ptr(this));
            return make_ready_future<>();
        } else if (pos.is_max()) {
            return advance_to_end(bound);
        }
        if (bound_eof(bound)) {
            sstlog.trace("index {}: eof", fmt::ptr(this));
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        bound.previous_summary_idx = std::distance(std::begin(summary.entries),
            std::lower_bound(summary.entries.begin() + bound.previous_summary_idx, summary.entries.end(), pos, index_comparator(*_sstable->_schema)));

        if (bound.previous_summary_idx == 0) {
            sstlog.trace("index {}: first entry", fmt::ptr(this));
            return make_ready_future<>();
        }

        auto summary_idx = bound.previous_summary_idx - 1;

        sstlog.trace("index {}: summary_idx={}", fmt::ptr(this), summary_idx);

        // Despite the requirement that the values of 'pos' in subsequent calls
        // are increasing we still may encounter a situation when we try to read
        // the previous bucket.
        // For example, let's say we have index like this:
        // summary:  A       K       ...
        // index:    A C D F K M N O ...
        // Now, we want to get positions for range [G, J]. We start with [G,
        // summary look up will tel us to check the first bucket. However, there
        // is no G in that bucket so we read the following one to get the
        // position (see the advance_to_page() call below). After we've got it, it's time to
        // get J] position. Again, summary points us to the first bucket and we
        // hit an SCYLLA_ASSERT since the reader is already at the second bucket and we
        // cannot go backward.
        // The solution is this condition above. If our lookup requires reading
        // the previous bucket we assume that the entry doesn't exist and return
        // the position of the first one in the current index bucket.
        if (summary_idx + 1 == bound.current_summary_idx) {
            return make_ready_future<>();
        }

        return advance_to_page(bound, summary_idx).then([this, &bound, pos, summary_idx] {
            sstlog.trace("index {}: old page index = {}", fmt::ptr(this), bound.current_index_idx);
            auto i = _alloc_section(_region, [&] {
                auto& entries = bound.current_list->_entries;
                return std::lower_bound(std::begin(entries) + bound.current_index_idx, std::end(entries), pos,
                    index_comparator(*_sstable->_schema));
            });
            // i is valid until next allocation point
            auto& entries = bound.current_list->_entries;
            if (i == std::end(entries)) {
                sstlog.trace("index {}: not found", fmt::ptr(this));
                return advance_to_page(bound, summary_idx + 1);
            }
            bound.current_index_idx = std::distance(std::begin(entries), i);
            bound.current_pi_idx = 0;
            bound.data_file_position = (*i)->position();
            bound.element = indexable_element::partition;
            bound.end_open_marker.reset();
            sstlog.trace("index {}: new page index = {}, pos={}", fmt::ptr(this), bound.current_index_idx, bound.data_file_position);
            return reset_clustered_cursor(bound);
        });
    }

    // Forwards the upper bound cursor to a position which is greater than given position in current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may not forward to the smallest position which is greater than pos.
    //
    // May advance to the next partition if it's not possible to find a suitable position inside
    // current partition.
    //
    // Must be called only when !eof().
    future<> advance_upper_past(position_in_partition_view pos) {
        sstlog.trace("index {}: advance_upper_past({})", fmt::ptr(this), pos);

        // We advance cursor within the current lower bound partition
        // So need to make sure first that it is read
        if (!partition_data_ready(_lower_bound)) {
            return read_partition_data().then([this, pos] {
                SCYLLA_ASSERT(partition_data_ready());
                return advance_upper_past(pos);
            });
        }

        if (!_upper_bound) {
            _upper_bound = _lower_bound;
        }

        index_entry& e = current_partition_entry(*_upper_bound);
        auto e_pos = e.position();
        clustered_index_cursor* cur = current_clustered_cursor(*_upper_bound);

        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return advance_to_next_partition(*_upper_bound);
        }

        return cur->probe_upper_bound(pos).then([this, e_pos] (std::optional<clustered_index_cursor::offset_in_partition> off) {
            if (!off) {
                return advance_to_next_partition(*_upper_bound);
            }
            _upper_bound->data_file_position = e_pos + *off;
            _upper_bound->element = indexable_element::cell;
            sstlog.trace("index {} upper bound: skipped to cell, _data_file_position={}", fmt::ptr(this), _upper_bound->data_file_position);
            return make_ready_future<>();
        });
    }

    // Returns position right after all partitions in the sstable
    uint64_t data_file_end() const {
        return _sstable->data_size();
    }

    static future<> close(index_bound& b) noexcept {
        auto close_context = make_ready_future<>();
        if (b.context) {
            close_context = b.context->close();
        }
        return seastar::when_all_succeed(std::move(close_context), reset_clustered_cursor(b)).discard_result().then([&b] {
            b.current_list = {};
        });
    }

    file_input_stream_options get_file_input_stream_options() {
        file_input_stream_options options;
        options.buffer_size = _sstable->sstable_buffer_size;
        options.read_ahead = 2;
        options.dynamic_adjustments = _sstable->_index_history;
        return options;
    }

public:
    index_reader_old(shared_sstable sst, reader_permit permit,
                 tracing::trace_state_ptr trace_state = {},
                 use_caching caching = use_caching::yes,
                 bool single_partition_read = false)
        : _sstable(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _local_index_cache(caching ? nullptr
            : std::make_unique<partition_index_cache>(_sstable->manager().get_cache_tracker().get_lru(),
                                                      _sstable->manager().get_cache_tracker().region(),
                                                      _sstable->manager().get_cache_tracker().get_partition_index_cache_stats()))
        , _index_cache(caching ? *_sstable->_index_cache : *_local_index_cache)
        , _region(_sstable->manager().get_cache_tracker().region())
        , _use_caching(caching)
        , _single_page_read(single_partition_read) // all entries for a given partition are within a single page
    {
        if (sstlog.is_enabled(logging::log_level::trace)) {
            sstlog.trace("index {}: index_reader for {}", fmt::ptr(this), _sstable->get_filename());
        }
    }

    // Ensures that partition_data_ready() returns true.
    // Can be called only when !eof()
    future<> read_partition_data() override {
        SCYLLA_ASSERT(!eof());
        if (partition_data_ready(_lower_bound)) {
            return make_ready_future<>();
        }
        // The only case when _current_list may be missing is when the cursor is at the beginning
        SCYLLA_ASSERT(_lower_bound.current_summary_idx == 0);
        return advance_to_page(_lower_bound, 0);
    }

    // Advance index_reader bounds to the bounds of the supplied range
    future<> advance_to(const dht::partition_range& range) override {
        return seastar::when_all_succeed(
            advance_lower_to_start(range),
            advance_upper_to_end(range)).discard_result();
    }

    // Get current index entry
    // The returned reference is LSA-managed so call with the region locked.
    index_entry& current_partition_entry() {
        return current_partition_entry(_lower_bound);
    }

    // Returns a pointer to the clustered index cursor for the current partition
    // or nullptr if there is no clustered index in the current partition.
    // Returns the same instance until we move to a different partition.
    //
    // Precondition: partition_data_ready(bound).
    //
    // For sstable versions >= mc the returned cursor (if not nullptr) will be of type `bsearch_clustered_cursor`.
    clustered_index_cursor* current_clustered_cursor(index_bound& bound) {
        if (!bound.clustered_cursor) {
            _alloc_section(_region, [&] {
                index_entry& e = current_partition_entry(bound);
                promoted_index* pi = e.get_promoted_index().get();
                if (pi) {
                    bound.clustered_cursor = pi->make_cursor(_sstable, _permit, _trace_state,
                        get_file_input_stream_options(), _use_caching);
                }
            });
            if (!bound.clustered_cursor) {
                return nullptr;
            }
        }
        return &*bound.clustered_cursor;
    }

    clustered_index_cursor* current_clustered_cursor() override {
        return current_clustered_cursor(_lower_bound);
    }

    // Returns tombstone for the current partition if it was recorded in the sstable.
    // It may be unavailable for old sstables for which this information was not generated.
    // Can be called only when partition_data_ready().
    std::optional<sstables::deletion_time> partition_tombstone() override {
        return current_partition_entry(_lower_bound).get_deletion_time();
    }

    // Returns the key for current partition.
    // Can be called only when partition_data_ready().
    std::optional<partition_key> get_partition_key() override {
        return get_partition_key_prefix();
    }
    
    partition_key get_partition_key_prefix() override {
        return _alloc_section(_region, [this] {
            index_entry& e = current_partition_entry(_lower_bound);
            return e.get_key().to_partition_key(*_sstable->_schema);
        });
    }
    // Returns the data file position for the current partition.
    // Can be called only when partition_data_ready().
    uint64_t get_data_file_position() override {
        index_entry& e = current_partition_entry(_lower_bound);
        return e.position();
    }

    // Returns the number of promoted index entries for the current partition.
    // Can be called only when partition_data_ready().
    uint64_t get_promoted_index_size() override {
        index_entry& e = current_partition_entry(_lower_bound);
        return e.get_promoted_index_size();
    }

    bool partition_data_ready() const override {
        return partition_data_ready(_lower_bound);
    }

    // Forwards the cursor to the given position in the current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may forward the cursor to some position pos' which precedes pos, even though
    // there exist rows with positions in the range [pos', pos].
    //
    // Must be called for non-decreasing positions.
    // Must be called only after advanced to some partition and !eof().
    future<> advance_to(position_in_partition_view pos) override {
        sstlog.trace("index {}: advance_to({}), current data_file_pos={}",
                 fmt::ptr(this), pos, _lower_bound.data_file_position);

        const schema& s = *_sstable->_schema;
        if (pos.is_before_all_fragments(s)) {
            return make_ready_future<>();
        }

        if (!partition_data_ready()) {
            return read_partition_data().then([this, pos] {
                sstlog.trace("index {}: page done", fmt::ptr(this));
                SCYLLA_ASSERT(partition_data_ready(_lower_bound));
                return advance_to(pos);
            });
        }

        index_entry& e = current_partition_entry();
        auto e_pos = e.position();
        clustered_index_cursor* cur = current_clustered_cursor(_lower_bound);

        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return make_ready_future<>();
        }

        return cur->advance_to(pos).then([this, e_pos] (std::optional<clustered_index_cursor::skip_info> si) {
            if (!si) {
                sstlog.trace("index {}: position in the same block", fmt::ptr(this));
                return;
            }
            if (!si->active_tombstone) {
                // End open marker can be only engaged in SSTables 3.x ('mc' format) and never in ka/la
                _lower_bound.end_open_marker.reset();
            } else {
                _lower_bound.end_open_marker = open_rt_marker{std::move(si->active_tombstone_pos), si->active_tombstone};
            }
            _lower_bound.data_file_position = e_pos + si->offset;
            _lower_bound.element = indexable_element::cell;
            sstlog.trace("index {}: skipped to cell, _data_file_position={}", fmt::ptr(this), _lower_bound.data_file_position);
        });
    }

    // Like advance_to(dht::ring_position_view), but returns information whether the key was found
    // If upper_bound is provided, the upper bound within position is looked up
    future<bool> advance_lower_and_check_if_present(
            dht::ring_position_view key, std::optional<position_in_partition_view> pos = {}) override {
        utils::get_local_injector().inject("advance_lower_and_check_if_present", [] { throw std::runtime_error("advance_lower_and_check_if_present"); });
        return advance_to(_lower_bound, key).then([this, key, pos] {
            if (eof()) {
                return make_ready_future<bool>(false);
            }
            return read_partition_data().then([this, key, pos] {
                index_comparator cmp(*_sstable->_schema);
                bool found = _alloc_section(_region, [&] {
                    return cmp(key, current_partition_entry(_lower_bound)) == 0;
                });
                if (!found || !pos) {
                    return make_ready_future<bool>(found);
                }

                return advance_upper_past(*pos).then([] {
                    return make_ready_future<bool>(true);
                });
            });
        });
    }

    // Advances the upper bound to the partition immediately following the partition of the lower bound.
    //
    // Precondition: the sstable version is >= mc.
    future<> advance_reverse_to_next_partition() override {
        return advance_reverse(position_in_partition_view::after_all_clustered_rows());
    }

    // Advances the upper bound to the start of the first promoted index block after `pos`,
    // or to the next partition if there are no blocks after `pos`.
    //
    // Supports advancing backwards (i.e. `pos` can be smaller than the previous upper bound position).
    //
    // Precondition: the sstable version is >= mc.
    future<> advance_reverse(position_in_partition_view pos) override {
        if (eof()) {
            return make_ready_future<>();
        }

        // The `clustered_cursor` of an index bound does not support moving backward;
        // we work around this by recreating the upper bound (if it already exists)
        // at the lower bound position, then moving forward.
        if (_upper_bound) {
            return close(*_upper_bound).then([this, pos] {
                _upper_bound.reset();
                return advance_reverse(pos);
            });
        }

        // We advance the clustered cursor within the current lower bound partition
        // so need to make sure first that the lower bound partition data is in memory.
        if (!partition_data_ready(_lower_bound)) {
            return read_partition_data().then([this, pos] {
                SCYLLA_ASSERT(partition_data_ready());
                return advance_reverse(pos);
            });
        }

        _upper_bound = _lower_bound;

        auto cur = current_clustered_cursor(*_upper_bound);
        if (!cur) {
            sstlog.trace("index {}: no promoted index", fmt::ptr(this));
            return advance_to_next_partition(*_upper_bound);
        }

        auto cur_bsearch = dynamic_cast<sstables::mc::bsearch_clustered_cursor*>(cur);
        // The dynamic cast must have succeeded by precondition (sstable version >= mc)
        // and `current_clustered_cursor` specification.
        if (!cur_bsearch) {
            on_internal_error(sstlog, format(
                "index {}: expected the cursor type to be bsearch_clustered_cursor, but it's not;"
                " sstable version (expected >= mc): {}", fmt::ptr(this), static_cast<int>(_sstable->get_version())));
        }

        return cur_bsearch->advance_past(pos).then([this, partition_start_pos = get_data_file_position()]
                (std::optional<clustered_index_cursor::skip_info> si) {
            if (!si) {
                return advance_to_next_partition(*_upper_bound);
            }
            if (!si->active_tombstone) {
                // End open marker can be only engaged in SSTables 3.x ('mc' format) and never in ka/la
                _upper_bound->end_open_marker.reset();
            } else {
                _upper_bound->end_open_marker = open_rt_marker{std::move(si->active_tombstone_pos), si->active_tombstone};
            }
            _upper_bound->data_file_position = partition_start_pos + si->offset;
            _upper_bound->element = indexable_element::cell;
            sstlog.trace("index {}: advanced end after cell, _data_file_position={}", fmt::ptr(this), _upper_bound->data_file_position);
            return make_ready_future<>();
        });
    }

    // Returns the offset in the data file of the first row in the last promoted index block
    // in the current partition or nullopt if there are no blocks in the current partition.
    //
    // Preconditions: sstable version >= mc, partition_data_ready().
    future<std::optional<uint64_t>> last_block_offset() override {
        SCYLLA_ASSERT(partition_data_ready());

        auto cur = current_clustered_cursor();
        if (!cur) {
            return make_ready_future<std::optional<uint64_t>>(std::nullopt);
        }

        auto cur_bsearch = dynamic_cast<sstables::mc::bsearch_clustered_cursor*>(cur);
        // The dynamic cast must have succeeded by precondition (sstable version >= mc)
        // and `current_clustered_cursor` specification.
        if (!cur_bsearch) {
            on_internal_error(sstlog, format(
                "index {}: expected the cursor type to be bsearch_clustered_cursor, but it's not;"
                " sstable version (expected >= mc): {}", fmt::ptr(this), static_cast<int>(_sstable->get_version())));
        }

        return cur_bsearch->last_block_offset();
    }

    // Moves the cursor to the beginning of next partition.
    // Can be called only when !eof().
    future<> advance_to_next_partition() override {
        return advance_to_next_partition(_lower_bound);
    }

    // Positions the cursor on the first partition which is not smaller than pos (like std::lower_bound).
    // Must be called for non-decreasing positions.
    future<> advance_to(dht::ring_position_view pos) override {
        return advance_to(_lower_bound, pos);
    }

    // Returns positions in the data file of the cursor.
    // End position may be unset
    data_file_positions_range data_file_positions() const override {
        data_file_positions_range result;
        result.start = _lower_bound.data_file_position;
        if (_upper_bound) {
            result.end = _upper_bound->data_file_position;
        }
        return result;
    }

    // Returns the kind of sstable element the cursor is pointing at.
    indexable_element element_kind() const override {
        return _lower_bound.element;
    }

    std::optional<open_rt_marker> end_open_marker() const override {
        return _lower_bound.end_open_marker;
    }

    std::optional<open_rt_marker> reverse_end_open_marker() const override {
        return _upper_bound->end_open_marker;
    }

    bool eof() const override {
        return bound_eof(_lower_bound);
    }

    const shared_sstable& sstable() const { return _sstable; }

    future<> close() noexcept override {
        // index_bound::close must not fail
        auto close_lb = close(_lower_bound);
        auto close_ub = _upper_bound ? close(*_upper_bound) : make_ready_future<>();
        return when_all(std::move(close_lb), std::move(close_ub)).discard_result().finally([this] {
            if (_local_index_cache) {
                return _local_index_cache->evict_gently();
            }
            return make_ready_future<>();
        });
    }
};

std::unique_ptr<index_reader>
make_index_reader(
    shared_sstable sst,
    reader_permit permit,
    tracing::trace_state_ptr trace_state,
    use_caching caching,
    bool single_partition_read
) {
    return std::make_unique<index_reader_old>(
        std::move(sst),
        std::move(permit),
        std::move(trace_state),
        caching,
        single_partition_read
    );
}

} // namespace sstables
