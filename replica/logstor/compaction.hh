/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "types.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "write_buffer.hh"
#include "utils/log_heap.hh"
#include <seastar/core/semaphore.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include <algorithm>
#include <array>
#include <limits>
#include <ranges>
#include <optional>
#include <vector>
#include <functional>
#include <utility>

namespace replica {
class table;
class compaction_group;
} // namespace replica

namespace replica::logstor {

extern seastar::logger logstor_logger;

class primary_index;
struct segment_descriptor;
struct segment_set;
struct separator_buffer;
class segment_ref;
class logstor_group;

using separator_write_completion = seastar::noncopyable_function<void(log_location, seastar::gate::holder)>;

// How much reclamation efficiency a batch may give up in exchange for being larger. On its own the
// efficiency score picks short batches, which is per-job overhead for no write-amplification
// benefit; extending within this tolerance trades about 1% of efficiency for half as many jobs.
constexpr double compaction_batch_extension_tolerance = 0.8;

// Watermarks, in available segments, that drive automatic compaction. It starts once the number of
// available segments drops below `low` - the free-segment target - and stops once it is back at
// `high`. Both are zero when the trigger is disabled.
struct free_segment_watermarks {
    uint64_t low;
    uint64_t high;
};

// The free-segment target is a fraction of the disk, which is the dominant write-amplification
// knob, with an absolute floor that keeps the target meaningful on small disks, where a fraction of
// the disk rounds down to a segment or two.  `target_fraction` is
// logstor_compaction_trigger_threshold; 0 disables the trigger.
free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction,
        size_t max_segments_per_compaction) noexcept;

// Whether the free-segment level wants automatic compaction running, given whether it is running
// now. The two watermarks are a hysteresis band: compaction starts once the free-segment target is
// breached and runs until the disk is back at the stop watermark, rather than stopping again at the
// first write that crosses back over the target. Both watermarks are zero when the trigger is
// disabled, which answers "no" at any number of available segments.
//
// This is only the space half of the decision; whether compaction may run at all is the caller's.
bool auto_compaction_wanted(bool running, uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

// The cost of one candidate compaction batch: `n_in` segments are read and rewritten into `n_out`
// segments, copying `live_bytes` of live data.
struct compaction_candidate_score {
    size_t n_in;
    size_t n_out;
    uint64_t live_bytes;

    size_t reclaimed() const noexcept {
        return n_in > n_out ? n_in - n_out : 0;
    }

    // Segments reclaimed per segment-worth of data copied, which is exactly the reciprocal of the
    // batch's marginal write amplification. Comparisons cancel the segment_size factor, so it is
    // needed only where the absolute value is wanted, i.e. for logging.
    double efficiency(uint64_t segment_size) const noexcept {
        return live_bytes == 0
                ? std::numeric_limits<double>::infinity()
                : double(reclaimed()) * double(segment_size) / double(live_bytes);
    }

    // Whether this batch's efficiency is at least `fraction` of `other`'s.
    bool efficiency_at_least(const compaction_candidate_score& other, double fraction) const noexcept {
        if (live_bytes == 0) {
            return true;
        }
        if (other.live_bytes == 0) {
            return false;
        }
        return double(reclaimed()) * double(other.live_bytes)
                >= fraction * double(other.reclaimed()) * double(live_bytes);
    }

    bool operator==(const compaction_candidate_score& other) const noexcept = default;

    // Orders batches by reclamation efficiency, so that the score is the reciprocal of the write
    // amplification the batch would incur, and ties by how much the batch reclaims.
    bool operator<(const compaction_candidate_score& other) const noexcept;
};

// The batch select_compaction_batch() chose: the segments to compact, least utilized first, and the
// score of the batch as a whole - which is also the score its group is ranked by, so that the batch
// that wins the ranking is the one that would run.
struct compaction_batch {
    std::vector<const segment_descriptor*> segments;
    compaction_candidate_score score;
};

// Picks the segments one compaction job should read, out of the ones a group owns.
//
// The set's free-space histogram is already ordered by ascending utilization, which is near-optimal
// for victim ordering, so the only decision left is where to stop: the candidate set is the
// histogram's first `batch_cap` segments, and select_compaction_prefix() picks a prefix of it.
// Returns nothing when no prefix reclaims a segment, which answers for the whole group rather than
// only for this batch, since a longer prefix would only add fuller segments.
std::optional<compaction_batch> select_compaction_batch(const segment_set& segments,
        uint64_t segment_size, size_t batch_cap);

// Where the free-segment target falls on the compaction_shares_pressure() ramp. It is a property of
// the watermarks rather than a tunable: with the relative hysteresis of make_free_segment_watermarks()
// the target sits a third of the way up the ramp on any disk size, up to the rounding of the two
// watermarks to whole segments. Exported so that the shares controller can put a control point on it.
constexpr float compaction_shares_pressure_at_target = 1.0f / 3.0f;

// Space pressure driving the compaction shares controller: 0 at or above the high watermark, where
// automatic compaction stops and there is no space demand at all, compaction_shares_pressure_at_target
// at the free-segment target - the intended steady-state operating point - and 1 once half the
// target has been consumed, linear in between.
//
// The free-segment count is the integral of the mismatch between the write rate and the rate at
// which compaction reclaims, so mapping it proportionally to shares is integral control on that
// mismatch: the loop settles at whatever shares match the write rate, and the slope only decides
// where in the free-segment range it settles. That is why pressure saturates while half the target
// is still in hand instead of at the point where writes stall: a workload that needs the maximum
// shares must be able to settle somewhere that still leaves the write path room.
float compaction_shares_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

inline constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};
    segment_set* owner{nullptr}; // non-owning, set when added to a segment_set
    int ref_count{0};
    // Position in segment_set::_segment_list, set to no_index when the segment has no owner.
    static constexpr uint32_t no_index = std::numeric_limits<uint32_t>::max();
    uint32_t index_in_set{no_index};

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_write(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space -= net_data_size;
        record_count += cnt;
    }

    void on_write(log_location loc) noexcept {
        on_write(loc.size);
    }

    void on_free(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space += net_data_size;
        record_count -= cnt;
    }

    void on_free(log_location loc) noexcept {
        on_free(loc.size);
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

struct segment_set {
    // Segments ordered by free space, used to pick the segments to compact.
    segment_descriptor_hist _segments;
    // The same segments, in no particular order. Used to iterate efficiently and safely over all segments.
    utils::chunked_vector<segment_descriptor*> _segment_list;

    segment_set() = default;

    // Descriptors point back at their set and are linked into its containers, so a set cannot
    // be copied or moved without leaving all of them dangling.
    segment_set(const segment_set&) = delete;
    segment_set& operator=(const segment_set&) = delete;
    segment_set(segment_set&&) = delete;
    segment_set& operator=(segment_set&&) = delete;

    ~segment_set() {
        clear();
    }

    future<> merge(segment_set& other) {
        while (!other.empty()) {
            // Make room before unlinking, so that failing to grow leaves the segment in `other`.
            // Reserving per segment rather than for all of them up front also covers `other`
            // growing while this yields, which link() could not report, being noexcept.
            reserve_one();
            auto& desc = *other._segment_list.back();
            other.unlink(desc);
            link(desc);
            co_await coroutine::maybe_yield();
        }
    }

    // Throws if the set cannot grow, in which case nothing is modified.
    void add_segment(segment_descriptor& desc) {
        if (desc.owner) {
            on_internal_error(logstor_logger, "add_segment called for segment that has an owner");
        }
        reserve_one();
        link(desc);
        ++desc.ref_count;
    }

    void update_segment(segment_descriptor& desc) {
        _segments.adjust_up(desc);
    }

    void remove_segment(segment_descriptor& desc) noexcept {
        unlink(desc);
        --desc.ref_count;
    }

    // unlink all segments for shutdown.
    // don't decrement ref_count because the segments are still owned
    // by this group and we don't want to free them.
    void clear() noexcept {
        while (!empty()) {
            unlink(*_segment_list.back());
        }
    }

    size_t segment_count() const noexcept {
        return _segment_list.size();
    }

    bool empty() const noexcept {
        return _segment_list.empty();
    }

private:
    // Makes room for one more segment, so that the following link() cannot fail. Only useful when
    // nothing can be added to the set in between, since the room is not reserved for a caller.
    void reserve_one() {
        static constexpr size_t min_reservation = 16;
        static constexpr size_t chunk_capacity = decltype(_segment_list)::max_chunk_capacity();
        const size_t capacity = _segment_list.capacity();
        if (_segment_list.size() < capacity) {
            return;
        }
        // While the list is under one chunk, grow geometrically: reserving a single element
        // reallocates the partially filled last chunk and migrates it, which would make appending
        // segments quadratic. Once the capacity is a whole number of chunks there is nothing to
        // migrate, so add exactly one chunk. Clamping to chunk_capacity keeps the capacity chunk
        // aligned, which is what makes the following reservations migration free.
        _segment_list.reserve(capacity < chunk_capacity
                ? std::min(std::max(capacity * 2, min_reservation), chunk_capacity)
                : capacity + chunk_capacity);
    }

    // The caller must have made room for the segment, see reserve_one() and merge().
    // Linking must not fail, or the segment would be left owned by a set that doesn't hold it.
    void link(segment_descriptor& desc) noexcept {
        _segment_list.push_back(&desc);
        desc.owner = this;
        desc.index_in_set = _segment_list.size() - 1;
        _segments.push(desc);
    }

    // Validates the invariants of every removal path, and aborts rather than throwing, both
    // because it cannot leave the set half-updated and because it runs from the destructor.
    void unlink(segment_descriptor& desc) noexcept {
        if (desc.owner != this) {
            on_fatal_internal_error(logstor_logger, "unlinking a segment from a set that is not its owner");
        }
        if (desc.index_in_set >= _segment_list.size() || _segment_list[desc.index_in_set] != &desc) {
            on_fatal_internal_error(logstor_logger, "segment is not at its recorded position in its set");
        }
        _segments.erase(desc);
        // Keep the list compact by moving the last segment into the freed slot.
        auto* last = _segment_list.back();
        _segment_list[desc.index_in_set] = last;
        last->index_in_set = desc.index_in_set;
        _segment_list.pop_back();
        desc.owner = nullptr;
        desc.index_in_set = segment_descriptor::no_index;
    }
};

class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_failure;
        bool flush_failure{false};
        ~state() {
            if (!flush_failure) {
                if (on_last_release) on_last_release();
            } else {
                if (on_failure) on_failure();
            }
        }
    };
    lw_shared_ptr<state> _state;
public:
    segment_ref() = default;

    // Copyable: copying increments the shared ref count
    segment_ref(const segment_ref&) = default;
    segment_ref& operator=(const segment_ref&) = default;
    segment_ref(segment_ref&&) noexcept = default;
    segment_ref& operator=(segment_ref&&) noexcept = default;

    log_segment_id id() const noexcept { return _state->id; }
    bool empty() const noexcept { return !_state; }

    void set_flush_failure() noexcept { if (_state) _state->flush_failure = true; }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_failure)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_failure)))
    {}
};

struct separator_buffer {
    write_buffer buf;
    utils::chunked_vector<future<>> pending_updates;
    utils::chunked_vector<segment_ref> held_segments;
    std::optional<segment_sequence> min_seq_num;

    separator_buffer(size_t buffer_size)
        : buf(buffer_size, segment_kind::full)
    {}

    separator_buffer(const separator_buffer&) = delete;
    separator_buffer& operator=(const separator_buffer&) = delete;

    separator_buffer(separator_buffer&&) noexcept = default;
    separator_buffer& operator=(separator_buffer&&) noexcept = default;

    template <log_record_writer_concept Writer>
    void write(segment_ref seg_ref, std::optional<segment_sequence> segment_seq_num, Writer writer, separator_write_completion after_written) {
        // The separator buffer holds a reference to the source segment until its updates are durable.
        if (held_segments.empty() || held_segments.back().id() != seg_ref.id()) {
            held_segments.push_back(std::move(seg_ref));
        }

        if (segment_seq_num && (!min_seq_num || *segment_seq_num < *min_seq_num)) {
            min_seq_num = *segment_seq_num;
        }

        pending_updates.push_back(
            buf.write(std::move(writer)).then_unpack(std::move(after_written))
        );
    }

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept {
        return buf.can_fit(writer);
    }

    bool can_fit(size_t write_size) const noexcept {
        return buf.can_fit(write_size);
    }

    bool empty() const noexcept {
        return !buf.has_data();
    }

    void reset() {
        buf.reset();
        pending_updates.clear();
        held_segments.clear();
        min_seq_num.reset();
    }

    future<> abort(std::exception_ptr);
};

class compaction_reenabler {
    std::function<void()> _release;
public:
    compaction_reenabler() = default;
    explicit compaction_reenabler(std::function<void()> release)
        : _release(std::move(release)) {}
    ~compaction_reenabler() { if (_release) _release(); }

    compaction_reenabler(compaction_reenabler&&) = default;
    compaction_reenabler& operator=(compaction_reenabler&&) = default;
    compaction_reenabler(const compaction_reenabler&) = delete;
    compaction_reenabler& operator=(const compaction_reenabler&) = delete;
};

class compaction_manager {
public:
    virtual ~compaction_manager() = default;

    virtual future<> flush_separator_buffer(separator_buffer&, logstor_group&) = 0;

    virtual void add(logstor_group&) = 0;
    virtual future<> remove(logstor_group&) = 0;

    virtual void submit(logstor_group&) = 0;

    virtual future<> submit_split_compaction(replica::table&, logstor_group&, mutation_writer::classify_by_token_group) = 0;

    virtual future<> stop_ongoing_compactions(logstor_group&) = 0;

    virtual future<compaction_reenabler> disable_compaction(logstor_group&) = 0;
    virtual compaction_reenabler disable_compaction_no_wait(logstor_group&) = 0;
};

// A compaction group in logstor.
//
// The group owns a `segment_set` of segments whose records all belong to it, and therefore to a
// single table and token range. Compaction rewrites the live records of a batch of those segments
// into new segments of the same group.
//
// The group is also the target of a write: the separator hands it the records of the shared active
// segment that belong to it, and buffers them in its separator buffer, which is flushed into a new
// segment in the group.
class logstor_group {
    segment_set _logstor_segments;
    std::array<separator_buffer, 2> _separator_buffers;
    size_t _active_separator_buffer{0};
    uint64_t _separator_generation{1};
    shared_future<> _separator_flush{make_ready_future<>()};

    auto& active_separator_buffer(this auto& self) noexcept {
        return self._separator_buffers[self._active_separator_buffer];
    }

    auto& inactive_separator_buffer(this auto& self) noexcept {
        return self._separator_buffers[1 - self._active_separator_buffer];
    }

    void switch_active_separator_buffer();

protected:
    explicit logstor_group(size_t separator_buffer_size);

    virtual compaction_manager& logstor_compaction_manager() noexcept = 0;

public:
    virtual ~logstor_group() = default;

    virtual table_id table_id() const noexcept = 0;

    virtual primary_index& logstor_index() noexcept = 0;
    virtual const primary_index& logstor_index() const noexcept = 0;

    segment_set& logstor_segments() noexcept {
        return _logstor_segments;
    }
    const segment_set& logstor_segments() const noexcept {
        return _logstor_segments;
    }

    void add_logstor_segment(segment_descriptor& desc) {
        _logstor_segments.add_segment(desc);
    }

    void clear_segments() {
        _logstor_segments.clear();
    }

    template <log_record_writer_concept Writer>
    future<> write_to_separator(Writer, segment_ref, std::optional<segment_sequence>, separator_write_completion);

    future<> flush_separator(std::optional<segment_sequence> seq_num = std::nullopt);

    bool empty() const noexcept {
        return _logstor_segments.empty()
            && std::ranges::all_of(_separator_buffers, [] (const auto& buf) { return buf.empty(); })
            && _separator_flush.available();
    }

    bool separator_has_data() const noexcept {
        return std::ranges::any_of(_separator_buffers, [] (const auto& buf) { return !buf.empty(); });
    }

    size_t separator_held_segment_count() const noexcept {
        return std::ranges::fold_left(_separator_buffers, size_t(0), [] (size_t count, const auto& buf) {
            return count + buf.held_segments.size();
        });
    }
};

} // namespace replica::logstor
