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
#include <array>
#include <ranges>
#include <optional>
#include <vector>

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

constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};
    segment_set* owner{nullptr}; // non-owning, set when added to a segment_set
    int ref_count{0};

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
    segment_descriptor_hist _segments;
    size_t _segment_count{0};

    ~segment_set() {
        clear();
    }

    future<> merge(segment_set& other) {
        while (!other._segments.empty()) {
            auto& desc = other._segments.one_of_largest();
            other._segments.erase(desc);
            --other._segment_count;
            desc.owner = this;
            _segments.push(desc);
            ++_segment_count;
            co_await coroutine::maybe_yield();
        }
    }

    void add_segment(segment_descriptor& desc) {
        if (desc.owner) {
            on_internal_error(logstor_logger, "add_segment called for segment that has an owner");
        }
        desc.owner = this;
        _segments.push(desc);
        ++desc.ref_count;
        ++_segment_count;
    }

    void update_segment(segment_descriptor& desc) {
        _segments.adjust_up(desc);
    }

    void remove_segment(segment_descriptor& desc) {
        if (desc.owner != this) {
            on_internal_error(logstor_logger, "remove_segment called not from the owner");
        }
        _segments.erase(desc);
        desc.owner = nullptr;
        --desc.ref_count;
        --_segment_count;
    }

    // unlink all segments for shutdown.
    // don't decrement ref_count because the segments are still owned
    // by this group and we don't want to free them.
    void clear() {
        while (!empty()) {
            auto& desc = _segments.one_of_largest();
            if (desc.owner != this) {
                on_internal_error(logstor_logger, "remove_segment called not from the owner");
            }
            _segments.erase(desc);
            desc.owner = nullptr;
            --_segment_count;
        }
    }

    size_t segment_count() const noexcept {
        return _segment_count;
    }

    bool empty() const noexcept {
        return _segment_count == 0;
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

    void write(segment_ref seg_ref, std::optional<segment_sequence> segment_seq_num, log_record_writer writer, separator_write_completion after_written) {
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

    bool can_fit(const log_record_writer& writer) const noexcept {
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

    virtual void submit(logstor_group&) = 0;

    virtual future<> submit_split_compaction(replica::table&, logstor_group&, mutation_writer::classify_by_token_group) = 0;

    virtual future<> stop_ongoing_compactions(logstor_group&) = 0;
    virtual future<> remove(logstor_group&) = 0;

    virtual future<compaction_reenabler> disable_compaction(logstor_group&) = 0;
    virtual compaction_reenabler disable_compaction_no_wait(logstor_group&) = 0;
};

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

    future<> write_to_separator(log_record_writer, segment_ref, std::optional<segment_sequence>, separator_write_completion);

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
