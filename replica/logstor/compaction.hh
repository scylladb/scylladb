/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "types.hh"
#include "utils/chunked_vector.hh"
#include "write_buffer.hh"
#include "utils/log_heap.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include "mutation_writer/token_group_based_splitting_writer.hh"

namespace replica {
class table;
} // namespace replica

namespace replica::logstor {

extern seastar::logger logstor_logger;

constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_set;

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};
    segment_generation seg_gen{1};
    segment_set* owner{nullptr}; // non-owning, set when added to a segment_set
    int ref_count{0};

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_free_segment() noexcept {
        ++seg_gen;
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
    write_buffer* buf;
    utils::chunked_vector<future<>> pending_updates;
    utils::chunked_vector<segment_ref> held_segments;
    std::optional<size_t> min_seq_num;
    bool flushed{false};

    separator_buffer(write_buffer* wb)
        : buf(wb)
    {}

    ~separator_buffer() {
        if (!flushed && buf && buf->has_data()) {
            for (auto& seg_ref : held_segments) {
                seg_ref.set_flush_failure();
            }
        }
    }

    separator_buffer(const separator_buffer&) = delete;
    separator_buffer& operator=(const separator_buffer&) = delete;

    separator_buffer(separator_buffer&&) noexcept = default;
    separator_buffer& operator=(separator_buffer&&) noexcept = default;

    future<log_location_with_holder> write(log_record_writer writer) {
        return buf->write(std::move(writer));
    }

    bool can_fit(const log_record_writer& writer) const noexcept {
        return buf->can_fit(writer);
    }

    bool can_fit(size_t write_size) const noexcept {
        return buf->can_fit(write_size);
    }

    bool empty() const noexcept {
        return !buf->has_data();
    }
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

    virtual separator_buffer allocate_separator_buffer() = 0;

    virtual future<> flush_separator_buffer(separator_buffer, replica::compaction_group&) = 0;

    virtual void submit(replica::compaction_group&) = 0;

    virtual future<> stop_ongoing_compactions(replica::compaction_group&) = 0;

    virtual future<compaction_reenabler> disable_compaction(replica::compaction_group&) = 0;
    virtual compaction_reenabler disable_compaction_no_wait(replica::compaction_group&) = 0;

    virtual future<> split_compaction(replica::table&, replica::compaction_group&, mutation_writer::classify_by_token_group) = 0;
};

} // namespace replica::logstor
