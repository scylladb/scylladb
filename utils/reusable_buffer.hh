/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include <seastar/core/timer.hh>
#include <seastar/core/memory.hh>
#include <bit>

namespace utils {

// Users of reusable_buffer don't need the templated parts,
// so the non-templated implementation parts are extracted
// to a separate class.
// This should prevent some unnecessary template instantiations.
class reusable_buffer_impl {
protected:
    friend class reusable_buffer_guard;

    // Max size observed since the last decay(), rounded up.
    // Currently we round to the smallest power of 2 greater or equal to
    // to the observed size.
    size_t _high_watermark = 0;
    std::unique_ptr<bytes::value_type[]> _buf; // The underlying contiguous buffer.
    size_t _buf_size = 0;
    size_t _refcount = 0;                // Used purely for anti-misuse checks.
    size_t _reallocs = 0;                // Number of size changes.
public:
    size_t size() const noexcept { return _buf_size; }
    size_t reallocs() const noexcept { return _reallocs; }
protected:
    // The guard keeps a reference to the buffer, so it must have a stable address.
    reusable_buffer_impl(const reusable_buffer_impl&) = delete;
    reusable_buffer_impl& operator=(const reusable_buffer_impl&) = delete;

    reusable_buffer_impl() = default;

    ~reusable_buffer_impl() {
        SCYLLA_ASSERT(_refcount == 0);
    }

    void resize(size_t new_size) & {
        // Reusable buffers are expected to be used when large contiguous
        // allocations are unavoidable. There is no point in warning
        // about them since there isn't much that can be done.
        seastar::memory::scoped_large_allocation_warning_disable g;

        // Clear before shrinking so that the old buffer
        // is freed before allocating the new buffer.
        // This lesses the pressure on the allocator and should guarantee
        // a success if the new size is smaller than the old size.
        _buf = nullptr;
        _buf_size = 0;
        _reallocs += 1;
        _buf.reset(new bytes::value_type[new_size]);
        _buf_size = new_size;
    }

// The below methods can be considered "public", but they are only accessible
// through their guard wrappers, to make misuse harder.

    bytes_mutable_view get_temporary_buffer(size_t size) & {
        _high_watermark = std::max(_high_watermark, std::bit_ceil(size));
        if (_high_watermark > _buf_size) {
            resize(_high_watermark);
        }
        return {_buf.get(), size};
    }

    // The helpers below only interact with the rest of the class through
    // get_temporary_buffer().
    // They could as well be free functions.

    // Returns a linearized view onto the provided data.
    // If the input view is already linearized, it is returned as-is.
    // Otherwise the contents are copied to the reusable buffer
    // and a view into the buffer is returned.
    bytes_view get_linearized_view(fragmented_temporary_buffer::view ftb) & {
        if (ftb.current_fragment().size() == ftb.size_bytes()) {
            return {ftb.current_fragment().data(), ftb.size_bytes()};
        }
        const auto out = get_temporary_buffer(ftb.size_bytes()).data();
        auto dst = out;
        for (bytes_view fragment : fragmented_temporary_buffer::view(ftb)) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        }
        return {out, ftb.size_bytes()};
    }

    // Returns a linearized view onto the provided data.
    // If the input view is already linearized, it is returned as-is.
    // Otherwise the contents are copied to the reusable buffer
    // and a view into the buffer is returned.
    bytes_view get_linearized_view(bytes_ostream& bo) & {
        if (bo.is_linearized()) {
            return bo.view();
        }
        const auto out = get_temporary_buffer(bo.size()).data();
        auto dst = out;
        for (bytes_view fragment : bo) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        }
        return {out, bo.size()};
    }

    // Provides a contiguous buffer of size `maximum_length` to `fn`.
    // `fn` writes to the buffer and returns the number of bytes written.
    // A fragmented buffer containing the data written by `fn` is returned.
    //
    // If the data fits into a single fragment, `fn` is ran directly on
    // the only fragment of a newly allocated fragmented_buffer. 
    // Otherwise the contiguous buffer for `fn` is allocated from the reusable
    // buffer, and its contents are copied to a new fragmented buffer after `fn`
    // returns.
    // This way a copy is avoided in the small case.
    template<typename Function>
    requires std::is_invocable_r_v<size_t, Function, bytes_mutable_view>
    bytes_ostream make_bytes_ostream(size_t maximum_length, Function&& fn) & {
        bytes_ostream output;
        if (maximum_length && maximum_length <= bytes_ostream::max_chunk_size()) {
            auto ptr = output.write_place_holder(maximum_length);
            size_t actual_length = fn(bytes_mutable_view(ptr, maximum_length));
            output.remove_suffix(output.size() - actual_length);
        } else {
            auto view = get_temporary_buffer(maximum_length);
            size_t actual_length = fn(view);
            output.write(bytes_view(view.data(), actual_length));
        }
        return output;
    }

    // Provides a contiguous buffer of size `maximum_length` to `fn`.
    // `fn` writes to the buffer and returns the number of bytes written.
    // A fragmented buffer containing the data written by `fn` is returned.
    //
    // If the data fits into a single fragment, `fn` is ran directly on
    // the only fragment of a newly allocated fragmented_buffer. 
    // Otherwise the contiguous buffer for `fn` is allocated from the reusable
    // buffer, and its contents are copied to a new fragmented buffer after `fn`
    // returns.
    // This way a copy is avoided in the small case.
    template<typename Function>
    requires std::is_invocable_r_v<size_t, Function, bytes_mutable_view>
    fragmented_temporary_buffer make_fragmented_temporary_buffer(size_t maximum_length, Function&& fn) & {
        if (maximum_length <= fragmented_temporary_buffer::default_fragment_size) {
            seastar::temporary_buffer<char> buf(maximum_length);
            auto view = bytes_mutable_view(reinterpret_cast<bytes::value_type*>(buf.get_write()), buf.size());
            size_t actual_length = fn(view);
            buf.trim(actual_length);
            std::vector<seastar::temporary_buffer<char>> chunks;
            chunks.push_back(std::move(buf));
            return fragmented_temporary_buffer(std::move(chunks), actual_length);
        } else {
            auto view = get_temporary_buffer(maximum_length);
            size_t actual_length = fn(view);
            return fragmented_temporary_buffer(reinterpret_cast<const char*>(view.data()), actual_length);
        }
    }
};

/* Sometimes (e.g. for compression), we need a big contiguous buffer
 * of a given size.
 *
 * Big buffers are a problem for the allocator, so if such a buffer
 * is needed regularly, we want to reuse it. In the optimum, we
 * only want to use one buffer per concurrent task (if all uses
 * of the buffer are synchronous, one buffer per shard is enough),
 * and only resize the buffer when it's too small for the current
 * request.
 *
 * At the same time we might not want to keep the buffer around forever.
 * A pathological request might cause the buffer to grow to some
 * unreasonable size. After the source of the pathology (e.g. bad compression
 * chunk size) is fixed, we want the buffer to free the capacity that will
 * most likely not be used again.
 *
 * This class provides a buffer which balances both tasks by periodically
 * shrinking its size to the largest recently seen size.
 * The buffer always has a power-of-2 size, to minimize the number of
 * allocations for small size changes.
 *
 * Even though the views returned by the buffer's methods have exactly the same
 * requested size, the entire underlying capacity (of size size()) is safe to use.
 * The returned views are always a prefix of the underlying capacity.
 *
 * Under a stable workload, the buffer's size will be stable.
 * If a pathological request comes, the buffer will grow to accommodate it.
 * 1-2 periods after the pathology ceases, the buffer will shrink back
 * to its steady-state size.
 *
 * Be very careful with this buffer.
 * Its content might disappear during a preemption point. This can be
 * checked by comparing reallocs() before and after the preemption point.
 * The content might be also easily invalidated by an unrelated future
 * sharing the same buffer, if you are not careful.
 *
 * To make this buffer slightly harder to misuse, it's only accessible
 * by reusable_buffer_guard.
 * There can only exist one guard at a time.
 * The guard can be used to obtain a view at most once.
 */
template <typename Clock>
class reusable_buffer : public reusable_buffer_impl {
public:
    using period_type = typename Clock::duration;
private:
    seastar::timer<Clock> _decay_timer;
    period_type _decay_period;

    void decay() & {
        SCYLLA_ASSERT(_refcount == 0);
        if (_high_watermark <= _buf_size / 16) {
            // We shrink when the size falls at least by four power-of-2
            // notches, instead of just one notch. This adds hysteresis:
            // it prevents small oscillations from inducing shrink/grow cycles.
            // With the factor of 16 above, only outliers at least 8x bigger than
            // the otherwise stable size might cause reallocations.
            //
            // In other words, we "define" a pathological request as one that is
            // at least 8x bigger than the stable size, where the stable size
            // is the max of all sizes seen within a decay period.
            resize(_high_watermark);
        }
        _high_watermark = 0;
    }
public:
    reusable_buffer(period_type period)
        : _decay_period(period)
    {
        _decay_timer.set_callback([this] {decay();});
        _decay_timer.arm_periodic(_decay_period);
    }
};

/* Exists only to SCYLLA_ASSERT that there exists at most one reference to the
 * reusable_buffer, to hopefully make it less of a footgun.
 *
 * The reference/use counts exist only for SCYLLA_ASSERT purposes.
 * They don't influence the program otherwise.
 *
 * Never keep the guard across preemption points.
 *
 * Don't let views obtained through the guard outlive the guard,
 * that defeats its purpose and is begging for trouble.
 *
 * The guard only accesses _refcount and its "public" methods.
 * It doesn't mess with its internals.
 */
class reusable_buffer_guard {
private:
    reusable_buffer_impl& _buf;
    bool used = false;
private:
    void mark_used() {
        SCYLLA_ASSERT(!used);
        used = true;
    }
public:
    reusable_buffer_guard(const reusable_buffer_guard&) = delete;
    reusable_buffer_guard& operator=(const reusable_buffer_guard&) = delete;

    reusable_buffer_guard(reusable_buffer_impl& _buf)
        : _buf(_buf)
    {
        SCYLLA_ASSERT(_buf._refcount == 0);
        _buf._refcount += 1;
    }

    ~reusable_buffer_guard() {
        _buf._refcount -= 1;
    }

    // The result mustn't outlive `this`.
    // No method of `this` may be called again.
    bytes_mutable_view get_temporary_buffer(size_t size) & {
        mark_used();
        return _buf.get_temporary_buffer(size);
    }

    // The result mustn't outlive `this`.
    // No method of `this` may be called again.
    bytes_view get_linearized_view(fragmented_temporary_buffer::view ftb) & {
        mark_used();
        return _buf.get_linearized_view(std::move(ftb));
    }

    // The result mustn't outlive `this`.
    // No method of `this` may be called again.
    bytes_view get_linearized_view(bytes_ostream& bo) & {
        mark_used();
        return _buf.get_linearized_view(bo);
    }

    // The result mustn't outlive `this`.
    // No method of `this` may be called again.
    template<typename Function>
    requires std::is_invocable_r_v<size_t, Function, bytes_mutable_view>
    bytes_ostream make_bytes_ostream(size_t maximum_length, Function&& fn) & {
        mark_used();
        return _buf.make_bytes_ostream(maximum_length, std::forward<Function>(fn));
    }

    // The result mustn't outlive `this`.
    // No method of `this` may be called again.
    template<typename Function>
    requires std::is_invocable_r_v<size_t, Function, bytes_mutable_view>
    fragmented_temporary_buffer make_fragmented_temporary_buffer(size_t maximum_length, Function&& fn) & {
        mark_used();
        return _buf.make_fragmented_temporary_buffer(maximum_length, std::forward<Function>(fn));
    }
};

} // namespace utils
