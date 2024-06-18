/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader.hh"

class queue_reader_v2;

/// Calls to different methods cannot overlap!
/// The handle can be used only while the reader is still alive. Once
/// `push_end_of_stream()` is called, the reader and the handle can be destroyed
/// in any order. The reader can be destroyed at any time.
class queue_reader_handle_v2 {
    friend std::pair<mutation_reader, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr, reader_permit);
    friend class queue_reader_v2;

private:
    queue_reader_v2* _reader = nullptr;
    std::exception_ptr _ex;

private:
    explicit queue_reader_handle_v2(queue_reader_v2& reader) noexcept;

    void abandon() noexcept;

public:
    queue_reader_handle_v2(queue_reader_handle_v2&& o) noexcept;
    ~queue_reader_handle_v2();
    queue_reader_handle_v2& operator=(queue_reader_handle_v2&& o);

    future<> push(mutation_fragment_v2 mf);

    /// Terminate the queue.
    ///
    /// The reader will be set to EOS. The handle cannot be used anymore.
    void push_end_of_stream();

    /// Aborts the queue.
    ///
    /// All future operations on the handle or the reader will raise `ep`.
    void abort(std::exception_ptr ep);

    /// Checks if the queue is already terminated with either a success or failure (abort)
    bool is_terminated() const;

    /// Get the stored exception, if any
    std::exception_ptr get_exception() const noexcept;
};

std::pair<mutation_reader, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr s, reader_permit permit);

