/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string>
#include <deque>
#include <optional>
#include <chrono>
#include <mutex>
#include <vector>

#include <seastar/core/sstring.hh>

namespace cql3 {

/**
 * Represents a single recorded error from a CQL session.
 */
struct error_record {
    size_t index;                                    // Display index (1-based, most recent = 1)
    seastar::sstring error_message;                  // The error message
    seastar::sstring query;                          // The query that caused the error
    std::chrono::system_clock::time_point timestamp; // When the error occurred

    error_record(size_t idx, seastar::sstring err, seastar::sstring q)
        : index(idx)
        , error_message(std::move(err))
        , query(std::move(q))
        , timestamp(std::chrono::system_clock::now())
    {}
};

/**
 * Thread-safe bounded history of errors for a CQL session.
 * Used by LIST ERRORS and DIAGNOSE ERROR statements.
 */
class error_history {
public:
    static constexpr size_t DEFAULT_MAX_ERRORS = 10;
    static constexpr size_t MIN_MAX_ERRORS = 1;
    static constexpr size_t MAX_MAX_ERRORS = 100;

    explicit error_history(size_t max_errors = DEFAULT_MAX_ERRORS);

    /**
     * Record a new error. If at capacity, oldest error is removed.
     */
    void record_error(const seastar::sstring& error_message, const seastar::sstring& query);

    /**
     * Get all errors, most recent first. Index 1 = most recent.
     */
    std::vector<error_record> get_all_errors() const;

    /**
     * Get a specific error by display index (1 = most recent).
     * Returns nullopt if index is out of range.
     */
    std::optional<error_record> get_error(size_t index) const;

    /**
     * Get the most recent error (equivalent to get_error(1)).
     */
    std::optional<error_record> get_most_recent_error() const;

    size_t size() const;
    bool empty() const;
    void clear();
    size_t max_size() const { return _max_errors; }
    void set_max_size(size_t max_errors);

private:
    size_t _max_errors;
    size_t _next_index;  // Monotonically increasing ID for internal tracking
    std::deque<error_record> _errors;
    mutable std::mutex _mutex;
};

} // namespace cql3
