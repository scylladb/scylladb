/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/error_history.hh"
#include <algorithm>

namespace cql3 {

error_history::error_history(size_t max_errors)
    : _max_errors(std::clamp(max_errors, MIN_MAX_ERRORS, MAX_MAX_ERRORS))
    , _next_index(1)
{}

void error_history::record_error(const seastar::sstring& error_message, const seastar::sstring& query) {
    std::lock_guard<std::mutex> lock(_mutex);
    
    // Remove oldest if at capacity
    if (_errors.size() >= _max_errors) {
        _errors.pop_front();
    }
    
    _errors.emplace_back(_next_index++, error_message, query);
}

std::vector<error_record> error_history::get_all_errors() const {
    std::lock_guard<std::mutex> lock(_mutex);
    
    std::vector<error_record> result;
    result.reserve(_errors.size());
    
    // Return in reverse order (most recent first) with 1-based display indices
    size_t display_index = 1;
    for (auto it = _errors.rbegin(); it != _errors.rend(); ++it) {
        error_record record = *it;
        record.index = display_index++;
        result.push_back(record);
    }
    
    return result;
}

std::optional<error_record> error_history::get_error(size_t index) const {
    std::lock_guard<std::mutex> lock(_mutex);
    
    // index is 1-based, 1 = most recent
    if (index < 1 || index > _errors.size()) {
        return std::nullopt;
    }
    
    // Convert 1-based "most recent first" index to actual deque position
    // index 1 -> _errors.back(), index N -> _errors.front()
    size_t actual_index = _errors.size() - index;
    
    error_record record = _errors[actual_index];
    record.index = index;  // Set display index
    return record;
}

std::optional<error_record> error_history::get_most_recent_error() const {
    return get_error(1);
}

size_t error_history::size() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _errors.size();
}

bool error_history::empty() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _errors.empty();
}

void error_history::clear() {
    std::lock_guard<std::mutex> lock(_mutex);
    _errors.clear();
}

void error_history::set_max_size(size_t max_errors) {
    std::lock_guard<std::mutex> lock(_mutex);
    _max_errors = std::clamp(max_errors, MIN_MAX_ERRORS, MAX_MAX_ERRORS);
    
    // Trim if necessary
    while (_errors.size() > _max_errors) {
        _errors.pop_front();
    }
}

} // namespace cql3
