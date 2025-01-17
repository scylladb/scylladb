/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "query_ranges_to_vnodes_async.hh"

static logging::logger qlogger("query_ranges_to_vnodes_async");

query_ranges_to_vnodes_generator_async::query_ranges_to_vnodes_generator_async(
            std::unique_ptr<locator::token_range_splitter> splitter,
            schema_ptr s,
            partition_ranges_generator& gen,
            bool local)
        : query_ranges_to_vnodes_generator(std::move(splitter), s, {}, local)
        , _gen(gen)
        , _gen_exhausted(false) {}

future<std::optional<dht::partition_range_vector>> query_ranges_to_vnodes_generator_async::operator()(size_t n) {
    if (!_error.empty()) {
        co_return std::nullopt;
    }
    n = std::min(n, size_t(1024));

    dht::partition_range_vector result;
    result.reserve(n);

    while (!_gen_exhausted && result.size() != n) {
        if (_i != _ranges.end()) {
            qlogger.trace("Iterator not exhausted, processing one range: result size = {}, goal = {}", result.size(), n);
            process_one_range(n, result);
        } else if (auto ranges_and_paging_state = co_await _gen()) {
            if (ranges_and_paging_state->has_error()) {
                _error = std::move(ranges_and_paging_state->assume_error());
                co_return std::nullopt;
            }
            auto&& [ranges, paging_state] = ranges_and_paging_state->assume_value();
            qlogger.trace("Iterator exhausted, obtained new partition range vector from generator: size = {}, goal = {}, result size = {}", ranges.size(), n, result.size());
            _ranges = std::move(ranges);
            _i = _ranges.begin();
            _paging_state = std::move(paging_state);
        } else {
            qlogger.trace("Iterator exhausted, called generator but returned nothing");
            _gen_exhausted = true;
        }
    }
    co_return result;
}

future<void> query_ranges_to_vnodes_generator_async::prefetch() {
    if (!_ranges.empty()) {
        co_return;
    }
    auto ranges_and_paging_state = co_await _gen();
    if (ranges_and_paging_state->has_error()) {
        _error = std::move(ranges_and_paging_state->assume_error());
        co_return;
    }
    auto&& [ranges, paging_state] = ranges_and_paging_state->assume_value();
    qlogger.trace("Prefetched first partition range vector: size = {}", ranges.size());
    _ranges = std::move(ranges);
    _i = _ranges.begin();
    _paging_state = std::move(paging_state);
}

bool query_ranges_to_vnodes_generator_async::empty() const {
    return _gen_exhausted;
}

lw_shared_ptr<const service::pager::paging_state> query_ranges_to_vnodes_generator_async::get_paging_state() {
    return _paging_state;
}

exceptions::coordinator_exception_container query_ranges_to_vnodes_generator_async::get_error() {
    return std::move(_error);
}