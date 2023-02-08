/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <exception>
#include <seastar/core/print.hh>

#include "mutation/mutation_fragment_v2.hh"

/// Converts a stream of range_tombstone_change fragments to an equivalent stream of range_tombstone objects.
/// The input fragments must be ordered by their position().
/// The produced range_tombstone objects are non-overlapping and ordered by their position().
///
/// on_end_of_stream() must be called after consuming all fragments to produce the final fragment.
///
/// Example usage:
///
///   range_tombstone_assembler rta;
///   if (auto rt_opt = rta.consume(range_tombstone_change(...))) {
///       produce(*rt_opt);
///   }
///   if (auto rt_opt = rta.consume(range_tombstone_change(...))) {
///       produce(*rt_opt);
///   }
///   if (auto rt_opt = rta.flush(position_in_partition(...)) {
///       produce(*rt_opt);
///   }
///   rta.on_end_of_stream();
///
class range_tombstone_assembler {
    std::optional<range_tombstone_change> _prev_rt;
private:
    bool has_active_tombstone() const {
        return _prev_rt && _prev_rt->tombstone();
    }
public:
    tombstone get_current_tombstone() const {
        return _prev_rt ? _prev_rt->tombstone() : tombstone();
    }

    std::optional<range_tombstone_change> get_range_tombstone_change() && {
        return std::move(_prev_rt);
    }

    void reset() {
        _prev_rt = std::nullopt;
    }

    std::optional<range_tombstone> consume(const schema& s, range_tombstone_change&& rt) {
        std::optional<range_tombstone> rt_opt;
        auto less = position_in_partition::less_compare(s);
        if (has_active_tombstone() && less(_prev_rt->position(), rt.position())) {
            rt_opt = range_tombstone(_prev_rt->position(), rt.position(), _prev_rt->tombstone());
        }
        _prev_rt = std::move(rt);
        return rt_opt;
    }

    void on_end_of_stream() {
        if (has_active_tombstone()) {
            throw std::logic_error(format("Stream ends with an active range tombstone: {}", *_prev_rt));
        }
    }

    // Returns true if and only if flush() may return something.
    // Returns false if flush() won't return anything for sure.
    bool needs_flush() const {
        return has_active_tombstone();
    }

    std::optional<range_tombstone> flush(const schema& s, position_in_partition_view pos) {
        auto less = position_in_partition::less_compare(s);
        if (has_active_tombstone() && less(_prev_rt->position(), pos)) {
            position_in_partition start = _prev_rt->position();
            _prev_rt->set_position(position_in_partition(pos));
            return range_tombstone(std::move(start), pos, _prev_rt->tombstone());
        }
        return std::nullopt;
    }

    bool discardable() const {
        return !has_active_tombstone();
    }
};
