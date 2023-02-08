/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/tombstone.hh"

// Merges range tombstone changes coming from different streams (readers).
//
// Add tombstones by calling apply().
// Updates to the same stream identified by stream_id overwrite each other.
// Applying an empty tombstone to a stream removes said stream.
//
// Call get() to get the tombstone with the highest timestamp from the currently
// active ones.
// Get returns disengaged optional when the last apply() call(s) didn't introduce
// a change in the max tombstone.
//
// Call clear() when leaving the range abruptly (next_partition() or
// fast_forward_to()).
//
// The merger doesn't keep track of the position component of the tombstones.
// Emit them with the current position.
template<typename stream_id_t>
class range_tombstone_change_merger {
    struct stream_tombstone {
        stream_id_t stream_id;
        ::tombstone tombstone;
    };

private:
    std::vector<stream_tombstone> _tombstones;
    tombstone _current_tombstone;

private:
    const stream_tombstone* get_tombstone() const {
        const stream_tombstone* max_tomb{nullptr};

        for (const auto& tomb : _tombstones) {
            if (!max_tomb || tomb.tombstone > max_tomb->tombstone) {
                max_tomb = &tomb;
            }
        }
        return max_tomb;
    }

public:
    void apply(stream_id_t stream_id, tombstone tomb) {
        auto it = std::find_if(_tombstones.begin(), _tombstones.end(), [&] (const stream_tombstone& tomb) {
            return tomb.stream_id == stream_id;
        });
        if (it == _tombstones.end()) {
            if (tomb) {
                _tombstones.push_back({stream_id, tomb});
            }
        } else {
            if (tomb) {
                it->tombstone = tomb;
            } else {
                auto last = _tombstones.end() - 1;
                if (it != last) {
                    std::swap(*it, *last);
                }
                _tombstones.pop_back();
            }
        }
    }

    std::optional<tombstone> get() {
        const auto* tomb = get_tombstone();
        if (tomb && tomb->tombstone == _current_tombstone) {
            return {};
        } else {
            _current_tombstone = tomb ? tomb->tombstone : tombstone();
            return _current_tombstone;
        }
    }

    tombstone peek() const {
        const stream_tombstone* tomb = get_tombstone();
        if (tomb) {
            return tomb->tombstone;
        } else {
            return {};
        }
    }

    void clear() {
        _tombstones.clear();
        _current_tombstone = {};
    }
};
