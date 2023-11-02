/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/ring_position.hh"
#include "dht/token.hh"
#include "locator/token_metadata_fwd.hh"

#include <optional>

namespace locator {

/// Generates split points which divide the ring into ranges which share the same replica set.
///
/// Initially the ring space the splitter works with is set to the whole ring.
/// The space can be changed using reset().
class token_range_splitter {
public:
    virtual ~token_range_splitter() = default;

    /// Resets the splitter to work with the ring range [pos, +inf).
    virtual void reset(dht::ring_position_view pos) = 0;

    /// Each token t returned by next_token() means that keys in the range:
    ///
    ///   [prev_pos, dht::ring_position_view::ending_at(t))
    ///
    /// share the same replica set.
    ///
    /// If this is the first call to next_token() after construction or reset() then prev_pos is the
    /// beginning of the ring space. Otherwise, it is dht::ring_position_view::ending_at(prev_t)
    /// where prev_t is the token returned by the previous call to next_token().
    /// If std::nullopt is returned it means that the ring space was exhausted.
    virtual std::optional<dht::token> next_token() = 0;
};

std::unique_ptr<locator::token_range_splitter> make_splitter(token_metadata_ptr);

}