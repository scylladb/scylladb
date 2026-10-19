/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/tablets.hh"

#include <algorithm>
#include <concepts>
#include <span>

namespace service::strong_consistency {

// One of the tablet's replica lists, optionally followed by the migration's pending
// replica. Refers to the lists in the tablet's metadata, so nothing is copied; the
// caller keeps that metadata alive.
class replica_set_view {
    std::span<const locator::tablet_replica> _base;
    const locator::tablet_replica* _extra;

    // The first replica that satisfies `pred`, or null.
    template <std::predicate<const locator::tablet_replica&> Pred>
    const locator::tablet_replica* find_if(Pred pred) const;

public:
    explicit replica_set_view(std::span<const locator::tablet_replica> base,
            const locator::tablet_replica* extra = nullptr)
        : _base(base)
        , _extra(extra)
    {}

    // The replica on `host`, or null.
    const locator::tablet_replica* find_replica(locator::host_id host) const;

    // `replica` itself, host and shard, or null.
    const locator::tablet_replica* find_replica(const locator::tablet_replica& replica) const;

    template <std::invocable<const locator::tablet_replica&> Func>
    void for_each(Func func) const {
        std::ranges::for_each(_base, func);
        if (_extra) {
            func(*_extra);
        }
    }

    locator::tablet_replica_set materialize() const;
};

// The serving set of a strongly consistent tablet at the stage its migration is in:
// the replicas where a request with that view may run, whether it needs the raft leader
// or reads locally.
//
// It holds every voter the group may have while a view of the stage is live, so every
// leader a serving replica may name is in it. And every one of those voters is caught up
// and stays a member while such a view is live: the pending replica is promoted only
// after its snapshot transfer, and a replica is removed only after it is demoted and the
// requests of views in which it serves are drained. The serving sets of any two stages
// that can be live at once are nested, so a request is served within two routing hops.
// See "Strongly-consistent tablets" in docs/dev/topology-over-raft.md.
replica_set_view serving_replicas(const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo);

} // namespace service::strong_consistency
