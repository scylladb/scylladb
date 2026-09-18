/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/mutation.hh"

/// A mutation which is small enough to be merged into another one without
/// yielding.
///
/// Wrapping is the producer's claim about the mutation, used in overload resolution.
///
/// Converting back is free, so that a small_mutation can be passed
/// wherever a mutation is expected.
class small_mutation {
    mutation _m;
public:
    explicit small_mutation(mutation m) noexcept : _m(std::move(m)) {}

    operator const mutation&() const& noexcept { return _m; }
    operator mutation() && noexcept { return std::move(_m); }

    const mutation& get() const& noexcept { return _m; }
    mutation get() && noexcept { return std::move(_m); }
};
