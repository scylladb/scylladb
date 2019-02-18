/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <unordered_map>
#include <vector>
#include "seastarx.hh"

namespace gms {

class feature;

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 */
class feature_service final {
    std::unordered_map<sstring, std::vector<feature*>> _registered_features;
public:
    feature_service();
    ~feature_service();
    future<> stop();
    void register_feature(feature* f);
    void unregister_feature(feature* f);
    // Has to run inside seastar::async context
    void enable(const sstring& name);
};

} // namespace gms
