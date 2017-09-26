/*
 * Copyright (C) 2016 ScyllaDB
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

#include <seastar/core/shared_future.hh>

namespace gms {

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A feature should only be created once the gossiper is available.
 */
class feature final {
    sstring _name;
    bool _enabled = false;
    mutable shared_promise<> _pr;
    friend class gossiper;
public:
    explicit feature(sstring name, bool enabled = false);
    feature() = default;
    ~feature();
    feature(const feature& other) = delete;
    void enable();
    feature& operator=(feature&& other);
    const sstring& name() const {
        return _name;
    }
    explicit operator bool() const {
        return _enabled;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const feature& f) {
        return os << "{ gossip feature = " << f._name << " }";
    }
    future<> when_enabled() const { return _pr.get_shared_future(); }
};

} // namespace gms
