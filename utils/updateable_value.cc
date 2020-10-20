/*
 * Copyright (C) 2019 ScyllaDB
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



#include "updateable_value.hh"
#include <seastar/core/seastar.hh>

namespace utils {

updateable_value_base::updateable_value_base(const updateable_value_source_base& source) : _owner(this_shard_id()) {
    source.add_ref(this);
    _source = &source;
}

updateable_value_base::~updateable_value_base() {
    if (_source) {
        _source->del_ref(this);
    }
}

updateable_value_base::updateable_value_base(const updateable_value_base& v) : _owner(this_shard_id()) {
    if (v._source) {
        v._source->add_ref(this);
        _source = v._source;
    }
}

updateable_value_base&
updateable_value_base::updateable_value_base::operator=(const updateable_value_base& v) {
    if (this != &v) {
        // If both sources are null, or non-null and equal, nothing needs to be done.
        // If owner shards are different, the source must be updated as well.
        if (_source != v._source || _owner != v._owner) {
            if (_source) {
                _source->del_ref(this);
            }
            _owner = this_shard_id();
            if (v._source) {
                v._source->add_ref(this);
            }
            _source = v._source;
        }
    }
    return *this;
}

updateable_value_base::updateable_value_base(updateable_value_base&& v) noexcept
        : _owner(this_shard_id()), _source(std::exchange(v._source, nullptr)) {
    if (_source) {
        _source->update_ref(&v, this);
    }
}

updateable_value_base&
updateable_value_base::operator=(updateable_value_base&& v) noexcept {
    if (this != &v) {
        if (_source) {
            _source->del_ref(this);
        }
        _source = std::exchange(v._source, nullptr);
        _owner = this_shard_id();
        if (_source) {
            _source->update_ref(&v, this);
        }
    }
    return *this;
}

updateable_value_base&
updateable_value_base::updateable_value_base::operator=(std::nullptr_t) {
    if (_source) {
        _source->del_ref(this);
        _source = nullptr;
    }
    return *this;
}

void
updateable_value_source_base::for_each_ref(std::function<void (updateable_value_base* ref)> func) {
    for (auto refs_per_shard : _refs) {
        for (auto ref : refs_per_shard) {
            func(ref);
        }
    }
}

updateable_value_source_base::~updateable_value_source_base() {
    for (auto refs_per_shard : _refs) {
        for (auto ref : refs_per_shard) {
            ref->_source = nullptr;
        }
    }
}

void
updateable_value_source_base::add_ref(updateable_value_base* ref) const {
    _refs[ref->_owner].push_back(ref);
}

void
updateable_value_source_base::del_ref(updateable_value_base* ref) const {
    auto& refs_for_shard = _refs[ref->_owner];
    refs_for_shard.erase(std::remove(refs_for_shard.begin(), refs_for_shard.end(), ref), refs_for_shard.end());
}

void
updateable_value_source_base::update_ref(updateable_value_base* old_ref, updateable_value_base* new_ref) const {
    del_ref(old_ref);
    add_ref(new_ref);
}

}
