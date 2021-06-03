/*
 * Copyright (C) 2019-present ScyllaDB
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

updateable_value_base::updateable_value_base(const updateable_value_source_base& source) {
    source.add_ref(this);
    _source = &source;
}

updateable_value_base::~updateable_value_base() {
    if (_source) {
        _source->del_ref(this);
    }
}

updateable_value_base::updateable_value_base(const updateable_value_base& v) {
    if (v._source) {
        v._source->add_ref(this);
        _source = v._source;
    }
}

updateable_value_base&
updateable_value_base::updateable_value_base::operator=(const updateable_value_base& v) {
    if (this != &v) {
        // If both sources are null, or non-null and equal, nothing needs to be done
        if (_source != v._source) {
            if (v._source) {
                v._source->add_ref(this);
            }
            if (_source) {
                _source->del_ref(this);
            }
            _source = v._source;
        }
    }
    return *this;
}

updateable_value_base::updateable_value_base(updateable_value_base&& v) noexcept
        : _source(std::exchange(v._source, nullptr)) {
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
    for (auto ref : _refs) {
        func(ref);
    }
}

updateable_value_source_base::~updateable_value_source_base() {
    for (auto ref : _refs) {
        ref->_source = nullptr;
    }
}

void
updateable_value_source_base::add_ref(updateable_value_base* ref) const {
    _refs.push_back(ref);
}

void
updateable_value_source_base::del_ref(updateable_value_base* ref) const {
    _refs.erase(std::remove(_refs.begin(), _refs.end(), ref), _refs.end());
}

void
updateable_value_source_base::update_ref(updateable_value_base* old_ref, updateable_value_base* new_ref) const {
    std::replace(_refs.begin(), _refs.end(), old_ref, new_ref);
}

}
