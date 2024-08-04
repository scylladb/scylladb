/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <unordered_map>

#include "bytes.hh"
#include "types/user.hh"

namespace data_dictionary {

class user_types_metadata {
    std::unordered_map<bytes, user_type> _user_types;
public:
    user_type get_type(const bytes& name) const {
        return _user_types.at(name);
    }
    const std::unordered_map<bytes, user_type>& get_all_types() const {
        return _user_types;
    }
    void add_type(user_type type) {
        auto i = _user_types.find(type->_name);
        SCYLLA_ASSERT(i == _user_types.end() || type->is_compatible_with(*i->second));
        _user_types.insert_or_assign(i, type->_name, type);
    }
    void remove_type(user_type type) {
        _user_types.erase(type->_name);
    }
    bool has_type(const bytes& name) const {
        return _user_types.contains(name);
    }
};

class user_types_storage {
public:
    virtual const user_types_metadata& get(const sstring& ks) const = 0;
    virtual ~user_types_storage() = default;
};

class dummy_user_types_storage : public user_types_storage {
    user_types_metadata _empty;
public:
    virtual const user_types_metadata& get(const sstring& ks) const override {
        return _empty;
    }
};

}
