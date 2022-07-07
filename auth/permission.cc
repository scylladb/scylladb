/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/permission.hh"

#include <boost/algorithm/string.hpp>

#include <unordered_map>

const auth::permission_set auth::permissions::ALL = auth::permission_set::of<
        auth::permission::CREATE,
        auth::permission::ALTER,
        auth::permission::DROP,
        auth::permission::SELECT,
        auth::permission::MODIFY,
        auth::permission::AUTHORIZE,
        auth::permission::DESCRIBE,
        auth::permission::EXECUTE>();

const auth::permission_set auth::permissions::NONE;

static const std::unordered_map<sstring, auth::permission> permission_names({
        {"READ", auth::permission::READ},
        {"WRITE", auth::permission::WRITE},
        {"CREATE", auth::permission::CREATE},
        {"ALTER", auth::permission::ALTER},
        {"DROP", auth::permission::DROP},
        {"SELECT", auth::permission::SELECT},
        {"MODIFY", auth::permission::MODIFY},
        {"AUTHORIZE", auth::permission::AUTHORIZE},
        {"DESCRIBE", auth::permission::DESCRIBE},
        {"EXECUTE", auth::permission::EXECUTE}});

const sstring& auth::permissions::to_string(permission p) {
    for (auto& v : permission_names) {
        if (v.second == p) {
            return v.first;
        }
    }
    throw std::out_of_range("unknown permission");
}

auth::permission auth::permissions::from_string(const sstring& s) {
    sstring upper(s);
    boost::to_upper(upper);
    return permission_names.at(upper);
}

std::unordered_set<sstring> auth::permissions::to_strings(const permission_set& set) {
    std::unordered_set<sstring> res;
    for (auto& v : permission_names) {
        if (set.contains(v.second)) {
            res.emplace(v.first);
        }
    }
    return res;
}

auth::permission_set auth::permissions::from_strings(const std::unordered_set<sstring>& set) {
    permission_set res = auth::permissions::NONE;
    for (auto& s : set) {
        res.set(from_string(s));
    }
    return res;
}

bool auth::operator<(const permission_set& p1, const permission_set& p2) {
    return p1.mask() < p2.mask();
}
