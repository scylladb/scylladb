/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

#include "utils/rjson.hh"
#include "utils/overloaded_functor.hh"
#include "alternator/error.hh"
#include "alternator/expressions_types.hh"

namespace alternator {

// An attribute_path_map object is used to hold data for various attributes
// paths (parsed::path) in a hierarchy of attribute paths. Each attribute path
// has a root attribute, and then modified by member and index operators -
// for example in "a.b[2].c" we have "a" as the root, then ".b" member, then
// "[2]" index, and finally ".c" member.
// Data can be added to an attribute_path_map using the add() function, but
// requires that attributes with data not be *overlapping* or *conflicting*:
//
// 1. Two attribute paths which are identical or an ancestor of one another
//    are considered *overlapping* and not allowed. If a.b.c has data,
//    we can't add more data in a.b.c or any of its descendants like a.b.c.d.
//
// 2. Two attribute paths which need the same parent to have both a member and
//    an index are considered *conflicting* and not allowed. E.g., if a.b has
//    data, you can't add a[1]. The meaning of adding both would be that the
//    attribute a is both a map and an array, which isn't sensible.
//
// These two requirements are common to the two places where Alternator uses
// this abstraction to describe how a hierarchical item is to be transformed:
//
// 1. In ProjectExpression: for filtering from a full top-level attribute
//    only the parts for which user asked in ProjectionExpression.
//
// 2. In UpdateExpression: for taking the previous value of a top-level
//    attribute, and modifying it based on the instructions in the user
//    wrote in UpdateExpression.

template<typename T>
class attribute_path_map_node {
public:
    using data_t = T;
    // We need the extra unique_ptr<> here because libstdc++ unordered_map
    // doesn't work with incomplete types :-(
    using members_t =  std::unordered_map<std::string, std::unique_ptr<attribute_path_map_node<T>>>;
    // The indexes list is sorted because DynamoDB requires handling writes
    // beyond the end of a list in index order.
    using indexes_t = std::map<unsigned, std::unique_ptr<attribute_path_map_node<T>>>;
    // The prohibition on "overlap" and "conflict" explained above means
    // That only one of data, members or indexes is non-empty.
    std::optional<std::variant<data_t, members_t, indexes_t>> _content;

    bool is_empty() const { return !_content; }
    bool has_value() const { return _content && std::holds_alternative<data_t>(*_content); }
    bool has_members() const { return _content && std::holds_alternative<members_t>(*_content); }
    bool has_indexes() const { return _content && std::holds_alternative<indexes_t>(*_content); }
    // get_members() assumes that has_members() is true
    members_t& get_members() { return std::get<members_t>(*_content); }
    const members_t& get_members() const { return std::get<members_t>(*_content); }
    indexes_t& get_indexes() { return std::get<indexes_t>(*_content); }
    const indexes_t& get_indexes() const { return std::get<indexes_t>(*_content); }
    T& get_value() { return std::get<T>(*_content); }
    const T& get_value() const { return std::get<T>(*_content); }
};

template<typename T>
using attribute_path_map = std::unordered_map<std::string, attribute_path_map_node<T>>;

using attrs_to_get_node = attribute_path_map_node<std::monostate>;
// attrs_to_get lists which top-level attribute are needed, and possibly also
// which part of the top-level attribute is really needed (when nested
// attribute paths appeared in the query).
// Most code actually uses optional<attrs_to_get>. There, a disengaged
// optional means we should get all attributes, not specific ones.
using attrs_to_get = attribute_path_map<std::monostate>;

// takes a given JSON value and drops its parts which weren't asked to be
// kept. It modifies the given JSON value, or returns false to signify that
// the entire object should be dropped.
// Note that The JSON value is assumed to be encoded using the DynamoDB
// conventions - i.e., it is really a map whose key has a type string,
// and the value is the real object.
template<typename T>
bool hierarchy_filter(rjson::value& val, const attribute_path_map_node<T>& h) {
    if (!val.IsObject() || val.MemberCount() != 1) {
        // This shouldn't happen. We shouldn't have stored malformed objects.
        // But today Alternator does not validate the structure of nested
        // documents before storing them, so this can happen on read.
        throw api_error::internal(format("Malformed value object read: {}", val));
    }
    const char* type = val.MemberBegin()->name.GetString();
    rjson::value& v = val.MemberBegin()->value;
    if (h.has_members()) {
        const auto& members = h.get_members();
        if (type[0] != 'M' || !v.IsObject()) {
            // If v is not an object (dictionary, map), none of the members
            // can match.
            return false;
        }
        rjson::value newv = rjson::empty_object();
        for (auto it = v.MemberBegin(); it != v.MemberEnd(); ++it) {
            std::string attr = rjson::to_string(it->name);
            auto x = members.find(attr);
            if (x != members.end()) {
                if (x->second) {
                    // Only a part of this attribute is to be filtered, do it.
                    if (hierarchy_filter(it->value, *x->second)) {
                        // because newv started empty and attr are unique
                        // (keys of v), we can use add() here
                        rjson::add_with_string_name(newv, attr, std::move(it->value));
                    }
                } else {
                    // The entire attribute is to be kept
                    rjson::add_with_string_name(newv, attr, std::move(it->value));
                }
            }
        }
        if (newv.MemberCount() == 0) {
            return false;
        }
        v = newv;
    } else if (h.has_indexes()) {
        const auto& indexes = h.get_indexes();
        if (type[0] != 'L' || !v.IsArray()) {
            return false;
        }
        rjson::value newv = rjson::empty_array();
        const auto& a = v.GetArray();
        for (unsigned i = 0; i < v.Size(); i++) {
            auto x = indexes.find(i);
            if (x != indexes.end()) {
                if (x->second) {
                    if (hierarchy_filter(a[i], *x->second)) {
                        rjson::push_back(newv, std::move(a[i]));
                    }
                } else {
                    // The entire attribute is to be kept
                    rjson::push_back(newv, std::move(a[i]));
                }
            }
        }
        if (newv.Size() == 0) {
            return false;
        }
        v = newv;
    }
    return true;
}

// Add a path to an attribute_path_map. Throws a validation error if the path
// "overlaps" with one already in the filter (one is a sub-path of the other)
// or "conflicts" with it (both a member and index is requested).
template<typename T>
void attribute_path_map_add(const char* source, attribute_path_map<T>& map, const parsed::path& p, T value = {}) {
   using node = attribute_path_map_node<T>;
    // The first step is to look for the top-level attribute (p.root()):
    auto it = map.find(p.root());
    if (it == map.end()) {
        if (p.has_operators()) {
            it = map.emplace(p.root(), node {std::nullopt}).first;
        } else {
            (void) map.emplace(p.root(), node {std::move(value)}).first;
            // Value inserted for top-level node. We're done.
            return;
        }
    } else if(!p.has_operators()) {
        // If p is top-level and we already have it or a part of it
        // in map, it's a forbidden overlapping path.
        throw api_error::validation(fmt::format(
            "Invalid {}: two document paths overlap at {}", source, p.root()));
    } else if (it->second.has_value()) {
        // If we're here, it != map.end() && p.has_operators && it->second.has_value().
        // This means the top-level attribute already has a value, and we're
        // trying to add a non-top-level value. It's an overlap.
        throw api_error::validation(fmt::format("Invalid {}: two document paths overlap at {}", source, p.root()));
    }
    node* h = &it->second;
    // The second step is to walk h from the top-level node to the inner node
    // where we're supposed to insert the value:
    for (const auto& op : p.operators()) {
        std::visit(overloaded_functor {
            [&] (const std::string& member) {
                if (h->is_empty()) {
                    *h = node {typename node::members_t()};
                } else if (h->has_indexes()) {
                    throw api_error::validation(format("Invalid {}: two document paths conflict at {}", source, p));
                } else if (h->has_value()) {
                    throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
                }
                typename node::members_t& members = h->get_members();
                auto it = members.find(member);
                if (it == members.end()) {
                    it = members.insert({member, std::make_unique<node>()}).first;
                }
                h = it->second.get();
            },
            [&] (unsigned index) {
                if (h->is_empty()) {
                    *h = node {typename node::indexes_t()};
                } else if (h->has_members()) {
                    throw api_error::validation(format("Invalid {}: two document paths conflict at {}", source, p));
                } else if (h->has_value()) {
                    throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
                }
                typename node::indexes_t& indexes = h->get_indexes();
                auto it = indexes.find(index);
                if (it == indexes.end()) {
                    it = indexes.insert({index, std::make_unique<node>()}).first;
                }
                h = it->second.get();
            }
        }, op);
    }
    // Finally, insert the value in the node h.
    if (h->is_empty()) {
        *h = node {std::move(value)};
    } else {
        throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
    }
}

// A very simplified version of the above function for the special case of
// adding only top-level attribute. It's not only simpler, we also use a
// different error message, referring to a "duplicate attribute" instead of
// "overlapping paths". DynamoDB also has this distinction (errors in
// AttributesToGet refer to duplicates, not overlaps, but errors in
// ProjectionExpression refer to overlap - even if it's an exact duplicate).
template<typename T>
void attribute_path_map_add(const char* source, attribute_path_map<T>& map, const std::string& attr, T value = {}) {
   using node = attribute_path_map_node<T>;
    auto it = map.find(attr);
    if (it == map.end()) {
        map.emplace(attr, node {std::move(value)});
    } else {
        throw api_error::validation(fmt::format(
            "Invalid {}: Duplicate attribute: {}", source, attr));
    }
}

} // namespace alternator
