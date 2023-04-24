/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <unordered_map>
#include <optional>
#include <seastar/core/sharded.hh>
#include "bytes.hh"
#include "types/types.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "serializer.hh"
#include "bytes_ostream.hh"
#include "transport/messages/result_message_base.hh"
#include "column_specification.hh"
#include "query-result.hh"

#pragma once

namespace query {
    class result;
    class partition_slice;
    // duplicate template def. But avoids a huge include chain
    using result_bytes_view = ser::buffer_view<bytes_ostream::fragment_iterator>;
}

namespace cql3 {
namespace selection {
    class selection;
}

class untyped_result_set;
class result;
class metadata;

class untyped_result_set_row {
public:
    using view_type = managed_bytes_view;
    using opt_view_type = std::optional<view_type>;
private:
    friend class untyped_result_set;
    using index_map = std::unordered_map<std::string_view, size_t>;
    using data_views = std::vector<managed_bytes_opt>;

    const index_map& _name_to_index;
    const cql3::metadata& _metadata;
    data_views _data;

    untyped_result_set_row(const index_map&, const cql3::metadata&, data_views);
    size_t index(const std::string_view&) const;
public:
    untyped_result_set_row(untyped_result_set_row&&) = default;
    untyped_result_set_row(const untyped_result_set_row&) = delete;

    bool has(std::string_view) const;
    view_type get_view(std::string_view name) const;
    bytes get_blob(std::string_view name) const {
        return to_bytes(get_view(name));
    }
    managed_bytes get_blob_fragmented(std::string_view name) const {
        return managed_bytes(get_view(name));
    }
    template<typename T>
    T get_as(std::string_view name) const {
        return value_cast<T>(data_type_for<T>()->deserialize(get_view(name)));
    }
    template<typename T>
    std::optional<T> get_opt(std::string_view name) const {
        return has(name) ? get_as<T>(name) : std::optional<T>{};
    }
    opt_view_type get_view_opt(const sstring& name) const {
        if (has(name)) {
            return get_view(name);
        }
        return std::nullopt;
    }
    template<typename T>
    T get_or(std::string_view name, T t) const {
        return has(name) ? get_as<T>(name) : t;
    }
    // this could maybe be done as an overload of get_as (or something), but that just
    // muddles things for no real gain. Let user (us) attempt to know what he is doing instead.
    template<typename K, typename V, typename Iter>
    void get_map_data(std::string_view name, Iter out, data_type keytype =
            data_type_for<K>(), data_type valtype =
            data_type_for<V>()) const {
        auto vec =
                value_cast<map_type_impl::native_type>(
                        map_type_impl::get_instance(keytype, valtype, false)->deserialize(
                                get_view(name)));
        std::transform(vec.begin(), vec.end(), out,
                [](auto& p) {
                    return std::pair<K, V>(value_cast<K>(p.first), value_cast<V>(p.second));
                });
    }
    template<typename K, typename V, typename ... Rest>
    std::unordered_map<K, V, Rest...> get_map(std::string_view name,
            data_type keytype = data_type_for<K>(), data_type valtype =
                    data_type_for<V>()) const {
        std::unordered_map<K, V, Rest...> res;
        get_map_data<K, V>(name, std::inserter(res, res.end()), keytype, valtype);
        return res;
    }
    template<typename V, typename Iter>
    void get_list_data(std::string_view name, Iter out, data_type valtype = data_type_for<V>()) const {
        auto vec =
                value_cast<list_type_impl::native_type>(
                        list_type_impl::get_instance(valtype, false)->deserialize(
                                get_view(name)));
        std::transform(vec.begin(), vec.end(), out, [](auto& v) { return value_cast<V>(v); });
    }
    template<typename V, typename ... Rest>
    std::vector<V, Rest...> get_list(std::string_view name, data_type valtype = data_type_for<V>()) const {
        std::vector<V, Rest...> res;
        get_list_data<V>(name, std::back_inserter(res), valtype);
        return res;
    }
    template<typename V, typename Iter>
    void get_set_data(std::string_view name, Iter out, data_type valtype =
                    data_type_for<V>()) const {
        auto vec =
                        value_cast<set_type_impl::native_type>(
                                        set_type_impl::get_instance(valtype,
                                                        false)->deserialize(
                                                        get_blob(name)));
        std::transform(vec.begin(), vec.end(), out, [](auto& p) {
            return value_cast<V>(p);
        });
    }
    template<typename V, typename ... Rest>
    std::unordered_set<V, Rest...> get_set(std::string_view name,
            data_type valtype =
                    data_type_for<V>()) const {
        std::unordered_set<V, Rest...> res;
        get_set_data<V>(name, std::inserter(res, res.end()), valtype);
        return res;
    }
    const cql3::metadata& get_metadata() const {
        return _metadata;
    }
    const std::vector<lw_shared_ptr<column_specification>>& get_columns() const;
};

class result_set;

/// A tabular result. Unlike result_set, untyped_result_set is optimized for safety
/// and convenience, not performance.
class untyped_result_set {
public:
    using row = untyped_result_set_row;
    using rows_type = std::vector<row>;
    using const_iterator = rows_type::const_iterator;
    using iterator = rows_type::const_iterator;

    untyped_result_set(::shared_ptr<cql_transport::messages::result_message>);
    untyped_result_set(const schema&, foreign_ptr<lw_shared_ptr<query::result>>, const cql3::selection::selection&, const query::partition_slice&);
    untyped_result_set(untyped_result_set&&) = default;
    ~untyped_result_set();

    const_iterator begin() const {
        return _rows.begin();
    }
    const_iterator end() const {
        return _rows.end();
    }
    size_t size() const {
        return _rows.size();
    }
    bool empty() const {
        return _rows.empty();
    }
    const row& one() const;
    const row& at(size_t i) const {
        return _rows.at(i);
    }
    const row& front() const {
        return _rows.front();
    }
    const row& back() const {
        return _rows.back();
    }
private:
    using index_map_ptr = std::unique_ptr<untyped_result_set_row::index_map>;
    using qr_tuple = std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, shared_ptr<const cql3::metadata>>;
    using storage = std::variant<std::monostate
        , ::shared_ptr<cql_transport::messages::result_message>
        , qr_tuple
    >;
    struct visitor;

    storage _storage;
    index_map_ptr _index;
    rows_type _rows;

    untyped_result_set() = default;
    static index_map_ptr make_index(const cql3::metadata&);
public:
    static untyped_result_set make_empty() {
        return untyped_result_set();
    }
};

}
