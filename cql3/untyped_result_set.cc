/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <algorithm>
#include <iterator>
#include <utility>
#include <stdexcept>
#include "untyped_result_set.hh"
#include "result_set.hh"
#include "cql3/column_identifier.hh"
#include "transport/messages/result_message.hh"

struct missing_column : public std::bad_variant_access {
private:
    sstring _msg;
public:
    missing_column(std::string_view column_name)
            : bad_variant_access()
            , _msg(format("missing column: {}", column_name))
    {}

    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

cql3::untyped_result_set_row::untyped_result_set_row(const index_map& index, const cql3::metadata& metadata, data_views data)
    : _name_to_index(index)
    , _metadata(metadata)
    , _data(std::move(data))
{}

size_t cql3::untyped_result_set_row::index(const std::string_view& name) const {
    auto i = _name_to_index.find(name);
    return i != _name_to_index.end() ? i->second : std::numeric_limits<size_t>::max();
}

bool cql3::untyped_result_set_row::has(std::string_view name) const {
    auto i = index(name);
    if (i < _data.size()) {
        return _data.at(i).has_value();
    }
    return false;
}

cql3::untyped_result_set_row::view_type cql3::untyped_result_set_row::get_view(std::string_view name) const {
    auto& data = _data.at(index(name));
    if (!data) {
        throw missing_column(name);
    }
    return *data;
}

const std::vector<lw_shared_ptr<cql3::column_specification>>& cql3::untyped_result_set_row::get_columns() const {
    return _metadata.get_names();
}

using cql_transport::messages::result_message;

cql3::untyped_result_set::index_map_ptr cql3::untyped_result_set::make_index(const cql3::metadata& metadata) {
    auto map = std::make_unique<untyped_result_set_row::index_map>();
    auto& names = metadata.get_names();
    size_t i = 0;
    std::transform(names.begin(), names.end(), std::inserter(*map, map->end()), [&](const lw_shared_ptr<column_specification>& c) mutable {
        return std::make_pair<std::string_view, size_t>(c->name->text(), i++);
    });
    return map;
}

struct cql3::untyped_result_set::visitor {
    rows_type& rows;
    const cql3::metadata& meta;
    const untyped_result_set_row::index_map& index;
    untyped_result_set_row::data_views tmp;

    visitor(rows_type& r, const cql3::metadata& m, const untyped_result_set_row::index_map& i)
        : rows(r)
        , meta(m)
        , index(i)
    {}

    void start_row() {
        tmp.reserve(index.size());
    }
    void accept_value(managed_bytes_view_opt&& v) {
        if (v) {
            tmp.emplace_back(*v);
        } else {
            tmp.emplace_back(std::nullopt);
        }
    }
    // somewhat weird dispatch, but when visiting directly via
    // result_generator, pk:s will be temporary - and sent 
    // as views, not opt_views. So we can dispatch on this and 
    // simply copy the temporaries.
    void accept_value(const query::result_bytes_view& v) {
        tmp.emplace_back(v.linearize());
    }
    void end_row() {
        rows.emplace_back(untyped_result_set_row(index, meta, std::exchange(tmp, {})));
    }
};

cql3::untyped_result_set::untyped_result_set(::shared_ptr<result_message> msg)
    : _storage(msg)
{
    class msg_visitor : public result_message::visitor_base {
    public:
        const cql3::result* res = nullptr;
        void visit(const result_message::rows& rmrs) override {
            res = &rmrs.rs();
        }
    };
    msg_visitor v;
    if (msg != nullptr) {
        msg->accept(v);
    }
    if (v.res) {
        auto& metadata = v.res->get_metadata();
        _index = make_index(metadata);
        v.res->visit(visitor{_rows, metadata, *_index});
    }
}

cql3::untyped_result_set::untyped_result_set(const schema& s, foreign_ptr<lw_shared_ptr<query::result>> qr, const cql3::selection::selection& selection, const query::partition_slice& slice)
    : _storage(std::make_tuple(std::move(qr), selection.get_result_metadata()))
{
    auto& qt = std::get<qr_tuple>(_storage);
    auto& qres = std::get<0>(qt);
    auto& metadata = *std::get<1>(qt);

    _index = make_index(metadata);
    visitor v{_rows, metadata, *_index};
    result_generator::query_result_visitor<visitor> vv(s, v, selection);
    query::result_view::consume(*qres, slice, vv);
}

cql3::untyped_result_set::~untyped_result_set() = default;

const cql3::untyped_result_set_row& cql3::untyped_result_set::one() const {
    if (_rows.size() != 1) {
        throw std::runtime_error("One row required, " + std::to_string(_rows.size()) + " found");
    }
    return at(0);
}
