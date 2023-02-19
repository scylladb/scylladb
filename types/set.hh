/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <vector>

#include "types/types.hh"
#include "types/collection.hh"

class user_type_impl;

namespace Json {
class Value;
}

class set_type_impl final : public concrete_type<std::vector<data_value>, listlike_collection_type_impl> {
    using set_type = shared_ptr<const set_type_impl>;
    using intern = type_interning_helper<set_type_impl, data_type, bool>;
public:
    static set_type get_instance(data_type elements, bool is_multi_cell);
    set_type_impl(data_type elements, bool is_multi_cell);
    virtual data_type name_comparator() const override { return _elements; }
    virtual data_type value_comparator() const override;
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v) const;
    static bytes serialize_partially_deserialized_form(
            const std::vector<bytes_view>& v);
    static managed_bytes serialize_partially_deserialized_form_fragmented(
            const std::vector<managed_bytes_view_opt>& v);
};

data_value make_set_value(data_type tuple_type, set_type_impl::native_type value);

template <typename NativeType>
data_value::data_value(const std::unordered_set<NativeType>& v)
    : data_value(new set_type_impl::native_type(v.begin(), v.end()), set_type_impl::get_instance(data_type_for<NativeType>(), true))
{}
