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

class list_type_impl final : public concrete_type<std::vector<data_value>, listlike_collection_type_impl> {
    using list_type = shared_ptr<const list_type_impl>;
    using intern = type_interning_helper<list_type_impl, data_type, bool>;
public:
    static list_type get_instance(data_type elements, bool is_multi_cell);
    list_type_impl(data_type elements, bool is_multi_cell);
    virtual data_type name_comparator() const override;
    virtual data_type value_comparator() const override;
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v) const;
};

data_value make_list_value(data_type type, list_type_impl::native_type value);

