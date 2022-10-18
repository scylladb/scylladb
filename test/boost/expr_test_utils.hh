/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/query_options.hh"
#include "cql3/selection/selection.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/impl.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "db/config.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3 {
namespace expr {
namespace test_utils {

template <class T>
raw_value make_raw(T t) {
    data_type val_type = data_type_for<T>();
    data_value data_val(t);
    return raw_value::make_value(val_type->decompose(data_val));
}

inline raw_value make_empty_raw() {
    return raw_value::make_value(managed_bytes());
}

inline raw_value make_bool_raw(bool val) {
    return make_raw(val);
}

inline raw_value make_int_raw(int32_t val) {
    return make_raw(val);
}

inline raw_value make_text_raw(const sstring_view& text) {
    return raw_value::make_value(utf8_type->decompose(text));
}

template <class T>
constant make_const(T t) {
    data_type val_type = data_type_for<T>();
    return constant(make_raw(t), val_type);
}

inline constant make_empty_const(data_type type) {
    return constant(make_empty_raw(), type);
}

inline constant make_bool_const(bool val) {
    return make_const(val);
}

inline constant make_int_const(int32_t val) {
    return make_const(val);
}

inline constant make_text_const(const sstring_view& text) {
    return constant(make_text_raw(text), utf8_type);
}

}  // namespace test_utils
}  // namespace expr
}  // namespace cql3