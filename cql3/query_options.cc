/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "cql3/cql_config.hh"
#include "query_options.hh"
#include "db/consistency_level_type.hh"
#include "cql3/column_identifier.hh"
#include "utils/assert.hh"

namespace cql3 {

const cql_config default_cql_config(cql_config::default_tag{});

thread_local const query_options::specific_options query_options::specific_options::DEFAULT{
    -1, {}, db::consistency_level::SERIAL, api::missing_timestamp, service::node_local_only::no};

thread_local query_options query_options::DEFAULT{default_cql_config,
    db::consistency_level::ONE, std::nullopt,
    cql3::raw_value_view_vector_with_unset(), false, query_options::specific_options::DEFAULT};

query_options::query_options(query_options&& o) noexcept
    : _cql_config(o._cql_config)
    , _consistency(o._consistency)
    , _skip_metadata(o._skip_metadata)
    , _names(std::move(o._names))
    , _values(std::move(o._values))
    , _value_views()
    , _unset(std::move(o._unset))
    , _options(std::move(o._options))
    , _batch_options(std::move(o._batch_options))
    , _list_append_seq(o._list_append_seq)
    , _cached_pk_fn_calls(std::move(o._cached_pk_fn_calls))
{
    // _value_views may contain views into _values's data. With small_vector,
    // inline storage moves to a new address, invalidating those views.
    // If _values is non-empty, re-create views from the new location.
    // If _values is empty, _value_views was set independently (view-only path)
    // and can be moved directly since it doesn't reference _values.
    if (!_values.empty()) {
        // Rebuilding from _values reproduces the 1:1, in-order view mapping
        // established by fill_value_views() at construction. This is only valid
        // while _value_views still mirrors _values one-to-one. prepare() reorders
        // _value_views (for named bind markers) to a different size/order; moving
        // such an object would silently rebuild the wrong views, so fail fast if
        // that invariant was broken. See the analysis in issue #29047.
        SCYLLA_ASSERT(o._value_views.size() == _values.size());
        fill_value_views();
    } else {
        _value_views = std::move(o._value_views);
    }
}

query_options::query_options(const cql_config& cfg,
                           db::consistency_level consistency,
                           std::optional<std::vector<std::string_view>> names,
                           raw_value_vector values,
                           raw_value_view_vector value_views,
                           cql3::unset_bind_variable_vector unset,
                           bool skip_metadata,
                           specific_options options
                           )
   : _cql_config(cfg)
   , _consistency(consistency)
   , _skip_metadata(skip_metadata)
   , _names(std::move(names))
   , _values(std::move(values))
   , _value_views(std::move(value_views))
   , _unset(std::move(unset))
   , _options(std::move(options))
{
}

query_options::query_options(const cql_config& cfg,
                             db::consistency_level consistency,
                             std::optional<std::vector<std::string_view>> names,
                             cql3::raw_value_vector_with_unset values,
                             bool skip_metadata,
                             specific_options options
                             )
    : _cql_config(cfg)
    , _consistency(consistency)
    , _skip_metadata(skip_metadata)
    , _names(std::move(names))
    , _values(std::move(values.values))
    , _value_views()
    , _unset(std::move(values.unset))
    , _options(std::move(options))
{
    fill_value_views();
}

query_options::query_options(const cql_config& cfg,
                             db::consistency_level consistency,
                             std::optional<std::vector<std::string_view>> names,
                             cql3::raw_value_view_vector_with_unset value_views,
                             bool skip_metadata,
                             specific_options options
                             )
    : _cql_config(cfg)
    , _consistency(consistency)
    , _skip_metadata(skip_metadata)
    , _names(std::move(names))
    , _values()
    , _value_views(std::move(value_views.values))
    , _unset(std::move(value_views.unset))
    , _options(std::move(options))
{
}

query_options::query_options(db::consistency_level cl, cql3::raw_value_vector_with_unset values,
        specific_options options)
    : query_options(
          default_cql_config,
          cl,
          {},
          std::move(values),
          false,
          std::move(options)
      )
{
}

query_options::query_options(std::unique_ptr<query_options> qo, lw_shared_ptr<service::pager::paging_state> paging_state)
        : query_options(qo->_cql_config,
        qo->_consistency,
        std::move(qo->_names),
        std::move(qo->_values),
        std::move(qo->_value_views),
        std::move(qo->_unset),
        qo->_skip_metadata,
        query_options::specific_options{qo->_options.page_size, paging_state, qo->_options.serial_consistency, qo->_options.timestamp}) {

}

query_options::query_options(std::unique_ptr<query_options> qo, lw_shared_ptr<service::pager::paging_state> paging_state, int32_t page_size)
        : query_options(qo->_cql_config,
        qo->_consistency,
        std::move(qo->_names),
        std::move(qo->_values),
        std::move(qo->_value_views),
        std::move(qo->_unset),
        qo->_skip_metadata,
        query_options::specific_options{page_size, paging_state, qo->_options.serial_consistency, qo->_options.timestamp}) {

}

query_options::query_options(cql3::raw_value_vector_with_unset values)
    : query_options(
          db::consistency_level::ONE, std::move(values))
{}

void query_options::prepare(const std::vector<lw_shared_ptr<column_specification>>& specs)
{
    if (!_names) {
        return;
    }

    auto& names = *_names;
    raw_value_view_vector ordered_values;
    ordered_values.reserve(specs.size());
    for (auto&& spec : specs) {
        auto& spec_name = spec->name->text();
        bool found_value_for_name = false;
        for (size_t j = 0; j < names.size(); j++) {
            if (names[j] == spec_name) {
                ordered_values.emplace_back(_value_views[j]);
                found_value_for_name = true;
                break;
            }
        }

        // No bound value was found with the name `spec_name`.
        // This means that the user forgot to include a bound value with such name.
        if (!found_value_for_name) {
            throw exceptions::invalid_request_exception(
                format("Missing value for bind marker with name: {}", spec_name));
        }
    }
    _value_views = std::move(ordered_values);
}

void query_options::fill_value_views()
{
    for (auto&& value : _values) {
        _value_views.emplace_back(value.view());
    }
}

utils::result_with_exception_ptr<db::consistency_level> query_options::check_serial_consistency() const {
    if (_options.serial_consistency.has_value()) {
        return *_options.serial_consistency;
    }
    return bo::failure(std::make_exception_ptr(exceptions::protocol_exception("Consistency level for LWT is missing for a request with conditions")));
}

void query_options::cache_pk_function_call(computed_function_values::key_type id, computed_function_values::mapped_type value) const {
    _cached_pk_fn_calls.emplace(id, std::move(value));
}

const computed_function_values& query_options::cached_pk_function_calls() const {
    return _cached_pk_fn_calls;
}

computed_function_values&& query_options::take_cached_pk_function_calls() {
    return std::move(_cached_pk_fn_calls);
}

void query_options::set_cached_pk_function_calls(computed_function_values vals) {
    _cached_pk_fn_calls = std::move(vals);
}

computed_function_values::mapped_type* query_options::find_cached_pk_function_call(computed_function_values::key_type id) const {
    auto it = _cached_pk_fn_calls.find(id);
    if (it != _cached_pk_fn_calls.end()) {
        return &it->second;
    }
    return nullptr;
}

}
