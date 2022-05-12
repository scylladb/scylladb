/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <set>
#include <seastar/core/print.hh>
#include "index_prop_defs.hh"
#include "index/secondary_index.hh"
#include "exceptions/exceptions.hh"

void cql3::statements::index_prop_defs::validate() {
    static std::set<sstring> keywords({ sstring(KW_OPTIONS) });

    property_definitions::validate(keywords);

    if (is_custom && !custom_class) {
        throw exceptions::invalid_request_exception("CUSTOM index requires specifying the index class");
    }

    if (!is_custom && custom_class) {
        throw exceptions::invalid_request_exception("Cannot specify index class for a non-CUSTOM index");
    }
    if (!is_custom && !_properties.empty()) {
        throw exceptions::invalid_request_exception("Cannot specify options for a non-CUSTOM index");
    }
    if (get_raw_options().count(
            db::index::secondary_index::custom_index_option_name)) {
        throw exceptions::invalid_request_exception(
                format("Cannot specify {} as a CUSTOM option",
                        db::index::secondary_index::custom_index_option_name));
    }

    // Currently, Scylla does not support *any* class of custom index
    // implementation. If in the future we do (e.g., SASI, or something
    // new), we'll need to check for valid values here.
    if (is_custom && custom_class) {
        throw exceptions::invalid_request_exception(
                format("Unsupported CUSTOM INDEX class {}. Note that currently, Scylla does not support SASI or any other CUSTOM INDEX class.",
                        *custom_class));

    }
}

index_options_map
cql3::statements::index_prop_defs::get_raw_options() {
    auto options = get_map(KW_OPTIONS);
    return !options ? std::unordered_map<sstring, sstring>() : std::unordered_map<sstring, sstring>(options->begin(), options->end());
}

index_options_map
cql3::statements::index_prop_defs::get_options() {
    auto options = get_raw_options();
    options.emplace(db::index::secondary_index::custom_index_option_name, *custom_class);
    return options;
}
