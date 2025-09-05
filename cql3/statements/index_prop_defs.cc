/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <set>
#include <seastar/core/format.hh>
#include "index_prop_defs.hh"
#include "index/secondary_index.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"

void check_system_option_specified(const index_options_map& options, const sstring& option_name) {
    if (options.count(option_name)) {
        throw exceptions::invalid_request_exception(
                fmt::format("Cannot specify {} as a CUSTOM option", option_name));
    }
}

void cql3::statements::index_prop_defs::validate() {
    static std::set<sstring> keywords({ sstring(KW_OPTIONS) });

    property_definitions::validate(keywords);

    if (is_custom && !custom_class) {
        throw exceptions::invalid_request_exception("CUSTOM index requires specifying the index class");
    }
    
    if (!custom_class && !_properties.empty()) {
        throw exceptions::invalid_request_exception("Cannot specify options for a non-CUSTOM index");
    }
    auto options = get_raw_options();
    check_system_option_specified(options, db::index::secondary_index::custom_class_option_name);
    check_system_option_specified(options, db::index::secondary_index::index_version_option_name);

}

index_options_map
cql3::statements::index_prop_defs::get_raw_options() {
    auto options = get_map(KW_OPTIONS);
    return !options ? std::unordered_map<sstring, sstring>() : std::unordered_map<sstring, sstring>(options->begin(), options->end());
}

index_options_map
cql3::statements::index_prop_defs::get_options() {
    auto options = get_raw_options();
    options.emplace(db::index::secondary_index::custom_class_option_name, *custom_class);
    if (index_version.has_value()) {
        options.emplace(db::index::secondary_index::index_version_option_name, index_version->to_sstring());
    }
    return options;
}
