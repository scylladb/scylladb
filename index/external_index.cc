/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cdc/cdc_options.hh"
#include "exceptions/exceptions.hh"
#include "index/external_index.hh"
#include "index/fulltext_index.hh"
#include "index/vector_index.hh"
#include "utils/UUID_gen.hh"

namespace secondary_index {

namespace {

using external_index_types = std::tuple<fulltext_index, vector_index>;

template <typename Fn>
void for_each_external_index_type(Fn&& fn) {
    [&]<typename... Ts>(std::tuple<Ts...>*) {
        (fn.template operator()<Ts>(), ...);
    }(static_cast<external_index_types*>(nullptr));
}

} // anonymous namespace

bool external_index::view_should_exist() const {
    return false;
}

/// Returns a timeuuid representing the time at which the index was created.
/// This is used to determine if the index needs to be rebuilt, and to enable
/// routing by creation time when multiple vector indexes exist on the same column.
utils::UUID external_index::index_version(const schema& schema) {
    return utils::UUID_gen::get_time_UUID();
}

void external_index::check_cdc_options(const schema& schema, std::string_view search_type_name, bool index_already_exists) {
    auto cdc_options = schema.cdc_options();
    if (cdc_options.enabled()) {
        auto ttl = cdc_options.ttl();
        auto delta_mode = cdc_options.get_delta_mode();
        auto postimage = cdc_options.postimage();
        if ((ttl && ttl < VS_TTL_SECONDS) ||
            (delta_mode != cdc::delta_mode::full && !postimage)) {
            throw exceptions::invalid_request_exception(
                index_already_exists ?
                format("{} is enabled on this table.\n"
                "The CDC log must meet the minimal requirements for external indexes.\n"
                "This means that the CDC's TTL must be at least {} seconds (24 hours), "
                "and the CDC's delta mode must be set to 'full' or postimage must be enabled.",
                search_type_name, VS_TTL_SECONDS) :
                format("To enable {} on this table, "
                "the CDC log must meet the minimal requirements for external indexes.\n"
                "CDC's TTL must be at least {} seconds (24 hours), "
                "and the CDC's delta mode must be set to 'full' or postimage must be enabled.",
                search_type_name, VS_TTL_SECONDS));
        }
    }
}

void external_index::check_uses_tablets(const schema& schema, const data_dictionary::database& db) const {
    const auto& keyspace = db.find_keyspace(schema.ks_name());
    if (!keyspace.uses_tablets()) {
        throw exceptions::invalid_request_exception(
            format("Creating a {} index requires the base table's keyspace to use tablets.\n"
            "Please alter the keyspace to use tablets and try again.", index_type_name()));
    }
}

void external_index::check_cdc_options_if_present(const schema& s) {
    for_each_external_index_type([&]<typename T>() {
        if (T::has_index(s)) {
            T::check_cdc_options(s);
        }
    });
}

void external_index::validate_cdc_not_disabled_if_present(const schema& s) {
    for_each_external_index_type([&]<typename T>() {
        if (T::has_index(s)) {
            throw exceptions::invalid_request_exception(
                format("Cannot disable CDC when {} is enabled on the table.\n"
                "Please drop the {} index first, then disable CDC.",
                T::SEARCH_TYPE_NAME, T::INDEX_TYPE_NAME));
        }
    });
}

} // namespace secondary_index
