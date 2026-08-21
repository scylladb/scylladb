/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "index/secondary_index_manager.hh"

namespace secondary_index {

// Base class for indexes backed by an external Vector Store engine.
class external_index : public custom_index {
public:
    // Minimum CDC TTL required by Vector Store (24 hours).
    // Ensures CDC data is retained long enough for index builds to complete.
    static constexpr int VS_TTL_SECONDS = 86400;

    bool view_should_exist() const override;
    utils::UUID index_version(const schema& schema) override;
    static void check_cdc_options(const schema& schema, std::string_view search_type_name, bool index_already_exists);
    static void check_cdc_options_if_present(const schema& s);
    static void validate_cdc_not_disabled_if_present(const schema& s);

    static bool has_index(const schema& s) {
        return has_index_impl<external_index>(s);
    }

protected:
    void check_uses_tablets(const schema& schema, const data_dictionary::database& db) const;

    template <typename T>
    static void check_cdc_options_impl(const schema& s) {
        check_cdc_options(s, T::SEARCH_TYPE_NAME, has_index_impl<T>(s));
    }

    template <typename T>
    static bool has_index_impl(const schema& s) {
        auto i = s.indices();
        return std::any_of(i.begin(), i.end(), [](const auto& index) {
            auto it = index.options().find(db::index::secondary_index::custom_class_option_name);
            if (it != index.options().end()) {
                auto custom_class = secondary_index_manager::get_custom_class_factory(it->second);
                return (custom_class && dynamic_cast<T*>((*custom_class)().get()));
            }
            return false;
        });
    }
};

} // namespace secondary_index
