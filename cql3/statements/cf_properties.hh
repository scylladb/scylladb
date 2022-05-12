/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/column_identifier.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace statements {

/**
 * Class for common statement properties.
 */
class cf_properties final {
    const ::shared_ptr<cf_prop_defs> _properties = ::make_shared<cf_prop_defs>();
    bool _use_compact_storage = false;
    std::vector<std::pair<::shared_ptr<column_identifier>, bool>> _defined_ordering; // Insertion ordering is important
public:
    auto& properties() const {
        return _properties;
    }

    bool use_compact_storage() const {
        return _use_compact_storage;
    }

    void set_compact_storage() {
        _use_compact_storage = true;
    }

    auto& defined_ordering() const {
        return _defined_ordering;
    }

    data_type get_reversable_type(const column_identifier& t, data_type type) const {
        auto is_reversed = find_ordering_info(t).value_or(false);
        if (!is_reversed && type->is_reversed()) {
            return static_pointer_cast<const reversed_type_impl>(type)->underlying_type();
        }
        if (is_reversed && !type->is_reversed()) {
            return reversed_type_impl::get_instance(type);
        }
        return type;
    }

    std::optional<bool> find_ordering_info(const column_identifier& type) const {
        for (auto& t: _defined_ordering) {
            if (*(t.first) == type) {
                return t.second;
            }
        }
        return {};
    }

    void set_ordering(::shared_ptr<column_identifier> alias, bool reversed) {
        _defined_ordering.emplace_back(alias, reversed);
    }

    void validate(const data_dictionary::database db, sstring ks_name, const schema::extensions_map& schema_extensions) const {
        _properties->validate(db, std::move(ks_name), schema_extensions);
    }
};

}

}
