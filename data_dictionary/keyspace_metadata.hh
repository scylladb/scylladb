/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_map>
#include <vector>
#include <iosfwd>
#include <seastar/core/sstring.hh>

#include "schema/schema.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "data_dictionary/storage_options.hh"
#include "data_dictionary/keyspace_element.hh"

namespace gms {
class feature_service;
}

namespace data_dictionary {

class keyspace_metadata final : public keyspace_element {
    sstring _name;
    sstring _strategy_name;
    locator::replication_strategy_config_options _strategy_options;
    std::optional<unsigned> _initial_tablets;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    user_types_metadata _user_types;
    lw_shared_ptr<const storage_options> _storage_options;
public:
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 locator::replication_strategy_config_options strategy_options,
                 std::optional<unsigned> initial_tablets,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{},
                 user_types_metadata user_types = user_types_metadata{},
                 storage_options storage_opts = storage_options{});
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(std::string_view name,
                 std::string_view strategy_name,
                 locator::replication_strategy_config_options options,
                 std::optional<unsigned> initial_tablets,
                 bool durables_writes = true,
                 storage_options storage_opts = {});
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(const keyspace_metadata& ksm);
    void validate(const gms::feature_service&, const locator::topology&) const;
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const locator::replication_strategy_config_options& strategy_options() const {
        return _strategy_options;
    }
    std::optional<unsigned> initial_tablets() const {
        return _initial_tablets;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    user_types_metadata& user_types() {
        return _user_types;
    }
    const user_types_metadata& user_types() const {
        return _user_types;
    }
    const storage_options& get_storage_options() const {
        return *_storage_options;
    }
    lw_shared_ptr<const storage_options> get_storage_options_ptr() {
        return _storage_options;
    }

    void add_or_update_column_family(const schema_ptr& s) {
        _cf_meta_data[s->cf_name()] = s;
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);
    std::vector<schema_ptr> tables() const;
    std::vector<view_ptr> views() const;

    virtual sstring keypace_name() const override { return name(); }
    virtual sstring element_name() const override { return name(); }
    virtual sstring element_type() const override { return "keyspace"; }
    virtual std::ostream& describe(replica::database& db, std::ostream& os, bool with_internals) const override;
};

}

template <>
struct fmt::formatter<data_dictionary::keyspace_metadata> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const data_dictionary::keyspace_metadata& ksm, fmt::format_context& ctx) const -> decltype(ctx.out());
};
