/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "utils/atomic_vector.hh"

namespace data_dictionary {
class keyspace_metadata;
}
using keyspace_metadata = data_dictionary::keyspace_metadata;
class view_ptr;
class user_type_impl;
using user_type = seastar::shared_ptr<const user_type_impl>;
class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;
class abstract_type;
using data_type = seastar::shared_ptr<const abstract_type>;
namespace db::functions {
class function_name;
}
namespace locator {
class tablet_metadata_change_hint;
}

#include "timestamp.hh"

#include "seastarx.hh"

class mutation;
class schema;

namespace service {

class migration_listener {
public:
    virtual ~migration_listener()
    {}

    // The callback runs inside seastar thread
    virtual void on_create_keyspace(const sstring& ks_name) = 0;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) = 0;

    // The callback runs inside seastar thread
    virtual void on_update_keyspace(const sstring& ks_name) = 0;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) = 0;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) = 0;
    virtual void on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) = 0;

    // The callback runs inside seastar thread
    virtual void on_drop_keyspace(const sstring& ks_name) = 0;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) = 0;

    // The callback runs inside seastar thread
    // called before adding/updating/dropping column family. 
    // listener can add additional type altering mutations if he knows what he is doing. 
    //
    // The `on_before_create_column_family` method is different as it doesn't assume the existence
    // of the column family's keyspace. The reason for this is that we sometimes create a keyspace
    // and its column families together. Therefore, listeners can't load the keyspace from the
    // database. Instead, they should use the `ksm` parameter if needed.
    virtual void on_before_create_column_family(const keyspace_metadata& ksm, const schema&, std::vector<mutation>&, api::timestamp_type) {}
    virtual void on_before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>&, api::timestamp_type) {}
    virtual void on_before_drop_column_family(const schema&, std::vector<mutation>&, api::timestamp_type) {}
    virtual void on_before_drop_keyspace(const sstring& keyspace_name, std::vector<mutation>&, api::timestamp_type) {}

    class only_view_notifications;
    class empty_listener;
};

class migration_listener::only_view_notifications : public migration_listener {
public:
    void on_create_keyspace(const sstring& ks_name) override {}
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}

    void on_update_keyspace(const sstring& ks_name) override {}
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override {}
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) override {}

    void on_drop_keyspace(const sstring& ks_name) override {}
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
};

class migration_listener::empty_listener : public only_view_notifications {
public:
    void on_create_view(const sstring& ks_name, const sstring& view_name) override {};
    void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {};
    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {};
};

class migration_notifier {
private:
    atomic_vector<migration_listener*> _listeners;

    future<> on_schema_change(std::function<void(migration_listener*)> notify, std::function<std::string(std::exception_ptr)> describe_error);
public:
    /// Register a migration listener on current shard.
    void register_listener(migration_listener* listener);

    /// Unregister a migration listener on current shard.
    future<> unregister_listener(migration_listener* listener);

    future<> create_keyspace(lw_shared_ptr<keyspace_metadata> ksm);
    future<> create_column_family(schema_ptr cfm);
    future<> create_user_type(user_type type);
    future<> create_view(view_ptr view);
    future<> update_keyspace(lw_shared_ptr<keyspace_metadata> ksm);
    future<> update_column_family(schema_ptr cfm, bool columns_changed);
    future<> update_user_type(user_type type);
    future<> update_view(view_ptr view, bool columns_changed);
    future<> update_tablet_metadata(locator::tablet_metadata_change_hint);
    future<> drop_keyspace(sstring ks_name);
    future<> drop_column_family(schema_ptr cfm);
    future<> drop_user_type(user_type type);
    future<> drop_view(view_ptr view);
    future<> drop_function(const db::functions::function_name& fun_name, const std::vector<data_type>& arg_types);
    future<> drop_aggregate(const db::functions::function_name& fun_name, const std::vector<data_type>& arg_types);

    void before_create_column_family(const keyspace_metadata& ksm, const schema&, std::vector<mutation>&, api::timestamp_type);
    void before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>&, api::timestamp_type);
    void before_drop_column_family(const schema&, std::vector<mutation>&, api::timestamp_type);
    void before_drop_keyspace(const sstring& keyspace_name, std::vector<mutation>&, api::timestamp_type);
};

}
