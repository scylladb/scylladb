/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <functional>
#include <vector>

#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "db/view/view_update_generator.hh"
#include "transport/messages/result_message_base.hh"
#include "cql3/query_options_fwd.hh"
#include "cql3/values.hh"
#include "cql3/prepared_statements_cache.hh"
#include "bytes.hh"
#include "schema.hh"
#include "test/lib/eventually.hh"

class database;

namespace db::view {
class view_builder;
}

namespace auth {
class service;
}

namespace cql3 {
    class query_processor;
}

class not_prepared_exception : public std::runtime_error {
public:
    not_prepared_exception(const cql3::prepared_cache_key_type& id) : std::runtime_error(format("Not prepared: {}", id)) {}
};

namespace db {
    class config;
}

struct scheduling_groups {
    scheduling_group compaction_scheduling_group;
    scheduling_group memory_compaction_scheduling_group;
    scheduling_group streaming_scheduling_group;
    scheduling_group statement_scheduling_group;
    scheduling_group memtable_scheduling_group;
    scheduling_group memtable_to_cache_scheduling_group;
    scheduling_group gossip_scheduling_group;
};

// Creating and destroying scheduling groups on each env setup and teardown
// doesn't work because it messes up execution stages due to scheduling groups
// having the same name but not having the same id on each run. So they are
// created once and used across all envs. This method allows retrieving them to
// be used in tests.
// Not thread safe!
future<scheduling_groups> get_scheduling_groups();

class cql_test_config {
public:
    seastar::shared_ptr<db::config> db_config;
    // Scheduling groups are overwritten unconditionally, see get_scheduling_groups().
    std::optional<database_config> dbcfg;
    std::set<sstring> disabled_features;

    cql_test_config();
    cql_test_config(const cql_test_config&);
    cql_test_config(shared_ptr<db::config>);
    ~cql_test_config();
};

class cql_test_env {
public:
    virtual ~cql_test_env() {};

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(sstring_view text) = 0;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(
            sstring_view text, std::unique_ptr<cql3::query_options> qo) = 0;

    /// Processes queries (which must be modifying queries) as a batch.
    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_batch(
        const std::vector<sstring_view>& queries, std::unique_ptr<cql3::query_options> qo) = 0;

    virtual future<cql3::prepared_cache_key_type> prepare(sstring query) = 0;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared(
        cql3::prepared_cache_key_type id,
        std::vector<cql3::raw_value> values,
        db::consistency_level cl = db::consistency_level::ONE) = 0;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared_with_qo(
        cql3::prepared_cache_key_type id,
        std::unique_ptr<cql3::query_options> qo) = 0;

    virtual future<std::vector<mutation>> get_modification_mutations(const sstring& text) = 0;

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) = 0;

    virtual future<> require_keyspace_exists(const sstring& ks_name) = 0;

    virtual future<> require_table_exists(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual future<> require_table_exists(std::string_view qualified_name) = 0;
    virtual future<> require_table_does_not_exist(const sstring& ks_name, const sstring& cf_name) = 0;

    virtual future<> require_column_has_value(
        const sstring& table_name,
        std::vector<data_value> pk,
        std::vector<data_value> ck,
        const sstring& column_name,
        data_value expected) = 0;

    virtual future<> stop() = 0;

    virtual service::client_state& local_client_state() = 0;

    virtual database& local_db() = 0;

    virtual cql3::query_processor& local_qp() = 0;

    virtual distributed<database>& db() = 0;

    virtual distributed<cql3::query_processor> & qp() = 0;

    virtual auth::service& local_auth_service() = 0;

    virtual db::view::view_builder& local_view_builder() = 0;

    virtual db::view::view_update_generator& local_view_update_generator() = 0;

    virtual service::migration_notifier& local_mnotifier() = 0;

    virtual sharded<service::migration_manager>& migration_manager() = 0;

    virtual future<> refresh_client_state() = 0;
};

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config = {});
future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config = {}, thread_attributes thread_attr = {});

reader_permit make_reader_permit(cql_test_env&);
