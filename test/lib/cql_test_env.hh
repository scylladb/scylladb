/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <vector>

#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "replica/database.hh"
#include "transport/messages/result_message_base.hh"
#include "cql3/query_options_fwd.hh"
#include "cql3/values.hh"
#include "cql3/prepared_statements_cache.hh"
#include "cql3/query_processor.hh"
#include "bytes.hh"
#include "schema/schema.hh"
#include "service/tablet_allocator.hh"

namespace replica {
class database;
}

namespace db {
class batchlog_manager;
}

namespace db::view {
class view_builder;
class view_update_generator;
}

namespace auth {
class service;
}

namespace cql3 {
    class query_processor;
}

namespace service {

class client_state;
class migration_manager;
class raft_group0_client;
class raft_group_registry;

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
    std::optional<replica::database_config> dbcfg;
    std::set<sstring> disabled_features;
    std::optional<cql3::query_processor::memory_config> qp_mcfg;
    bool need_remote_proxy = false;
    std::optional<uint64_t> initial_tablets; // When engaged, the default keyspace will use tablets.
    locator::host_id host_id;
    gms::inet_address broadcast_address = gms::inet_address("localhost");
    bool ms_listen = false;
    bool run_with_raft_recovery = false;

    cql_test_config();
    cql_test_config(const cql_test_config&);
    cql_test_config(shared_ptr<db::config>);
    ~cql_test_config();
};

struct cql_test_init_configurables {
    db::extensions& extensions;
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
        cql3::raw_value_vector_with_unset values,
        db::consistency_level cl = db::consistency_level::ONE) = 0;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared_with_qo(
        cql3::prepared_cache_key_type id,
        std::unique_ptr<cql3::query_options> qo) = 0;

    virtual future<std::vector<mutation>> get_modification_mutations(const sstring& text) = 0;

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) = 0;

    virtual service::client_state& local_client_state() = 0;

    virtual replica::database& local_db() = 0;

    virtual sharded<locator::shared_token_metadata>& shared_token_metadata() = 0;

    virtual cql3::query_processor& local_qp() = 0;

    virtual distributed<replica::database>& db() = 0;

    virtual distributed<cql3::query_processor> & qp() = 0;

    virtual auth::service& local_auth_service() = 0;

    virtual db::view::view_builder& local_view_builder() = 0;

    virtual db::view::view_update_generator& local_view_update_generator() = 0;

    virtual service::migration_notifier& local_mnotifier() = 0;

    virtual sharded<service::migration_manager>& migration_manager() = 0;

    virtual sharded<db::batchlog_manager>& batchlog_manager() = 0;

    virtual sharded<netw::messaging_service>& get_messaging_service() = 0;

    virtual sharded<gms::gossiper>& gossiper() = 0;

    virtual future<> refresh_client_state() = 0;

    virtual service::raft_group0_client& get_raft_group0_client() = 0;

    virtual sharded<service::raft_group_registry>& get_raft_group_registry() = 0;

    virtual sharded<db::system_keyspace>& get_system_keyspace() = 0;

    virtual sharded<service::tablet_allocator>& get_tablet_allocator() = 0;

    virtual sharded<service::storage_proxy>& get_storage_proxy() = 0;

    virtual sharded<gms::feature_service>& get_feature_service() = 0;

    virtual sharded<sstables::storage_manager>& get_sstorage_manager() = 0;

    virtual sharded<service::storage_service>& get_storage_service() = 0;

    virtual sharded<tasks::task_manager>& get_task_manager() = 0;

    data_dictionary::database data_dictionary();
};

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config = {}, std::optional<cql_test_init_configurables> = {});
future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config = {}, thread_attributes thread_attr = {}, std::optional<cql_test_init_configurables> = {});

// this function should be called in seastar thread
void do_with_mc(cql_test_env& env, std::function<void(service::group0_batch&)> func);

reader_permit make_reader_permit(cql_test_env&);
