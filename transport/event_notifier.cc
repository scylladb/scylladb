/*
 * Copyright 2015 Cloudius Systems
 */

#include "transport/server.hh"

static logging::logger logger("event_notifier");

cql_server::event_notifier::event_notifier(uint16_t port)
    : _port{port}
{
}

void cql_server::event_notifier::register_event(transport::event::event_type et, cql_server::connection* conn)
{
    switch (et) {
    case transport::event::event_type::TOPOLOGY_CHANGE:
        _topology_change_listeners.emplace(conn);
        break;
    case transport::event::event_type::STATUS_CHANGE:
        _status_change_listeners.emplace(conn);
        break;
    case transport::event::event_type::SCHEMA_CHANGE:
        _schema_change_listeners.emplace(conn);
        break;
    }
}

void cql_server::event_notifier::unregister_connection(cql_server::connection* conn)
{
    _topology_change_listeners.erase(conn);
    _status_change_listeners.erase(conn);
    _schema_change_listeners.erase(conn);
}

void cql_server::event_notifier::on_create_keyspace(const sstring& ks_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::CREATED,
            ks_name
        });
    }
}

void cql_server::event_notifier::on_create_column_family(const sstring& ks_name, const sstring& cf_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::CREATED,
            event::schema_change::target_type::TABLE,
            ks_name,
            cf_name
        });
    }
}

void cql_server::event_notifier::on_create_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::CREATED,
            event::schema_change::target_type::TYPE,
            ks_name,
            type_name
        });
    }
}

void cql_server::event_notifier::on_create_function(const sstring& ks_name, const sstring& function_name)
{
    logger.warn("{} event ignored", __func__);
}

void cql_server::event_notifier::on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    logger.warn("{} event ignored", __func__);
}

void cql_server::event_notifier::on_update_keyspace(const sstring& ks_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::UPDATED,
            ks_name
        });
    }
}

void cql_server::event_notifier::on_update_column_family(const sstring& ks_name, const sstring& cf_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            ks_name,
            cf_name
        });
    }
}

void cql_server::event_notifier::on_update_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TYPE,
            ks_name,
            type_name
        });
    }
}

void cql_server::event_notifier::on_update_function(const sstring& ks_name, const sstring& function_name)
{
    logger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    logger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_drop_keyspace(const sstring& ks_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::DROPPED,
            ks_name
        });
    }
}

void cql_server::event_notifier::on_drop_column_family(const sstring& ks_name, const sstring& cf_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::DROPPED,
            event::schema_change::target_type::TABLE,
            ks_name,
            cf_name
        });
    }
}

void cql_server::event_notifier::on_drop_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace transport;
        conn->write_schema_change_event(event::schema_change{
            event::schema_change::change_type::DROPPED,
            event::schema_change::target_type::TYPE,
            ks_name,
            type_name
        });
    }
}

void cql_server::event_notifier::on_drop_function(const sstring& ks_name, const sstring& function_name)
{
    logger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    logger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_join_cluster(const gms::inet_address& endpoint)
{
    for (auto&& conn : _topology_change_listeners) {
        using namespace transport;
        conn->write_topology_change_event(event::topology_change::new_node(endpoint, _port));
    }
}

void cql_server::event_notifier::on_leave_cluster(const gms::inet_address& endpoint)
{
    for (auto&& conn : _topology_change_listeners) {
        using namespace transport;
        conn->write_topology_change_event(event::topology_change::removed_node(endpoint, _port));
    }
}

void cql_server::event_notifier::on_move(const gms::inet_address& endpoint)
{
    for (auto&& conn : _topology_change_listeners) {
        using namespace transport;
        conn->write_topology_change_event(event::topology_change::moved_node(endpoint, _port));
    }
}

void cql_server::event_notifier::on_up(const gms::inet_address& endpoint)
{
    for (auto&& conn : _status_change_listeners) {
        using namespace transport;
        conn->write_status_change_event(event::status_change::node_up(endpoint, _port));
    }
}

void cql_server::event_notifier::on_down(const gms::inet_address& endpoint)
{
    for (auto&& conn : _status_change_listeners) {
        using namespace transport;
        conn->write_status_change_event(event::status_change::node_down(endpoint, _port));
    }
}


