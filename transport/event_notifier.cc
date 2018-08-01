/*
 * Copyright (C) 2015 ScyllaDB
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

#include "transport/server.hh"
#include "core/gate.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "transport/response.hh"

namespace cql_transport {

static logging::logger elogger("event_notifier");

cql_server::event_notifier::event_notifier()
{
    service::get_local_migration_manager().register_listener(this);
    service::get_local_storage_service().register_subscriber(this);
}

cql_server::event_notifier::~event_notifier()
{
    service::get_local_storage_service().unregister_subscriber(this);
    service::get_local_migration_manager().unregister_listener(this);
}

void cql_server::event_notifier::register_event(event::event_type et, cql_server::connection* conn)
{
    switch (et) {
    case event::event_type::TOPOLOGY_CHANGE:
        _topology_change_listeners.emplace(conn);
        break;
    case event::event_type::STATUS_CHANGE:
        _status_change_listeners.emplace(conn);
        break;
    case event::event_type::SCHEMA_CHANGE:
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
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::CREATED,
                ks_name
            }));
        };
    }
}

void cql_server::event_notifier::on_create_column_family(const sstring& ks_name, const sstring& cf_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::CREATED,
                event::schema_change::target_type::TABLE,
                ks_name,
                cf_name
            }));
        };
    }
}

void cql_server::event_notifier::on_create_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::CREATED,
                event::schema_change::target_type::TYPE,
                ks_name,
                type_name
            }));
        };
    }
}

void cql_server::event_notifier::on_create_view(const sstring& ks_name, const sstring& view_name)
{
    on_create_column_family(ks_name, view_name);
}

void cql_server::event_notifier::on_create_function(const sstring& ks_name, const sstring& function_name)
{
    elogger.warn("{} event ignored", __func__);
}

void cql_server::event_notifier::on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    elogger.warn("{} event ignored", __func__);
}

void cql_server::event_notifier::on_update_keyspace(const sstring& ks_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::UPDATED,
                ks_name
            }));
        };
    }
}

void cql_server::event_notifier::on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                ks_name,
                cf_name
            }));
        };
    }
}

void cql_server::event_notifier::on_update_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TYPE,
                ks_name,
                type_name
            }));
        };
    }
}

void cql_server::event_notifier::on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed)
{
    on_update_column_family(ks_name, view_name, columns_changed);
}

void cql_server::event_notifier::on_update_function(const sstring& ks_name, const sstring& function_name)
{
    elogger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    elogger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_drop_keyspace(const sstring& ks_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::DROPPED,
                ks_name
            }));
        };
    }
}

void cql_server::event_notifier::on_drop_column_family(const sstring& ks_name, const sstring& cf_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TABLE,
                ks_name,
                cf_name
            }));
        };
    }
}

void cql_server::event_notifier::on_drop_user_type(const sstring& ks_name, const sstring& type_name)
{
    for (auto&& conn : _schema_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_schema_change_event(event::schema_change{
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TYPE,
                ks_name,
                type_name
            }));
        };
    }
}

void cql_server::event_notifier::on_drop_view(const sstring& ks_name, const sstring& view_name)
{
    on_drop_column_family(ks_name, view_name);
}

void cql_server::event_notifier::on_drop_function(const sstring& ks_name, const sstring& function_name)
{
    elogger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    elogger.warn("%s event ignored", __func__);
}

void cql_server::event_notifier::on_join_cluster(const gms::inet_address& endpoint)
{
    for (auto&& conn : _topology_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_topology_change_event(event::topology_change::new_node(endpoint, conn->_server_addr.port)));
        };
    }
}

void cql_server::event_notifier::on_leave_cluster(const gms::inet_address& endpoint)
{
    for (auto&& conn : _topology_change_listeners) {
        using namespace cql_transport;
        if (!conn->_pending_requests_gate.is_closed()) {
            conn->write_response(conn->make_topology_change_event(event::topology_change::removed_node(endpoint, conn->_server_addr.port)));
        };
    }
}

void cql_server::event_notifier::on_up(const gms::inet_address& endpoint)
{
    bool was_up = _last_status_change.count(endpoint) && _last_status_change.at(endpoint) == event::status_change::status_type::UP;
    _last_status_change[endpoint] = event::status_change::status_type::UP;
    if (!was_up) {
        for (auto&& conn : _status_change_listeners) {
            using namespace cql_transport;
            if (!conn->_pending_requests_gate.is_closed()) {
                conn->write_response(conn->make_status_change_event(event::status_change::node_up(endpoint, conn->_server_addr.port)));
            };
        }
    }
}

void cql_server::event_notifier::on_down(const gms::inet_address& endpoint)
{
    bool was_down = _last_status_change.count(endpoint) && _last_status_change.at(endpoint) == event::status_change::status_type::DOWN;
    _last_status_change[endpoint] = event::status_change::status_type::DOWN;
    if (!was_down) {
        for (auto&& conn : _status_change_listeners) {
            using namespace cql_transport;
            if (!conn->_pending_requests_gate.is_closed()) {
                conn->write_response(conn->make_status_change_event(event::status_change::node_down(endpoint, conn->_server_addr.port)));
            };
        }
    }
}

}
