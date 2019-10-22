/*
 * Copyright (C) 2019 ScyllaDB
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
#include <optional>
#include <map>
#include <string>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "exceptions/exceptions.hh"
#include "json.hh"
#include "timestamp.hh"

class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

namespace locator {

class snitch_ptr;
class token_metadata;

} // namespace locator

namespace service {

class migration_manager;
class storage_proxy;
class query_state;

} // namespace service

namespace dht {

class i_partitioner;

} // namespace dht

class mutation;
class partition_key;

namespace cdc {

class options final {
    bool _enabled = false;
    bool _preimage = false;
    bool _postimage = false;
    int _ttl = 86400; // 24h in seconds
public:
    options() = default;
    options(const std::map<sstring, sstring>& map) {
        if (map.find("enabled") == std::end(map)) {
            throw exceptions::configuration_exception("Missing enabled CDC option");
        }

        for (auto& p : map) {
            if (p.first == "enabled") {
                _enabled = p.second == "true";
            } else if (p.first == "preimage") {
                _preimage = p.second == "true";
            } else if (p.first == "postimage") {
                _postimage = p.second == "true";
            } else if (p.first == "ttl") {
                _ttl = std::stoi(p.second);
            } else {
                throw exceptions::configuration_exception("Invalid CDC option: " + p.first);
            }
        }
    }
    std::map<sstring, sstring> to_map() const {
        return {
            { "enabled", _enabled ? "true" : "false" },
            { "preimage", _preimage ? "true" : "false" },
            { "postimage", _postimage ? "true" : "false" },
            { "ttl", std::to_string(_ttl) },
        };
    }

    sstring to_sstring() const {
        return json::to_json(to_map());
    }

    bool enabled() const { return _enabled; }
    bool preimage() const { return _preimage; }
    bool postimage() const { return _postimage; }
    int ttl() const { return _ttl; }

    bool operator==(const options& o) const {
        return _enabled == o._enabled && _preimage == o._preimage && _postimage == o._postimage && _ttl == o._ttl;
    }
    bool operator!=(const options& o) const {
        return !(*this == o);
    }
};

struct db_context final {
    service::storage_proxy& _proxy;
    service::migration_manager& _migration_manager;
    locator::token_metadata& _token_metadata;
    locator::snitch_ptr& _snitch;
    dht::i_partitioner& _partitioner;

    class builder final {
        service::storage_proxy& _proxy;
        std::optional<std::reference_wrapper<service::migration_manager>> _migration_manager;
        std::optional<std::reference_wrapper<locator::token_metadata>> _token_metadata;
        std::optional<std::reference_wrapper<locator::snitch_ptr>> _snitch;
        std::optional<std::reference_wrapper<dht::i_partitioner>> _partitioner;
    public:
        builder(service::storage_proxy& proxy) : _proxy(proxy) { }

        builder& with_migration_manager(service::migration_manager& migration_manager) {
            _migration_manager = migration_manager;
            return *this;
        }

        builder& with_token_metadata(locator::token_metadata& token_metadata) {
            _token_metadata = token_metadata;
            return *this;
        }

        builder& with_snitch(locator::snitch_ptr& snitch) {
            _snitch = snitch;
            return *this;
        }

        builder& with_partitioner(dht::i_partitioner& partitioner) {
            _partitioner = partitioner;
            return *this;
        }

        db_context build();
    };
};

/// \brief Sets up CDC related tables for a given table
///
/// This function not only creates CDC Log and CDC Description for a given table
/// but also populates CDC Description with a list of change streams.
///
/// param[in] ctx object with references to database components
/// param[in] schema schema of a table for which CDC tables are being created
seastar::future<> setup(db_context ctx, schema_ptr schema);

/// \brief Deletes CDC Log and CDC Description tables for a given table
///
/// This function cleans up all CDC related tables created for a given table.
/// At the moment, CDC Log and CDC Description are the only affected tables.
/// It's ok if some/all of them don't exist.
///
/// \param[in] ctx object with references to database components
/// \param[in] ks_name keyspace name of a table for which CDC tables are removed
/// \param[in] table_name name of a table for which CDC tables are removed
///
/// \pre This function works correctly no matter if CDC Log and/or CDC Description
///      exist.
seastar::future<>
remove(db_context ctx, const seastar::sstring& ks_name, const seastar::sstring& table_name);

seastar::sstring log_name(const seastar::sstring& table_name);

seastar::sstring desc_name(const seastar::sstring& table_name);

/// \brief For each mutation in the set appends related CDC Log mutation
///
/// This function should be called with a set of mutations of a table
/// with CDC enabled. Returned set of mutations contains all original mutations
/// and for each original mutation appends a mutation to CDC Log that reflects
/// the change.
///
/// \param[in] ctx object with references to database components
/// \param[in] s schema of a CDC enabled table which is being modified
/// \param[in] timeout period of time after which a request is considered timed out
/// \param[in] qs the state of the query that's being executed
/// \param[in] mutations set of changes of a CDC enabled table
///
/// \return set of mutations from input parameter with relevant CDC Log mutations appended
///
/// \pre CDC Log and CDC Description have to exist
/// \pre CDC Description has to be in sync with cluster topology
///
/// \note At the moment, cluster topology changes are not supported
//        so the assumption that CDC Description is in sync with cluster topology
//        is easy to enforce. When support for cluster topology changes is added
//        it has to make sure the assumption holds.
seastar::future<std::vector<mutation>>append_log_mutations(
        db_context ctx,
        schema_ptr s,
        lowres_clock::time_point timeout,
        service::query_state& qs,
        std::vector<mutation> mutations);

} // namespace cdc
