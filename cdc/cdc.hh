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
#include "timestamp.hh"
#include "cdc_options.hh"

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

class db_context;

/// \brief CDC service, responsible for schema listeners
///
/// CDC service will listen for schema changes and iff CDC is enabled/changed
/// create/modify/delete corresponding log tables etc as part of the schema change. 
///
class cdc_service {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    cdc_service(service::storage_proxy&);
    cdc_service(db_context);
    ~cdc_service();
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
        builder(service::storage_proxy& proxy);

        builder& with_migration_manager(service::migration_manager& migration_manager);
        builder& with_token_metadata(locator::token_metadata& token_metadata);
        builder& with_snitch(locator::snitch_ptr& snitch);
        builder& with_partitioner(dht::i_partitioner& partitioner);

        db_context build();
    };
};

// cdc log table operation
enum class operation : int8_t {
    // note: these values will eventually be read by a third party, probably not privvy to this
    // enum decl, so don't change the constant values (or the datatype).
    pre_image = 0, update = 1, row_delete = 2, range_delete_start = 3, range_delete_end = 4, partition_delete = 5
};

// cdc log data column operation
enum class column_op : int8_t {
    // same as "operation". Do not edit values or type/type unless you _really_ want to.
    set = 0, del = 1, add = 2,
};

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
