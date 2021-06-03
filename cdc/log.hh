/*
 * Copyright (C) 2019-present ScyllaDB
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

/*
 * This module manages CDC log tables. It contains facilities used to:
 * - perform schema changes to CDC log tables correspondingly when base tables are changed,
 * - perform writes to CDC log tables correspondingly when writes to base tables are made.
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
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"

class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

namespace locator {

class token_metadata;

} // namespace locator

namespace service {

class migration_notifier;
class storage_proxy;
class query_state;

} // namespace service

class mutation;
class partition_key;
class database;

namespace cdc {

struct operation_result_tracker;
class db_context;
class metadata;

/// \brief CDC service, responsible for schema listeners
///
/// CDC service will listen for schema changes and iff CDC is enabled/changed
/// create/modify/delete corresponding log tables etc as part of the schema change. 
///
class cdc_service final : public async_sharded_service<cdc::cdc_service> {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    future<> stop();
    cdc_service(service::storage_proxy&, cdc::metadata&, service::migration_notifier&);
    cdc_service(db_context);
    ~cdc_service();

    // If any of the mutations are cdc enabled, optionally selects preimage, and adds the
    // appropriate augments to set the log entries.
    // Iff post-image is enabled for any of these, a non-empty callback is also
    // returned to be invoked post the mutation query.
    future<std::tuple<std::vector<mutation>, lw_shared_ptr<operation_result_tracker>>> augment_mutation_call(
        lowres_clock::time_point timeout,
        std::vector<mutation>&& mutations,
        tracing::trace_state_ptr tr_state,
        db::consistency_level write_cl
        );
    bool needs_cdc_augmentation(const std::vector<mutation>&) const;
};

struct db_context final {
    service::storage_proxy& _proxy;
    service::migration_notifier& _migration_notifier;
    cdc::metadata& _cdc_metadata;
    db_context(service::storage_proxy& proxy, cdc::metadata& cdc_meta, service::migration_notifier& notifier) noexcept
        : _proxy(proxy), _migration_notifier(notifier), _cdc_metadata(cdc_meta) {}
};

// cdc log table operation
enum class operation : int8_t {
    // note: these values will eventually be read by a third party, probably not privvy to this
    // enum decl, so don't change the constant values (or the datatype).
    pre_image = 0, update = 1, insert = 2, row_delete = 3, partition_delete = 4,
    range_delete_start_inclusive = 5, range_delete_start_exclusive = 6, range_delete_end_inclusive = 7, range_delete_end_exclusive = 8,
    post_image = 9,
};

bool is_log_for_some_table(const sstring& ks_name, const std::string_view& table_name);

schema_ptr get_base_table(const database&, const schema&);
schema_ptr get_base_table(const database&, sstring_view, std::string_view);

seastar::sstring base_name(std::string_view log_name);
seastar::sstring log_name(std::string_view table_name);
seastar::sstring log_data_column_name(std::string_view column_name);
seastar::sstring log_meta_column_name(std::string_view column_name);
bytes log_data_column_name_bytes(const bytes& column_name);
bytes log_meta_column_name_bytes(const bytes& column_name);

seastar::sstring log_data_column_deleted_name(std::string_view column_name);
bytes log_data_column_deleted_name_bytes(const bytes& column_name);

seastar::sstring log_data_column_deleted_elements_name(std::string_view column_name);
bytes log_data_column_deleted_elements_name_bytes(const bytes& column_name);

bool is_cdc_metacolumn_name(const sstring& name);

utils::UUID generate_timeuuid(api::timestamp_type t);

} // namespace cdc
