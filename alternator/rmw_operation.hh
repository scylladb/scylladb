/*
 * Copyright 2020-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "seastarx.hh"
#include "service/paxos/cas_request.hh"
#include "utils/rjson.hh"
#include "executor.hh"

namespace alternator {

// An rmw_operation encapsulates the common logic of all the item update
// operations which may involve a read of the item before the write
// (so-called Read-Modify-Write operations). These operations include PutItem,
// UpdateItem and DeleteItem: All of these may be conditional operations (the
// "Expected" parameter) which requir a read before the write, and UpdateItem
// may also have an update expression which refers to the item's old value.
//
// The code below supports running the read and the write together as one
// transaction using LWT (this is why rmw_operation is a subclass of
// cas_request, as required by storage_proxy::cas()), but also has optional
// modes not using LWT.
class rmw_operation : public service::cas_request, public enable_shared_from_this<rmw_operation> {
public:
    // The following options choose which mechanism to use for isolating
    // parallel write operations:
    // * The FORBID_RMW option forbids RMW (read-modify-write) operations
    //   such as conditional updates. For the remaining write-only
    //   operations, ordinary quorum writes are isolated enough.
    // * The LWT_ALWAYS option always uses LWT (lightweight transactions)
    //   for any write operation - whether or not it also has a read.
    // * The LWT_RMW_ONLY option uses LWT only for RMW operations, and uses
    //   ordinary quorum writes for write-only operations.
    //   This option is not safe if the user may send both RMW and write-only
    //   operations on the same item.
    // * The UNSAFE_RMW option does read-modify-write operations as separate
    //   read and write. It is unsafe - concurrent RMW operations are not
    //   isolated at all. This option will likely be removed in the future.
    enum class write_isolation {
        FORBID_RMW, LWT_ALWAYS, LWT_RMW_ONLY, UNSAFE_RMW
    };
    static constexpr auto WRITE_ISOLATION_TAG_KEY = "system:write_isolation";

    static write_isolation get_write_isolation_for_schema(schema_ptr schema);

    static write_isolation default_write_isolation;
public:
    static void set_default_write_isolation(std::string_view mode);

protected:
    // The full request JSON
    rjson::value _request;
    // All RMW operations involve a single item with a specific partition
    // and optional clustering key, in a single table, so the following
    // information is common to all of them:
    schema_ptr _schema;
    partition_key _pk = partition_key::make_empty();
    clustering_key _ck = clustering_key::make_empty();
    write_isolation _write_isolation;

    // All RMW operations can have a ReturnValues parameter from the following
    // choices. But note that only UpdateItem actually supports all of them:
    enum class returnvalues {
        NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW
    } _returnvalues;
    static returnvalues parse_returnvalues(const rjson::value& request);
    // When _returnvalues != NONE, apply() should store here, in JSON form,
    // the values which are to be returned in the "Attributes" field.
    // The default null JSON means do not return an Attributes field at all.
    // This field is marked "mutable" so that the const apply() can modify
    // it (see explanation below), but note that because apply() may be
    // called more than once, if apply() will sometimes set this field it
    // must set it (even if just to the default empty value) every time.
    mutable rjson::value _return_attributes;
public:
    // The constructor of a rmw_operation subclass should parse the request
    // and try to discover as many input errors as it can before really
    // attempting the read or write operations.
    rmw_operation(service::storage_proxy& proxy, rjson::value&& request);
    // rmw_operation subclasses (update_item_operation, put_item_operation
    // and delete_item_operation) shall implement an apply() function which
    // takes the previous value of the item (if it was read) and creates the
    // write mutation. If the previous value of item does not pass the needed
    // conditional expression, apply() should return an empty optional.
    // apply() may throw if it encounters input errors not discovered during
    // the constructor.
    // apply() may be called more than once in case of contention, so it must
    // not change the state saved in the object (issue #7218 was caused by
    // violating this). We mark apply() "const" to let the compiler validate
    // this for us. The output-only field _return_attributes is marked
    // "mutable" above so that apply() can still write to it.
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts) const = 0;
    // Convert the above apply() into the signature needed by cas_request:
    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts) override;
    virtual ~rmw_operation() = default;
    schema_ptr schema() const { return _schema; }
    const rjson::value& request() const { return _request; }
    rjson::value&& move_request() && { return std::move(_request); }
    future<executor::request_return_type> execute(service::storage_proxy& proxy,
            service::client_state& client_state,
            tracing::trace_state_ptr trace_state,
            service_permit permit,
            bool needs_read_before_write,
            stats& stats);
    std::optional<shard_id> shard_for_execute(bool needs_read_before_write);
};

} // namespace alternator
