/*
 * Copyright 2019-present ScyllaDB
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

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include <seastar/json/json_elements.hh>
#include <seastar/core/sharded.hh>

#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "db/timeout_clock.hh"

#include "alternator/error.hh"
#include "stats.hh"
#include "utils/rjson.hh"

namespace db {
    class system_distributed_keyspace;
}

namespace query {
class partition_slice;
class result;
}

namespace cql3::selection {
    class selection;
}

namespace service {
    class storage_service;
    class storage_proxy;
}

namespace cdc {
    class metadata;
}

namespace alternator {

class rmw_operation;

struct make_jsonable : public json::jsonable {
    rjson::value _value;
public:
    explicit make_jsonable(rjson::value&& value);
    std::string to_json() const override;
};
struct json_string : public json::jsonable {
    std::string _value;
public:
    explicit json_string(std::string&& value);
    std::string to_json() const override;
};

namespace parsed {
class path;
};

// An attribute_path_map object is used to hold data for various attributes
// paths (parsed::path) in a hierarchy of attribute paths. Each attribute path
// has a root attribute, and then modified by member and index operators -
// for example in "a.b[2].c" we have "a" as the root, then ".b" member, then
// "[2]" index, and finally ".c" member.
// Data can be added to an attribute_path_map using the add() function, but
// requires that attributes with data not be *overlapping* or *conflicting*:
//
// 1. Two attribute paths which are identical or an ancestor of one another
//    are considered *overlapping* and not allowed. If a.b.c has data,
//    we can't add more data in a.b.c or any of its descendants like a.b.c.d.
//
// 2. Two attribute paths which need the same parent to have both a member and
//    an index are considered *conflicting* and not allowed. E.g., if a.b has
//    data, you can't add a[1]. The meaning of adding both would be that the
//    attribute a is both a map and an array, which isn't sensible.
//
// These two requirements are common to the two places where Alternator uses
// this abstraction to describe how a hierarchical item is to be transformed:
//
// 1. In ProjectExpression: for filtering from a full top-level attribute
//    only the parts for which user asked in ProjectionExpression.
//
// 2. In UpdateExpression: for taking the previous value of a top-level
//    attribute, and modifying it based on the instructions in the user
//    wrote in UpdateExpression.

template<typename T>
class attribute_path_map_node {
public:
    using data_t = T;
    // We need the extra unique_ptr<> here because libstdc++ unordered_map
    // doesn't work with incomplete types :-(
    using members_t =  std::unordered_map<std::string, std::unique_ptr<attribute_path_map_node<T>>>;
    // The indexes list is sorted because DynamoDB requires handling writes
    // beyond the end of a list in index order.
    using indexes_t = std::map<unsigned, std::unique_ptr<attribute_path_map_node<T>>>;
    // The prohibition on "overlap" and "conflict" explained above means
    // That only one of data, members or indexes is non-empty.
    std::optional<std::variant<data_t, members_t, indexes_t>> _content;

    bool is_empty() const { return !_content; }
    bool has_value() const { return _content && std::holds_alternative<data_t>(*_content); }
    bool has_members() const { return _content && std::holds_alternative<members_t>(*_content); }
    bool has_indexes() const { return _content && std::holds_alternative<indexes_t>(*_content); }
    // get_members() assumes that has_members() is true
    members_t& get_members() { return std::get<members_t>(*_content); }
    const members_t& get_members() const { return std::get<members_t>(*_content); }
    indexes_t& get_indexes() { return std::get<indexes_t>(*_content); }
    const indexes_t& get_indexes() const { return std::get<indexes_t>(*_content); }
    T& get_value() { return std::get<T>(*_content); }
    const T& get_value() const { return std::get<T>(*_content); }
};

template<typename T>
using attribute_path_map = std::unordered_map<std::string, attribute_path_map_node<T>>;

using attrs_to_get_node = attribute_path_map_node<std::monostate>;
using attrs_to_get = attribute_path_map<std::monostate>;


class executor : public peering_sharded_service<executor> {
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;
    db::system_distributed_keyspace& _sdks;
    service::storage_service& _ss;
    cdc::metadata& _cdc_metadata;
    // An smp_service_group to be used for limiting the concurrency when
    // forwarding Alternator request between shards - if necessary for LWT.
    smp_service_group _ssg;

public:
    using client_state = service::client_state;
    using request_return_type = std::variant<json::json_return_type, api_error>;
    stats _stats;
    static constexpr auto ATTRS_COLUMN_NAME = ":attrs";
    static constexpr auto KEYSPACE_NAME_PREFIX = "alternator_";
    static constexpr std::string_view INTERNAL_TABLE_PREFIX = ".scylla.alternator.";

    executor(service::storage_proxy& proxy, service::migration_manager& mm, db::system_distributed_keyspace& sdks, service::storage_service& ss, cdc::metadata& cdc_metadata, smp_service_group ssg)
        : _proxy(proxy), _mm(mm), _sdks(sdks), _ss(ss), _cdc_metadata(cdc_metadata), _ssg(ssg) {}

    future<request_return_type> create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tables(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header);
    future<request_return_type> batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> tag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> untag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_streams(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_stream(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_records(client_state& client_state, tracing::trace_state_ptr, service_permit permit, rjson::value request);

    future<> start();
    future<> stop() { return make_ready_future<>(); }

    future<> create_keyspace(std::string_view keyspace_name);

    static sstring table_name(const schema&);
    static db::timeout_clock::time_point default_timeout();
    static void set_default_timeout(db::timeout_clock::duration timeout);
private:
    static db::timeout_clock::duration s_default_timeout;
public:
    static schema_ptr find_table(service::storage_proxy&, const rjson::value& request);

private:
    friend class rmw_operation;

    static bool is_alternator_keyspace(const sstring& ks_name);
    static sstring make_keyspace_name(const sstring& table_name);
    static void describe_key_schema(rjson::value& parent, const schema&, std::unordered_map<std::string,std::string> * = nullptr);
    static void describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>&);
    
public:    
    static std::optional<rjson::value> describe_single_item(schema_ptr,
        const query::partition_slice&,
        const cql3::selection::selection&,
        const query::result&,
        const attrs_to_get&);

    static void describe_single_item(const cql3::selection::selection&,
        const std::vector<bytes_opt>&,
        const attrs_to_get&,
        rjson::value&,
        bool = false);

    void add_stream_options(const rjson::value& stream_spec, schema_builder&) const;
    void supplement_table_info(rjson::value& descr, const schema& schema) const;
    void supplement_table_stream_info(rjson::value& descr, const schema& schema) const;
};

}
