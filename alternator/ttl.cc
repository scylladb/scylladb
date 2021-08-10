/*
 * Copyright 2021-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include <seastar/core/coroutine.hh>

#include "executor.hh"
#include "service/storage_proxy.hh"
#include "gms/feature_service.hh"
#include "database.hh"
#include "utils/rjson.hh"

namespace alternator {

// We write the expiration-time attribute enabled on a table using a
// tag TTL_TAG_KEY.
// Currently, the *value* of this tag is simply the name of the attribute,
// and the expiration scanner interprets it as an Alternator attribute name -
// It can refer to a real column or if that doesn't exist, to a member of
// the ":attrs" map column. Although this is designed for Alternator, it may
// be good enough for CQL as well (there, the ":attrs" column won't exist).
static const sstring TTL_TAG_KEY("system:ttl_attribute");

future<executor::request_return_type> executor::update_time_to_live(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_time_to_live++;
    if (!_proxy.get_db().local().features().cluster_supports_alternator_ttl()) {
        co_return api_error::unknown_operation("UpdateTimeToLive not yet supported. Experimental support is available if the 'alternator_ttl' experimental feature is enabled on all nodes.");
    }

    schema_ptr schema = get_table(_proxy, request);
    rjson::value* spec = rjson::find(request, "TimeToLiveSpecification");
    if (!spec || !spec->IsObject()) {
        co_return api_error::validation("UpdateTimeToLive missing mandatory TimeToLiveSpecification");
    }
    const rjson::value* v = rjson::find(*spec, "Enabled");
    if (!v || !v->IsBool()) {
        co_return api_error::validation("UpdateTimeToLive requires boolean Enabled");
    }
    bool enabled = v->GetBool();
    v = rjson::find(*spec, "AttributeName");
    if (!v || !v->IsString()) {
        co_return api_error::validation("UpdateTimeToLive requires string AttributeName");
    }
    // Although the DynamoDB documentation specifies that attribute names
    // should be between 1 and 64K bytes, in practice, it only allows
    // between 1 and 255 bytes. There are no other limitations on which
    // characters are allowed in the name.
    if (v->GetStringLength() < 1 || v->GetStringLength() > 255) {
        co_return api_error::validation("The length of AttributeName must be between 1 and 255");
    }
    sstring attribute_name(v->GetString(), v->GetStringLength());

    std::map<sstring, sstring> tags_map = get_tags_of_table(schema);
    if (enabled) {
        if (tags_map.contains(TTL_TAG_KEY)) {
            co_return api_error::validation("TTL is already enabled");
        }
        tags_map[TTL_TAG_KEY] = attribute_name;
    } else {
        auto i = tags_map.find(TTL_TAG_KEY);
        if (i == tags_map.end()) {
            co_return api_error::validation("TTL is already disabled");
        } else if (i->second != attribute_name) {
            co_return api_error::validation(format(
                "Requested to disable TTL on attribute {}, but a different attribute {} is enabled.",
                attribute_name, i->second));
        }
        tags_map.erase(TTL_TAG_KEY);
    }
    co_await update_tags(_mm, schema, std::move(tags_map));
    // Prepare the response, which contains a TimeToLiveSpecification
    // basically identical to the request's
    rjson::value response = rjson::empty_object();
    rjson::set(response, "TimeToLiveSpecification", std::move(*spec));
    co_return make_jsonable(std::move(response));
}

future<executor::request_return_type> executor::describe_time_to_live(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_time_to_live++;
    if (!_proxy.get_db().local().features().cluster_supports_alternator_ttl()) {
        co_return api_error::unknown_operation("DescribeTimeToLive not yet supported. Experimental support is available if the 'alternator_ttl' experimental feature is enabled on all nodes.");
    }
    schema_ptr schema = get_table(_proxy, request);
    std::map<sstring, sstring> tags_map = get_tags_of_table(schema);
    rjson::value desc = rjson::empty_object();
    auto i = tags_map.find(TTL_TAG_KEY);
    if (i == tags_map.end()) {
        rjson::set(desc, "TimeToLiveStatus", "DISABLED");
    } else {
        rjson::set(desc, "TimeToLiveStatus", "ENABLED");
        rjson::set(desc, "AttributeName", rjson::from_string(i->second));
    }
    rjson::value response = rjson::empty_object();
    rjson::set(response, "TimeToLiveDescription", std::move(desc));
    co_return make_jsonable(std::move(response));
}

} // namespace alternator
