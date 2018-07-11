/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "cql3/attributes.hh"

namespace cql3 {

std::unique_ptr<attributes> attributes::none() {
    return std::unique_ptr<attributes>{new attributes{{}, {}}};
}

attributes::attributes(::shared_ptr<term>&& timestamp, ::shared_ptr<term>&& time_to_live)
    : _timestamp{std::move(timestamp)}
    , _time_to_live{std::move(time_to_live)}
{ }

bool attributes::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return (_timestamp && _timestamp->uses_function(ks_name, function_name))
        || (_time_to_live && _time_to_live->uses_function(ks_name, function_name));
}

bool attributes::is_timestamp_set() const {
    return bool(_timestamp);
}

bool attributes::is_time_to_live_set() const {
    return bool(_time_to_live);
}

int64_t attributes::get_timestamp(int64_t now, const query_options& options) {
    if (!_timestamp) {
        return now;
    }

    auto tval = _timestamp->bind_and_get(options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of timestamp");
    }
    if (tval.is_unset_value()) {
        return now;
    }
  return with_linearized(*tval, [] (bytes_view val) {
    try {
        data_type_for<int64_t>()->validate(val);
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid timestamp value");
    }
    return value_cast<int64_t>(data_type_for<int64_t>()->deserialize(val));
  });
}

int32_t attributes::get_time_to_live(const query_options& options) {
    if (!_time_to_live)
        return 0;

    auto tval = _time_to_live->bind_and_get(options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of TTL");
    }
    if (tval.is_unset_value()) {
        return 0;
    }
  auto ttl = with_linearized(*tval, [] (bytes_view val) {
    try {
        data_type_for<int32_t>()->validate(val);
    }
    catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid TTL value");
    }

    return value_cast<int32_t>(data_type_for<int32_t>()->deserialize(val));
  });
    if (ttl < 0) {
        throw exceptions::invalid_request_exception("A TTL must be greater or equal to 0");
    }

    if (ttl > max_ttl.count()) {
        throw exceptions::invalid_request_exception("ttl is too large. requested (" + std::to_string(ttl) +
            ") maximum (" + std::to_string(max_ttl.count()) + ")");
    }

    return ttl;
}

void attributes::collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
    if (_timestamp) {
        _timestamp->collect_marker_specification(bound_names);
    }
    if (_time_to_live) {
        _time_to_live->collect_marker_specification(bound_names);
    }
}

std::unique_ptr<attributes> attributes::raw::prepare(database& db, const sstring& ks_name, const sstring& cf_name) {
    auto ts = !timestamp ? ::shared_ptr<term>{} : timestamp->prepare(db, ks_name, timestamp_receiver(ks_name, cf_name));
    auto ttl = !time_to_live ? ::shared_ptr<term>{} : time_to_live->prepare(db, ks_name, time_to_live_receiver(ks_name, cf_name));
    return std::unique_ptr<attributes>{new attributes{std::move(ts), std::move(ttl)}};
}

::shared_ptr<column_specification> attributes::raw::timestamp_receiver(const sstring& ks_name, const sstring& cf_name) {
    return ::make_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timestamp]", true), data_type_for<int64_t>());
}

::shared_ptr<column_specification> attributes::raw::time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) {
    return ::make_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[ttl]", true), data_type_for<int32_t>());
}

}
