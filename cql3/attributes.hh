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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_ATTRIBUTES_HH
#define CQL3_ATTRIBUTES_HH


#include "exceptions/exceptions.hh"
#include "db/expiring_cell.hh"
#include "cql3/term.hh"
#include <experimental/optional>

namespace cql3 {
/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
class attributes final {
private:
    const std::experimental::optional<::shared_ptr<term>> _timestamp;
    const std::experimental::optional<::shared_ptr<term>> _time_to_live;

public:
    static std::unique_ptr<attributes> none() {
        return std::unique_ptr<attributes>{new attributes{std::move(std::experimental::optional<::shared_ptr<term>>{}), std::move(std::experimental::optional<::shared_ptr<term>>{})}};
    }

private:
    attributes(std::experimental::optional<::shared_ptr<term>>&& timestamp, std::experimental::optional<::shared_ptr<term>>&& time_to_live)
        : _timestamp{std::move(timestamp)}
        , _time_to_live{std::move(time_to_live)}
    { }

public:
    bool uses_function(sstring ks_name, sstring function_name) const {
        return (_timestamp && _timestamp.value()->uses_function(ks_name, function_name))
            || (_time_to_live && _time_to_live.value()->uses_function(ks_name, function_name));
    }

    bool is_timestamp_set() const {
        return bool(_timestamp);
    }

    bool is_time_to_live_set() const {
        return bool(_time_to_live);
    }

    int64_t get_timestamp(int64_t now, const query_options& options) {
        if (!_timestamp) {
            return now;
        }

        bytes_opt tval = _timestamp.value()->bind_and_get(options);
        if (!tval) {
            throw exceptions::invalid_request_exception("Invalid null value of timestamp");
        }

        try {
            data_type_for<int64_t>()->validate(*tval);
        } catch (exceptions::marshal_exception e) {
            throw exceptions::invalid_request_exception("Invalid timestamp value");
        }
        return boost::any_cast<int64_t>(data_type_for<int64_t>()->compose(*tval));
    }

    int32_t get_time_to_live(const query_options& options) {
        if (!_time_to_live)
            return 0;

        bytes_opt tval = _time_to_live.value()->bind_and_get(options);
        if (!tval) {
            throw exceptions::invalid_request_exception("Invalid null value of TTL");
        }

        try {
            data_type_for<int32_t>()->validate(*tval);
        }
        catch (exceptions::marshal_exception e) {
            throw exceptions::invalid_request_exception("Invalid TTL value");
        }

        auto ttl = boost::any_cast<int32_t>(data_type_for<int32_t>()->compose(*tval));
        if (ttl < 0) {
            throw exceptions::invalid_request_exception("A TTL must be greater or equal to 0");
        }

        if (ttl > db::expiring_cell::MAX_TTL) {
            throw exceptions::invalid_request_exception("ttl is too large. requested (" + std::to_string(ttl) +") maximum (" + std::to_string(db::expiring_cell::MAX_TTL) + ")");
        }

        return ttl;
    }

    void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
        if (_timestamp) {
            _timestamp.value()->collect_marker_specification(bound_names);
        }
        if (_time_to_live) {
            _time_to_live.value()->collect_marker_specification(bound_names);
        }
    }

    class raw {
    public:
        ::shared_ptr<term::raw> timestamp;
        ::shared_ptr<term::raw> time_to_live;

        std::unique_ptr<attributes> prepare(sstring ks_name, sstring cf_name) {
            auto ts = timestamp.get() == nullptr ? std::experimental::optional<::shared_ptr<term>>{} : std::experimental::optional<::shared_ptr<term>>{timestamp->prepare(ks_name, timestamp_receiver(ks_name, cf_name))};
            auto ttl = time_to_live.get() == nullptr ? std::experimental::optional<::shared_ptr<term>>{} : std::experimental::optional<::shared_ptr<term>>{time_to_live->prepare(ks_name, time_to_live_receiver(ks_name, cf_name))};
            return std::unique_ptr<attributes>{new attributes{std::move(ts), std::move(ttl)}};
        }

    private:
        ::shared_ptr<column_specification> timestamp_receiver(const sstring ks_name, sstring cf_name) {
            return ::make_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timestamp]", true), data_type_for<int64_t>());
        }

        ::shared_ptr<column_specification> time_to_live_receiver(sstring ks_name, sstring cf_name) {
            return ::make_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[ttl]", true), data_type_for<int32_t>());
        }
    };
};

}

#endif
