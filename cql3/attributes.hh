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

#pragma once

#include "exceptions/exceptions.hh"
#include "cql3/term.hh"
#include <experimental/optional>

namespace cql3 {
/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
class attributes final {
private:
    const ::shared_ptr<term> _timestamp;
    const ::shared_ptr<term> _time_to_live;
public:
    static std::unique_ptr<attributes> none();
private:
    attributes(::shared_ptr<term>&& timestamp, ::shared_ptr<term>&& time_to_live);
public:
    bool uses_function(const sstring& ks_name, const sstring& function_name) const;

    bool is_timestamp_set() const;

    bool is_time_to_live_set() const;

    int64_t get_timestamp(int64_t now, const query_options& options);

    int32_t get_time_to_live(const query_options& options);

    void collect_marker_specification(::shared_ptr<variable_specifications> bound_names);

    class raw {
    public:
        ::shared_ptr<term::raw> timestamp;
        ::shared_ptr<term::raw> time_to_live;

        std::unique_ptr<attributes> prepare(database& db, const sstring& ks_name, const sstring& cf_name);
    private:
        ::shared_ptr<column_specification> timestamp_receiver(const sstring& ks_name, const sstring& cf_name);

        ::shared_ptr<column_specification> time_to_live_receiver(const sstring& ks_name, const sstring& cf_name);
    };
};

}
