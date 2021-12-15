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
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once

#include "cql3/expr/expression.hh"
#include "db/timeout_clock.hh"

namespace cql3 {

class query_options;
class prepare_context;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
class attributes final {
private:
    std::optional<cql3::expr::expression> _timestamp;
    std::optional<cql3::expr::expression> _time_to_live;
    std::optional<cql3::expr::expression> _timeout;
public:
    static std::unique_ptr<attributes> none();
private:
    attributes(std::optional<cql3::expr::expression>&& timestamp,
               std::optional<cql3::expr::expression>&& time_to_live,
               std::optional<cql3::expr::expression>&& timeout);
public:
    bool is_timestamp_set() const;

    bool is_time_to_live_set() const;

    bool is_timeout_set() const;

    int64_t get_timestamp(int64_t now, const query_options& options);

    int32_t get_time_to_live(const query_options& options);

    db::timeout_clock::duration get_timeout(const query_options& options) const;

    void fill_prepare_context(prepare_context& ctx);

    class raw final {
    public:
        std::optional<cql3::expr::expression> timestamp;
        std::optional<cql3::expr::expression> time_to_live;
        std::optional<cql3::expr::expression> timeout;

        std::unique_ptr<attributes> prepare(data_dictionary::database db, const sstring& ks_name, const sstring& cf_name) const;
    private:
        lw_shared_ptr<column_specification> timestamp_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> timeout_receiver(const sstring& ks_name, const sstring& cf_name) const;
    };
};

}
