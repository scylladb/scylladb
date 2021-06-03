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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
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

#include "selector.hh"
#include "selection.hh"

namespace cql3 {

namespace selection {

class writetime_or_ttl_selector : public selector {
    sstring _column_name;
    int _idx;
    bool _is_writetime;
    bytes_opt _current;
public:
    static shared_ptr<selector::factory> new_factory(sstring column_name, int idx, bool is_writetime) {
        class wtots_factory : public selector::factory {
            sstring _column_name;
            int _idx;
            bool _is_writetime;
        public:
            wtots_factory(sstring column_name, int idx, bool is_writetime)
                : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
            }

            virtual sstring column_name() const override {
                return format("{}({})", _is_writetime ? "writetime" : "ttl", _column_name);
            }

            virtual data_type get_return_type() const override {
                return _is_writetime ? long_type : int32_type;
            }

            virtual shared_ptr<selector> new_instance() const override {
                return ::make_shared<writetime_or_ttl_selector>(_column_name, _idx, _is_writetime);
            }

            virtual bool is_write_time_selector_factory() const override {
                return _is_writetime;
            }

            virtual bool is_ttl_selector_factory() const override {
                return !_is_writetime;
            }
        };
        return ::make_shared<wtots_factory>(std::move(column_name), idx, is_writetime);
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        if (_is_writetime) {
            int64_t ts = rs.timestamp_of(_idx);
            if (ts != api::missing_timestamp) {
                _current = bytes(bytes::initialized_later(), 8);
                auto i = _current->begin();
                serialize_int64(i, ts);
            } else {
                _current = std::nullopt;
            }
        } else {
            int ttl = rs.ttl_of(_idx);
            if (ttl > 0) {
                _current = bytes(bytes::initialized_later(), 4);
                auto i = _current->begin();
                serialize_int32(i, ttl);
            } else {
                _current = std::nullopt;
            }
        }
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        return _current;
    }

    virtual void reset() override {
        _current = std::nullopt;
    }

    virtual data_type get_type() const override {
        return _is_writetime ? long_type : int32_type;
    }

    virtual sstring assignment_testable_source_context() const override {
        return _column_name;
    }

#if 0
    @Override
    public String toString()
    {
        return columnName;
    }
#endif

    writetime_or_ttl_selector(sstring column_name, int idx, bool is_writetime)
            : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
    }
};

}
}
