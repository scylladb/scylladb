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
 * Copyright (C) 2016 ScyllaDB
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
#include <chrono>
#include "cql3/statements/prepared_statement.hh"
#include "tracing/trace_state.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "service/storage_proxy.hh"
#include "to_string.hh"
#include "timestamp.hh"

#include "cql3/values.hh"
#include "cql3/query_options.hh"

namespace tracing {

logging::logger trace_state_logger("trace_state");

struct trace_state::params_values {
    std::experimental::optional<std::unordered_set<gms::inet_address>> batchlog_endpoints;
    std::experimental::optional<api::timestamp_type> user_timestamp;
    std::vector<sstring> queries;
    std::experimental::optional<db::consistency_level> cl;
    std::experimental::optional<db::consistency_level> serial_cl;
    std::experimental::optional<int32_t> page_size;
    std::vector<prepared_checked_weak_ptr> prepared_statements;
};

trace_state::params_values* trace_state::params_ptr::get_ptr_safe() {
    if (!_vals) {
        _vals = std::unique_ptr<params_values, params_values_deleter>(new params_values, params_values_deleter());
    }
    return _vals.get();
}

void trace_state::params_values_deleter::operator()(params_values* pv) {
    delete pv;
}

void trace_state::set_batchlog_endpoints(const std::unordered_set<gms::inet_address>& val) {
    _params_ptr->batchlog_endpoints.emplace(val);
}

void trace_state::set_consistency_level(db::consistency_level val) {
    _params_ptr->cl.emplace(val);
}

void trace_state::set_optional_serial_consistency_level(const std::experimental::optional<db::consistency_level>& val) {
    if (val) {
        _params_ptr->serial_cl.emplace(*val);
    }
}

void trace_state::set_page_size(int32_t val) {
    if (val > 0) {
        _params_ptr->page_size.emplace(val);
    }
}

void trace_state::set_request_size(size_t s) noexcept {
    _records->session_rec.request_size = s;
}

void trace_state::set_response_size(size_t s) noexcept {
    _records->session_rec.response_size = s;
}

void trace_state::add_query(sstring_view val) {
    _params_ptr->queries.emplace_back(std::move(val));
}

void trace_state::set_user_timestamp(api::timestamp_type val) {
    _params_ptr->user_timestamp.emplace(val);
}

void trace_state::add_prepared_statement(prepared_checked_weak_ptr& prepared) {
    _params_ptr->prepared_statements.emplace_back(prepared->checked_weak_from_this());
}

void trace_state::build_parameters_map(const cql3::query_options* prepared_options_ptr) {
    if (!_params_ptr) {
        return;
    }

    auto& params_map = _records->session_rec.parameters;
    params_values& vals = *_params_ptr;

    if (vals.batchlog_endpoints) {
        params_map.emplace("batch_endpoints", join(sstring(","),  *vals.batchlog_endpoints | boost::adaptors::transformed([](gms::inet_address ep) {return seastar::format("/{}", ep);})));
    }

    if (vals.cl) {
        params_map.emplace("consistency_level", seastar::format("{}", *vals.cl));
    }

    if (vals.serial_cl) {
        params_map.emplace("serial_consistency_level", seastar::format("{}", *vals.serial_cl));
    }

    if (vals.page_size) {
        params_map.emplace("page_size", seastar::format("{:d}", *vals.page_size));
    }

    auto& queries = vals.queries;
    if (!queries.empty()) {
        if (queries.size() == 1) {
            params_map.emplace("query", queries[0]);
        } else {
            // BATCH
            for (size_t i = 0; i < queries.size(); ++i) {
                params_map.emplace(format("query[{:d}]", i), queries[i]);
            }
        }
    }

    if (vals.user_timestamp) {
        params_map.emplace("user_timestamp", seastar::format("{:d}", *vals.user_timestamp));
    }

    if (prepared_options_ptr) {
        auto& prepared_statements = vals.prepared_statements;

        if (prepared_statements.empty()) {
            throw std::logic_error("Tracing a prepared statement but no prepared statement is stored");
        }

        // Parameter's key in the map will be "param[X]" for a single query CQL command and "param[Y][X] for a multiple
        // queries CQL command, where X is an index of the parameter in a corresponding query and Y is an index of the
        // corresponding query in the BATCH.
        if (prepared_statements.size() == 1) {
            build_parameters_map_for_one_prepared(prepared_statements[0], prepared_options_ptr->for_statement(0), "param");
        } else {
            // BATCH
            for (size_t i = 0; i < prepared_statements.size(); ++i) {
                build_parameters_map_for_one_prepared(prepared_statements[i], prepared_options_ptr->for_statement(i), format("param[{:d}]", i));
            }
        }
    }
}

void trace_state::build_parameters_map_for_one_prepared(const prepared_checked_weak_ptr& prepared_ptr, const cql3::query_options& options, const sstring& param_name_prefix) {
    auto& params_map = _records->session_rec.parameters;
    auto& names_opt = options.get_names();
    size_t i = 0;

    // Trace parameters native values representations only if the current prepared statement has not been evicted from the cache by the time we got here.
    // Such an eviction is a very unlikely event, however if it happens, since we are unable to recover their types, trace raw representations of the values.

    if (names_opt) {
        if (names_opt->size() != options.get_values_count()) {
            throw std::logic_error(format("Number of \"names\" ({}) doesn't match the number of positional variables ({})", names_opt->size(), options.get_values_count()).c_str());
        }

        auto& names = names_opt.value();
        for (; i < options.get_values_count(); ++i) {
            params_map.emplace(format("{}[{:d}]({})", param_name_prefix, i, names[i]), raw_value_to_sstring(options.get_value_at(i), prepared_ptr ? prepared_ptr->bound_names[i]->type : nullptr));
        }
    } else {
        for (; i < options.get_values_count(); ++i) {
            params_map.emplace(format("{}[{:d}]", param_name_prefix, i), raw_value_to_sstring(options.get_value_at(i), prepared_ptr ? prepared_ptr->bound_names[i]->type : nullptr));
        }
    }
}

trace_state::~trace_state() {
    if (!is_primary() && is_in_state(state::background)) {
        trace_state_logger.error("Secondary session is in a background state! session_id: {}", session_id());
    }

    stop_foreground_and_write();
    _local_tracing_ptr->end_session();

    trace_state_logger.trace("{}: destructing", session_id());
}

void trace_state::stop_foreground_and_write(const cql3::query_options* prepared_options_ptr) noexcept {
    // Do nothing if state hasn't been initiated
    if (is_in_state(state::inactive)) {
        return;
    }

    if (is_in_state(state::foreground)) {
        auto e = elapsed();
        _records->do_log_slow_query = should_log_slow_query(e);

        if (is_primary()) {
            // We don't account the session_record event when checking a limit
            // of maximum events per session because there may be only one such
            // event and we don't want to cripple the primary session by
            // "stealing" one trace() event from it.
            //
            // We do want to account them however. If for instance there are a
            // lot of tracing sessions that only open itself and then do nothing
            // - they will create a lot of session_record events and we do want
            // to handle this case properly.
            _records->consume_from_budget();

            _records->session_rec.elapsed = e;

            // build_parameters_map() may throw. We don't want to record the
            // session's record in this case since its data may be incomplete.
            // These events should be really rare however, therefore we don't
            // want to optimize this flow (e.g. rollback the corresponding
            // events' records that have already been sent to I/O).
            if (should_write_records()) {
                try {
                    build_parameters_map(prepared_options_ptr);
                } catch (...) {
                    // Bump up an error counter, drop any pending records and
                    // continue
                    ++_local_tracing_ptr->stats.trace_errors;
                    _records->drop_records();
                }
            }
        }

        set_state(state::background);
    }

    trace_state_logger.trace("{}: Current records count is {}",  session_id(), _records->size());

    if (should_write_records()) {
        _local_tracing_ptr->write_session_records(_records, write_on_close());
    } else {
        _records->drop_records();
    }
}

sstring trace_state::raw_value_to_sstring(const cql3::raw_value_view& v, const data_type& t) {
    static constexpr int max_val_bytes = 64;

    if (v.is_null()) {
        return "null";
    } else if (v.is_unset_value()) {
        return "unset value";
    } else {
      return with_linearized(*v, [&] (bytes_view val) {
        sstring str_rep;

        if (t) {
            str_rep = t->to_string(to_bytes(val));
        } else {
            trace_state_logger.trace("{}: data types are unavailable - tracing a raw value", session_id());
            str_rep = to_hex(val);
        }

        if (str_rep.size() > max_val_bytes) {
            return format("{}...", str_rep.substr(0, max_val_bytes));
        } else {
            return str_rep;
        }
      });
    }
}
}
