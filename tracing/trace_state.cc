/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <chrono>
#include "cql3/statements/prepared_statement.hh"
#include "tracing/trace_state.hh"
#include "timestamp.hh"

#include "cql3/values.hh"
#include "cql3/query_options.hh"

namespace tracing {

logging::logger trace_state_logger("trace_state");

struct trace_state::params_values {
    struct prepared_statement_info {
        prepared_checked_weak_ptr statement;
        std::optional<std::vector<sstring_view>> query_option_names;
        cql3::raw_value_view_vector_with_unset query_option_values;
        explicit prepared_statement_info(prepared_checked_weak_ptr statement) : statement(std::move(statement)) {}
    };

    std::optional<inet_address_vector_replica_set> batchlog_endpoints;
    std::optional<api::timestamp_type> user_timestamp;
    std::vector<sstring> queries;
    std::optional<db::consistency_level> cl;
    std::optional<db::consistency_level> serial_cl;
    std::optional<int32_t> page_size;
    std::vector<prepared_statement_info> prepared_statements;
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

void trace_state::set_batchlog_endpoints(const inet_address_vector_replica_set& val) {
    _params_ptr->batchlog_endpoints.emplace(val);
}

void trace_state::set_consistency_level(db::consistency_level val) {
    _params_ptr->cl.emplace(val);
}

void trace_state::set_optional_serial_consistency_level(const std::optional<db::consistency_level>& val) {
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

void trace_state::add_session_param(sstring_view key, sstring_view val) {
    _records->session_rec.parameters.emplace(std::move(key), std::move(val));
}

void trace_state::set_user_timestamp(api::timestamp_type val) {
    _params_ptr->user_timestamp.emplace(val);
}

void trace_state::add_prepared_statement(prepared_checked_weak_ptr& prepared) {
    _params_ptr->prepared_statements.emplace_back(prepared->checked_weak_from_this());
}

void trace_state::add_prepared_query_options(const cql3::query_options& prepared_options_ptr) {
    if (_params_ptr->prepared_statements.empty()) {
        throw std::logic_error("Tracing a prepared statement but no prepared statement is stored");
    }

    for (size_t i = 0; i < _params_ptr->prepared_statements.size(); ++i) {
        const cql3::query_options& opts = prepared_options_ptr.for_statement(i);
        _params_ptr->prepared_statements[i].query_option_names = opts.get_names();
        _params_ptr->prepared_statements[i].query_option_values = opts.get_values();
    }
}

void trace_state::build_parameters_map() {
    if (!_params_ptr) {
        return;
    }

    auto& params_map = _records->session_rec.parameters;
    params_values& vals = *_params_ptr;

    if (vals.batchlog_endpoints) {
        auto batch_endpoints = fmt::format("{}", fmt::join(*vals.batchlog_endpoints | boost::adaptors::transformed([](gms::inet_address ep) {return seastar::format("/{}", ep);}), ","));
        params_map.emplace("batch_endpoints", std::move(batch_endpoints));
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

    auto& prepared_statements = vals.prepared_statements;

    if (!prepared_statements.empty()) {
        // Parameter's key in the map will be "param[X]" for a single query CQL command and "param[Y][X] for a multiple
        // queries CQL command, where X is an index of the parameter in a corresponding query and Y is an index of the
        // corresponding query in the BATCH.
        if (prepared_statements.size() == 1) {
            auto& stmt_info = prepared_statements[0];
            build_parameters_map_for_one_prepared(stmt_info.statement, stmt_info.query_option_names, stmt_info.query_option_values, "param");
        } else {
            // BATCH
            for (size_t i = 0; i < prepared_statements.size(); ++i) {
                auto& stmt_info = prepared_statements[i];
                build_parameters_map_for_one_prepared(stmt_info.statement, stmt_info.query_option_names, stmt_info.query_option_values, format("param[{:d}]", i));
            }
        }
    }
}

void trace_state::build_parameters_map_for_one_prepared(const prepared_checked_weak_ptr& prepared_ptr,
        std::optional<std::vector<sstring_view>>& names_opt,
        cql3::raw_value_view_vector_with_unset& values, const sstring& param_name_prefix) {
    auto& params_map = _records->session_rec.parameters;
    size_t i = 0;

    // Trace parameters native values representations only if the current prepared statement has not been evicted from the cache by the time we got here.
    // Such an eviction is a very unlikely event, however if it happens, since we are unable to recover their types, trace raw representations of the values.

    if (names_opt) {
        if (names_opt->size() != values.values.size()) {
            throw std::logic_error(format("Number of \"names\" ({}) doesn't match the number of positional variables ({})", names_opt->size(), values.values.size()).c_str());
        }

        auto& names = names_opt.value();
        for (; i < values.values.size(); ++i) {
            params_map.emplace(format("{}[{:d}]({})", param_name_prefix, i, names[i]), raw_value_to_sstring(values.values[i], values.unset[i], prepared_ptr ? prepared_ptr->bound_names[i]->type : nullptr));
        }
    } else {
        for (; i < values.values.size(); ++i) {
            params_map.emplace(format("{}[{:d}]", param_name_prefix, i), raw_value_to_sstring(values.values[i], values.unset[i], prepared_ptr ? prepared_ptr->bound_names[i]->type : nullptr));
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

void trace_state::stop_foreground_and_write() noexcept {
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
                    build_parameters_map();
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

sstring trace_state::raw_value_to_sstring(const cql3::raw_value_view& v, bool is_unset, const data_type& t) {
    static constexpr int max_val_bytes = 64;

    if (is_unset) {
        return "unset value";
    }

    if (v.is_null()) {
        return "null";
    } else {
      return v.with_linearized([&] (bytes_view val) {
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
