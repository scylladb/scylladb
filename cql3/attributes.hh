/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/unset.hh"
#include "db/timeout_clock.hh"

namespace qos {
class service_level_controller;
struct service_level_options;
}

namespace cql3 {

class query_options;
class prepare_context;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
class attributes final {
private:
    expr::unset_bind_variable_guard _timestamp_unset_guard;
    std::optional<cql3::expr::expression> _timestamp;
    expr::unset_bind_variable_guard _time_to_live_unset_guard;
    std::optional<cql3::expr::expression> _time_to_live;
    std::optional<cql3::expr::expression> _timeout;
    std::optional<sstring> _service_level;
    std::optional<cql3::expr::expression> _concurrency;
public:
    static std::unique_ptr<attributes> none();
private:
    attributes(std::optional<cql3::expr::expression>&& timestamp,
               std::optional<cql3::expr::expression>&& time_to_live,
               std::optional<cql3::expr::expression>&& timeout,
               std::optional<sstring> service_level,
               std::optional<cql3::expr::expression>&& concurrency);
public:
    bool is_timestamp_set() const;

    bool is_time_to_live_set() const;

    bool is_timeout_set() const;

    bool is_service_level_set() const;

    bool is_concurrency_set() const;

    int64_t get_timestamp(int64_t now, const query_options& options);

    std::optional<int32_t> get_time_to_live(const query_options& options);

    db::timeout_clock::duration get_timeout(const query_options& options) const;

    qos::service_level_options get_service_level(qos::service_level_controller& sl_controller) const;

    std::optional<int32_t> get_concurrency(const query_options& options) const;

    void fill_prepare_context(prepare_context& ctx);

    class raw final {
    public:
        std::optional<cql3::expr::expression> timestamp;
        std::optional<cql3::expr::expression> time_to_live;
        std::optional<cql3::expr::expression> timeout;
        std::optional<sstring> service_level;
        std::optional<cql3::expr::expression> concurrency;

        std::unique_ptr<attributes> prepare(data_dictionary::database db, const sstring& ks_name, const sstring& cf_name) const;
    private:
        lw_shared_ptr<column_specification> timestamp_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> timeout_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> concurrency_receiver(const sstring& ks_name, const sstring& cf_name) const;
    };
};

}
