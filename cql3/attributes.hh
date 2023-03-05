/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/unset.hh"
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
    expr::unset_bind_variable_guard _timestamp_unset_guard;
    std::optional<cql3::expr::expression> _timestamp;
    expr::unset_bind_variable_guard _time_to_live_unset_guard;
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

    std::optional<int32_t> get_time_to_live(const query_options& options);

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
