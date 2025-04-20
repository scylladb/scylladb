/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/prepared_statement.hh"
#include "service/query_state.hh"

namespace service {
class migration_manager;
}

namespace cql3 {
class query_processor;
class dialect;
namespace statements {
class modification_statement;
}}

/**
 * \class table_helper
 * \brief A helper class that unites the operations on a single table under the same roof.
 */
class table_helper {
private:
    const sstring _keyspace; /** a keyspace name */
    const sstring _name; /** a table name */
    const sstring _create_cql; /** a CQL CREATE TABLE statement for the table */
    const sstring _insert_cql; /** a CQL INSERT statement */
    const std::optional<sstring> _insert_cql_fallback; /** a fallback CQL INSERT statement */

    cql3::statements::prepared_statement::checked_weak_ptr _prepared_stmt; /** a raw prepared statement object (containing the INSERT statement) */
    shared_ptr<cql3::statements::modification_statement> _insert_stmt; /** INSERT prepared statement */
    /*
     * Tells whether the _insert_stmt is a prepared fallback INSERT statement or the regular one.
     * Should be changed alongside every _insert_stmt reassignment
     * */
    bool _is_fallback_stmt = false;
private:
    // Returns true is prepare succeeded, false if failed and there's still a chance to recover, exception if prepare failed and it's not possible to recover
    future<bool> try_prepare(bool fallback, cql3::query_processor& qp, service::query_state& qs, cql3::dialect dialect);
public:
    table_helper(std::string_view keyspace, std::string_view name, sstring create_cql, sstring insert_cql, std::optional<sstring> insert_cql_fallback = std::nullopt)
        : _keyspace(keyspace)
        , _name(name)
        , _create_cql(std::move(create_cql))
        , _insert_cql(std::move(insert_cql))
        , _insert_cql_fallback(std::move(insert_cql_fallback)) {}

    /**
     * Tries to create a table using create_cql command.
     *
     * @return A future that resolves when the operation is complete. Any
     *         possible errors are ignored.
     */
    static future<> setup_table(cql3::query_processor& qp, service::migration_manager& mm, const sstring& create_cql);

    /**
     * @return a future that resolves when the given t_helper is ready to be used for
     * data insertion.
     */
    future<> cache_table_info(cql3::query_processor& qp, service::migration_manager& mm, service::query_state&);

    /**
     * @return The table name
     */
    const sstring& name() const {
        return _name;
    }

    /**
     * @return A pointer to the INSERT prepared statement
     */
    shared_ptr<cql3::statements::modification_statement> insert_stmt() const {
        return _insert_stmt;
    }

    /**
     * Execute a single insertion into the table.
     *
     * @tparam OptMaker cql_options maker functor type
     * @tparam Args OptMaker arguments' types
     * @param opt_maker cql_options maker functor
     * @param opt_maker_args opt_maker arguments
     */
    template <typename OptMaker, typename... Args>
    requires seastar::CanInvoke<OptMaker, Args...>
    future<> insert(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs, OptMaker opt_maker, Args... opt_maker_args) {
        return insert(qp, mm, qs, noncopyable_function<cql3::query_options ()>([opt_maker = std::move(opt_maker), args = std::make_tuple(std::move(opt_maker_args)...)] () mutable {
            return apply(opt_maker, std::move(args));
        }));
    }

    future<> insert(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs, noncopyable_function<cql3::query_options ()> opt_maker);

    static future<> setup_keyspace(cql3::query_processor& qp, service::migration_manager& mm, std::string_view keyspace_name, sstring replication_strategy_name,
                                   sstring replication_factor, service::query_state& qs, std::vector<table_helper*> tables);

    /**
     * Makes a monotonically increasing value in 100ns ("nanos") based on the given time
     * stamp and the "nanos" value of the previous event.
     *
     * If the amount of 100s of ns evaluated from the @param tp is equal to the
     * given @param last_event_nanos increment @param last_event_nanos by one
     * and return a time point based its new value.
     *
     * @param last_event_nanos a reference to the last nanos to align the given time point to.
     * @param tp the amount of time passed since the Epoch that will be used for the calculation.
     *
     * @return the monotonically increasing value in 100s of ns based on the
     * given time stamp and on the "nanos" value of the previous event.
     */
    static std::chrono::system_clock::time_point make_monotonic_UUID_tp(int64_t& last_event_nanos, std::chrono::system_clock::time_point tp) {
        using namespace std::chrono;

        auto tp_nanos = duration_cast<nanoseconds>(tp.time_since_epoch()).count() / 100;
        if (tp_nanos > last_event_nanos) {
            last_event_nanos = tp_nanos;
            return tp;
        } else {
            return std::chrono::system_clock::time_point(nanoseconds((++last_event_nanos) * 100));
        }
    }
};

struct bad_column_family : public std::exception {
private:
    sstring _keyspace;
    sstring _cf;
    sstring _what;
public:
    bad_column_family(const sstring& keyspace, const sstring& cf)
        : _keyspace(keyspace)
        , _cf(cf)
        , _what(format("{}.{} doesn't meet expected schema.", _keyspace, _cf))
    { }
    bad_column_family(const sstring& keyspace, const sstring& cf, const std::exception& e)
        : _keyspace(keyspace)
        , _cf(cf)
        , _what(format("{}.{} doesn't meet expected schema: {}", _keyspace, _cf, e.what()))
    { }
    const char* what() const noexcept override {
        return _what.c_str();
    }
};
