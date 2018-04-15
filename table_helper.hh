/*
 * Copyright (C) 2017 ScyllaDB
 *
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

#include <seastar/util/gcc6-concepts.hh>
#include <seastar/core/apply.hh>
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "database.hh"


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

    cql3::statements::prepared_statement::checked_weak_ptr _prepared_stmt; /** a raw prepared statement object (containing the INSERT statement) */
    shared_ptr<cql3::statements::modification_statement> _insert_stmt; /** INSERT prepared statement */

public:
    table_helper(sstring keyspace, sstring name, sstring create_cql, sstring insert_cql)
        : _keyspace(std::move(keyspace))
        , _name(std::move(name))
        , _create_cql(std::move(create_cql))
        , _insert_cql(std::move(insert_cql)) {}

    /**
     * Tries to create a table using create_cql command.
     *
     * @return A future that resolves when the operation is complete. Any
     *         possible errors are ignored.
     */
    future<> setup_table() const;

    /**
     * @return a future that resolves when the given t_helper is ready to be used for
     * data insertion.
     */
    future<> cache_table_info(service::query_state&);

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
    GCC6_CONCEPT( requires seastar::CanApply<OptMaker, Args...> )
    future<> insert(service::query_state& qs, OptMaker opt_maker, Args&&... opt_maker_args) {
        return cache_table_info(qs).then([this, &qs, opt_maker = std::move(opt_maker), args = std::forward_as_tuple(std::forward<Args>(opt_maker_args)...)] () mutable {
            return do_with(apply(opt_maker, std::move(args)), [this, &qs] (auto& opts) {
                return _insert_stmt->execute(service::get_storage_proxy().local(), qs, opts);
            });
        }).discard_result();
    }

    template <typename... Args>
    static inline future<> setup_keyspace(const sstring& keyspace_name, sstring replication_factor, service::query_state& qs, const Args&... args) {
        if (engine().cpu_id() == 0) {
            size_t n = sizeof...(args);
            const table_helper* tables[sizeof...(args)] = {&args...};
            for (size_t i = 0; i < n; ++i) {
                if (tables[i]->_keyspace != keyspace_name) {
                    throw std::invalid_argument("setup_keyspace called with table_helper for different keyspace");
                }
            }
            return seastar::async([&keyspace_name, replication_factor, &qs, &args...] {
                auto& db = cql3::get_local_query_processor().db().local();

                // Create a keyspace
                if (!db.has_keyspace(keyspace_name)) {
                    std::map<sstring, sstring> opts;
                    opts["replication_factor"] = replication_factor;
                    auto ksm = keyspace_metadata::new_keyspace(keyspace_name, "org.apache.cassandra.locator.SimpleStrategy", std::move(opts), true);
                    // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments. See issue #2129.
                    service::get_local_migration_manager().announce_new_keyspace(ksm, api::min_timestamp, false).get();
                }

                qs.get_client_state().set_keyspace(cql3::get_local_query_processor().db(), keyspace_name);


                // Create tables
                size_t n = sizeof...(args);
                const table_helper* tables[sizeof...(args)] = {&args...};
                for (size_t i = 0; i < n; ++i) {
                    tables[i]->setup_table().get();
                }
            });
        } else {
            return make_ready_future<>();
        }
    }

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
     * @return the monotonically increasing vlaue in 100s of ns based on the
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
        , _what(sprint("%s.%s doesn't meet expected schema.", _keyspace, _cf))
    { }
    bad_column_family(const sstring& keyspace, const sstring& cf, const std::exception& e)
        : _keyspace(keyspace)
        , _cf(cf)
        , _what(sprint("%s.%s doesn't meet expected schema: %s", _keyspace, _cf, e.what()))
    { }
    const char* what() const noexcept override {
        return _what.c_str();
    }
};
