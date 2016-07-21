/*
 * Copyright (C) 2014 ScyllaDB
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

// Some thrift headers include other files from within namespaces,
// which is totally broken.  Include those files here to avoid
// breakage:
#include <sys/param.h>
// end thrift workaround
#include "Cassandra.h"
#include "core/distributed.hh"
#include "database.hh"
#include "core/sstring.hh"
#include "core/print.hh"
#include "frozen_mutation.hh"
#include "utils/UUID_gen.hh"
#include <thrift/protocol/TBinaryProtocol.h>
#include <boost/move/iterator.hpp>
#include "db/marshal/type_parser.hh"
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"
#include "noexcept_traits.hh"
#include "schema_registry.hh"
#include "thrift/utils.hh"
#include "schema_builder.hh"
#include "thrift/thrift_validation.hh"
#include "service/storage_service.hh"
#include "service/query_state.hh"
#include "cql3/query_processor.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include "query-result-reader.hh"
#include "thrift/server.hh"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using namespace  ::org::apache::cassandra;

using namespace thrift;

class unimplemented_exception : public std::exception {
public:
    virtual const char* what() const throw () override { return "sorry, not implemented"; }
};

void pass_unimplemented(const tcxx::function<void(::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    exn_cob(::apache::thrift::TDelayedException::delayException(unimplemented_exception()));
}

class delayed_exception_wrapper : public ::apache::thrift::TDelayedException {
    std::exception_ptr _ex;
public:
    delayed_exception_wrapper(std::exception_ptr ex) : _ex(std::move(ex)) {}
    virtual void throw_it() override {
        // Thrift auto-wraps unexpected exceptions (those not derived from TException)
        // with a TException, but with a fairly bad what().  So detect this, and
        // provide our own TException with a better what().
        try {
            std::rethrow_exception(std::move(_ex));
        } catch (const ::apache::thrift::TException&) {
            // It's an expected exception, so assume the message
            // is fine.  Also, we don't want to change its type.
            throw;
        } catch (no_such_class& nc) {
            throw make_exception<InvalidRequestException>(nc.what());
        } catch (marshal_exception& me) {
            throw make_exception<InvalidRequestException>(me.what());
        } catch (exceptions::already_exists_exception& ae) {
            throw make_exception<InvalidRequestException>(ae.what());
        } catch (exceptions::configuration_exception& ce) {
            throw make_exception<InvalidRequestException>(ce.what());
        } catch (no_such_column_family&) {
            throw NotFoundException();
        } catch (no_such_keyspace&) {
            throw NotFoundException();
        } catch (exceptions::syntax_exception& se) {
            throw make_exception<InvalidRequestException>("syntax error: %s", se.what());
        } catch (exceptions::authentication_exception& ae) {
            throw make_exception<AuthenticationException>(ae.what());
        } catch (exceptions::unauthorized_exception& ue) {
            throw make_exception<AuthorizationException>(ue.what());
        } catch (std::exception& e) {
            // Unexpected exception, wrap it
            throw ::apache::thrift::TException(std::string("Internal server error: ") + e.what());
        } catch (...) {
            // Unexpected exception, wrap it, unfortunately without any info
            throw ::apache::thrift::TException("Internal server error");
        }
    }
};

template <typename Func, typename T>
void
with_cob(tcxx::function<void (const T& ret)>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<noexcept_movable_t<T>>::apply([func = std::forward<Func>(func)] {
        return noexcept_movable<T>::wrap(func());
    }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (auto&& f) {
        try {
            cob(noexcept_movable<T>::unwrap(f.get0()));
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func>
void
with_cob(tcxx::function<void ()>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<void>::apply(func).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<> f) {
        try {
            f.get();
            cob();
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
        }
    });
}

template <typename Func>
void
with_exn_cob(tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob, Func&& func) {
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<void>::apply(func).then_wrapped([exn_cob = std::move(exn_cob)] (future<> f) {
        try {
            f.get();
        } catch (...) {
            delayed_exception_wrapper dew(std::current_exception());
            exn_cob(&dew);
       }
    });
}

std::string bytes_to_string(bytes_view v) {
    return { reinterpret_cast<const char*>(v.begin()), v.size() };
}

class thrift_handler : public CassandraCobSvIf {
    distributed<database>& _db;
    distributed<cql3::query_processor>& _query_processor;
    service::query_state _query_state;
private:
    template <typename Cob, typename Func>
    void
    with_schema(Cob&& cob,
                tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
                const std::string& cf,
                Func&& func) {
        with_cob(std::move(cob), std::move(exn_cob), [this, &cf, func = std::move(func)] {
            auto schema = lookup_schema(_db.local(), current_keyspace(), cf);
            return func(std::move(schema));
        });
    }
public:
    explicit thrift_handler(distributed<database>& db, distributed<cql3::query_processor>& qp)
        : _db(db)
        , _query_processor(qp)
        , _query_state(service::client_state::for_external_thrift_calls())
    { }

    const sstring& current_keyspace() const {
        return _query_state.get_client_state().get_raw_keyspace();
    }

    void validate_login() const {
        return _query_state.get_client_state().validate_login();
    };

    void login(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const AuthenticationRequest& auth_request) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auth::authenticator::credentials_map creds(auth_request.credentials.begin(), auth_request.credentials.end());
            return auth::authenticator::get().authenticate(creds).then([this] (auto user) {
                _query_state.get_client_state().set_login(std::move(user));
            });
        });
    }

    void set_keyspace(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            _query_state.get_client_state().set_keyspace(_db, keyspace);
        });
    }

    void get(tcxx::function<void(ColumnOrSuperColumn const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel::type consistency_level) {
        return get_slice([cob = std::move(cob), &column_path](auto&& results) {
            if (results.empty()) {
                throw NotFoundException();
            }
            return cob(std::move(results.front()));
        }, exn_cob, key, column_path_to_column_parent(column_path), column_path_to_slice_predicate(column_path), std::move(consistency_level));
    }

    void get_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        return multiget_slice([cob = std::move(cob)](auto&& results) {
            if (!results.empty()) {
                return cob(std::move(results.begin()->second));
            }
            return cob({ });
        }, exn_cob, {key}, column_parent, predicate, consistency_level);
    }

    void get_count(tcxx::function<void(int32_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        return multiget_count([cob = std::move(cob)](auto&& results) {
            if (!results.empty()) {
                return cob(results.begin()->second);
            }
            return cob(0);
        }, exn_cob, {key}, column_parent, predicate, consistency_level);
    }

    void multiget_slice(tcxx::function<void(std::map<std::string, std::vector<ColumnOrSuperColumn> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_parent.column_family, [&](schema_ptr schema) {
            if (!column_parent.super_column.empty()) {
                fail(unimplemented::cause::SUPER);
            }
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            auto cell_limit = predicate.__isset.slice_range ? static_cast<uint32_t>(predicate.slice_range.count) : std::numeric_limits<uint32_t>::max();
            auto pranges = make_partition_ranges(*schema, keys);
            auto f = _query_state.get_client_state().has_schema_access(*schema, auth::permission::SELECT);
            return f.then([schema, cmd, pranges = std::move(pranges), cell_limit, consistency_level]() mutable {
                return service::get_local_storage_proxy().query(
                        schema,
                        cmd,
                        std::move(pranges),
                        cl_from_thrift(consistency_level),
                        nullptr).then([schema, cmd, cell_limit](auto result) {
                    return query::result_view::do_with(*result, [schema, cmd, cell_limit](query::result_view v) {
                        column_aggregator aggregator(*schema, cmd->slice, cell_limit);
                        v.consume(cmd->slice, aggregator);
                        return aggregator.release();
                    });
                });
            });
        });
    }

    void multiget_count(tcxx::function<void(std::map<std::string, int32_t>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_parent.column_family, [&](schema_ptr schema) {
            if (!column_parent.super_column.empty()) {
                fail(unimplemented::cause::SUPER);
            }
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            auto cell_limit = predicate.__isset.slice_range ? static_cast<uint32_t>(predicate.slice_range.count) : std::numeric_limits<uint32_t>::max();
            auto pranges = make_partition_ranges(*schema, keys);
            auto f = _query_state.get_client_state().has_schema_access(*schema, auth::permission::SELECT);
            return f.then([schema, cmd, pranges = std::move(pranges), cell_limit, consistency_level]() mutable {
                return service::get_local_storage_proxy().query(
                        schema,
                        cmd,
                        std::move(pranges),
                        cl_from_thrift(consistency_level),
                        nullptr).then([schema, cmd, cell_limit](auto&& result) {
                    return query::result_view::do_with(*result, [schema, cmd, cell_limit](query::result_view v) {
                        column_counter counter(*schema, cmd->slice, cell_limit);
                        v.consume(cmd->slice, counter);
                        return counter.release();
                    });
                });
            });
        });
    }

    /**
     * In origin, empty partitions are returned as part of the KeySlice, for which the key will be filled
     * in but the columns vector will be empty. Since in our case we don't return empty partitions, we
     * don't know which partition keys in the specified range we should return back to the client. So for
     * now our behavior differs from Origin.
     */
    void get_range_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const SlicePredicate& predicate, const KeyRange& range, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_parent.column_family, [&](schema_ptr schema) {
            if (!column_parent.super_column.empty()) {
                fail(unimplemented::cause::SUPER);
            }
            auto&& prange = make_partition_range(*schema, range);
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            // KeyRange::count is the number of thrift rows to return, while
            // SlicePredicte::slice_range::count limits the number of thrift colums.
            if (schema->thrift().is_dynamic()) {
                // For dynamic CFs we must limit the number of partitions returned.
                cmd->partition_limit = range.count;
            } else {
                // For static CFs each thrift row maps to a CQL row.
                cmd->row_limit = range.count;
            }
            auto f = _query_state.get_client_state().has_schema_access(*schema, auth::permission::SELECT);
            return f.then([schema, cmd, prange = std::move(prange), consistency_level] {
                return service::get_local_storage_proxy().query(
                        schema,
                        cmd,
                        {std::move(prange)},
                        cl_from_thrift(consistency_level),
                        nullptr).then([schema, cmd](auto result) {
                    return query::result_view::do_with(*result, [schema, cmd](query::result_view v) {
                        return to_key_slices(*schema, cmd->slice, v, std::numeric_limits<uint32_t>::max());
                    });
                });
            });
        });
    }

    static lw_shared_ptr<query::read_command> make_paged_read_cmd(const schema& s, uint32_t column_limit, const std::string* start_column, const query::partition_range& range) {
        auto opts = query_opts(s);
        std::vector<query::clustering_range> clustering_ranges;
        std::vector<column_id> regular_columns;
        uint32_t row_limit;
        uint32_t partition_limit;
        std::unique_ptr<query::specific_ranges> specific_ranges = nullptr;
        // KeyRange::count is the number of thrift columns to return (unlike get_range_slices).
        if (s.thrift().is_dynamic()) {
            // For dynamic CFs we must limit the number of rows returned. We use the query::specific_ranges to constrain
            // the first partition, of which we are only interested in the columns after start_column.
            row_limit = column_limit;
            partition_limit = query::max_partitions;
            if (start_column) {
                auto sr = query::specific_ranges(*range.start()->value().key(), {make_clustering_range(s, *start_column, std::string())});
                specific_ranges = std::make_unique<query::specific_ranges>(std::move(sr));
            }
            regular_columns.emplace_back(s.regular_begin()->id);
        } else {
            // For static CFs we must limit the number of columns returned. Since we don't implement a cell limit,
            // we ask for as many partitions as those that are capable of exhausting the limit and later filter out
            // any excess cells.
            row_limit = query::max_rows;
            partition_limit = (column_limit + s.regular_columns_count() - 1) / s.regular_columns_count();
            schema::const_iterator start_col = start_column
                                             ? s.regular_lower_bound(to_bytes(*start_column))
                                             : s.regular_begin();
            regular_columns = add_columns(start_col, s.regular_end(), false);
        }
        clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
        auto slice = query::partition_slice(std::move(clustering_ranges), { }, std::move(regular_columns), opts,
                std::move(specific_ranges), cql_serialization_format::internal());
        return make_lw_shared<query::read_command>(s.id(), s.version(), std::move(slice), row_limit, gc_clock::now(), stdx::nullopt, partition_limit);
    }

    static future<> do_get_paged_slice(
            schema_ptr schema,
            uint32_t column_limit,
            query::partition_range range,
            const std::string* start_column,
            db::consistency_level consistency_level,
            std::vector<KeySlice>& output) {
        auto cmd = make_paged_read_cmd(*schema, column_limit, start_column, range);
        stdx::optional<partition_key> start_key;
        auto end = range.end();
        if (start_column && !schema->thrift().is_dynamic()) {
            // For static CFs, we must first query for a specific key so as to consume the remainder
            // of columns in that partition.
            start_key = range.start()->value().key();
            range = query::partition_range::make_singular(std::move(range.start()->value()));
        }
        return service::get_local_storage_proxy().query(schema, cmd, {std::move(range)}, consistency_level, nullptr).then([schema, cmd, column_limit](auto result) {
            return query::result_view::do_with(*result, [schema, cmd, column_limit](query::result_view v) {
                return to_key_slices(*schema, cmd->slice, v, column_limit);
            });
        }).then([schema, cmd, column_limit, consistency_level, start_key = std::move(start_key), end = std::move(end), &output](auto&& slices) mutable {
            auto columns = std::accumulate(slices.begin(), slices.end(), 0u, [](auto&& acc, auto&& ks) {
                return acc + ks.columns.size();
            });
            std::move(slices.begin(), slices.end(), std::back_inserter(output));
            if (columns == 0 || columns == column_limit || (slices.size() < cmd->partition_limit && columns < cmd->row_limit)) {
                if (!output.empty() || !start_key) {
                    return make_ready_future();
                }
                // The single, first partition we queried was empty, so retry with no start column.
            } else {
                start_key = key_from_thrift(*schema, to_bytes_view(output.back().key));
            }
            auto start = dht::global_partitioner().decorate_key(*schema, std::move(*start_key));
            auto new_range = query::partition_range(query::partition_range::bound(std::move(start), false), std::move(end));
            return do_get_paged_slice(schema, column_limit - columns, std::move(new_range), nullptr, consistency_level, output);
        });
    }

    void get_paged_slice(tcxx::function<void(std::vector<KeySlice> const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family, const KeyRange& range, const std::string& start_column, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_family, [&](schema_ptr schema) {
            return do_with(std::vector<KeySlice>(), [&](auto& output) {
                if (range.__isset.row_filter) {
                    throw make_exception<InvalidRequestException>("Cross-row paging is not supported along with index clauses");
                }
                if (range.count <= 0) {
                    throw make_exception<InvalidRequestException>("Count must be positive");
                }
                auto&& prange = make_partition_range(*schema, range);
                if (!start_column.empty()) {
                    auto&& start_bound = prange.start();
                    if (!(start_bound && start_bound->is_inclusive() && start_bound->value().has_key())) {
                        // According to Orign's DataRange#Paging#slicesForKey.
                        throw make_exception<InvalidRequestException>("If start column is provided, so must the start key");
                    }
                }
                auto f = _query_state.get_client_state().has_schema_access(*schema, auth::permission::SELECT);
                return f.then([schema, count = range.count, start_column, prange = std::move(prange), consistency_level, &output] {
                    return do_get_paged_slice(std::move(schema), count, std::move(prange), &start_column,
                            cl_from_thrift(consistency_level), output).then([&output] {
                        return std::move(output);
                    });
                });
            });
        });
    }

    void get_indexed_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const IndexClause& index_clause, const SlicePredicate& column_predicate, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        warn(unimplemented::cause::INDEXES);
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void insert(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_parent.column_family, [&](schema_ptr schema) {
            if (column_parent.__isset.super_column) {
                fail(unimplemented::cause::SUPER);
            }

            mutation m_to_apply(key_from_thrift(*schema, to_bytes_view(key)), schema);
            add_to_mutation(*schema, column, m_to_apply);
            return _query_state.get_client_state().has_schema_access(*schema, auth::permission::MODIFY).then([m_to_apply = std::move(m_to_apply), consistency_level] {
                return service::get_local_storage_proxy().mutate({std::move(m_to_apply)}, cl_from_thrift(consistency_level), nullptr);
            });
        });
    }

    void add(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const CounterColumn& column, const ConsistencyLevel::type consistency_level) {
        warn(unimplemented::cause::COUNTERS);
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void cas(tcxx::function<void(CASResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const std::string& column_family, const std::vector<Column> & expected, const std::vector<Column> & updates, const ConsistencyLevel::type serial_consistency_level, const ConsistencyLevel::type commit_consistency_level) {
        CASResult _return;
        warn(unimplemented::cause::LWT);
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void remove(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const int64_t timestamp, const ConsistencyLevel::type consistency_level) {
        with_schema(std::move(cob), std::move(exn_cob), column_path.column_family, [&](schema_ptr schema) {
            mutation m_to_apply(key_from_thrift(*schema, to_bytes_view(key)), schema);

            if (column_path.__isset.super_column) {
                fail(unimplemented::cause::SUPER);
            } else if (column_path.__isset.column) {
                Deletion d;
                d.__set_timestamp(timestamp);
                d.__set_predicate(column_path_to_slice_predicate(column_path));
                Mutation m;
                m.__set_deletion(d);
                add_to_mutation(*schema, m, m_to_apply);
            } else {
                m_to_apply.partition().apply(tombstone(timestamp, gc_clock::now()));
            }

            return _query_state.get_client_state().has_schema_access(*schema, auth::permission::MODIFY).then([m_to_apply = std::move(m_to_apply), consistency_level] {
                return service::get_local_storage_proxy().mutate({std::move(m_to_apply)}, cl_from_thrift(consistency_level), nullptr);
            });
        });
    }

    void remove_counter(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& path, const ConsistencyLevel::type consistency_level) {
        warn(unimplemented::cause::COUNTERS);
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto p = prepare_mutations(_db.local(), current_keyspace(), mutation_map);
            return parallel_for_each(std::move(p.second), [this](auto&& schema) {
                return _query_state.get_client_state().has_schema_access(*schema, auth::permission::MODIFY);
            }).then([muts = std::move(p.first), consistency_level] {
                return service::get_local_storage_proxy().mutate(std::move(muts), cl_from_thrift(consistency_level), nullptr);
            });
        });
    }

    void atomic_batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto p = prepare_mutations(_db.local(), current_keyspace(), mutation_map);
            return parallel_for_each(std::move(p.second), [this](auto&& schema) {
                return _query_state.get_client_state().has_schema_access(*schema, auth::permission::MODIFY);
            }).then([muts = std::move(p.first), consistency_level] {
                return service::get_local_storage_proxy().mutate_atomically(std::move(muts), cl_from_thrift(consistency_level), nullptr);
            });
        });
    }

    void truncate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfname) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (current_keyspace().empty()) {
                throw make_exception<InvalidRequestException>("keyspace not set");
            }

            return _query_state.get_client_state().has_column_family_access(current_keyspace(), cfname, auth::permission::MODIFY).then([=] {
                return service::get_local_storage_proxy().truncate_blocking(current_keyspace(), cfname);
            });
        });
    }

    void get_multi_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const MultiSliceRequest& request) {
        with_schema(std::move(cob), std::move(exn_cob), request.column_parent.column_family, [&](schema_ptr schema) {
            if (!request.__isset.key) {
                throw make_exception<InvalidRequestException>("Key may not be empty");
            }
            if (!request.__isset.column_parent || request.column_parent.column_family.empty()) {
                throw make_exception<InvalidRequestException>("non-empty table is required");
            }
            if (!request.column_parent.super_column.empty()) {
                throw make_exception<InvalidRequestException>("get_multi_slice does not support super columns");
            }
            auto& s = *schema;
            auto pk = key_from_thrift(s, to_bytes(request.key));
            auto dk = dht::global_partitioner().decorate_key(s, pk);
            std::vector<column_id> regular_columns;
            std::vector<query::clustering_range> clustering_ranges;
            auto opts = query_opts(s);
            uint32_t row_limit;
            if (s.thrift().is_dynamic()) {
                row_limit = request.count;
                clustering_ranges = make_non_overlapping_ranges<clustering_key_prefix>(std::move(request.column_slices), [&s](auto&& cslice) {
                    return make_clustering_range(s, cslice.start, cslice.finish);
                }, clustering_key_prefix::prefix_equal_tri_compare(s), request.reversed);
                regular_columns.emplace_back(s.regular_begin()->id);
                if (request.reversed) {
                    opts.set(query::partition_slice::option::reversed);
                }
            } else {
                row_limit = query::max_rows;
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
                auto ranges = make_non_overlapping_ranges<bytes>(std::move(request.column_slices), [](auto&& cslice) {
                    return make_range(cslice.start, cslice.finish);
                }, [&s](auto&& s1, auto&& s2) { return s.regular_column_name_type()->compare(s1, s2); }, request.reversed);
                auto on_range = [&](auto&& range) {
                    auto start = range.start() ? s.regular_lower_bound(range.start()->value()) : s.regular_begin();
                    auto end  = range.end() ? s.regular_upper_bound(range.end()->value()) : s.regular_end();
                    regular_columns = add_columns(start, end, request.reversed);
                };
                if (request.reversed) {
                    std::for_each(ranges.rbegin(), ranges.rend(), on_range);
                } else {
                    std::for_each(ranges.begin(), ranges.end(), on_range);
                }
            }
            auto slice = query::partition_slice(std::move(clustering_ranges), {}, std::move(regular_columns), opts, nullptr);
            auto cmd = make_lw_shared<query::read_command>(schema->id(), schema->version(), std::move(slice), row_limit);
            auto f = _query_state.get_client_state().has_schema_access(*schema, auth::permission::SELECT);
            return f.then([dk = std::move(dk), cmd, schema, column_limit = request.count, cl = request.consistency_level] {
                return service::get_local_storage_proxy().query(
                        schema,
                        cmd,
                        {query::partition_range::make_singular(dk)},
                        cl_from_thrift(cl),
                        nullptr).then([schema, cmd, column_limit](auto result) {
                    return query::result_view::do_with(*result, [schema, cmd, column_limit](query::result_view v) {
                        column_aggregator aggregator(*schema, cmd->slice, column_limit);
                        v.consume(cmd->slice, aggregator);
                        auto cols = aggregator.release();
                        return !cols.empty() ? std::move(cols.begin()->second) : std::vector<ColumnOrSuperColumn>();
                    });
                });
            });
        });
    }

    void describe_schema_versions(tcxx::function<void(std::map<std::string, std::vector<std::string> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        with_cob(std::move(cob), std::move(exn_cob), [] {
            return service::get_local_storage_service().describe_schema_versions().then([](auto&& m) {
                std::map<std::string, std::vector<std::string>> ret;
                for (auto&& p : m) {
                    ret[p.first] = std::vector<std::string>(p.second.begin(), p.second.end());
                }
                return ret;
            });
        });
    }

    void describe_keyspaces(tcxx::function<void(std::vector<KsDef>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            validate_login();
            std::vector<KsDef>  ret;
            for (auto&& ks : _db.local().keyspaces()) {
                ret.emplace_back(get_keyspace_definition(ks.second));
            }
            return ret;
        });
    }

    void describe_cluster_name(tcxx::function<void(std::string const& _return)> cob) {
        cob(_db.local().get_config().cluster_name());
    }

    void describe_version(tcxx::function<void(std::string const& _return)> cob) {
        cob(org::apache::cassandra::thrift_version);
    }

    void do_describe_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace, bool local) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto& ks = _db.local().find_keyspace(keyspace);
            if (ks.get_replication_strategy().get_type() == locator::replication_strategy_type::local) {
                throw make_exception<InvalidRequestException>("There is no ring for the keyspace: %s", keyspace);
            }

            auto ring = service::get_local_storage_service().describe_ring(keyspace, local);
            std::vector<TokenRange> ret;
            ret.reserve(ring.size());
            std::transform(ring.begin(), ring.end(), std::back_inserter(ret), [](auto&& tr) {
                TokenRange token_range;
                token_range.__set_start_token(std::move(tr._start_token));
                token_range.__set_end_token(std::move(tr._end_token));
                token_range.__set_endpoints(std::vector<std::string>(tr._endpoints.begin(), tr._endpoints.end()));
                std::vector<EndpointDetails> eds;
                std::transform(tr._endpoint_details.begin(), tr._endpoint_details.end(), std::back_inserter(eds), [](auto&& ed) {
                    EndpointDetails detail;
                    detail.__set_host(ed._host);
                    detail.__set_datacenter(ed._datacenter);
                    detail.__set_rack(ed._rack);
                    return detail;
                });
                token_range.__set_endpoint_details(std::move(eds));
                token_range.__set_rpc_endpoints(std::vector<std::string>(tr._rpc_endpoints.begin(), tr._rpc_endpoints.end()));
                return token_range;
            });
            return ret;
        });
    }

    void describe_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        do_describe_ring(std::move(cob), std::move(exn_cob), keyspace, false);
    }

    void describe_local_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        do_describe_ring(std::move(cob), std::move(exn_cob), keyspace, true);
    }

    void describe_token_map(tcxx::function<void(std::map<std::string, std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        with_cob(std::move(cob), std::move(exn_cob), [] {
            auto m = service::get_local_storage_service().get_token_to_endpoint_map();
            std::map<std::string, std::string> ret;
            for (auto&& p : m) {
                ret[sprint("%s", p.first)] = p.second.to_sstring();
            }
            return ret;
        });
    }

    void describe_partitioner(tcxx::function<void(std::string const& _return)> cob) {
        cob(dht::global_partitioner().name());
    }

    void describe_snitch(tcxx::function<void(std::string const& _return)> cob) {
        cob(sprint("org.apache.cassandra.locator.%s", _db.local().get_snitch_name()));
    }

    void describe_keyspace(tcxx::function<void(KsDef const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            validate_login();
            auto& ks = _db.local().find_keyspace(keyspace);
            return get_keyspace_definition(ks);
        });
    }

    void describe_splits(tcxx::function<void(std::vector<std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        // FIXME: Maybe implement.
        // Origin's thrift interface has this to say about the verb:
        //      "experimental API for hadoop/parallel query support. may change violently and without warning.".
        // Some drivers have moved away from depending on this verb (SPARKC-94). The correct way to implement
        // this, as well as describe_splits_ex, is to use the size_estimates system table (CASSANDRA-7688).
        // However, we currently don't populate that table, which is done by SizeEstimatesRecorder.java in Origin.
        return pass_unimplemented(exn_cob);
    }

    void trace_next_query(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy trace");
    }

    void describe_splits_ex(tcxx::function<void(std::vector<CfSplit>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        // FIXME: To implement. See describe_splits.
        return pass_unimplemented(exn_cob);
    }

    void system_add_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!_db.local().has_keyspace(cf_def.keyspace)) {
                throw NotFoundException();
            }
            if (_db.local().has_schema(cf_def.keyspace, cf_def.name)) {
                throw make_exception<InvalidRequestException>("Column family %s already exists", cf_def.name);
            }

            auto s = schema_from_thrift(cf_def, cf_def.keyspace);
            return _query_state.get_client_state().has_keyspace_access(cf_def.keyspace, auth::permission::CREATE).then([this, s = std::move(s)] {
                return service::get_local_migration_manager().announce_new_column_family(std::move(s), false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void system_drop_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            return _query_state.get_client_state().has_column_family_access(current_keyspace(), column_family, auth::permission::DROP).then([=] {
                return service::get_local_migration_manager().announce_column_family_drop(current_keyspace(), column_family, false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void system_add_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto ksm = keyspace_from_thrift(ks_def);
            return _query_state.get_client_state().has_all_keyspaces_access(auth::permission::CREATE).then([this, ksm = std::move(ksm)] {
                return service::get_local_migration_manager().announce_new_keyspace(std::move(ksm), false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void system_drop_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            thrift_validation::validate_keyspace_not_system(keyspace);
            if (!_db.local().has_keyspace(keyspace)) {
                throw NotFoundException();
            }

            return _query_state.get_client_state().has_keyspace_access(keyspace, auth::permission::DROP).then([=] {
                return service::get_local_migration_manager().announce_keyspace_drop(keyspace, false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void system_update_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            thrift_validation::validate_keyspace_not_system(ks_def.name);

            if (!_db.local().has_keyspace(ks_def.name)) {
                throw NotFoundException();
            }
            if (!ks_def.cf_defs.empty()) {
                throw make_exception<InvalidRequestException>("Keyspace update must not contain any column family definitions.");
            }

            auto ksm = keyspace_from_thrift(ks_def);
            return _query_state.get_client_state().has_keyspace_access(ks_def.name, auth::permission::ALTER).then([this, ksm = std::move(ksm)] {
                return service::get_local_migration_manager().announce_keyspace_update(std::move(ksm), false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void system_update_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto schema = _db.local().find_schema(cf_def.keyspace, cf_def.name);

            if (schema->is_cql3_table()) {
                throw make_exception<InvalidRequestException>("Cannot modify CQL3 table {} as it may break the schema. You should use cqlsh to modify CQL3 tables instead.", cf_def.name);
            }

            auto s = schema_from_thrift(cf_def, cf_def.keyspace, schema->id());
            return _query_state.get_client_state().has_schema_access(*schema, auth::permission::ALTER).then([this, s = std::move(s)] {
                return service::get_local_migration_manager().announce_column_family_update(std::move(s), true, false).then([this] {
                    return std::string(_db.local().get_version().to_sstring());
                });
            });
        });
    }

    void execute_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        throw make_exception<InvalidRequestException>("CQL2 is not supported");
    }

    class cql3_result_visitor final : public ::transport::messages::result_message::visitor {
        CqlResult _result;
    public:
        const CqlResult& result() const {
            return _result;
        }
        virtual void visit(const ::transport::messages::result_message::void_message&) override {
            _result.__set_type(CqlResultType::VOID);
        }
        virtual void visit(const ::transport::messages::result_message::set_keyspace& m) override {
            _result.__set_type(CqlResultType::VOID);
        }
        virtual void visit(const ::transport::messages::result_message::prepared::cql& m) override {
            throw make_exception<InvalidRequestException>("Cannot convert prepared query result to CqlResult");
        }
        virtual void visit(const ::transport::messages::result_message::prepared::thrift& m) override {
            throw make_exception<InvalidRequestException>("Cannot convert prepared query result to CqlResult");
        }
        virtual void visit(const ::transport::messages::result_message::schema_change& m) override {
            _result.__set_type(CqlResultType::VOID);
        }
        virtual void visit(const ::transport::messages::result_message::rows& m) override {
            _result = to_thrift_result(m.rs());
        }
    };

    void execute_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression, const ConsistencyLevel::type consistency) {
        with_exn_cob(std::move(exn_cob), [&] {
            if (compression != Compression::type::NONE) {
                throw make_exception<InvalidRequestException>("Compressed query strings are not supported");
            }
            auto opts = std::make_unique<cql3::query_options>(cl_from_thrift(consistency), stdx::nullopt, std::vector<bytes_view_opt>(),
                            false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
            auto f = _query_processor.local().process(query, _query_state, *opts);
            return f.then([cob = std::move(cob), opts = std::move(opts)](auto&& ret) {
                cql3_result_visitor visitor;
                ret->accept(visitor);
                return cob(visitor.result());
            });
        });
    }

    void prepare_cql_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        throw make_exception<InvalidRequestException>("CQL2 is not supported");
    }

    class prepared_result_visitor final : public ::transport::messages::result_message::visitor_base {
        CqlPreparedResult _result;
    public:
        const CqlPreparedResult& result() const {
            return _result;
        }
        virtual void visit(const ::transport::messages::result_message::prepared::cql& m) override {
            throw std::runtime_error("Unexpected result message type.");
        }
        virtual void visit(const ::transport::messages::result_message::prepared::thrift& m) override {
            _result.__set_itemId(m.get_id());
            auto& names = m.metadata()->names();
            _result.__set_count(names.size());
            std::vector<std::string> variable_types;
            std::vector<std::string> variable_names;
            for (auto csp : names) {
                variable_types.emplace_back(csp->type->name());
                variable_names.emplace_back(csp->name->to_string());
            }
            _result.__set_variable_types(std::move(variable_types));
            _result.__set_variable_names(std::move(variable_names));
        }
    };

    void prepare_cql3_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        with_exn_cob(std::move(exn_cob), [&] {
            validate_login();
            if (compression != Compression::type::NONE) {
                throw make_exception<InvalidRequestException>("Compressed query strings are not supported");
            }
            return _query_processor.local().prepare(query, _query_state).then([cob = std::move(cob)](auto&& stmt) {
                prepared_result_visitor visitor;
                stmt->accept(visitor);
                cob(visitor.result());
            });
        });
    }

    void execute_prepared_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values) {
        throw make_exception<InvalidRequestException>("CQL2 is not supported");
    }

    void execute_prepared_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values, const ConsistencyLevel::type consistency) {
        with_exn_cob(std::move(exn_cob), [&] {
            auto prepared = _query_processor.local().get_prepared_for_thrift(itemId);
            if (!prepared) {
                throw make_exception<InvalidRequestException>("Prepared query with id %d not found", itemId);
            }
            auto stmt = prepared->statement;
            if (stmt->get_bound_terms() != values.size()) {
                throw make_exception<InvalidRequestException>("Wrong number of values specified. Expected %d, got %d.", stmt->get_bound_terms(), values.size());
            }
            std::vector<bytes_opt> bytes_values;
            std::transform(values.begin(), values.end(), std::back_inserter(bytes_values), [](auto&& s) {
                return to_bytes(s);
            });
            auto opts = std::make_unique<cql3::query_options>(cl_from_thrift(consistency), stdx::nullopt, std::move(bytes_values),
                            false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
            auto f = _query_processor.local().process_statement(stmt, _query_state, *opts);
            return f.then([cob = std::move(cob), opts = std::move(opts)](auto&& ret) {
                cql3_result_visitor visitor;
                ret->accept(visitor);
                return cob(visitor.result());
            });
        });
    }

    void set_cql_version(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& version) {
        // No-op.
        cob();
    }

private:
    template<allow_prefixes IsPrefixable>
    static sstring class_from_compound_type(const compound_type<IsPrefixable>& ct) {
        if (ct.is_singular()) {
            return ct.types().front()->name();
        }
        sstring type = "org.apache.cassandra.db.marshal.CompositeType(";
        for (auto& dt : ct.types()) {
            type += dt->name();
            if (&dt != &*ct.types().rbegin()) {
                type += ",";
            }
        }
        type += ")";
        return type;
    }
    static std::pair<std::vector<data_type>, bool> get_types(const std::string& thrift_type) {
        static const char composite_type[] = "CompositeType";
        std::vector<data_type> ret;
        auto t = sstring_view(thrift_type);
        auto composite_idx = t.find(composite_type);
        bool is_compound = false;
        if (composite_idx == sstring_view::npos) {
            ret.emplace_back(db::marshal::type_parser::parse(t));
        } else {
            t.remove_prefix(composite_idx + sizeof(composite_type) - 1);
            auto types = db::marshal::type_parser(t).get_type_parameters(false);
            std::move(types.begin(), types.end(), std::back_inserter(ret));
            is_compound = true;
        }
        return std::make_pair(std::move(ret), is_compound);
    }
    static CqlResult to_thrift_result(const cql3::result_set& rs) {
        CqlResult result;
        result.__set_type(CqlResultType::ROWS);

        constexpr static const char* utf8 = "UTF8Type";

        CqlMetadata mtd;
        std::map<std::string, std::string> name_types;
        std::map<std::string, std::string> value_types;
        for (auto&& c : rs.get_metadata().get_names()) {
            auto&& name = c->name->to_string();
            name_types.emplace(name, utf8);
            value_types.emplace(name, c->type->name());
        }
        mtd.__set_name_types(name_types);
        mtd.__set_value_types(value_types);
        mtd.__set_default_name_type(utf8);
        mtd.__set_default_value_type(utf8);
        result.__set_schema(mtd);

        std::vector<CqlRow> rows;
        rows.reserve(rs.rows().size());
        for (auto&& row : rs.rows()) {
            std::vector<Column> columns;
            columns.reserve(rs.get_metadata().column_count());
            for (unsigned i = 0; i < row.size(); i++) { // iterator
                auto& col = rs.get_metadata().get_names()[i];
                Column c;
                c.__set_name(col->name->to_string());
                auto& data = row[i];
                if (data) {
                    c.__set_value(bytes_to_string(*data));
                }
                columns.emplace_back(std::move(c));
            }
            CqlRow r;
            r.__set_key(std::string());
            r.__set_columns(columns);
            rows.emplace_back(std::move(r));
        }
        result.__set_rows(rows);
        return result;
    }
    static KsDef get_keyspace_definition(const keyspace& ks) {
        auto make_options = [](auto&& m) {
            return std::map<std::string, std::string>(m.begin(), m.end());
        };
        auto&& meta = ks.metadata();
        KsDef def;
        def.__set_name(meta->name());
        def.__set_strategy_class(meta->strategy_name());
        def.__set_strategy_options(make_options(meta->strategy_options()));
        std::vector<CfDef> cfs;
        for (auto&& cf : meta->cf_meta_data()) {
            auto&& s = cf.second;
            if (s->is_cql3_table()) {
                continue;
            }
            CfDef cf_def;
            cf_def.__set_keyspace(s->ks_name());
            cf_def.__set_name(s->cf_name());
            cf_def.__set_column_type(cf_type_to_sstring(s->type()));
            if (s->clustering_key_size()) {
                cf_def.__set_comparator_type(class_from_compound_type(*s->clustering_key_type()));
            } else {
                cf_def.__set_comparator_type(s->regular_column_name_type()->name());
            }
            cf_def.__set_comment(s->comment());
            cf_def.__set_read_repair_chance(s->read_repair_chance());
            if (!s->thrift().is_dynamic()) {
                std::vector<ColumnDef> columns;
                for (auto&& c : s->regular_columns()) {
                    ColumnDef c_def;
                    c_def.__set_name(c.name_as_text());
                    c_def.__set_validation_class(c.type->name());
                    columns.emplace_back(std::move(c_def));
                }
                cf_def.__set_column_metadata(columns);
            }
            cf_def.__set_gc_grace_seconds(s->gc_grace_seconds().count());
            cf_def.__set_default_validation_class(s->default_validator()->name());
            cf_def.__set_min_compaction_threshold(s->min_compaction_threshold());
            cf_def.__set_max_compaction_threshold(s->max_compaction_threshold());
            cf_def.__set_key_validation_class(class_from_compound_type(*s->partition_key_type()));
            cf_def.__set_key_alias(s->partition_key_columns().begin()->name_as_text());
            cf_def.__set_compaction_strategy(sstables::compaction_strategy::name(s->compaction_strategy()));
            cf_def.__set_compaction_strategy_options(make_options(s->compaction_strategy_options()));
            cf_def.__set_compression_options(make_options(s->get_compressor_params().get_options()));
            cf_def.__set_bloom_filter_fp_chance(s->bloom_filter_fp_chance());
            cf_def.__set_caching("all");
            cf_def.__set_dclocal_read_repair_chance(s->dc_local_read_repair_chance());
            cf_def.__set_memtable_flush_period_in_ms(s->memtable_flush_period());
            cf_def.__set_default_time_to_live(s->default_time_to_live().count());
            cf_def.__set_speculative_retry(s->speculative_retry().to_sstring());
            cfs.emplace_back(std::move(cf_def));
        }
        def.__set_cf_defs(cfs);
        def.__set_durable_writes(meta->durable_writes());
        return std::move(def);
    }
    static index_info index_info_from_thrift(const ColumnDef& def) {
        stdx::optional<sstring> idx_name;
        stdx::optional<std::unordered_map<sstring, sstring>> idx_opts;
        auto idx_type = ::index_type::none;
        if (def.__isset.index_type) {
            idx_type = [&def] {
                switch (def.index_type) {
                case IndexType::type::KEYS: return ::index_type::keys;
                case IndexType::type::COMPOSITES: return ::index_type::composites;
                case IndexType::type::CUSTOM: return ::index_type::custom;
                default: return ::index_type::none;
                };
            }();
        }
        if (def.__isset.index_name) {
            idx_name = to_sstring(def.index_name);
        }
        if (def.__isset.index_options) {
            idx_opts = std::unordered_map<sstring, sstring>(def.index_options.begin(), def.index_options.end());
        }
        return index_info(idx_type, idx_name, idx_opts);
    }
    static schema_ptr schema_from_thrift(const CfDef& cf_def, const sstring ks_name, std::experimental::optional<utils::UUID> id = { }) {
        thrift_validation::validate_cf_def(cf_def);
        schema_builder builder(ks_name, cf_def.name, id);

        if (cf_def.__isset.key_validation_class) {
            auto pk_types = std::move(get_types(cf_def.key_validation_class).first);
            if (pk_types.size() == 1 && cf_def.__isset.key_alias) {
                builder.with_column(to_bytes(cf_def.key_alias), std::move(pk_types.back()), column_kind::partition_key);
            } else {
                for (uint32_t i = 0; i < pk_types.size(); ++i) {
                    builder.with_column(to_bytes(sprint("key%d", i + 1)), std::move(pk_types[i]), column_kind::partition_key);
                }
            }
        } else {
            builder.with_column(to_bytes("key"), bytes_type, column_kind::partition_key);
        }

        data_type regular_column_name_type;
        if (cf_def.column_metadata.empty()) {
            // Dynamic CF
            builder.set_is_dense(true);
            regular_column_name_type = utf8_type;
            auto p = get_types(cf_def.comparator_type);
            auto ck_types = std::move(p.first);
            builder.set_is_compound(p.second);
            for (uint32_t i = 0; i < ck_types.size(); ++i) {
                builder.with_column(to_bytes(sprint("column%d", i + 1)), std::move(ck_types[i]), column_kind::clustering_key);
            }
            auto&& vtype = cf_def.__isset.default_validation_class
                         ? db::marshal::type_parser::parse(to_sstring(cf_def.default_validation_class))
                         : bytes_type;
            builder.with_column(to_bytes("value"), std::move(vtype));
        } else {
            // Static CF
            builder.set_is_compound(false);
            regular_column_name_type = db::marshal::type_parser::parse(to_sstring(cf_def.comparator_type));
            for (const ColumnDef& col_def : cf_def.column_metadata) {
                auto col_name = to_bytes(col_def.name);
                regular_column_name_type->validate(col_name);
                builder.with_column(std::move(col_name), db::marshal::type_parser::parse(to_sstring(col_def.validation_class)),
                                    index_info_from_thrift(col_def), column_kind::regular_column);
            }
        }
        builder.set_regular_column_name_type(regular_column_name_type);
        if (cf_def.__isset.comment) {
            builder.set_comment(cf_def.comment);
        }
        if (cf_def.__isset.read_repair_chance) {
            builder.set_read_repair_chance(cf_def.read_repair_chance);
        }
        if (cf_def.__isset.gc_grace_seconds) {
            builder.set_gc_grace_seconds(cf_def.gc_grace_seconds);
        }
        if (cf_def.__isset.min_compaction_threshold) {
            builder.set_min_compaction_threshold(cf_def.min_compaction_threshold);
        }
        if (cf_def.__isset.max_compaction_threshold) {
            builder.set_max_compaction_threshold(cf_def.max_compaction_threshold);
        }
        if (cf_def.__isset.compaction_strategy) {
            builder.set_compaction_strategy(sstables::compaction_strategy::type(cf_def.compaction_strategy));
        }
        auto make_options = [](const std::map<std::string, std::string>& m) {
            return std::map<sstring, sstring>{m.begin(), m.end()};
        };
        if (cf_def.__isset.compaction_strategy_options) {
            builder.set_compaction_strategy_options(make_options(cf_def.compaction_strategy_options));
        }
        if (cf_def.__isset.compression_options) {
            builder.set_compressor_params(compression_parameters(make_options(cf_def.compression_options)));
        }
        if (cf_def.__isset.bloom_filter_fp_chance) {
            builder.set_bloom_filter_fp_chance(cf_def.bloom_filter_fp_chance);
        }
        if (cf_def.__isset.dclocal_read_repair_chance) {
            builder.set_dc_local_read_repair_chance(cf_def.dclocal_read_repair_chance);
        }
        if (cf_def.__isset.memtable_flush_period_in_ms) {
            builder.set_memtable_flush_period(cf_def.memtable_flush_period_in_ms);
        }
        if (cf_def.__isset.default_time_to_live) {
            builder.set_default_time_to_live(gc_clock::duration(cf_def.default_time_to_live));
        }
        if (cf_def.__isset.speculative_retry) {
            builder.set_speculative_retry(cf_def.speculative_retry);
        }
        if (cf_def.__isset.min_index_interval) {
            builder.set_min_index_interval(cf_def.min_index_interval);
        }
        if (cf_def.__isset.max_index_interval) {
            builder.set_max_index_interval(cf_def.max_index_interval);
        }
        return builder.build();
    }
    static lw_shared_ptr<keyspace_metadata> keyspace_from_thrift(const KsDef& ks_def) {
        thrift_validation::validate_ks_def(ks_def);
        std::vector<schema_ptr> cf_defs;
        cf_defs.reserve(ks_def.cf_defs.size());
        for (const CfDef& cf_def : ks_def.cf_defs) {
            if (cf_def.keyspace != ks_def.name) {
                throw make_exception<InvalidRequestException>("CfDef (%s) had a keyspace definition that did not match KsDef", cf_def.keyspace);
            }
            cf_defs.emplace_back(schema_from_thrift(cf_def, ks_def.name));
        }
        return make_lw_shared<keyspace_metadata>(
            to_sstring(ks_def.name),
            to_sstring(ks_def.strategy_class),
            std::map<sstring, sstring>{ks_def.strategy_options.begin(), ks_def.strategy_options.end()},
            ks_def.durable_writes,
            std::move(cf_defs));
    }
    static schema_ptr lookup_schema(database& db, const sstring& ks_name, const sstring& cf_name) {
        if (ks_name.empty()) {
            throw make_exception<InvalidRequestException>("keyspace not set");
        }
        return db.find_schema(ks_name, cf_name);
    }
    static partition_key key_from_thrift(const schema& s, bytes_view k) {
        thrift_validation::validate_key(s, k);
        if (s.partition_key_size() == 1) {
            return partition_key::from_single_value(s, to_bytes(k));
        }
        auto composite = composite_view(k);
        return partition_key::from_exploded(composite.values());
    }
    static db::consistency_level cl_from_thrift(const ConsistencyLevel::type consistency_level) {
        switch(consistency_level) {
        case ConsistencyLevel::type::ONE: return db::consistency_level::ONE;
        case ConsistencyLevel::type::QUORUM: return db::consistency_level::QUORUM;
        case ConsistencyLevel::type::LOCAL_QUORUM: return db::consistency_level::LOCAL_QUORUM;
        case ConsistencyLevel::type::EACH_QUORUM: return db::consistency_level::EACH_QUORUM;
        case ConsistencyLevel::type::ALL: return db::consistency_level::ALL;
        case ConsistencyLevel::type::ANY: return db::consistency_level::ANY;
        case ConsistencyLevel::type::TWO: return db::consistency_level::TWO;
        case ConsistencyLevel::type::THREE: return db::consistency_level::THREE;
        case ConsistencyLevel::type::SERIAL: return db::consistency_level::SERIAL;
        case ConsistencyLevel::type::LOCAL_SERIAL: return db::consistency_level::LOCAL_SERIAL;
        case ConsistencyLevel::type::LOCAL_ONE: return db::consistency_level::LOCAL_ONE;
        default: throw make_exception<InvalidRequestException>("undefined consistency_level %s", consistency_level);
        }
    }
    static ttl_opt maybe_ttl(const schema& s, const Column& col) {
        if (col.__isset.ttl) {
            auto ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::seconds(col.ttl));
            if (ttl.count() <= 0) {
                throw make_exception<InvalidRequestException>("ttl must be positive");
            }
            if (ttl > max_ttl) {
                throw make_exception<InvalidRequestException>("ttl is too large");
            }
            return {ttl};
        } else if (s.default_time_to_live().count() > 0) {
            return {s.default_time_to_live()};
        } else {
            return { };
        }
    }
    static clustering_key_prefix make_clustering_prefix(const schema& s, bytes_view v) {
        auto composite = composite_view(v, s.thrift().has_compound_comparator());
        return clustering_key_prefix::from_exploded(composite.values());
    }
    static query::clustering_range::bound make_clustering_bound(const schema& s, bytes_view v, composite::eoc exclusiveness_marker) {
        auto composite = composite_view(v, s.thrift().has_compound_comparator());
        auto last = composite::eoc::none;
        auto&& ck = clustering_key_prefix::from_exploded(composite.components() | boost::adaptors::transformed([&](auto&& c) {
            last = c.second;
            return c.first;
        }));
        return query::clustering_range::bound(std::move(ck), last != exclusiveness_marker);
    }
    static query::clustering_range make_clustering_range(const schema& s, const std::string& start, const std::string& end) {
        using bound = query::clustering_range::bound;
        stdx::optional<bound> start_bound;
        if (!start.empty()) {
            start_bound = make_clustering_bound(s, to_bytes_view(start), composite::eoc::end);
        }
        stdx::optional<bound> end_bound;
        if (!end.empty()) {
            end_bound = make_clustering_bound(s, to_bytes_view(end), composite::eoc::start);
        }
        query::clustering_range range = { std::move(start_bound), std::move(end_bound) };
        if (range.is_wrap_around(clustering_key_prefix::prefix_equal_tri_compare(s))) {
            throw make_exception<InvalidRequestException>("Range finish must come after start in the order of traversal");
        }
        return range;
    }
    static range<bytes> make_range(const std::string& start, const std::string& end) {
        using bound = range<bytes>::bound;
        stdx::optional<bound> start_bound;
        if (!start.empty()) {
            start_bound = bound(to_bytes(start));
        }
        stdx::optional<bound> end_bound;
        if (!end.empty()) {
            end_bound = bound(to_bytes(end));
        }
        return { std::move(start_bound), std::move(end_bound) };
    }
    static std::pair<schema::const_iterator, schema::const_iterator> make_column_range(const schema& s, const std::string& start, const std::string& end) {
        auto start_it = start.empty() ? s.regular_begin() : s.regular_lower_bound(to_bytes(start));
        auto end_it = end.empty() ? s.regular_end() : s.regular_upper_bound(to_bytes(end));
        if (start_it > end_it) {
            throw make_exception<InvalidRequestException>("Range finish must come after start in the order of traversal");
        }
        return std::make_pair(std::move(start_it), std::move(end_it));
    }
    // Adds the column_ids from the specified range of column_definitions to the out vector,
    // according to the order defined by reversed.
    static std::vector<column_id> add_columns(auto&& beg, auto&& end, bool reversed) {
        auto range = boost::make_iterator_range(std::move(beg), std::move(end))
                     | boost::adaptors::filtered(std::mem_fn(&column_definition::is_atomic))
                     | boost::adaptors::transformed(std::mem_fn(&column_definition::id));
        return reversed ? boost::copy_range<std::vector<column_id>>(range | boost::adaptors::reversed)
                        : boost::copy_range<std::vector<column_id>>(range);
    }
    static query::partition_slice::option_set query_opts(const schema& s) {
        query::partition_slice::option_set opts;
        opts.set(query::partition_slice::option::send_timestamp);
        opts.set(query::partition_slice::option::send_ttl);
        if (s.thrift().is_dynamic()) {
            opts.set(query::partition_slice::option::send_clustering_key);
        }
        opts.set(query::partition_slice::option::send_partition_key);
        return opts;
    }
    static lw_shared_ptr<query::read_command> slice_pred_to_read_cmd(const schema& s, const SlicePredicate& predicate) {
        auto opts = query_opts(s);
        std::vector<query::clustering_range> clustering_ranges;
        std::vector<column_id> regular_columns;
        uint32_t per_partition_row_limit = query::max_rows;
        if (predicate.__isset.column_names) {
            thrift_validation::validate_column_names(predicate.column_names);
            auto unique_column_names = boost::copy_range<std::vector<std::string>>(predicate.column_names | boost::adaptors::uniqued);
            if (s.thrift().is_dynamic()) {
                for (auto&& name : unique_column_names) {
                    auto ckey = make_clustering_prefix(s, to_bytes(name));
                    clustering_ranges.emplace_back(query::clustering_range::make_singular(std::move(ckey)));
                }
                regular_columns.emplace_back(s.regular_begin()->id);
            } else {
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
                auto&& defs = unique_column_names
                    | boost::adaptors::transformed([&s](auto&& name) { return s.get_column_definition(to_bytes(name)); })
                    | boost::adaptors::filtered([](auto* def) { return def; })
                    | boost::adaptors::indirected;
                regular_columns = add_columns(defs.begin(), defs.end(), false);
            }
        } else if (predicate.__isset.slice_range) {
            auto range = predicate.slice_range;
            if (range.count < 0) {
                throw make_exception<InvalidRequestException>("SliceRange requires non-negative count");
            }
            if (range.reversed) {
                std::swap(range.start, range.finish);
                opts.set(query::partition_slice::option::reversed);
            }
            per_partition_row_limit = static_cast<uint32_t>(range.count);
            if (s.thrift().is_dynamic()) {
                clustering_ranges.emplace_back(make_clustering_range(s, range.start, range.finish));
                regular_columns.emplace_back(s.regular_begin()->id);
            } else {
                clustering_ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
                auto r = make_column_range(s, range.start, range.finish);
                // For static CFs, the limit is enforced on the result as we do not implement
                // a cell limit in the database engine.
                regular_columns = add_columns(r.first, r.second, range.reversed);
            }
        } else {
            throw make_exception<InvalidRequestException>("SlicePredicate column_names and slice_range may not both be null");
        }
        auto slice = query::partition_slice(std::move(clustering_ranges), {}, std::move(regular_columns), opts,
                nullptr, cql_serialization_format::internal(), per_partition_row_limit);
        return make_lw_shared<query::read_command>(s.id(), s.version(), std::move(slice));
    }
    static ColumnParent column_path_to_column_parent(const ColumnPath& column_path) {
        ColumnParent ret;
        ret.__set_column_family(column_path.column_family);
        if (column_path.__isset.super_column) {
            ret.__set_super_column(column_path.super_column);
        }
        return ret;
    }
    static SlicePredicate column_path_to_slice_predicate(const ColumnPath& column_path) {
        SlicePredicate ret;
        if (column_path.__isset.column) {
            ret.__set_column_names({column_path.column});
        }
        return ret;
    }
    static std::vector<query::partition_range> make_partition_ranges(const schema& s, const std::vector<std::string>& keys) {
        std::vector<query::partition_range> ranges;
        for (auto&& key : keys) {
            auto pk = key_from_thrift(s, to_bytes_view(key));
            auto dk = dht::global_partitioner().decorate_key(s, pk);
            ranges.emplace_back(query::partition_range::make_singular(std::move(dk)));
        }
        return ranges;
    }
    static Column make_column(const bytes& col, const query::result_atomic_cell_view& cell) {
        Column ret;
        ret.__set_name(bytes_to_string(col));
        ret.__set_value(bytes_to_string(cell.value()));
        ret.__set_timestamp(cell.timestamp());
        if (cell.ttl()) {
            ret.__set_ttl(cell.ttl()->count());
        }
        return ret;
    }
    static ColumnOrSuperColumn column_to_column_or_supercolumn(Column&& col) {
        ColumnOrSuperColumn ret;
        ret.__set_column(std::move(col));
        return ret;
    }
    static ColumnOrSuperColumn make_column_or_supercolumn(const bytes& col, const query::result_atomic_cell_view& cell) {
        return column_to_column_or_supercolumn(make_column(col, cell));
    }
    static std::string partition_key_to_string(const schema& s, const partition_key& key) {
        return bytes_to_string(to_legacy(*s.partition_key_type(), key));
    }
    /*
    template<typename T>
    concept bool Aggregator() {
        return requires (T aggregator, typename T::type* aggregation, const bytes& name, const query::result_atomic_cell_view& cell) {
            { aggregator.on_column(aggregation, name, cell) } -> void;
        };
    }
    */
    template<typename Aggregator>
    class column_visitor : public Aggregator {
        const schema& _s;
        const query::partition_slice& _slice;
        uint32_t _cell_limit;
        std::map<std::string, typename Aggregator::type> _aggregation;
        typename Aggregator::type* _current_aggregation;
    public:
        column_visitor(const schema& s, const query::partition_slice& slice, uint32_t cell_limit)
                : _s(s), _slice(slice), _cell_limit(cell_limit)
        { }
        std::map<std::string, typename Aggregator::type>&& release() {
            return std::move(_aggregation);
        }
        void accept_new_partition(const partition_key& key, uint32_t row_count) {
            _current_aggregation = &_aggregation[partition_key_to_string(_s, key)];
        }
        void accept_new_partition(uint32_t row_count) {
            // We always ask for the partition_key to be sent in query_opts().
            abort();
        }
        void accept_new_row(const clustering_key_prefix& key, const query::result_row_view& static_row, const query::result_row_view& row) {
            auto it = row.iterator();
            auto cell = it.next_atomic_cell();
            if (cell && _cell_limit > 0) {
                bytes column_name = composite::serialize_value(key.components(), _s.thrift().has_compound_comparator());
                Aggregator::on_column(_current_aggregation, column_name, *cell);
                _cell_limit -= 1;
            }
        }
        void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
            auto it = row.iterator();
            for (auto&& id : _slice.regular_columns) {
                auto cell = it.next_atomic_cell();
                if (cell && _cell_limit > 0) {
                    Aggregator::on_column(_current_aggregation, _s.regular_column_at(id).name(), *cell);
                    _cell_limit -= 1;
                }
            }
        }
        void accept_partition_end(const query::result_row_view& static_row) {
        }
    };
    struct column_or_supercolumn_builder {
        using type = std::vector<ColumnOrSuperColumn>;
        void on_column(std::vector<ColumnOrSuperColumn>* current_cols, const bytes& name, const query::result_atomic_cell_view& cell) {
            current_cols->emplace_back(make_column_or_supercolumn(name, cell));
        }
    };
    using column_aggregator = column_visitor<column_or_supercolumn_builder>;
    struct counter {
        using type = int32_t;
        void on_column(int32_t* current_cols, const bytes_view& name, const query::result_atomic_cell_view& cell) {
            *current_cols += 1;
        }
    };
    using column_counter = column_visitor<counter>;
    static query::partition_range make_partition_range(const schema& s, const KeyRange& range) {
        if (range.__isset.row_filter) {
            fail(unimplemented::cause::INDEXES);
        }
        if ((range.__isset.start_key == range.__isset.start_token)
                || (range.__isset.end_key == range.__isset.end_token)) {
            throw make_exception<InvalidRequestException>(
                    "Exactly one each of {start key, start token} and {end key, end token} must be specified");
        }
        if (range.__isset.start_token && range.__isset.end_key) {
            throw make_exception<InvalidRequestException>("Start token + end key is not a supported key range");
        }

        auto&& partitioner = dht::global_partitioner();

        if (range.__isset.start_key && range.__isset.end_key) {
            auto start = range.start_key.empty()
                       ? dht::ring_position::starting_at(dht::minimum_token())
                       : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.start_key)));
            auto end = range.end_key.empty()
                     ? dht::ring_position::ending_at(dht::maximum_token())
                     : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.end_key)));
            if (end.less_compare(s, start)) {
                if (partitioner.preserves_order()) {
                    throw make_exception<InvalidRequestException>(
                            "Start key must sort before (or equal to) finish key in the partitioner");
                } else {
                    throw make_exception<InvalidRequestException>(
                            "Start key's token sorts after end key's token. This is not allowed; you probably should not specify end key at all except with an ordered partitioner");
                }
            }
            return {query::partition_range::bound(std::move(start), true),
                    query::partition_range::bound(std::move(end), true)};
        }

        if (range.__isset.start_key && range.__isset.end_token) {
            // start_token/end_token can wrap, but key/token should not
            auto start = range.start_key.empty()
                       ? dht::ring_position::starting_at(dht::minimum_token())
                       : partitioner.decorate_key(s, key_from_thrift(s, to_bytes(range.start_key)));
            auto end = dht::ring_position::ending_at(partitioner.from_sstring(sstring(range.end_token)));
            if (end.token().is_minimum()) {
                end = dht::ring_position::ending_at(dht::maximum_token());
            } else if (end.less_compare(s, start)) {
                throw make_exception<InvalidRequestException>("Start key's token sorts after end token");
            }
            return {query::partition_range::bound(std::move(start), true),
                    query::partition_range::bound(std::move(end), true)};
        }

        // Token range can wrap; the start token is exclusive.
        auto start = dht::ring_position::ending_at(partitioner.from_sstring(sstring(range.start_token)));
        auto end = dht::ring_position::ending_at(partitioner.from_sstring(sstring(range.end_token)));
        if (end.token().is_minimum()) {
            end = dht::ring_position::ending_at(dht::maximum_token());
        }
        if (start.token() == end.token()) {
            return query::partition_range::make_open_ended_both_sides();
        }
        return {query::partition_range::bound(std::move(start), false),
                query::partition_range::bound(std::move(end), true)};
    }
    static std::vector<KeySlice> to_key_slices(const schema& s, const query::partition_slice& slice, query::result_view v, uint32_t cell_limit) {
        column_aggregator aggregator(s, slice, cell_limit);
        v.consume(slice, aggregator);
        auto&& cols = aggregator.release();
        std::vector<KeySlice> ret;
        std::transform(
                std::make_move_iterator(cols.begin()),
                std::make_move_iterator(cols.end()),
                boost::back_move_inserter(ret),
                [](auto&& p) {
            KeySlice ks;
            ks.__set_key(std::move(p.first));
            ks.__set_columns(std::move(p.second));
            return ks;
        });
        return ret;
    }
    template<typename RangeType, typename Comparator>
    static std::vector<range<RangeType>> make_non_overlapping_ranges(
            std::vector<ColumnSlice> column_slices,
            const std::function<range<RangeType>(ColumnSlice&&)> mapper,
            const Comparator&& cmp,
            bool reversed) {
        std::vector<range<RangeType>> ranges;
        std::transform(column_slices.begin(), column_slices.end(), std::back_inserter(ranges), [&](auto&& cslice) {
            auto range = mapper(std::move(cslice));
            if (!reversed && range.is_wrap_around(cmp)) {
                throw make_exception<InvalidRequestException>("Column slice had start %s greater than finish %s", cslice.start, cslice.finish);
            } else if (reversed && !range.is_wrap_around(cmp)) {
                throw make_exception<InvalidRequestException>("Reversed column slice had start %s less than finish %s", cslice.start, cslice.finish);
            } else if (reversed) {
                range.reverse();
            }
            return range;
        });
        return range<RangeType>::deoverlap(std::move(ranges), cmp);
    }
    static range_tombstone make_range_tombstone(const schema& s, const SliceRange& range, tombstone tomb) {
        using bound = query::clustering_range::bound;
        stdx::optional<bound> start_bound;
        if (!range.start.empty()) {
            start_bound = make_clustering_bound(s, to_bytes_view(range.start), composite::eoc::end);
        }
        stdx::optional<bound> end_bound;
        if (!range.finish.empty()) {
            end_bound = make_clustering_bound(s, to_bytes_view(range.finish), composite::eoc::start);
        }
        return {start_bound ? std::move(*start_bound).value() : clustering_key_prefix::make_empty(),
                !start_bound || start_bound->is_inclusive() ? bound_kind::incl_start : bound_kind::excl_start,
                end_bound ? std::move(*end_bound).value() : clustering_key_prefix::make_empty(),
                !end_bound || end_bound->is_inclusive() ? bound_kind::incl_end : bound_kind::excl_end,
                std::move(tomb)};
    }
    static void delete_cell(const column_definition& def, api::timestamp_type timestamp, gc_clock::time_point deletion_time, mutation& m_to_apply) {
        if (def.is_atomic()) {
            auto dead_cell = atomic_cell::make_dead(timestamp, deletion_time);
            m_to_apply.set_clustered_cell(clustering_key_prefix::make_empty(), def, std::move(dead_cell));
        }
    }
    static void delete_column(const schema& s, const sstring& column_name, api::timestamp_type timestamp, gc_clock::time_point deletion_time, mutation& m_to_apply) {
        auto&& def = s.get_column_definition(to_bytes(column_name));
        if (def) {
            delete_cell(*def, timestamp, deletion_time, m_to_apply);
        }
    }
    static void apply_delete(const schema& s, const SlicePredicate& predicate, api::timestamp_type timestamp, mutation& m_to_apply) {
        auto deletion_time = gc_clock::now();
        if (predicate.__isset.column_names) {
            thrift_validation::validate_column_names(predicate.column_names);
            if (s.thrift().is_dynamic()) {
                for (auto&& name : predicate.column_names) {
                    auto ckey = make_clustering_prefix(s, to_bytes(name));
                    m_to_apply.partition().apply_delete(s, std::move(ckey), tombstone(timestamp, deletion_time));
                }
            } else {
                for (auto&& name : predicate.column_names) {
                    delete_column(s, name, timestamp, deletion_time, m_to_apply);
                }
            }
        } else if (predicate.__isset.slice_range) {
            auto&& range = predicate.slice_range;
            if (s.thrift().is_dynamic()) {
                m_to_apply.partition().apply_delete(s, make_range_tombstone(s, range, tombstone(timestamp, deletion_time)));
            } else {
                auto r = make_column_range(s, range.start, range.finish);
                std::for_each(r.first, r.second, [&](auto&& def) {
                    delete_cell(def, timestamp, deletion_time, m_to_apply);
                });
            }
        } else {
            throw make_exception<InvalidRequestException>("SlicePredicate column_names and slice_range may not both be null");
        }
    }
    static void add_live_cell(const schema& s, const Column& col, const column_definition& def, clustering_key_prefix ckey, mutation& m_to_apply) {
        thrift_validation::validate_column(col, def);
        auto cell = atomic_cell::make_live(col.timestamp, to_bytes_view(col.value), maybe_ttl(s, col));
        m_to_apply.set_clustered_cell(std::move(ckey), def, std::move(cell));
    }
    static void add_to_mutation(const schema& s, const Column& col, mutation& m_to_apply) {
        thrift_validation::validate_column_name(col.name);
        if (s.thrift().is_dynamic()) {
            auto&& value_col = s.regular_begin();
            add_live_cell(s, col, *value_col, make_clustering_prefix(s, to_bytes_view(col.name)), m_to_apply);
        } else {
            auto def = s.get_column_definition(to_bytes(col.name));
            if (def) {
                if (def->kind != column_kind::regular_column) {
                    throw make_exception<InvalidRequestException>("Column %s is not settable", col.name);
                }
                add_live_cell(s, col, *def, clustering_key_prefix::make_empty(s), m_to_apply);
            } else {
                throw make_exception<InvalidRequestException>("No such column %s", col.name);
            }
        }
    }
    static void add_to_mutation(const schema& s, const Mutation& m, mutation& m_to_apply) {
        if (m.__isset.column_or_supercolumn) {
            if (m.__isset.deletion) {
                throw make_exception<InvalidRequestException>("Mutation must have one and only one of column_or_supercolumn or deletion");
            }
            auto&& cosc = m.column_or_supercolumn;
            if (cosc.__isset.column + cosc.__isset.super_column + cosc.__isset.counter_column + cosc.__isset.counter_super_column != 1) {
                throw make_exception<InvalidRequestException>("ColumnOrSuperColumn must have one (and only one) of column, super_column, counter and counter_super_column");
            }
            if (cosc.__isset.column) {
                add_to_mutation(s, cosc.column, m_to_apply);
            } else if (cosc.__isset.super_column) {
                fail(unimplemented::cause::SUPER);
            } else if (cosc.__isset.counter_column) {
                fail(unimplemented::cause::COUNTERS);
            } else if (cosc.__isset.counter_super_column) {
                fail(unimplemented::cause::SUPER);
            }
        } else if (m.__isset.deletion) {
            auto&& del = m.deletion;
            if (!del.__isset.timestamp) {
                fail(unimplemented::cause::COUNTERS);
            } else if (del.__isset.super_column) {
                fail(unimplemented::cause::SUPER);
            } else if (del.__isset.predicate) {
                apply_delete(s, del.predicate, del.timestamp, m_to_apply);
            } else {
                m_to_apply.partition().apply(tombstone(del.timestamp, gc_clock::now()));
            }
        } else {
            throw make_exception<InvalidRequestException>("Mutation must have either column or deletion");
        }
    }
    using mutation_map = std::map<std::string, std::map<std::string, std::vector<Mutation>>>;
    using mutation_map_by_cf = std::unordered_map<std::string, std::unordered_map<std::string, std::vector<Mutation>>>;
    static mutation_map_by_cf group_by_cf(mutation_map& m) {
        mutation_map_by_cf ret;
        for (auto&& key_cf : m) {
            for (auto&& cf_mutations : key_cf.second) {
                auto& mutations = ret[std::move(cf_mutations.first)][std::move(key_cf.first)];
                std::move(cf_mutations.second.begin(), cf_mutations.second.end(), std::back_inserter(mutations));
            }
        }
        return ret;
    }
    static std::pair<std::vector<mutation>, std::vector<schema_ptr>> prepare_mutations(database& db, const sstring& ks_name, const mutation_map& m) {
        std::vector<mutation> muts;
        std::vector<schema_ptr> schemas;
        auto m_by_cf = group_by_cf(const_cast<mutation_map&>(m));
        for (auto&& cf_key : m_by_cf) {
            auto schema = lookup_schema(db, ks_name, cf_key.first);
            schemas.emplace_back(schema);
            for (auto&& key_mutations : cf_key.second) {
                mutation m_to_apply(key_from_thrift(*schema, to_bytes_view(key_mutations.first)), schema);
                for (auto&& m : key_mutations.second) {
                    add_to_mutation(*schema, m, m_to_apply);
                }
                muts.emplace_back(std::move(m_to_apply));
            }
        }
        return {std::move(muts), std::move(schemas)};
    }
};

class handler_factory : public CassandraCobSvIfFactory {
    distributed<database>& _db;
    distributed<cql3::query_processor>& _query_processor;
public:
    explicit handler_factory(distributed<database>& db,
                             distributed<cql3::query_processor>& qp)
        : _db(db), _query_processor(qp) {}
    typedef CassandraCobSvIf Handler;
    virtual CassandraCobSvIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) {
        return new thrift_handler(_db, _query_processor);
    }
    virtual void releaseHandler(CassandraCobSvIf* handler) {
        delete handler;
    }
};

std::unique_ptr<CassandraCobSvIfFactory>
create_handler_factory(distributed<database>& db, distributed<cql3::query_processor>& qp) {
    return std::make_unique<handler_factory>(db, qp);
}
