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

template <typename Func, typename T>
void
with_cob_dereference(tcxx::function<void (const T& ret)>&& cob,
        tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        Func&& func) {
    using ptr_type = foreign_ptr<lw_shared_ptr<T>>;
    // then_wrapped() terminates the fiber by calling one of the cob objects
    futurize<ptr_type>::apply(func).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<ptr_type> f) {
        try {
            cob(*f.get0());
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
public:
    explicit thrift_handler(distributed<database>& db, distributed<cql3::query_processor>& qp)
        : _db(db)
        , _query_processor(qp)
        , _query_state(service::client_state::for_external_thrift_calls())
    { }

    const sstring& current_keyspace() const {
        return _query_state.get_client_state().get_raw_keyspace();
    }

    void login(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const AuthenticationRequest& auth_request) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void set_keyspace(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            _query_state.get_client_state().set_keyspace(_db, keyspace);
        });
    }

    void get(tcxx::function<void(ColumnOrSuperColumn const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel::type consistency_level) {
        ColumnOrSuperColumn _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_cob_dereference(std::move(cob), std::move(exn_cob), [&] {
            schema_ptr schema;
            try {
                schema = _db.local().find_schema(current_keyspace(), column_parent.column_family);
            } catch (...) {
                throw make_exception<InvalidRequestException>("column family %s not found", column_parent.column_family);
            }
            auto pk = key_from_thrift(*schema, to_bytes_view(key));
            auto dk = dht::global_partitioner().decorate_key(*schema, pk);
            auto shard = _db.local().shard_of(dk._token);

            auto do_get = [this,
                           dk = std::move(dk),
                           column_parent = std::move(column_parent),
                           predicate = std::move(predicate)] (database& db) {
                if (!column_parent.super_column.empty()) {
                    throw unimplemented_exception();
                }
                auto& cf = lookup_column_family(_db.local(), current_keyspace(), column_parent.column_family);
                if (predicate.__isset.column_names) {
                    throw unimplemented_exception();
                } else if (predicate.__isset.slice_range) {
                  auto&& range = predicate.slice_range;
                    auto s = cf.schema();
                    return cf.find_row(s, dk, clustering_key::make_empty()).then(
                          [s, &cf, range = std::move(range)] (column_family::const_row_ptr rw) {
                    std::vector<ColumnOrSuperColumn> ret;
                    if (rw) {
                        auto beg = s->regular_begin();
                        if (!range.start.empty()) {
                            beg = s->regular_lower_bound(to_bytes(range.start));
                        }
                        auto end = s->regular_end();
                        if (!range.finish.empty()) {
                            end = s->regular_upper_bound(to_bytes(range.finish));
                        }
                        auto count = range.count;
                        // FIXME: force limit count?
                        while (beg != end && count--) {
                            const column_definition& def = range.reversed ? *--end : *beg++;
                            atomic_cell_view cell = (*rw).cell_at(def.id).as_atomic_cell();
                            if (def.is_atomic()) {
                                if (cell.is_live()) { // FIXME: we should actually use tombstone information from all levels
                                    Column col;
                                    col.__set_name(bytes_to_string(def.name()));
                                    col.__set_value(bytes_to_string(cell.value()));
                                    col.__set_timestamp(cell.timestamp());
                                    // FIXME: set ttl
                                    ColumnOrSuperColumn v;
                                    v.__set_column(std::move(col));
                                    ret.push_back(std::move(v));
                                };
                            }
                        }
                    }
                    return make_foreign(make_lw_shared(std::move(ret)));
                  });
                } else {
                    throw make_exception<InvalidRequestException>("empty SlicePredicate");
                }
            };
            return _db.invoke_on(shard, [do_get = std::move(do_get)] (database& db) {
                return do_get(db);
            });
        });
    }

    void get_count(tcxx::function<void(int32_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void multiget_slice(tcxx::function<void(std::map<std::string, std::vector<ColumnOrSuperColumn> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!column_parent.super_column.empty()) {
                fail(unimplemented::cause::SUPER);
            }
            auto schema = lookup_schema(_db.local(), current_keyspace(), column_parent.column_family);
            auto cmd = slice_pred_to_read_cmd(*schema, predicate);
            auto cell_limit = predicate.__isset.slice_range ? static_cast<uint32_t>(predicate.slice_range.count) : std::numeric_limits<uint32_t>::max();
            return service::get_local_storage_proxy().query(
                    schema,
                    cmd,
                    make_partition_ranges(*schema, keys),
                    cl_from_thrift(consistency_level)).then([schema, cmd, cell_limit](auto result) {
                return query::result_view::do_with(*result, [schema, cmd, cell_limit](query::result_view v) {
                    column_aggregator aggregator(*schema, cmd->slice, cell_limit);
                    v.consume(cmd->slice, aggregator);
                    return aggregator.release();
                });
            });
        });
    }

    void multiget_count(tcxx::function<void(std::map<std::string, int32_t>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        std::map<std::string, int32_t>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_range_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const SlicePredicate& predicate, const KeyRange& range, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_paged_slice(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family, const KeyRange& range, const std::string& start_column, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_indexed_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const IndexClause& index_clause, const SlicePredicate& column_predicate, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void insert(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void add(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const CounterColumn& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void cas(tcxx::function<void(CASResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const std::string& column_family, const std::vector<Column> & expected, const std::vector<Column> & updates, const ConsistencyLevel::type serial_consistency_level, const ConsistencyLevel::type commit_consistency_level) {
        CASResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void remove(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const int64_t timestamp, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void remove_counter(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& path, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (current_keyspace().empty()) {
                throw make_exception<InvalidRequestException>("keyspace not set");
            }
            // Would like to use move_iterator below, but Mutation is filled with some const stuff.
            return parallel_for_each(mutation_map.begin(), mutation_map.end(),
                    [this] (std::pair<std::string, std::map<std::string, std::vector<Mutation>>> key_cf) {
                bytes thrift_key = to_bytes(key_cf.first);
                std::map<std::string, std::vector<Mutation>>& cf_mutations_map = key_cf.second;
                return parallel_for_each(
                        boost::make_move_iterator(cf_mutations_map.begin()),
                        boost::make_move_iterator(cf_mutations_map.end()),
                        [this, thrift_key] (std::pair<std::string, std::vector<Mutation>> cf_mutations) {
                    sstring cf_name = cf_mutations.first;
                    const std::vector<Mutation>& mutations = cf_mutations.second;
                    auto schema = lookup_schema(_db.local(), current_keyspace(), cf_name);
                    mutation m_to_apply(key_from_thrift(*schema, thrift_key), schema);
                    auto empty_clustering_key = clustering_key::make_empty();
                    for (const Mutation& m : mutations) {
                        if (m.__isset.column_or_supercolumn) {
                            auto&& cosc = m.column_or_supercolumn;
                            if (cosc.__isset.column) {
                                auto&& col = cosc.column;
                                bytes cname = to_bytes(col.name);
                                auto def = schema->get_column_definition(cname);
                                if (!def) {
                                    throw make_exception<InvalidRequestException>("column %s not found", col.name);
                                }
                                if (def->kind != column_kind::regular_column) {
                                    throw make_exception<InvalidRequestException>("Column %s is not settable", col.name);
                                }
                                m_to_apply.set_clustered_cell(empty_clustering_key, *def,
                                    atomic_cell::make_live(col.timestamp, to_bytes(col.value), maybe_ttl(*schema, col)));
                            } else if (cosc.__isset.super_column) {
                                // FIXME: implement
                            } else if (cosc.__isset.counter_column) {
                                // FIXME: implement
                            } else if (cosc.__isset.counter_super_column) {
                                // FIXME: implement
                            } else {
                                throw make_exception<InvalidRequestException>("Empty ColumnOrSuperColumn");
                            }
                        } else if (m.__isset.deletion) {
                            // FIXME: implement
                            abort();
                        } else {
                            throw make_exception<InvalidRequestException>("Mutation must have either column or deletion");
                        }
                    }
                    auto shard = _db.local().shard_of(m_to_apply);
                    return _db.invoke_on(shard, [this, gs = global_schema_ptr(schema), cf_name, m = freeze(m_to_apply)] (database& db) {
                        return db.apply(gs, m);
                    });
                });
            });
        });
    }

    void atomic_batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void truncate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfname) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_multi_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const MultiSliceRequest& request) {
        std::vector<ColumnOrSuperColumn>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
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
        cob("20.1.0");
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
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            if (!_db.local().has_keyspace(cf_def.keyspace)) {
                throw NotFoundException();
            }
            if (_db.local().has_schema(cf_def.keyspace, cf_def.name)) {
                throw make_exception<InvalidRequestException>("Column family %s already exists", cf_def.name);
            }

            auto s = schema_from_thrift(cf_def, cf_def.keyspace);
            return service::get_local_migration_manager().announce_new_column_family(std::move(s), false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
            });
        });
    }

    void system_drop_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            _db.local().find_schema(current_keyspace(), column_family); // Throws if column family doesn't exist.
            return service::get_local_migration_manager().announce_column_family_drop(current_keyspace(), column_family, false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
            });
        });
    }

    void system_add_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto ksm = keyspace_from_thrift(ks_def);
            return service::get_local_migration_manager().announce_new_keyspace(std::move(ksm), false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
            });
        });
    }

    void system_drop_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            thrift_validation::validate_keyspace_not_system(keyspace);
            if (!_db.local().has_keyspace(keyspace)) {
                throw NotFoundException();
            }

            return service::get_local_migration_manager().announce_keyspace_drop(keyspace, false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
            });
        });
    }

    void system_update_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            thrift_validation::validate_keyspace_not_system(ks_def.name);

            if (!_db.local().has_keyspace(ks_def.name)) {
                throw NotFoundException();
            }
            if (!ks_def.cf_defs.empty()) {
                throw make_exception<InvalidRequestException>("Keyspace update must not contain any column family definitions.");
            }

            auto ksm = keyspace_from_thrift(ks_def);
            return service::get_local_migration_manager().announce_keyspace_update(ksm, false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
            });
        });
    }

    void system_update_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        return with_cob(std::move(cob), std::move(exn_cob), [&] {
            auto cf = _db.local().find_schema(cf_def.keyspace, cf_def.name);

            // FIXME: don't update a non thrift-compatible CQL3 table.

            auto s = schema_from_thrift(cf_def, cf_def.keyspace, cf->id());
            return service::get_local_migration_manager().announce_column_family_update(std::move(s), true, false).then([this] {
                return std::string(_db.local().get_version().to_sstring());
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
        return with_exn_cob(std::move(exn_cob), [&] {
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
        return with_exn_cob(std::move(exn_cob), [&] {
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
        return with_exn_cob(std::move(exn_cob), [&] {
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
            // FIXME: skip cql3 column families
            auto&& s = cf.second;
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
            if (s->regular_columns_count()) {
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
                    builder.with_column(to_bytes("key" + (i + 1)), std::move(pk_types[i]), column_kind::partition_key);
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
                builder.with_column(to_bytes("column" + (i + 1)), std::move(ck_types[i]), column_kind::clustering_key);
            }
            auto&& vtype = cf_def.__isset.default_validation_class
                         ? db::marshal::type_parser::parse(to_sstring(cf_def.default_validation_class))
                         : bytes_type;
            builder.with_column(to_bytes("value"), std::move(vtype));
        } else {
            // Static CF
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
    static column_family& lookup_column_family(database& db, const sstring& ks_name, const sstring& cf_name) {
        if (ks_name.empty()) {
            throw make_exception<InvalidRequestException>("keyspace not set");
        }
        try {
            return db.find_column_family(ks_name, cf_name);
        } catch (no_such_column_family&) {
            throw make_exception<InvalidRequestException>("column family %s not found", cf_name);
        }
    }
    static schema_ptr lookup_schema(database& db, const sstring& ks_name, const sstring& cf_name) {
        return lookup_column_family(db, ks_name, cf_name).schema();
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
            auto pk = key_from_thrift(s, to_bytes(key));
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
