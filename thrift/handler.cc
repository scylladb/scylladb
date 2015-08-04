/*
 * Copyright 2014 Cloudius Systems
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
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using namespace  ::org::apache::cassandra;

class unimplemented_exception : public std::exception {
public:
    virtual const char* what() const throw () override { return "sorry, not implemented"; }
};

void pass_unimplemented(const tcxx::function<void(::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    exn_cob(::apache::thrift::TDelayedException::delayException(unimplemented_exception()));
}

template <typename Ex, typename... Args>
Ex
make_exception(const char* fmt, Args&&... args) {
    Ex ex;
    ex.why = sprint(fmt, std::forward<Args>(args)...);
    return ex;
}

template <typename Ex, typename... Args>
void complete_with_exception(tcxx::function<void (::apache::thrift::TDelayedException* _throw)>&& exn_cob,
        const char* fmt, Args&&... args) {
    exn_cob(TDelayedException::delayException(make_exception<Ex>(fmt, std::forward<Args>(args)...)));
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
        } catch (std::exception& e) {
            // Unexpected exception, wrap it
            throw ::apache::thrift::TException(std::string("Internal server error: ") + e.what());
        } catch (...) {
            // Unexpected exception, wrap it, unfortunately without any info
            throw ::apache::thrift::TException("Internal server error");
        }
    }
};

template <typename... T>
void complete(future<T...>& fut,
        const tcxx::function<void (const T&...)>& cob,
        const tcxx::function<void (::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    try {
        apply(std::move(cob), fut.get());
    } catch (...) {
        delayed_exception_wrapper dew(std::current_exception());
        exn_cob(&dew);
    }
}

template <typename T>
void complete(future<foreign_ptr<lw_shared_ptr<T>>>& fut,
        const tcxx::function<void (const T&)>& cob,
        const tcxx::function<void (::apache::thrift::TDelayedException* _throw)>& exn_cob) {
    try {
        cob(*std::get<0>(fut.get()));
    } catch (...) {
        delayed_exception_wrapper dew(std::current_exception());
        exn_cob(&dew);
    }
}

std::string bytes_to_string(bytes_view v) {
    return { reinterpret_cast<const char*>(v.begin()), v.size() };
}

class thrift_handler : public CassandraCobSvIf {
    distributed<database>& _db;
    sstring _ks_name;
    sstring _cql_version;
public:
    explicit thrift_handler(distributed<database>& db) : _db(db) {}
    void login(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const AuthenticationRequest& auth_request) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void set_keyspace(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        if (!_db.local().has_keyspace(keyspace)) {
            complete_with_exception<InvalidRequestException>(std::move(exn_cob),
                "keyspace %s does not exist", keyspace);
        } else {
            _ks_name = keyspace;
            cob();
        }
    }

    void get(tcxx::function<void(ColumnOrSuperColumn const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel::type consistency_level) {
        ColumnOrSuperColumn _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void get_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        schema_ptr schema;
        try {
            schema = _db.local().find_schema(_ks_name, column_parent.column_family);
        } catch (...) {
            return complete_with_exception<InvalidRequestException>(std::move(exn_cob), "column family %s not found", column_parent.column_family);
        }
        auto pk = key_from_thrift(schema, to_bytes(key));
        auto dk = dht::global_partitioner().decorate_key(*schema, pk);
        auto shard = _db.local().shard_of(dk._token);

        auto do_get = [this,
                       dk = std::move(dk),
                       column_parent = std::move(column_parent),
                       predicate = std::move(predicate)] (database& db) {
            if (!column_parent.super_column.empty()) {
                throw unimplemented_exception();
            }
            auto& cf = lookup_column_family(_db.local(), _ks_name, column_parent.column_family);
            if (predicate.__isset.column_names) {
                throw unimplemented_exception();
            } else if (predicate.__isset.slice_range) {
              auto&& range = predicate.slice_range;
              return cf.find_row(dk, clustering_key::make_empty(*cf.schema())).then([&cf, range = std::move(range)] (column_family::const_row_ptr rw) {
                std::vector<ColumnOrSuperColumn> ret;
                if (rw) {
                    auto beg = cf.schema()->regular_begin();
                    if (!range.start.empty()) {
                        beg = cf.schema()->regular_lower_bound(to_bytes(range.start));
                    }
                    auto end = cf.schema()->regular_end();
                    if (!range.finish.empty()) {
                        end = cf.schema()->regular_upper_bound(to_bytes(range.finish));
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
        _db.invoke_on(shard, [do_get = std::move(do_get)] (database& db) {
            return do_get(db);
        }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)]
                         (future<foreign_ptr<lw_shared_ptr<std::vector<ColumnOrSuperColumn>>>> ret) {
            complete(ret, cob, exn_cob);
            return make_ready_future<>();
        });
    }

    void get_count(tcxx::function<void(int32_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void multiget_slice(tcxx::function<void(std::map<std::string, std::vector<ColumnOrSuperColumn> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        std::map<std::string, std::vector<ColumnOrSuperColumn> >  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
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
        if (_ks_name.empty()) {
            return complete_with_exception<InvalidRequestException>(std::move(exn_cob), "keyspace not set");
        }
        // Would like to use move_iterator below, but Mutation is filled with some const stuff.
        parallel_for_each(mutation_map.begin(), mutation_map.end(),
                [this] (std::pair<std::string, std::map<std::string, std::vector<Mutation>>> key_cf) {
            bytes thrift_key = to_bytes(key_cf.first);
            std::map<std::string, std::vector<Mutation>>& cf_mutations_map = key_cf.second;
            return parallel_for_each(
                    boost::make_move_iterator(cf_mutations_map.begin()),
                    boost::make_move_iterator(cf_mutations_map.end()),
                    [this, thrift_key] (std::pair<std::string, std::vector<Mutation>> cf_mutations) {
                sstring cf_name = cf_mutations.first;
                const std::vector<Mutation>& mutations = cf_mutations.second;
                auto& cf = lookup_column_family(_db.local(), _ks_name, cf_name);
                mutation m_to_apply(key_from_thrift(cf.schema(), thrift_key), cf.schema());
                auto empty_clustering_key = clustering_key::make_empty(*cf.schema());
                for (const Mutation& m : mutations) {
                    if (m.__isset.column_or_supercolumn) {
                        auto&& cosc = m.column_or_supercolumn;
                        if (cosc.__isset.column) {
                            auto&& col = cosc.column;
                            bytes cname = to_bytes(col.name);
                            auto def = cf.schema()->get_column_definition(cname);
                            if (!def) {
                                throw make_exception<InvalidRequestException>("column %s not found", col.name);
                            }
                            if (def->kind != column_kind::regular_column) {
                                throw make_exception<InvalidRequestException>("Column %s is not settable", col.name);
                            }
                            gc_clock::duration ttl;
                            if (col.__isset.ttl) {
                                ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::seconds(col.ttl));
                            }
                            if (ttl.count() <= 0) {
                                ttl = cf.schema()->default_time_to_live();
                            }
                            ttl_opt maybe_ttl;
                            if (ttl.count() > 0) {
                                maybe_ttl = ttl;
                            }
                            m_to_apply.set_clustered_cell(empty_clustering_key, *def,
                                atomic_cell::make_live(col.timestamp, to_bytes(col.value), maybe_ttl));
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
                return _db.invoke_on(shard, [this, cf_name, m = freeze(m_to_apply)] (database& db) {
                    return db.apply(m);
                });
            });
        }).then_wrapped([this, cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<> ret) {
            complete(ret, cob, exn_cob);
            return make_ready_future<>();
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
        std::map<std::string, std::vector<std::string> >  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_keyspaces(tcxx::function<void(std::vector<KsDef>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::vector<KsDef>  ret;
        for (auto&& ks : _db.local().keyspaces()) {
            ret.emplace_back(get_keyspace_definition(ks.second));
        }
        cob(ret);
    }

    void describe_cluster_name(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        cob("seastar");
    }

    void describe_version(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        cob("0.0.0");
    }

    void describe_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::vector<TokenRange>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_local_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::vector<TokenRange>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_token_map(tcxx::function<void(std::map<std::string, std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::map<std::string, std::string>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void describe_partitioner(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy paritioner");
    }

    void describe_snitch(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy snitch");
    }

    void describe_keyspace(tcxx::function<void(KsDef const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        try {
            auto& ks = _db.local().find_keyspace(keyspace);
            KsDef ret = get_keyspace_definition(ks);
            cob(ret);
        }
        catch (no_such_keyspace& nsk) {
            complete_with_exception<InvalidRequestException>(std::move(exn_cob),
                "keyspace %s does not exist", keyspace);
        }
    }

    void describe_splits(tcxx::function<void(std::vector<std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<std::string>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void trace_next_query(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy trace");
    }

    void describe_splits_ex(tcxx::function<void(std::vector<CfSplit>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<CfSplit>  _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_add_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_drop_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_add_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        std::string schema_id = "schema-id";  // FIXME: make meaningful
        if (_db.local().has_keyspace(ks_def.name)) {
            InvalidRequestException ire;
            ire.why = sprint("Keyspace %s already exists", ks_def.name);
            exn_cob(TDelayedException::delayException(ire));
        }

        std::vector<schema_ptr> cf_defs;
        cf_defs.reserve(ks_def.cf_defs.size());
        for (const CfDef& cf_def : ks_def.cf_defs) {
            std::vector<schema::column> partition_key;
            std::vector<schema::column> clustering_key;
            std::vector<schema::column> regular_columns;
            // FIXME: get this from comparator
            auto column_name_type = utf8_type;
            // FIXME: look at key_alias and key_validator first
            partition_key.push_back({"key", bytes_type});
            // FIXME: guess clustering keys
            for (const ColumnDef& col_def : cf_def.column_metadata) {
                // FIXME: look at all fields, not just name
                regular_columns.push_back({to_bytes(col_def.name), bytes_type});
            }
            auto id = utils::UUID_gen::get_time_UUID();
            auto s = make_lw_shared(schema(id, ks_def.name, cf_def.name,
                std::move(partition_key), std::move(clustering_key), std::move(regular_columns),
                std::vector<schema::column>(), column_name_type));
            cf_defs.push_back(s);
        }
        auto ksm = make_lw_shared<keyspace_metadata>(to_sstring(ks_def.name),
            to_sstring(ks_def.strategy_class),
            std::map<sstring, sstring>{ks_def.strategy_options.begin(), ks_def.strategy_options.end()},
            ks_def.durable_writes,
            std::move(cf_defs));
        service::get_local_migration_manager().announce_new_keyspace(service::get_storage_proxy(), ksm, false).then([schema_id = std::move(schema_id)] {
            return make_ready_future<std::string>(std::move(schema_id));
        }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<std::string> result) {
            complete(result, cob, exn_cob);
        });
    }

    void system_drop_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_update_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void system_update_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression, const ConsistencyLevel::type consistency) {
        print("warning: ignoring query %s\n", query);
        cob({});
#if 0
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
#endif
    }

    void prepare_cql_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void prepare_cql3_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_prepared_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void execute_prepared_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values, const ConsistencyLevel::type consistency) {
        CqlResult _return;
        // FIXME: implement
        return pass_unimplemented(exn_cob);
    }

    void set_cql_version(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& version) {
        _cql_version = version;
        cob();
    }

private:
    static sstring class_from_data_type(const data_type& dt) {
        static const std::unordered_map<sstring, sstring> types = {
            { "boolean", "BooleanType" },
            { "bytes", "BytesType" },
            { "double", "DoubleType" },
            { "int32", "Int32Type" },
            { "long", "LongType" },
            { "timestamp", "DateType" },
            { "timeuuid", "TimeUUIDType" },
            { "utf8", "UTF8Type" },
            { "uuid", "UUIDType" },
            // FIXME: missing types
        };
        auto it = types.find(dt->name());
        if (it == types.end()) {
            return sstring("<unknown> ") + dt->name();
        }
        return sstring("org.apache.cassandra.db.marshal.") + it->second;
    }
    static sstring class_from_compound_type(const compound_type<allow_prefixes::no>& ct) {
        if (ct.is_singular()) {
            return class_from_data_type(ct.types().front());
        }
        sstring type = "org.apache.cassandra.db.marshal.CompositeType(";
        for (auto& dt : ct.types()) {
            type += class_from_data_type(dt);
            if (&dt != &*ct.types().rbegin()) {
                type += ",";
            }
        }
        type += ")";
        return type;
    }
    static KsDef get_keyspace_definition(const keyspace& ks) {
        auto&& meta = ks.metadata();
        KsDef def;
        def.__set_name(meta->name());
        def.__set_strategy_class(meta->strategy_name());
        std::map<std::string, std::string> options(
            meta->strategy_options().begin(),
            meta->strategy_options().end());
        def.__set_strategy_options(options);
        std::vector<CfDef> cfs;
        for (auto&& cf : meta->cf_meta_data()) {
            // FIXME: skip cql3 column families
            auto&& s = cf.second;
            CfDef cf_def;
            cf_def.__set_keyspace(s->ks_name());
            cf_def.__set_name(s->cf_name());
            cf_def.__set_key_validation_class(class_from_compound_type(*s->partition_key_type()));
            if (s->clustering_key_size()) {
                cf_def.__set_comparator_type(class_from_compound_type(*s->clustering_key_type()));
            } else {
                cf_def.__set_comparator_type(class_from_data_type(s->regular_column_name_type()));
            }
            cf_def.__set_comment(s->comment());
            cf_def.__set_bloom_filter_fp_chance(s->bloom_filter_fp_chance());
            if (s->regular_columns_count()) {
                std::vector<ColumnDef> columns;
                for (auto&& c : s->regular_columns()) {
                    ColumnDef c_def;
                    c_def.__set_name(c.name_as_text());
                    c_def.__set_validation_class(class_from_data_type(c.type));
                    columns.emplace_back(std::move(c_def));
                }
                cf_def.__set_column_metadata(columns);
            }
            // FIXME: there are more fields that should be filled...
            cfs.emplace_back(std::move(cf_def));
        }
        def.__set_cf_defs(cfs);
        def.__set_durable_writes(meta->durable_writes());
        return std::move(def);
    }
    static column_family& lookup_column_family(database& db, const sstring& ks_name, const sstring& cf_name) {
        try {
            return db.find_column_family(ks_name, cf_name);
        } catch (std::out_of_range&) {
            throw make_exception<InvalidRequestException>("column family %s not found", cf_name);
        }
    }
    static partition_key key_from_thrift(schema_ptr s, bytes k) {
        if (s->partition_key_size() != 1) {
            fail(unimplemented::cause::THRIFT);
        }
        return partition_key::from_single_value(*s, std::move(k));
    }
};

class handler_factory : public CassandraCobSvIfFactory {
    distributed<database>& _db;
public:
    explicit handler_factory(distributed<database>& db) : _db(db) {}
    typedef CassandraCobSvIf Handler;
    virtual CassandraCobSvIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) {
        return new thrift_handler(_db);
    }
    virtual void releaseHandler(CassandraCobSvIf* handler) {
        delete handler;
    }
};

std::unique_ptr<CassandraCobSvIfFactory>
create_handler_factory(distributed<database>& db) {
    return std::make_unique<handler_factory>(db);
}
