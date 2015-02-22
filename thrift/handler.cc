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
#include <thrift/protocol/TBinaryProtocol.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using namespace  ::org::apache::cassandra;

class unimplemented_exception : public std::exception {
public:
    virtual const char* what() const throw () override { return "sorry, not implemented"; }
};

void unimplemented(const tcxx::function<void(::apache::thrift::TDelayedException* _throw)>& exn_cob) {
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
        std::rethrow_exception(std::move(_ex));
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

class CassandraAsyncHandler : public CassandraCobSvIf {
    distributed<database>& _db;
    keyspace* _ks = nullptr;  // FIXME: reference counting for in-use detection?
    sstring _ks_name;
    sstring _cql_version;
public:
    explicit CassandraAsyncHandler(distributed<database>& db) : _db(db) {}
    void login(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const AuthenticationRequest& auth_request) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void set_keyspace(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        try {
            _ks = &_db.local().keyspaces.at(keyspace);
            _ks_name = keyspace;
            cob();
        } catch (std::out_of_range& e) {
            return complete_with_exception<InvalidRequestException>(std::move(exn_cob),
                    "keyspace %s does not exist", keyspace);
        }
    }

    void get(tcxx::function<void(ColumnOrSuperColumn const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel::type consistency_level) {
        ColumnOrSuperColumn _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void get_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        try {
            std::vector<ColumnOrSuperColumn> ret;
            auto keyb = to_bytes(key);
            if (!column_parent.super_column.empty()) {
                throw unimplemented_exception();
            }
            auto& cf = lookup_column_family(column_parent.column_family);
            if (predicate.__isset.column_names) {
                throw unimplemented_exception();
            } else if (predicate.__isset.slice_range) {
                auto&& range = predicate.slice_range;
                row* rw = cf.find_row(keyb, bytes());
                if (rw) {
                    auto beg = cf._schema->regular_begin();
                    if (!range.start.empty()) {
                        beg = cf._schema->regular_lower_bound(to_bytes(range.start));
                    }
                    auto end = cf._schema->regular_end();
                    if (!range.finish.empty()) {
                        end = cf._schema->regular_upper_bound(to_bytes(range.finish));
                    }
                    auto count = range.count;
                    // FIXME: force limit count?
                    while (beg != end && count--) {
                        column_definition& def = range.reversed ? *--end : *beg++;
                        if (def.is_atomic()) {
                            const auto& cell = boost::any_cast<atomic_cell&>((*rw)[def.id]);
                            if (cell.is_live()) { // FIXME: we should actually use tombstone information from all levels
                                Column col;
                                col.__set_name(def.name());
                                col.__set_value(cell.as_live().value);
                                col.__set_timestamp(cell.timestamp);
                                // FIXME: set ttl
                                ColumnOrSuperColumn v;
                                v.__set_column(std::move(col));
                                ret.push_back(std::move(v));
                            };
                        }
                    }
                }
            } else {
                throw make_exception<InvalidRequestException>("empty SlicePredicate");
            }
            cob(std::move(ret));
        } catch (InvalidRequestException& ex) {
            exn_cob(TDelayedException::delayException(ex));
        } catch (std::exception& ex) {
            exn_cob(TDelayedException::delayException(ex));
        }
    }

    void get_count(tcxx::function<void(int32_t const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void multiget_slice(tcxx::function<void(std::map<std::string, std::vector<ColumnOrSuperColumn> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        std::map<std::string, std::vector<ColumnOrSuperColumn> >  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void multiget_count(tcxx::function<void(std::map<std::string, int32_t>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel::type consistency_level) {
        std::map<std::string, int32_t>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void get_range_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const SlicePredicate& predicate, const KeyRange& range, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void get_paged_slice(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family, const KeyRange& range, const std::string& start_column, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void get_indexed_slices(tcxx::function<void(std::vector<KeySlice>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const ColumnParent& column_parent, const IndexClause& index_clause, const SlicePredicate& column_predicate, const ConsistencyLevel::type consistency_level) {
        std::vector<KeySlice>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void insert(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void add(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnParent& column_parent, const CounterColumn& column, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void cas(tcxx::function<void(CASResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const std::string& column_family, const std::vector<Column> & expected, const std::vector<Column> & updates, const ConsistencyLevel::type serial_consistency_level, const ConsistencyLevel::type commit_consistency_level) {
        CASResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void remove(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& column_path, const int64_t timestamp, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void remove_counter(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& key, const ColumnPath& path, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        if (!_ks) {
            return complete_with_exception<InvalidRequestException>(std::move(exn_cob), "keyspace not set");
        }
        static bytes null_clustering_key = to_bytes("");
        // Would like to use move_iterator below, but Mutation is filled with some const stuff.
        parallel_for_each(mutation_map.begin(), mutation_map.end(),
                [this] (std::pair<std::string, std::map<std::string, std::vector<Mutation>>> key_cf) {
            bytes key = to_bytes(key_cf.first);
            std::map<std::string, std::vector<Mutation>>& cf_mutations_map = key_cf.second;
            return parallel_for_each(
                    boost::make_move_iterator(cf_mutations_map.begin()),
                    boost::make_move_iterator(cf_mutations_map.end()),
                    [this, key] (std::pair<std::string, std::vector<Mutation>> cf_mutations) {
                sstring cf_name = cf_mutations.first;
                const std::vector<Mutation>& mutations = cf_mutations.second;
                auto& cf = lookup_column_family(cf_name);
                mutation m_to_apply(key, cf._schema);
                for (const Mutation& m : mutations) {
                    if (m.__isset.column_or_supercolumn) {
                        auto&& cosc = m.column_or_supercolumn;
                        if (cosc.__isset.column) {
                            auto&& col = cosc.column;
                            bytes cname = to_bytes(col.name);
                            auto def = cf._schema->get_column_definition(cname);
                            if (!def) {
                                throw make_exception<InvalidRequestException>("column %s not found", col.name);
                            }
                            if (def->kind != column_definition::column_kind::REGULAR) {
                                throw make_exception<InvalidRequestException>("Column %s is not settable", col.name);
                            }
                            gc_clock::duration ttl;
                            if (col.__isset.ttl) {
                                ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::seconds(col.ttl));
                            }
                            if (ttl.count() <= 0) {
                                ttl = cf._schema->default_time_to_live;
                            }
                            auto ttl_option = ttl.count() > 0 ? ttl_opt(gc_clock::now() + ttl) : ttl_opt();
                            m_to_apply.set_clustered_cell(null_clustering_key, *def,
                                atomic_cell{col.timestamp, atomic_cell::live{ttl_option, to_bytes(col.value)}});
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
                return _db.invoke_on_all([this, cf_name, m_to_apply = std::move(m_to_apply)] (database& db) {
                    auto& ks = db.keyspaces.at(_ks_name);
                    auto& cf = ks.column_families.at(cf_name);
                    cf.apply(std::move(m_to_apply));
                });
            });
        }).then_wrapped([this, cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<> ret) {
            complete(ret, cob, exn_cob);
            return make_ready_future<>();
        });
    }

    void atomic_batch_mutate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel::type consistency_level) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void truncate(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfname) {
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void get_multi_slice(tcxx::function<void(std::vector<ColumnOrSuperColumn>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const MultiSliceRequest& request) {
        std::vector<ColumnOrSuperColumn>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void describe_schema_versions(tcxx::function<void(std::map<std::string, std::vector<std::string> >  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::map<std::string, std::vector<std::string> >  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void describe_keyspaces(tcxx::function<void(std::vector<KsDef>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::vector<KsDef>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
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
        return unimplemented(exn_cob);
    }

    void describe_local_ring(tcxx::function<void(std::vector<TokenRange>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::vector<TokenRange>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void describe_token_map(tcxx::function<void(std::map<std::string, std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob) {
        std::map<std::string, std::string>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
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
        KsDef _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void describe_splits(tcxx::function<void(std::vector<std::string>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<std::string>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void trace_next_query(tcxx::function<void(std::string const& _return)> cob) {
        std::string _return;
        // FIXME: implement
        return cob("dummy trace");
    }

    void describe_splits_ex(tcxx::function<void(std::vector<CfSplit>  const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) {
        std::vector<CfSplit>  _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void system_add_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void system_drop_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& column_family) {
        std::string _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void system_add_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        std::string schema_id = "schema-id";  // FIXME: make meaningful
        if (_db.local().keyspaces.count(ks_def.name)) {
            InvalidRequestException ire;
            ire.why = sprint("Keyspace %s already exists", ks_def.name);
            exn_cob(TDelayedException::delayException(ire));
        }
        _db.invoke_on_all([this, ks_def = std::move(ks_def)] (database& db) {
            keyspace& ks = db.keyspaces[ks_def.name];
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
                auto s = make_lw_shared<schema>(ks_def.name, cf_def.name,
                    std::move(partition_key), std::move(clustering_key), std::move(regular_columns), column_name_type);
                column_family cf(s);
                ks.column_families.emplace(cf_def.name, std::move(cf));
            }
        }).then([schema_id = std::move(schema_id)] {
            return make_ready_future<std::string>(std::move(schema_id));
        }).then_wrapped([cob = std::move(cob), exn_cob = std::move(exn_cob)] (future<std::string> result) {
            complete(result, cob, exn_cob);
            return make_ready_future<>();
        });
    }

    void system_drop_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& keyspace) {
        std::string _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void system_update_keyspace(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const KsDef& ks_def) {
        std::string _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void system_update_column_family(tcxx::function<void(std::string const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const CfDef& cf_def) {
        std::string _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void execute_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void execute_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression, const ConsistencyLevel::type consistency) {
        print("warning: ignoring query %s\n", query);
        cob({});
#if 0
        CqlResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
#endif
    }

    void prepare_cql_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void prepare_cql3_query(tcxx::function<void(CqlPreparedResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& query, const Compression::type compression) {
        CqlPreparedResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void execute_prepared_cql_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values) {
        CqlResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void execute_prepared_cql3_query(tcxx::function<void(CqlResult const& _return)> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t itemId, const std::vector<std::string> & values, const ConsistencyLevel::type consistency) {
        CqlResult _return;
        // FIXME: implement
        return unimplemented(exn_cob);
    }

    void set_cql_version(tcxx::function<void()> cob, tcxx::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const std::string& version) {
        _cql_version = version;
        cob();
    }

private:
    column_family& lookup_column_family(const sstring& cf_name) {
        try {
            return _ks->column_families.at(cf_name);
        } catch (std::out_of_range&) {
            throw make_exception<InvalidRequestException>("column family %s not found", cf_name);
        }
    }
};

class handler_factory : public CassandraCobSvIfFactory {
    distributed<database>& _db;
public:
    explicit handler_factory(distributed<database>& db) : _db(db) {}
    typedef CassandraCobSvIf Handler;
    virtual CassandraCobSvIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) {
        return new CassandraAsyncHandler(_db);
    }
    virtual void releaseHandler(CassandraCobSvIf* handler) {
        delete handler;
    }
};

std::unique_ptr<CassandraCobSvIfFactory>
create_handler_factory(distributed<database>& db) {
    return std::make_unique<handler_factory>(db);
}
