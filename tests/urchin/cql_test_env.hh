/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "core/shared_ptr.hh"

class cql_test_env {
public:
    static auto constexpr ks_name = "ks";
private:
    ::shared_ptr<distributed<database>> _db;
    ::shared_ptr<distributed<cql3::query_processor>> _qp;
    ::shared_ptr<service::storage_proxy> _proxy;
private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state()
                : client_state(service::client_state::for_internal_calls()) {
            client_state.set_keyspace(ks_name);
        }

        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<core_local_state> _core_local;
private:
    auto make_query_state() {
        return ::make_shared<service::query_state>(_core_local.local().client_state);
    }
public:
    cql_test_env(::shared_ptr<distributed<database>> db, ::shared_ptr<distributed<cql3::query_processor>> qp,
            ::shared_ptr<service::storage_proxy> proxy)
        : _db(db)
        , _qp(qp)
        , _proxy(proxy)
    { }

    auto execute_cql(const sstring& text) {
        auto qs = make_query_state();
        return _qp->local().process(text, *qs, cql3::query_options::DEFAULT).finally([qs] {});
    }

    future<bytes> prepare(sstring query) {
        return _qp->invoke_on_all([query, this] (auto& local_qp) {
            auto qs = this->make_query_state();
            return local_qp.prepare(query, *qs).finally([qs] {}).discard_result();
        }).then([query, this] {
            return _qp->local().compute_id(query, ks_name);
        });
    }

    auto execute_prepared(bytes id, std::vector<bytes_opt> values) {
        auto prepared = _qp->local().get_prepared(id);
        assert(bool(prepared));
        auto stmt = prepared->statement;
        assert(stmt->get_bound_terms() == values.size());

        int32_t protocol_version = 3;
        auto options = ::make_shared<cql3::default_query_options>(db::consistency_level::ONE, std::move(values), false,
            cql3::query_options::specific_options::DEFAULT, protocol_version, serialization_format::use_32_bit());
        options->prepare(prepared->bound_names);

        auto qs = make_query_state();
        return _qp->local().process_statement(stmt, *qs, *options)
            .finally([options, qs] {});
    }

    template <typename SchemaMaker>
    future<> create_table(SchemaMaker schema_maker) {
        return _db->invoke_on_all([schema_maker, this] (database& db) {
            auto cf_schema = make_lw_shared(schema_maker(ks_name));
            auto& ks = db.find_or_create_keyspace(ks_name);
            ks.column_families.emplace(cf_schema->cf_name, column_family(cf_schema));
        });
    }

    future<> start() {
        return _core_local.start();
    }

    future<> stop() {
        return _core_local.stop().then([this] {
            return _qp->stop().then([this] {
                return _db->stop();
            });
        });
    }
};

static inline
future<::shared_ptr<cql_test_env>> make_env_for_test() {
    auto db = ::make_shared<distributed<database>>();
    return db->start().then([db] {
        engine().at_exit([db] { return db->stop(); });
        auto proxy = ::make_shared<service::storage_proxy>(std::ref(*db));
        auto qp = ::make_shared<distributed<cql3::query_processor>>();
        return qp->start(std::ref(*proxy), std::ref(*db)).then([db, proxy, qp] {
            engine().at_exit([qp] { return qp->stop(); });
            auto env = ::make_shared<cql_test_env>(db, qp, proxy);
            return env->start().then([env] {
                engine().at_exit([env] { return env->stop(); });
                return env;
            });
        });
    });
}

template <typename Func>
static inline
future<> do_with_cql_env(Func&& func) {
    return make_env_for_test().then([func = std::forward<Func>(func)] (auto e) {
        return func(*e).finally([e] {
            return e->stop().finally([e] {});
        });
    });
}
