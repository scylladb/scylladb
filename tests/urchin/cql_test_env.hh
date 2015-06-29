/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <functional>
#include <vector>

#include "core/sstring.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"

#include "transport/messages/result_message_base.hh"
#include "cql3/query_options_fwd.hh"
#include "bytes.hh"
#include "schema.hh"

class database;

namespace cql3 {
    class query_processor;
}

class cql_test_env {
public:
    virtual ~cql_test_env() {};

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(const sstring& text) = 0;

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(
        const sstring& text, std::unique_ptr<cql3::query_options> qo) = 0;

    virtual future<bytes> prepare(sstring query) = 0;

    virtual future<::shared_ptr<transport::messages::result_message>> execute_prepared(
        bytes id, std::vector<bytes_opt> values) = 0;

    virtual future<> create_table(std::function<schema(const sstring&)> schema_maker) = 0;

    virtual future<> require_keyspace_exists(const sstring& ks_name) = 0;

    virtual future<> require_table_exists(const sstring& ks_name, const sstring& cf_name) = 0;

    virtual future<> require_column_has_value(
        const sstring& table_name,
        std::vector<boost::any> pk,
        std::vector<boost::any> ck,
        const sstring& column_name,
        boost::any expected) = 0;

    virtual future<> stop() = 0;

    virtual database& local_db() = 0;

    virtual cql3::query_processor& local_qp() = 0;
};

future<::shared_ptr<cql_test_env>> make_env_for_test();

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func);
