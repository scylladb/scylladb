
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <concepts>

#include "cql3/result_set.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_options.hh"

#include "transport/messages/result_message_base.hh"
#include "transport/event.hh"
#include "exceptions/coordinator_result.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

namespace cql_transport {

namespace messages {

class result_message::prepared : public result_message {
private:
    cql3::statements::prepared_statement::checked_weak_ptr _prepared;
    cql3::prepared_metadata _metadata;
    ::shared_ptr<const cql3::metadata> _result_metadata;
protected:
    prepared(cql3::statements::prepared_statement::checked_weak_ptr prepared, bool support_lwt_opt);
public:
    cql3::statements::prepared_statement::checked_weak_ptr& get_prepared() {
        return _prepared;
    }

    const cql3::prepared_metadata& metadata() const {
        return _metadata;
    }

    ::shared_ptr<const cql3::metadata> result_metadata() const {
        return _result_metadata;
    }

    class cql;
private:
    static ::shared_ptr<const cql3::metadata> extract_result_metadata(::shared_ptr<cql3::cql_statement> statement);
};

class result_message::visitor {
public:
    virtual void visit(const result_message::void_message&) = 0;
    virtual void visit(const result_message::set_keyspace&) = 0;
    virtual void visit(const result_message::prepared::cql&) = 0;
    virtual void visit(const result_message::schema_change&) = 0;
    virtual void visit(const result_message::rows&) = 0;
    virtual void visit(const result_message::bounce_to_shard&) = 0;
    virtual void visit(const result_message::exception&) = 0;
};

class result_message::visitor_base : public visitor {
public:
    void visit(const result_message::void_message&) override {};
    void visit(const result_message::set_keyspace&) override {};
    void visit(const result_message::prepared::cql&) override {};
    void visit(const result_message::schema_change&) override {};
    void visit(const result_message::rows&) override {};
    void visit(const result_message::bounce_to_shard&) override { SCYLLA_ASSERT(false); };
    void visit(const result_message::exception&) override;
};

class result_message::void_message : public result_message {
public:
    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::void_message& msg);

// This result is handled internally and should never be returned
// to a client. Any visitor should abort while handling it since
// it is a sure sign of a error.
class result_message::bounce_to_shard : public result_message {
    unsigned _shard;
    cql3::computed_function_values _cached_fn_calls;
public:
    bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls)
        : _shard(shard), _cached_fn_calls(std::move(cached_fn_calls))
    {}
    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
    virtual std::optional<unsigned> move_to_shard() const override {
        return _shard;
    }

    cql3::computed_function_values&& take_cached_pk_function_calls() {
        return std::move(_cached_fn_calls);
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::bounce_to_shard& msg);

// This result is handled internally. It can be used to indicate an exception
// which needs to be handled without involving the C++ exception machinery,
// e.g. when a rate limit is reached.
class result_message::exception : public result_message {
private:
    exceptions::coordinator_exception_container _ex;
public:
    exception(exceptions::coordinator_exception_container ex) : _ex(std::move(ex)) {}
    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
    [[noreturn]] void throw_me() const {
        _ex.throw_me();
    }
    const exceptions::coordinator_exception_container& get_exception() const & {
        return _ex;
    }
    exceptions::coordinator_exception_container&& get_exception() && {
        return std::move(_ex);
    }

    virtual bool is_exception() const override {
        return true;
    }

    virtual void throw_if_exception() const override {
        throw_me();
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::exception& msg);

class result_message::set_keyspace : public result_message {
private:
    sstring _keyspace;
public:
    set_keyspace(const sstring& keyspace)
        : _keyspace{keyspace}
    { }

    const sstring& get_keyspace() const {
        return _keyspace;
    }

    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::set_keyspace& msg);

class result_message::prepared::cql : public result_message::prepared {
    bytes _id;
public:
    cql(const bytes& id, cql3::statements::prepared_statement::checked_weak_ptr p, bool support_lwt_opt)
        : result_message::prepared(std::move(p), support_lwt_opt)
        , _id{id}
    { }

    const bytes& get_id() const {
        return _id;
    }

    static const bytes& get_id(::shared_ptr<cql_transport::messages::result_message::prepared> prepared) {
        auto msg_cql = dynamic_pointer_cast<const messages::result_message::prepared::cql>(prepared);
        if (msg_cql == nullptr) {
            throw std::bad_cast();
        }
        return msg_cql->get_id();
    }

    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::prepared::cql& msg);


class result_message::schema_change : public result_message {
private:
    shared_ptr<event::schema_change> _change;
public:
    schema_change(shared_ptr<event::schema_change> change)
        : _change{change}
    { }

    shared_ptr<event::schema_change> get_change() const {
        return _change;
    }

    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
};

std::ostream& operator<<(std::ostream& os, const result_message::schema_change& msg);

class result_message::rows : public result_message {
private:
    cql3::result _result;
public:
    rows(cql3::result rs) : _result(std::move(rs)) {}

    const cql3::result& rs() const {
        return _result;
    }

    virtual void accept(result_message::visitor& v) const override {
        v.visit(*this);
    }
};


template<typename ResultMessagePtr>
requires requires (ResultMessagePtr ptr) {
    { ptr.operator->() } -> std::convertible_to<result_message*>;
    { ptr.get() }        -> std::convertible_to<result_message*>;
}
inline future<ResultMessagePtr> propagate_exception_as_future(ResultMessagePtr&& ptr) {
    if (!ptr.get() || !ptr->is_exception()) {
        return make_ready_future<ResultMessagePtr>(std::move(ptr));
    }
    auto eptr = dynamic_cast<result_message::exception*>(ptr.get());
    return std::move(*eptr).get_exception().into_exception_future<ResultMessagePtr>();
}

}

}

template <>
struct fmt::formatter<cql_transport::messages::result_message::rows> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql_transport::messages::result_message::rows&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
