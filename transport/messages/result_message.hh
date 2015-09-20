
/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "cql3/result_set.hh"
#include "cql3/statements/parsed_statement.hh"

#include "transport/messages/result_message_base.hh"
#include "transport/event.hh"

#include "core/shared_ptr.hh"
#include "core/sstring.hh"

namespace transport {

namespace messages {

class result_message::visitor {
public:
    virtual void visit(const result_message::void_message&) = 0;
    virtual void visit(const result_message::set_keyspace&) = 0;
    virtual void visit(const result_message::prepared&) = 0;
    virtual void visit(const result_message::schema_change&) = 0;
    virtual void visit(const result_message::rows&) = 0;
};

class result_message::visitor_base : public visitor {
public:
    void visit(const result_message::void_message&) override {};
    void visit(const result_message::set_keyspace&) override {};
    void visit(const result_message::prepared&) override {};
    void visit(const result_message::schema_change&) override {};
    void visit(const result_message::rows&) override {};
};

class result_message::void_message : public result_message {
public:
    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

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

    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

class result_message::prepared : public result_message {
private:
    bytes _id;
   ::shared_ptr<cql3::statements::parsed_statement::prepared> _prepared;
public:
    prepared(const bytes& id, ::shared_ptr<cql3::statements::parsed_statement::prepared> prepared)
        : _id{id}
        , _prepared{prepared}
    { }

    const bytes& get_id() const {
        return _id;
    }

    const ::shared_ptr<cql3::statements::parsed_statement::prepared>& get_prepared() const {
        return _prepared;
    }

    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

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

    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

class result_message::rows : public result_message {
private:
    std::unique_ptr<cql3::result_set> _rs;
public:
    rows(std::unique_ptr<cql3::result_set> rs) : _rs(std::move(rs)) {}

    const cql3::result_set& rs() const {
        return *_rs;
    }

    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

}

}
