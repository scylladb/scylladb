#pragma once

#include "cql3/result_set.hh"

#include "transport/event.hh"

#include "core/shared_ptr.hh"
#include "core/sstring.hh"

namespace transport {

namespace messages {

class result_message {
public:
    class visitor;

    virtual ~result_message() {}

    virtual void accept(visitor&) = 0;

    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class schema_change;
    class rows;
};

class result_message::visitor {
public:
    virtual void visit(const result_message::void_message&) = 0;
    virtual void visit(const result_message::set_keyspace&) = 0;
    virtual void visit(const result_message::schema_change&) = 0;
    virtual void visit(const result_message::rows&) = 0;
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
    ::shared_ptr<cql3::result_set> _rs;
public:
    rows(::shared_ptr<cql3::result_set> rs) : _rs(std::move(rs)) {}

    const cql3::result_set& rs() const {
        return *_rs;
    }

    virtual void accept(result_message::visitor& v) override {
        v.visit(*this);
    }
};

}

}
