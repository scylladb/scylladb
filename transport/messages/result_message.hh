#ifndef TRANSPORT_MESSAGES_RESULT_MESSAGE_HH
#define TRANSPORT_MESSAGES_RESULT_MESSAGE_HH

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
};

class result_message::visitor {
public:
    virtual void visit(const result_message::void_message&) = 0;
    virtual void visit(const result_message::set_keyspace&) = 0;
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

}

}

#endif
