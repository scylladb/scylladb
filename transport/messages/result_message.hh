#ifndef TRANSPORT_MESSAGES_RESULT_MESSAGE_HH
#define TRANSPORT_MESSAGES_RESULT_MESSAGE_HH

namespace transport {

namespace messages {

// FIXME: stub
class result_message {
public:
    virtual ~result_message() {}

    class void_message;
    class set_keyspace;
};

class result_message::void_message : public result_message {
};

class result_message::set_keyspace : public result_message {
private:
    sstring _keyspace;
public:
    set_keyspace(const sstring& keyspace)
        : _keyspace{keyspace}
    { }
};

}

}

#endif
