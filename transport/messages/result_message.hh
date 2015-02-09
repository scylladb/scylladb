#ifndef TRANSPORT_MESSAGES_RESULT_MESSAGE_HH
#define TRANSPORT_MESSAGES_RESULT_MESSAGE_HH

namespace transport {

namespace messages {

// FIXME: stub
class result_message {
    virtual ~result_message() {}

    class void_message;
};

class result_message::void_message : public result_message {
};

}

}

#endif
