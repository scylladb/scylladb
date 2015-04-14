#pragma once

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
    class prepared;
    class schema_change;
    class rows;
};

}
}
