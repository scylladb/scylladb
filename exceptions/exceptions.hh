#ifndef EXCEPTIONS_HH
#define EXCEPTIONS_HH

#include <stdexcept>

namespace exceptions {

class invalid_request_exception : public std::logic_error {
public:
    invalid_request_exception(std::string cause)
        : logic_error(cause)
    { }
};

class keyspace_not_defined_exception : public invalid_request_exception {
public:
    keyspace_not_defined_exception(std::string cause)
        : invalid_request_exception(cause)
    { }
};

class marshal_exception : public std::logic_error {
public:
    marshal_exception(std::string cause)
        : logic_error(cause)
    { }
};

}

#endif
