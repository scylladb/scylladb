#ifndef EXCEPTIONS_HH
#define EXCEPTIONS_HH

#include <exception>

namespace exceptions {

class invalid_request_exception : public std::logic_error {
public:
    invalid_request_exception(std::string cause)
        : logic_error(cause)
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
