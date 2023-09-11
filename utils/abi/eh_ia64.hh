
/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unwind.h>
#include <typeinfo>
#include <exception>

// This file defines structures/functions derived from the Itanium C++ ABI.
// Source: https://itanium-cxx-abi.github.io/cxx-abi/abi-eh.html

namespace utils {
namespace abi {

// __cxa_exception (exception object header), as defined in section 2.2.1
struct cxa_exception { 
    std::type_info* exceptionType;
    void (*exceptionDestructor)(void*); 
    void (*unexpectedHandler)();
    std::terminate_handler terminateHandler;
    cxa_exception* nextException;

    int handlerCount;
    int handlerSwitchValue;
    const char* actionRecord;
    const char* languageSpecificData;
    void* catchTemp;
    void* adjustedPtr;

    _Unwind_Exception unwindHeader;
};

// Given a pointer to the exception data, returns the pointer
// to the __cxa_exception header.
inline cxa_exception* get_cxa_exception(void* eptr) {
    // From section 2.2.1:
    // "By convention, a __cxa_exception pointer points at the C++ object
    // representing the exception being thrown, immediately following
    // the header. The header structure is accessed at a negative offset
    // from the __cxa_exception pointer."
    return reinterpret_cast<cxa_exception*>(eptr) - 1;
}

} // abi
} // utils
