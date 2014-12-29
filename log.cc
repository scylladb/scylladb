/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include <cxxabi.h>

namespace logging {

logger::logger(sstring name) : _name(std::move(name)) {
    g_registry.register_logger(this);
}

logger::logger(logger&& x) : _name(std::move(x._name)), _level(x._level) {
    g_registry.moved(&x, this);
}

logger::~logger() {
    g_registry.unregister_logger(this);
}

void
logger::really_do_log(log_level level, const char* fmt, stringer** s, size_t n) {
    const char* p = fmt;
    while (*p != '\0') {
        if (*p == '{' && *(p+1) == '}') {
            p += 2;
            if (n > 0) {
                (*s++)->append(std::cout);
                --n;
            } else {
                std::cout << "???";
            }
        } else {
            std::cout << *p++;
        }
    }
    std::cout << "\n";
}

void
registry::register_logger(logger* l) {
    _loggers[l->name()] = l;
}

void
registry::unregister_logger(logger* l) {
    _loggers.erase(l->name());
}

void
registry::moved(logger* from, logger* to) {
    _loggers[from->name()] = to;
}

sstring pretty_type_name(const std::type_info& ti) {
    int status;
    std::unique_ptr<char[], void (*)(void*)> result(
            abi::__cxa_demangle(ti.name(), 0, 0, &status), std::free);
    return result.get() ? result.get() : ti.name();
}

thread_local registry g_registry;

}
