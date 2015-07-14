/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include <cxxabi.h>
#include <system_error>
#include <boost/range/adaptor/map.hpp>

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
registry::set_all_loggers_level(log_level level) {
    for (auto&& l : _loggers | boost::adaptors::map_values) {
        l->set_level(level);
    }
}

log_level
registry::get_logger_level(sstring name) const {
    return _loggers.at(name)->level();
}

void
registry::set_logger_level(sstring name, log_level level) {
    _loggers.at(name)->set_level(level);
}

std::vector<sstring>
registry::get_all_logger_names() {
    auto ret = _loggers | boost::adaptors::map_keys;
    return std::vector<sstring>(ret.begin(), ret.end());
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

std::ostream& operator<<(std::ostream&out, std::exception_ptr eptr) {
    if (!eptr) {
        out << "<no exception>";
        return out;
    }
    try {
        std::rethrow_exception(eptr);
    } catch(...) {
        auto tp = abi::__cxa_current_exception_type();
        if (tp) {
            out << logging::pretty_type_name(*tp);
        } else {
            // This case shouldn't happen...
            out << "<unknown exception>";
        }
        // Print more information on some familiar exception types
        try {
            throw;
        } catch(const std::system_error &e) {
            out << " (error " << e.code() << ", " << e.code().message() << ")";
        } catch(const std::exception& e) {
            out << " (" << e.what() << ")";
        }
    }
    return out;
}
