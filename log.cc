/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "core/array_map.hh"
#include <cxxabi.h>
#include <system_error>
#include <boost/range/adaptor/map.hpp>
#include <map>
#include <syslog.h>
#include <seastar/core/reactor.hh>

namespace logging {

registry& logger_registry();

const std::map<log_level, sstring> log_level_names = {
        { log_level::trace, "trace" },
        { log_level::debug, "debug" },
        { log_level::info, "info" },
        { log_level::warn, "warn" },
        { log_level::error, "error" },
};

}

using namespace logging;

std::ostream& operator<<(std::ostream& out, log_level level) {
    return out << log_level_names.at(level);
}

std::istream& operator>>(std::istream& in, log_level& level) {
    sstring s;
    in >> s;
    if (!in) {
        return in;
    }
    for (auto&& x : log_level_names) {
        if (s == x.second) {
            level = x.first;
            return in;
        }
    }
    in.setstate(std::ios::failbit);
    return in;
}

namespace logging {

std::atomic<bool> logger::_stdout = { true };
std::atomic<bool> logger::_syslog = { false };

logger::logger(sstring name) : _name(std::move(name)) {
    logger_registry().register_logger(this);
}

logger::logger(logger&& x) : _name(std::move(x._name)), _level(x._level.load(std::memory_order_relaxed)) {
    logger_registry().moved(&x, this);
}

logger::~logger() {
    logger_registry().unregister_logger(this);
}

void
logger::really_do_log(log_level level, const char* fmt, stringer** s, size_t n) {
    std::ostringstream out;
    out << " [shard " << engine().cpu_id() << "] " << _name << " - ";
    const char* p = fmt;
    while (*p != '\0') {
        if (*p == '{' && *(p+1) == '}') {
            p += 2;
            if (n > 0) {
                (*s++)->append(out);
                --n;
            } else {
                out << "???";
            }
        } else {
            out << *p++;
        }
    }
    out << "\n";
    auto msg = out.str();
    if (_stdout.load(std::memory_order_relaxed)) {
        static array_map<sstring, 20> level_map = {
                { int(log_level::debug), "DEBUG" },
                { int(log_level::info),  "INFO "  },
                { int(log_level::trace), "TRACE" },
                { int(log_level::warn),  "WARN "  },
                { int(log_level::error), "ERROR" },
        };
        std::cout << level_map[int(level)] << "  " << msg;
    }
    if (_syslog.load(std::memory_order_relaxed)) {
        static array_map<int, 20> level_map = {
                { int(log_level::debug), LOG_DEBUG },
                { int(log_level::info), LOG_INFO },
                { int(log_level::trace), LOG_DEBUG },  // no LOG_TRACE
                { int(log_level::warn), LOG_WARNING },
                { int(log_level::error), LOG_ERR },
        };
        // NOTE: syslog() can block, which will stall the reactor thread.
        //       this should be rare (will have to fill the pipe buffer
        //       before syslogd can clear it) but can happen.  If it does,
        //       we'll have to implement some internal buffering (which
        //       still means the problem can happen, just less frequently).
        // syslog() interprets % characters, so send msg as a parameter
        syslog(level_map[int(level)], "%s", msg.c_str());
    }
}

void
logger::set_stdout_enabled(bool enabled) {
    _stdout.store(enabled, std::memory_order_relaxed);
}

void
logger::set_syslog_enabled(bool enabled) {
    _syslog.store(enabled, std::memory_order_relaxed);
}

void
registry::set_all_loggers_level(log_level level) {
    std::lock_guard<std::mutex> g(_mutex);
    for (auto&& l : _loggers | boost::adaptors::map_values) {
        l->set_level(level);
    }
}

log_level
registry::get_logger_level(sstring name) const {
    std::lock_guard<std::mutex> g(_mutex);
    return _loggers.at(name)->level();
}

void
registry::set_logger_level(sstring name, log_level level) {
    std::lock_guard<std::mutex> g(_mutex);
    _loggers.at(name)->set_level(level);
}

std::vector<sstring>
registry::get_all_logger_names() {
    std::lock_guard<std::mutex> g(_mutex);
    auto ret = _loggers | boost::adaptors::map_keys;
    return std::vector<sstring>(ret.begin(), ret.end());
}

void
registry::register_logger(logger* l) {
    std::lock_guard<std::mutex> g(_mutex);
    _loggers[l->name()] = l;
}

void
registry::unregister_logger(logger* l) {
    std::lock_guard<std::mutex> g(_mutex);
    _loggers.erase(l->name());
}

void
registry::moved(logger* from, logger* to) {
    std::lock_guard<std::mutex> g(_mutex);
    _loggers[from->name()] = to;
}

sstring pretty_type_name(const std::type_info& ti) {
    int status;
    std::unique_ptr<char[], void (*)(void*)> result(
            abi::__cxa_demangle(ti.name(), 0, 0, &status), std::free);
    return result.get() ? result.get() : ti.name();
}

registry& logger_registry() {
    static registry g_registry;
    return g_registry;
}

}

namespace boost {

template <>
logging::log_level lexical_cast(const std::string& source) {
    std::istringstream in(source);
    logging::log_level level;
    // Using the operator normall fails.
    if (!::operator>>(in, level)) {
        throw boost::bad_lexical_cast();
    }
    return level;
}

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
