/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef LOG_HH_
#define LOG_HH_

#include "core/sstring.hh"
#include <unordered_map>
#include <exception>
#include <iosfwd>
#include <atomic>
#include <mutex>
#include <boost/lexical_cast.hpp>

namespace logging {

enum class log_level {
    error,
    warn,
    info,
    debug,
    trace,
};

}

// Must exist logging namespace, or ADL gets confused in logger::stringer
std::ostream& operator<<(std::ostream& out, logging::log_level level);
std::istream& operator>>(std::istream& in, logging::log_level& level);

// Boost doesn't auto-deduce the existence of the streaming operators for some reason

namespace boost {

template <>
logging::log_level lexical_cast(const std::string& source);

}

namespace logging {

class logger;
class registry;

class logger {
    sstring _name;
    std::atomic<log_level> _level = { log_level::warn };
    static std::atomic<bool> _stdout;
    static std::atomic<bool> _syslog;
private:
    struct stringer {
        // no need for virtual dtor, since not dynamically destroyed
        virtual void append(std::ostream& os) = 0;
    };
    template <typename Arg>
    struct stringer_for final : stringer {
        explicit stringer_for(const Arg& arg) : arg(arg) {}
        const Arg& arg;
        virtual void append(std::ostream& os) override {
            os << arg;
        }
    };
    template <typename... Args>
    void do_log(log_level level, const char* fmt, Args&&... args);
    template <typename Arg, typename... Args>
    void do_log_step(log_level level, const char* fmt, stringer** s, size_t n, size_t idx, Arg&& arg, Args&&... args);
    void do_log_step(log_level level, const char* fmt, stringer** s, size_t n, size_t idx);
    void really_do_log(log_level level, const char* fmt, stringer** stringers, size_t n);
public:
    explicit logger(sstring name);
    logger(logger&& x);
    ~logger();
    bool is_enabled(log_level level) const {
        return level <= _level.load(std::memory_order_relaxed);
    }
    template <typename... Args>
    void log(log_level level, const char* fmt, Args&&... args) {
        if (is_enabled(level)) {
            do_log(level, fmt, std::forward<Args>(args)...);
        }
    }
    template <typename... Args>
    void error(const char* fmt, Args&&... args) {
        log(log_level::error, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    void warn(const char* fmt, Args&&... args) {
        log(log_level::warn, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    void info(const char* fmt, Args&&... args) {
        log(log_level::info, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    void debug(const char* fmt, Args&&... args) {
        log(log_level::debug, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    void trace(const char* fmt, Args&&... args) {
        log(log_level::trace, fmt, std::forward<Args>(args)...);
    }
    const sstring& name() const {
        return _name;
    }
    log_level level() const {
        return _level.load(std::memory_order_relaxed);
    }
    void set_level(log_level level) {
        _level.store(level, std::memory_order_relaxed);
    }
    static void set_stdout_enabled(bool enabled);
    static void set_syslog_enabled(bool enabled);
};

class registry {
    mutable std::mutex _mutex;
    std::unordered_map<sstring, logger*> _loggers;
public:
    void set_all_loggers_level(log_level level);
    log_level get_logger_level(sstring name) const;
    void set_logger_level(sstring name, log_level level);
    std::vector<sstring> get_all_logger_names();
    void register_logger(logger* l);
    void unregister_logger(logger* l);
    void moved(logger* from, logger* to);
};

sstring pretty_type_name(const std::type_info&);

registry& logger_registry();

template <typename T>
class logger_for : public logger {
public:
    logger_for() : logger(pretty_type_name(typeid(T))) {}
};

inline
void
logger::do_log_step(log_level level, const char* fmt, stringer** s, size_t n, size_t idx) {
    really_do_log(level, fmt, s, n);
}

template <typename Arg, typename... Args>
inline
void
logger::do_log_step(log_level level, const char* fmt, stringer** s, size_t n, size_t idx, Arg&& arg, Args&&... args) {
    stringer_for<Arg> sarg{arg};
    s[idx] = &sarg;
    do_log_step(level, fmt, s, n, idx + 1, std::forward<Args>(args)...);
}


template <typename... Args>
void
logger::do_log(log_level level, const char* fmt, Args&&... args) {
    stringer* s[sizeof...(Args)];
    do_log_step(level, fmt, s, sizeof...(Args), 0, std::forward<Args>(args)...);
}

}

// Pretty-printer for exceptions to be logged, e.g., std::current_exception().
namespace std {
std::ostream& operator<<(std::ostream&, const std::exception_ptr);
}

#endif /* LOG_HH_ */
