/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef PRINT_HH_
#define PRINT_HH_

#include <boost/format.hpp>
#include <iostream>
#include <iomanip>
#include <chrono>

inline void
apply_format(boost::format& fmt) {
}

template <typename A0, typename... Arest>
inline void
apply_format(boost::format& fmt, A0&& a0, Arest&&... arest) {
    apply_format(fmt % std::forward<A0>(a0), std::forward<Arest>(arest)...);
}

template <typename... A>
std::ostream&
fprint(std::ostream& os, boost::format& fmt, A&&... a) {
    apply_format(fmt, std::forward<A>(a)...);
    return os << fmt;
}

template <typename... A>
void
print(boost::format& fmt, A&&... a) {
    fprint(std::cout, fmt, std::forward<A>(a)...);
}

template <typename... A>
std::ostream&
fprint(std::ostream& os, const char* fmt, A&&... a) {
    boost::format bfmt(fmt);
    return fprint(os, bfmt, std::forward<A>(a)...);
}

template <typename... A>
void
print(const char* fmt, A&&... a) {
    boost::format bfmt(fmt);
    return print(bfmt, std::forward<A>(a)...);
}

template <typename... A>
std::string
sprint(const char* fmt, A&&... a) {
    boost::format bfmt(fmt);
    apply_format(bfmt, std::forward<A>(a)...);
    return bfmt.str();
}

template <typename Iterator>
std::string
format_separated(Iterator b, Iterator e, const char* sep = ", ") {
    std::string ret;
    if (b == e) {
        return ret;
    }
    ret += *b++;
    while (b != e) {
        ret += sep;
        ret += *b++;
    }
    return ret;
}

template <typename TimePoint>
struct usecfmt_wrapper {
    TimePoint val;
};

template <typename TimePoint>
inline
usecfmt_wrapper<TimePoint>
usecfmt(TimePoint tp) {
    return { tp };
};

template <typename Clock, typename Rep, typename Period>
std::ostream&
operator<<(std::ostream& os, usecfmt_wrapper<std::chrono::time_point<Clock, std::chrono::duration<Rep, Period>>> tp) {
    auto usec = std::chrono::duration_cast<std::chrono::microseconds>(tp.val.time_since_epoch()).count();
    std::ostream tmp(os.rdbuf());
    tmp << std::setw(12) << (usec / 1000000) << "." << std::setw(6) << std::setfill('0') << (usec % 1000000);
    return os;
}

template <typename... A>
void
log(A&&... a) {
    std::cout << usecfmt(std::chrono::high_resolution_clock::now()) << " ";
    print(std::forward<A>(a)...);
}

#endif /* PRINT_HH_ */
