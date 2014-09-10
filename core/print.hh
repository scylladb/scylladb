/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef PRINT_HH_
#define PRINT_HH_

#include <boost/format.hpp>
#include <iostream>

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

#endif /* PRINT_HH_ */
