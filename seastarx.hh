/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/asio/ip/address_v4.hpp>  // avoid conflict between ::socket and seastar::socket
#include <fmt/ostream.h> // remove once all seastar types can be formatted via formatter
#include <filesystem>
#include <seastar/util/log.hh>

namespace seastar {

template <typename T>
class shared_ptr;

template <typename T>
class lw_shared_ptr;


template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

}


using namespace seastar;
using seastar::shared_ptr;
using seastar::make_shared;

namespace seastar {

template <typename Tag> class bool_class;
template <typename T> class lazy_eval;
template <typename T> class lazy_deref_wrapper;
  
}

template <typename Tag> struct fmt::formatter<seastar::bool_class<Tag>> : fmt::ostream_formatter {};
template <typename T> struct fmt::formatter<seastar::lazy_eval<T>> : fmt::ostream_formatter {};
template <typename T> struct fmt::formatter<seastar::lazy_deref_wrapper<T>> : fmt::ostream_formatter {};
template <> struct fmt::formatter<std::filesystem::path> : fmt::ostream_formatter {};
template <typename T> struct fmt::formatter<seastar::shared_ptr<T>> : fmt::ostream_formatter {};
template <typename T> struct fmt::formatter<seastar::lw_shared_ptr<T>> : fmt::ostream_formatter {};
template <> struct fmt::formatter<seastar::log_level> : fmt::ostream_formatter {};
