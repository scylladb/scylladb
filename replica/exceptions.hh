/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <exception>
#include <variant>

#include "seastar/core/sstring.hh"
#include "seastar/core/timed_out_error.hh"

#include "utils/exception_container.hh"
#include "utils/result.hh"

namespace replica {

class replica_exception : public std::exception {
public:
    replica_exception() noexcept {};
};

class unknown_exception final : public replica_exception {
private:
    seastar::sstring _msg;
public:
    unknown_exception(seastar::sstring desc) noexcept
            : replica_exception()
            , _msg(std::move(desc))
    { }

    virtual const char* what() const noexcept override { return _msg.c_str(); };

    const seastar::sstring& get_cause() const noexcept { return _msg; }
};

class timeout_exception final : public replica_exception {
public:
    timeout_exception() noexcept
            : replica_exception()
    { }

    virtual const char* what() const noexcept override { return "timeout"; };
};

class forward_exception final : public replica_exception {
public:
    forward_exception() noexcept
            : replica_exception()
    { }

    virtual const char* what() const noexcept override { return "failed to forward mutations"; };
};

class virtual_table_update_exception final : public replica_exception {
private:
    seastar::sstring _msg;
public:
    virtual_table_update_exception(seastar::sstring msg) noexcept
            : replica_exception()
            , _msg(std::move(msg))
    { }

    virtual const char* what() const noexcept override { return _msg.c_str(); }

    const seastar::sstring& get_cause() const noexcept { return _msg; }

    // This method is to avoid potential exceptions while copying the string
    // and thus to be used when the exception is handled and is about to
    // be thrown away
    seastar::sstring grab_cause() noexcept { return std::move(_msg); }
};

struct exception_variant {
    std::variant<std::monostate,
            unknown_exception,
            timeout_exception,
            forward_exception,
            virtual_table_update_exception
    > reason;

    exception_variant()
            : reason()
    { }

    template<typename Ex>
    exception_variant(Ex&& ex)
            : reason(std::move(ex))
    { }

    std::exception_ptr into_exception_ptr() noexcept;
};

exception_variant encode_replica_exception(std::exception& ex) noexcept;
exception_variant encode_replica_exception(std::exception_ptr eptr) noexcept;

exception_variant encode_unknown_replica_exception(std::exception_ptr eptr) noexcept;

}
