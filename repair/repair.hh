/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <unordered_map>
#include <exception>

#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>

#include "database.hh"
#include "utils/UUID.hh"


class repair_exception : public std::exception {
private:
    sstring _what;
public:
    repair_exception(sstring msg) : _what(std::move(msg)) { }
    virtual const char* what() const noexcept override { return _what.c_str(); }
};

// NOTE: repair_start() can be run on any node, but starts a node-global
// operation.
int repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options);
