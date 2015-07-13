/*
 * Copyright 2015 Cloudius Systems
 */

#include <stdlib.h>
#include <boost/filesystem.hpp>

#pragma once

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
struct tmpdir {
    tmpdir() {
        char tmp[] = "tmpdir_XXXXXX";
        auto * dir = ::mkdtemp(tmp);
        if (dir == NULL) {
            throw std::runtime_error("Could not create temp dir");
        }
        path = dir;
        //std::cout << path << std::endl;
    }
    tmpdir(tmpdir&& v)
        : path(std::move(v.path)) {
        assert(v.path.empty());
    }
    tmpdir(const tmpdir&) = delete;
    ~tmpdir() {
        if (!path.empty()) {
            boost::filesystem::remove_all(path.c_str());
        }
    }
    tmpdir & operator=(tmpdir&& v) {
        if (&v != this) {
            this->~tmpdir();
            new (this) tmpdir(std::move(v));
        }
        return *this;
    }
    tmpdir & operator=(const tmpdir&) = delete;
    sstring path;
};
