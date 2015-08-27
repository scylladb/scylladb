/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once
namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(sprint("Buffer improperly sized to hold requested data. Got: %ld. Expected: %ld", size, expected))
    {}
};
}
