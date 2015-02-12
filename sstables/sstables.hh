/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include "core/file.hh"
#include "core/future.hh"
#include "core/sstring.hh"
#include "core/enum.hh"
#include <unordered_set> 
#include <unordered_map>
#include "types.hh"

namespace sstables {

template <typename T, typename V>
using unordered_map = std::unordered_map<T, V>;

class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

class sstable {
public:
    enum class component_type {
        Index,
        CompressionInfo,
        Data,
        TOC,
        Summary,
        Digest,
        CRC,
        Filter,
        Statistics,
    };
    enum class version_types { la };
    enum class format_types { big };

private:
    static unordered_map<version_types, sstring> _version_string;
    static unordered_map<format_types, sstring> _format_string;
    static unordered_map<component_type, sstring> _component_map;

    std::unordered_set<component_type> _components;

    compression _compression;
    filter _filter;

    sstring _dir;
    unsigned long _epoch = 0;
    version_types _version;
    format_types _format;

    const bool has_component(component_type f);

    const sstring filename(component_type f);
    future<> read_toc();

    template <typename T, sstable::component_type Type, T sstable::* Comptr>
    future<> read_simple();

    future<> read_compression();
    future<> read_filter() {
        return read_simple<filter, component_type::Filter, &sstable::_filter>();
    }

public:
    sstable(sstring dir, unsigned long epoch, version_types v, format_types f) : _dir(dir), _epoch(epoch), _version(v), _format(f) {}
    sstable& operator=(const sstable&) = delete;
    sstable(const sstable&) = delete;
    sstable(sstable&&) = default;

    future<> load();
};
}
