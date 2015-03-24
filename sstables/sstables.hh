/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include "core/file.hh"
#include "core/fstream.hh"
#include "core/future.hh"
#include "core/sstring.hh"
#include "core/enum.hh"
#include "core/shared_ptr.hh"
#include <unordered_set>
#include <unordered_map>
#include "types.hh"
#include "core/enum.hh"

namespace sstables {

class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

using index_list = std::vector<index_entry>;

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
    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;
    static std::unordered_map<component_type, sstring, enum_hash<component_type>> _component_map;

    std::unordered_set<component_type, enum_hash<component_type>> _components;

    compression _compression;
    filter _filter;
    summary _summary;
    statistics _statistics;
    lw_shared_ptr<file> _index_file;
    lw_shared_ptr<file> _data_file;

    sstring _dir;
    unsigned long _generation = 0;
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
    future<> read_summary() {
        return read_simple<summary, component_type::Summary, &sstable::_summary>();
    }
    future<> read_statistics();
    future<> open_data();

    future<index_list> read_indexes(uint64_t position, uint64_t quantity);

public:
    sstable(sstring dir, unsigned long generation, version_types v, format_types f) : _dir(dir), _generation(generation), _version(v), _format(f) {}
    sstable& operator=(const sstable&) = delete;
    sstable(const sstable&) = delete;
    sstable(sstable&&) = default;

    static version_types version_from_sstring(sstring& s);
    static format_types format_from_sstring(sstring& s);

    future<index_list> read_indexes(uint64_t position) {
        return read_indexes(position, _summary.header.sampling_level);
    }

    future<index_list> read_indexes_for_testing(uint64_t position, uint64_t quantity) {
        return read_indexes(position, quantity);
    }
    future<> load();

    future<summary_entry&> read_summary_entry(size_t i);
};
}
