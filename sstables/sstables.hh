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
#include "compress.hh"
#include "row.hh"

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
    size_t _data_file_size;

    sstring _dir;
    unsigned long _generation = 0;
    version_types _version;
    format_types _format;

    const bool has_component(component_type f);

    const sstring filename(component_type f);

    template <typename T, sstable::component_type Type, T sstable::* Comptr>
    future<> read_simple();

    template <typename T, sstable::component_type Type, T sstable::* Comptr>
    future<> write_simple();

    future<> read_toc();
    future<> write_toc();

    future<> read_compression();
    future<> write_compression();

    future<> read_filter() {
        return read_simple<filter, component_type::Filter, &sstable::_filter>();
    }
    future<> write_filter() {
        return write_simple<filter, component_type::Filter, &sstable::_filter>();
    }

    future<> read_summary() {
        return read_simple<summary, component_type::Summary, &sstable::_summary>();
    }
    future<> write_summary() {
        return write_simple<summary, component_type::Summary, &sstable::_summary>();
    }

    future<> read_statistics();
    future<> write_statistics();

    future<> open_data();

    future<index_list> read_indexes(uint64_t position, uint64_t quantity);

    input_stream<char> data_stream_at(uint64_t pos);
    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len);


public:
    // Read one or few rows at the given byte range from the data file,
    // feeding them into the consumer. This function reads the entire given
    // byte range at once into memory, so it should not be used for iterating
    // over all the rows in the data file (see the next function for that.
    // The function returns a future which completes after all the data has
    // been fed into the consumer. The caller needs to ensure the "consumer"
    // object lives until then (e.g., using the do_with() idiom).
    future<> data_consume_rows_at_once(row_consumer& consumer,
            uint64_t pos, uint64_t end);

    // Iterate over all rows in the data file (or rows in a particular range),
    // feeding them into the consumer. The iteration is done as efficiently as
    // possible - reading only the data file (not the summary or index files)
    // and reading data in batches.
    // The function returns a future which completes after all the data has
    // been fed into the consumer. The caller needs to ensure the "consumer"
    // object lives until then (e.g., using the do_with() idiom).
    future<> data_consume_rows(row_consumer& consumer,
            uint64_t start = 0, uint64_t end = 0);

    sstable(sstring dir, unsigned long generation, version_types v, format_types f) : _dir(dir), _generation(generation), _version(v), _format(f) {}
    sstable& operator=(const sstable&) = delete;
    sstable(const sstable&) = delete;
    sstable(sstable&&) = default;

    static version_types version_from_sstring(sstring& s);
    static format_types format_from_sstring(sstring& s);

    future<index_list> read_indexes(uint64_t position) {
        return read_indexes(position, _summary.header.sampling_level);
    }

    future<> load();
    future<> store();

    future<summary_entry&> read_summary_entry(size_t i);

    void set_generation(unsigned long generation) { _generation = generation; }

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;
};
}
