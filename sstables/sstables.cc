/*
 * Copyright 2015 Cloudius Systems
 */

#include "log.hh"
#include <vector>
#include <typeinfo>
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include <boost/algorithm/string.hpp>

#include "types.hh"
#include "sstables.hh"

namespace sstables {

class random_access_reader {
    input_stream<char> _in;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;
public:
    future<temporary_buffer<char>> read_exactly(size_t n) {
        return _in.read_exactly(n);
    }
    void seek(uint64_t pos) {
        _in = open_at(pos);
    }
    virtual ~random_access_reader() { }
};

class file_input_stream : public random_access_reader {
    lw_shared_ptr<file> _file;
    size_t _buffer_size;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        return make_file_input_stream(_file, pos, _buffer_size);
    }
    explicit file_input_stream(file&& f, size_t buffer_size = 8192)
        : _file(make_lw_shared<file>(std::move(f))), _buffer_size(buffer_size)
    {
        seek(0);
    }
};

thread_local logging::logger sstlog("sstable");

std::unordered_map<sstable::version_types, sstring, enum_hash<sstable::version_types>> sstable::_version_string = {
    { sstable::version_types::la , "la" }
};

std::unordered_map<sstable::format_types, sstring, enum_hash<sstable::format_types>> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

std::unordered_map<sstable::component_type, sstring, enum_hash<sstable::component_type>> sstable::_component_map = {
    { component_type::Index, "Index.db"},
    { component_type::CompressionInfo, "CompressionInfo.db" },
    { component_type::Data, "Data.db" },
    { component_type::TOC, "TOC.txt" },
    { component_type::Summary, "Summary.db" },
    { component_type::Digest, "Digest.sha1" },
    { component_type::CRC, "CRC.db" },
    { component_type::Filter, "Filter.db" },
    { component_type::Statistics, "Statistics.db" },
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception("Buffer to small to hold requested data. Got: " + to_sstring(size) + ". Expected: " + to_sstring(expected))
    {}
};

// This should be used every time we use read_exactly directly.
//
// read_exactly is a lot more convenient of an interface to use, because we'll
// be parsing known quantities.
//
// However, anything other than the size we have asked for, is certainly a bug,
// and we need to do something about it.
static void check_buf_size(temporary_buffer<char>& buf, size_t expected) {
    if (buf.size() < expected) {
        throw bufsize_mismatch_exception(buf.size(), expected);
    }
}

// Base parser, parses an integer type
template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, future<>>
parse(file_input_stream& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));

        auto *nr = reinterpret_cast<const net::packed<T> *>(buf.get());
        i = net::ntoh(*nr);
        return make_ready_future<>();
    });
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
parse(file_input_stream& in, T& i) {
    return parse(in, reinterpret_cast<typename std::underlying_type<T>::type&>(i));
}

future<> parse(file_input_stream& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

template <typename To, typename From>
static inline To convert(From f) {
    static_assert(sizeof(To) == sizeof(From), "Sizes must match");
    union {
        To to;
        From from;
    } conv;

    conv.from = f;
    return conv.to;
}

future<> parse(file_input_stream& in, double& d) {
    return in.read_exactly(sizeof(double)).then([&d] (auto buf) {
        check_buf_size(buf, sizeof(double));

        auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(buf.get());
        d = convert<double>(net::ntoh(*nr));
        return make_ready_future<>();
    });
}

template <typename T>
future<> parse(file_input_stream& in, T& len, sstring& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        s = sstring(buf.get(), len);
    });
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(file_input_stream& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

// For all types that take a size, we provide a template that takes the type
// alone, and another, separate one, that takes a size parameter as well, of
// type Size. This is because although most of the time the size and the data
// are contiguous, it is not always the case. So we want to have the
// flexibility of parsing them separately.
template <typename Size>
future<> parse(file_input_stream& in, disk_string<Size>& s) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &s, len = std::move(len)] {
        return parse(in, *len, s.value);
    });
}

// We cannot simply read the whole array at once, because we don't know its
// full size. We know the number of elements, but if we are talking about
// disk_strings, for instance, we have no idea how much of the stream each
// element will take.
//
// Sometimes we do know the size, like the case of integers. There, all we have
// to do is to convert each member because they are all stored big endian.
// We'll offer a specialization for that case below.
template <typename Size, typename Members>
typename std::enable_if_t<!std::is_integral<Members>::value, future<>>
parse(file_input_stream& in, Size& len, std::vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(file_input_stream& in, Size& len, std::vector<Members>& arr) {
    return in.read_exactly(len * sizeof(Members)).then([&arr, len] (auto buf) {
        check_buf_size(buf, len * sizeof(Members));

        auto *nr = reinterpret_cast<const net::packed<Members> *>(buf.get());
        for (size_t i = 0; i < len; ++i) {
            arr[i] = net::ntoh(nr[i]);
        }
        return make_ready_future<>();
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(file_input_stream& in, disk_array<Size, Members>& arr) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &arr, len = std::move(len)] {
        arr.elements.resize(*len);
        return parse(in, *len, arr.elements);
    });
}

template <typename Size, typename Key, typename Value>
future<> parse(file_input_stream& in, Size& len, std::unordered_map<Key, Value>& map) {
    auto count = make_lw_shared<Size>();
    auto eos = [len, count] { return len == *count; };
    return do_until(eos, [len, count, &in, &map] {
        struct kv {
            Key key;
            Value value;
        };
        ++*count;

        auto el = std::make_unique<kv>();
        auto f = parse(in, el->key, el->value);
        return f.then([el = std::move(el), &map] {
            map.emplace(el->key, el->value);
        });
    });
}

template <typename Size, typename Key, typename Value>
future<> parse(file_input_stream& in, disk_hash<Size, Key, Value>& h) {
    auto w = std::make_unique<Size>();
    auto f = parse(in, *w);
    return f.then([&in, &h, w = std::move(w)] {
        return parse(in, *w, h.map);
    });
}

future<> parse(file_input_stream& in, option& op) {
    return parse(in, op.key, op.value);
}

future<> parse(file_input_stream& in, compression& c) {
    return parse(in, c.name, c.options, c.chunk_len, c.data_len, c.offsets);
}

future<> parse(file_input_stream& in, filter& f) {
    return parse(in, f.hashes, f.buckets);
}

future<> parse(file_input_stream& in, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    return parse(in, s.header.min_index_interval,
                     s.header.size,
                     s.header.memory_size,
                     s.header.sampling_level,
                     s.header.size_at_full_sampling).then([&in, &s] {
        return in.read_exactly(s.header.size * sizeof(pos_type)).then([&in, &s] (auto buf) {
            auto len = s.header.size * sizeof(pos_type);
            check_buf_size(buf, len);

            s.positions.resize(s.header.size);

            auto *nr = reinterpret_cast<const pos_type *>(buf.get());
            s.positions = std::vector<pos_type>(nr, nr + s.header.size);
        }).then([&in, &s] {
            // FIXME: Read the actual indexes
            return make_ready_future<>();
        });
    });
}

future<> parse(file_input_stream& in, struct replay_position& rp) {
    return parse(in, rp.segment, rp.position);
}

future<> parse(file_input_stream& in, estimated_histogram::eh_elem &e) {
    return parse(in, e.offset, e.bucket);
}

future<> parse(file_input_stream& in, estimated_histogram &e) {
    return parse(in, e.elements);
}

future<> parse(file_input_stream& in, streaming_histogram &h) {
    return parse(in, h.max_bin_size, h.hash);
}

future<> parse(file_input_stream& in, validation_metadata& m) {
    return parse(in, m.partitioner, m.filter_chance);
}

future<> parse(file_input_stream& in, compaction_metadata& m) {
    return parse(in, m.ancestors, m.cardinality);
}

template <typename Child>
future<> parse(file_input_stream& in, std::unique_ptr<metadata>& p) {
    p.reset(new Child);
    return parse(in, *static_cast<Child *>(p.get()));
}

future<> parse(file_input_stream& in, stats_metadata& m) {
    return parse(in,
        m.estimated_row_size,
        m.estimated_column_count,
        m.position,
        m.min_timestamp,
        m.max_timestamp,
        m.max_local_deletion_time,
        m.compression_ratio,
        m.estimated_tombstone_drop_time,
        m.sstable_level,
        m.repaired_at,
        m.min_column_names,
        m.max_column_names,
        m.has_legacy_counter_shards
    );
}

future<> parse(file_input_stream& in, statistics& s) {
    return parse(in, s.hash).then([&in, &s] {
        return do_for_each(s.hash.map.begin(), s.hash.map.end(), [&in, &s] (auto val) mutable {
            in.seek(val.second);

            switch (val.first) {
                case metadata_type::Validation:
                    return parse<validation_metadata>(in, s.contents[val.first]);
                case metadata_type::Compaction:
                    return parse<compaction_metadata>(in, s.contents[val.first]);
                case metadata_type::Stats:
                    return parse<stats_metadata>(in, s.contents[val.first]);
                default:
                    sstlog.warn("Invalid metadata type at Statistics file: {} ", int(val.first));
                    return make_ready_future<>();
                }
        });
    });
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() {
    auto file_path = filename(sstable::component_type::TOC);

    sstlog.debug("Reading TOC file {} ", file_path);

    return engine().open_file_dma(file_path, open_flags::ro).then([this] (file f) {
        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
        auto buf = bufptr.get();

        return f.dma_read(0, buf, 4096).then([this, bufptr = std::move(bufptr)] (size_t size) {
            // This file is supposed to be very small. Theoretically we should check its size,
            // but if we so much as read a whole page from it, there is definitely something fishy
            // going on - and this simplifies the code.
            if (size >= 4096) {
                throw malformed_sstable_exception("SSTable too big: " + to_sstring(size) + " bytes.");
            }

            std::experimental::string_view buf(bufptr.get(), size);
            std::vector<sstring> comps;

            boost::split(comps , buf, boost::is_any_of("\n"));

            for (auto& c: comps) {
                // accept trailing newlines
                if (c == "") {
                    continue;
                }
                auto found = false;
                for (auto& cmap: _component_map) {
                    // Remember that this map is a { index => string } one.
                    // Note that we match the string...
                    if (c == cmap.second) {
                        // but add the index to the components list.
                        sstlog.debug("\tFound at TOC file: {} ", c);
                        _components.insert(cmap.first);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw malformed_sstable_exception("Unrecognized TOC component: " + c);
                }
            }
            if (!_components.size()) {
                throw malformed_sstable_exception("Empty TOC");
            }
            return make_ready_future<>();
        });
    }).then_wrapped([file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(file_path + ": file not found");
            }
        }
    });

}

template <typename T, sstable::component_type Type, T sstable::* Comptr>
future<> sstable::read_simple() {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + _component_map[Type] + " file {} ").c_str(), file_path);
    return engine().open_file_dma(file_path, open_flags::ro).then([this] (file f) {

        auto r = std::make_unique<file_input_stream>(std::move(f), 4096);
        auto fut = parse(*r, *this.*Comptr);
        return fut.then([r = std::move(r)] {});
    }).then_wrapped([this, file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(file_path + ": file not found");
            }
        }
    });
}

future<> sstable::read_compression() {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<compression, component_type::CompressionInfo, &sstable::_compression>();
}

future<> sstable::read_statistics() {
    return read_simple<statistics, component_type::Statistics, &sstable::_statistics>();
}

future<> sstable::load() {
    return read_toc().then([this] {
        return read_statistics();
    }).then([this] {
        return read_compression();
    }).then([this] {
        return read_filter();
    }).then([this] {;
        return read_summary();
    });
}

const bool sstable::has_component(component_type f) {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) {

    auto& version = _version_string.at(_version);
    auto& format = _format_string.at(_format);
    auto& component = _component_map.at(f);
    auto epoch =  to_sstring(_epoch);

    return _dir + "/" + version + "-" + epoch + "-" + format + "-" + component;
}
}
