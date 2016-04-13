/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "log.hh"
#include <vector>
#include <typeinfo>
#include <limits>
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include <seastar/core/shared_future.hh>
#include <iterator>

#include "types.hh"
#include "sstables.hh"
#include "compress.hh"
#include "unimplemented.hh"
#include "index_reader.hh"
#include "remove.hh"
#include "memtable.hh"
#include "range.hh"
#include "downsampling.hh"
#include <boost/filesystem/operations.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm_ext/insert.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <regex>
#include <core/align.hh>
#include "utils/phased_barrier.hh"

#include "checked-file-impl.hh"
#include "disk-error-handler.hh"

thread_local disk_error_signal_type sstable_read_error;
thread_local disk_error_signal_type sstable_write_error;

namespace sstables {

logging::logger sstlog("sstable");

future<file> new_sstable_component_file(disk_error_signal_type& signal, sstring name, open_flags flags) {
    return open_checked_file_dma(signal, name, flags).handle_exception([name] (auto ep) {
        sstlog.error("Could not create SSTable component {}. Found exception: {}", name, ep);
        return make_exception_future<file>(ep);
    });
}

future<file> new_sstable_component_file(disk_error_signal_type& signal, sstring name, open_flags flags, file_open_options options) {
    return open_checked_file_dma(signal, name, flags, options).handle_exception([name] (auto ep) {
        sstlog.error("Could not create SSTable component {}. Found exception: {}", name, ep);
        return make_exception_future<file>(ep);
    });
}

thread_local std::unordered_map<sstring, std::unordered_set<unsigned>> sstable::_shards_agreeing_to_remove_sstable;

static utils::phased_barrier& background_jobs() {
    static thread_local utils::phased_barrier gate;
    return gate;
}

future<> await_background_jobs() {
    sstlog.debug("Waiting for background jobs");
    return background_jobs().advance_and_await().finally([] {
        sstlog.debug("Waiting done");
    });
}

future<> await_background_jobs_on_all_shards() {
    return smp::invoke_on_all([] {
        return await_background_jobs();
    });
}

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
    bool eof() { return _in.eof(); }
    virtual future<> close() {
        return make_ready_future<>();
        // FIXME: return _in.close();
    }
    virtual ~random_access_reader() { }
};

class file_random_access_reader : public random_access_reader {
    file _file;
    size_t _buffer_size;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        file_input_stream_options options;
        options.buffer_size = _buffer_size;

        return make_file_input_stream(_file, pos, std::move(options));
    }
    explicit file_random_access_reader(file f, size_t buffer_size = 8192)
        : _file(std::move(f)), _buffer_size(buffer_size)
    {
        seek(0);
    }
    virtual future<> close() override {
        return random_access_reader::close().then([this] {
            return _file.close().handle_exception([save = _file] (auto ep) {
                sstlog.warn("sstable close failed: {}", ep);
                general_disk_error();
            });
        });
    }
};

std::unordered_map<sstable::version_types, sstring, enum_hash<sstable::version_types>> sstable::_version_string = {
    { sstable::version_types::ka , "ka" },
    { sstable::version_types::la , "la" }
};

std::unordered_map<sstable::format_types, sstring, enum_hash<sstable::format_types>> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

static const sstring TOC_SUFFIX = "TOC.txt";
static const sstring TEMPORARY_TOC_SUFFIX = "TOC.txt.tmp";

// FIXME: this should be version-dependent
std::unordered_map<sstable::component_type, sstring, enum_hash<sstable::component_type>> sstable::_component_map = {
    { component_type::Index, "Index.db"},
    { component_type::CompressionInfo, "CompressionInfo.db" },
    { component_type::Data, "Data.db" },
    { component_type::TOC, TOC_SUFFIX },
    { component_type::Summary, "Summary.db" },
    { component_type::Digest, "Digest.sha1" },
    { component_type::CRC, "CRC.db" },
    { component_type::Filter, "Filter.db" },
    { component_type::Statistics, "Statistics.db" },
    { component_type::TemporaryTOC, TEMPORARY_TOC_SUFFIX },
};

// This assumes that the mappings are small enough, and called unfrequent
// enough.  If that changes, it would be adviseable to create a full static
// reverse mapping, even if it is done at runtime.
template <typename Map>
static typename Map::key_type reverse_map(const typename Map::mapped_type& value, Map& map) {
    for (auto& pair: map) {
        if (pair.second == value) {
            return pair.first;
        }
    }
    throw std::out_of_range("unable to reverse map");
}

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

template <typename T, typename U>
static void check_truncate_and_assign(T& to, const U from) {
    static_assert(std::is_integral<T>::value && std::is_integral<U>::value, "T and U must be integral");
    to = from;
    if (to != from) {
        throw std::overflow_error("assigning U to T caused an overflow");
    }
}

// Base parser, parses an integer type
template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, void>
read_integer(temporary_buffer<char>& buf, T& i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(buf.get());
    i = net::ntoh(*nr);
}

template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, future<>>
parse(random_access_reader& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));

        read_integer(buf, i);
        return make_ready_future<>();
    });
}

template <typename T>
inline typename std::enable_if_t<std::is_integral<T>::value, void>
write(file_writer& out, T i) {
    auto *nr = reinterpret_cast<const net::packed<T> *>(&i);
    i = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    out.write(p, sizeof(T)).get();
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
parse(random_access_reader& in, T& i) {
    return parse(in, reinterpret_cast<typename std::underlying_type<T>::type&>(i));
}

template <typename T>
inline typename std::enable_if_t<std::is_enum<T>::value, void>
write(file_writer& out, T i) {
    write(out, static_cast<typename std::underlying_type<T>::type>(i));
}

future<> parse(random_access_reader& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

inline void write(file_writer& out, bool i) {
    write(out, static_cast<uint8_t>(i));
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

future<> parse(random_access_reader& in, double& d) {
    return in.read_exactly(sizeof(double)).then([&d] (auto buf) {
        check_buf_size(buf, sizeof(double));

        auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(buf.get());
        d = convert<double>(net::ntoh(*nr));
        return make_ready_future<>();
    });
}

inline void write(file_writer& out, double d) {
    auto *nr = reinterpret_cast<const net::packed<unsigned long> *>(&d);
    auto tmp = net::hton(*nr);
    auto p = reinterpret_cast<const char*>(&tmp);
    out.write(p, sizeof(unsigned long)).get();
}

template <typename T>
future<> parse(random_access_reader& in, T& len, bytes& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        // Likely a different type of char. Most bufs are unsigned, whereas the bytes type is signed.
        s = bytes(reinterpret_cast<const bytes::value_type *>(buf.get()), len);
    });
}

inline void write(file_writer& out, bytes& s) {
    out.write(s).get();
}

inline void write(file_writer& out, bytes_view s) {
    out.write(reinterpret_cast<const char*>(s.data()), s.size()).get();
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(random_access_reader& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

template<typename First, typename... Rest>
inline void write(file_writer& out, First& first, Rest&&... rest) {
    write(out, first);
    write(out, std::forward<Rest>(rest)...);
}

// Intended to be used for a type that describes itself through describe_type().
template <class T>
typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, future<>>
parse(random_access_reader& in, T& t) {
    return t.describe_type([&in] (auto&&... what) -> future<> {
        return parse(in, what...);
    });
}

template <class T>
inline typename std::enable_if_t<!std::is_integral<T>::value && !std::is_enum<T>::value, void>
write(file_writer& out, T& t) {
    t.describe_type([&out] (auto&&... what) -> void {
        write(out, std::forward<decltype(what)>(what)...);
    });
}

// For all types that take a size, we provide a template that takes the type
// alone, and another, separate one, that takes a size parameter as well, of
// type Size. This is because although most of the time the size and the data
// are contiguous, it is not always the case. So we want to have the
// flexibility of parsing them separately.
template <typename Size>
future<> parse(random_access_reader& in, disk_string<Size>& s) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &s, len = std::move(len)] {
        return parse(in, *len, s.value);
    });
}

template <typename Size>
inline void write(file_writer& out, disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    write(out, len);
    write(out, s.value);
}

template <typename Size>
inline void write(file_writer& out, disk_string_view<Size>& s) {
    Size len;
    check_truncate_and_assign(len, s.value.size());
    write(out, len, s.value);
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
parse(random_access_reader& in, Size& len, std::deque<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(random_access_reader& in, Size& len, std::deque<Members>& arr) {
    auto done = make_lw_shared<size_t>(0);
    return repeat([&in, &len, &arr, done]  {
        auto now = std::min(len - *done, 100000 / sizeof(Members));
        return in.read_exactly(now * sizeof(Members)).then([&arr, len, now, done] (auto buf) {
            check_buf_size(buf, now * sizeof(Members));

            auto *nr = reinterpret_cast<const net::packed<Members> *>(buf.get());
            for (size_t i = 0; i < now; ++i) {
                arr[*done + i] = net::ntoh(nr[i]);
            }
            *done += now;
            return make_ready_future<stop_iteration>(*done == len ? stop_iteration::yes : stop_iteration::no);
        });
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(random_access_reader& in, disk_array<Size, Members>& arr) {
    auto len = make_lw_shared<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &arr, len] {
        arr.elements.resize(*len);
        return parse(in, *len, arr.elements);
    }).finally([len] {});
}

template <typename Members>
inline typename std::enable_if_t<!std::is_integral<Members>::value, void>
write(file_writer& out, std::deque<Members>& arr) {
    for (auto& a : arr) {
        write(out, a);
    }
}

template <typename Members>
inline typename std::enable_if_t<std::is_integral<Members>::value, void>
write(file_writer& out, std::deque<Members>& arr) {
    std::vector<Members> tmp;
    size_t per_loop = 100000 / sizeof(Members);
    tmp.resize(per_loop);
    size_t idx = 0;
    while (idx != arr.size()) {
        auto now = std::min(arr.size() - idx, per_loop);
        // copy arr into tmp converting each entry into big-endian representation.
        auto nr = arr.begin() + idx;
        for (size_t i = 0; i < now; i++) {
            tmp[i] = net::hton(nr[i]);
        }
        auto p = reinterpret_cast<const char*>(tmp.data());
        auto bytes = now * sizeof(Members);
        out.write(p, bytes).get();
        idx += now;
    }
}

template <typename Size, typename Members>
inline void write(file_writer& out, disk_array<Size, Members>& arr) {
    Size len = 0;
    check_truncate_and_assign(len, arr.elements.size());
    write(out, len);
    write(out, arr.elements);
}

template <typename Size, typename Key, typename Value>
future<> parse(random_access_reader& in, Size& len, std::unordered_map<Key, Value>& map) {
    return do_with(Size(), [&in, len, &map] (Size& count) {
        auto eos = [len, &count] { return len == count++; };
        return do_until(eos, [len, &in, &map] {
            struct kv {
                Key key;
                Value value;
            };

            return do_with(kv(), [&in, &map] (auto& el) {
                return parse(in, el.key, el.value).then([&el, &map] {
                    map.emplace(el.key, el.value);
                });
            });
        });
    });
}

template <typename Size, typename Key, typename Value>
future<> parse(random_access_reader& in, disk_hash<Size, Key, Value>& h) {
    auto w = std::make_unique<Size>();
    auto f = parse(in, *w);
    return f.then([&in, &h, w = std::move(w)] {
        return parse(in, *w, h.map);
    });
}

template <typename Key, typename Value>
inline void write(file_writer& out, std::unordered_map<Key, Value>& map) {
    for (auto& val: map) {
        write(out, val.first, val.second);
    };
}

template <typename Size, typename Key, typename Value>
inline void write(file_writer& out, disk_hash<Size, Key, Value>& h) {
    Size len = 0;
    check_truncate_and_assign(len, h.map.size());
    write(out, len);
    write(out, h.map);
}

future<> parse(random_access_reader& in, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    return parse(in, s.header.min_index_interval,
                     s.header.size,
                     s.header.memory_size,
                     s.header.sampling_level,
                     s.header.size_at_full_sampling).then([&in, &s] {
        return in.read_exactly(s.header.size * sizeof(pos_type)).then([&in, &s] (auto buf) {
            auto len = s.header.size * sizeof(pos_type);
            check_buf_size(buf, len);

            s.entries.resize(s.header.size);

            auto *nr = reinterpret_cast<const pos_type *>(buf.get());
            s.positions = std::deque<pos_type>(nr, nr + s.header.size);

            // Since the keys in the index are not sized, we need to calculate
            // the start position of the index i+1 to determine the boundaries
            // of index i. The "memory_size" field in the header determines the
            // total memory used by the map, so if we push it to the vector, we
            // can guarantee that no conditionals are used, and we can always
            // query the position of the "next" index.
            s.positions.push_back(s.header.memory_size);
        }).then([&in, &s] {
            in.seek(sizeof(summary::header) + s.header.memory_size);
            return parse(in, s.first_key, s.last_key);
        }).then([&in, &s] {

            in.seek(s.positions[0] + sizeof(summary::header));

            assert(s.positions.size() == (s.entries.size() + 1));

            auto idx = make_lw_shared<size_t>(0);
            return do_for_each(s.entries.begin(), s.entries.end(), [idx, &in, &s] (auto& entry) {
                auto pos = s.positions[(*idx)++];
                auto next = s.positions[*idx];

                auto entrysize = next - pos;

                return in.read_exactly(entrysize).then([&entry, entrysize] (auto buf) {
                    check_buf_size(buf, entrysize);

                    auto keysize = entrysize - 8;
                    entry.key = bytes(reinterpret_cast<const int8_t*>(buf.get()), keysize);
                    buf.trim_front(keysize);
                    // FIXME: This is a le read. We should make this explicit
                    entry.position = *(reinterpret_cast<const net::packed<uint64_t> *>(buf.get()));

                    return make_ready_future<>();
                });
            }).then([&s] {
                // Delete last element which isn't part of the on-disk format.
                s.positions.pop_back();
            });
        });
    });
}

inline void write(file_writer& out, summary_entry& entry) {
    // FIXME: summary entry is supposedly written in memory order, but that
    // would prevent portability of summary file between machines of different
    // endianness. We can treat it as little endian to preserve portability.
    write(out, entry.key);
    auto p = reinterpret_cast<const char*>(&entry.position);
    out.write(p, sizeof(uint64_t)).get();
}

inline void write(file_writer& out, summary& s) {
    // NOTE: positions and entries must be stored in NATIVE BYTE ORDER, not BIG-ENDIAN.
    write(out, s.header.min_index_interval,
                  s.header.size,
                  s.header.memory_size,
                  s.header.sampling_level,
                  s.header.size_at_full_sampling);
    for (auto&& e : s.positions) {
        out.write(reinterpret_cast<const char*>(&e), sizeof(e)).get();
    }
    write(out, s.entries);
    write(out, s.first_key, s.last_key);
}

future<summary_entry&> sstable::read_summary_entry(size_t i) {
    // The last one is the boundary marker
    if (i >= (_summary.entries.size())) {
        throw std::out_of_range(sprint("Invalid Summary index: %ld", i));
    }

    return make_ready_future<summary_entry&>(_summary.entries[i]);
}

future<> parse(random_access_reader& in, deletion_time& d) {
    return parse(in, d.local_deletion_time, d.marked_for_delete_at);
}

template <typename Child>
future<> parse(random_access_reader& in, std::unique_ptr<metadata>& p) {
    p.reset(new Child);
    return parse(in, *static_cast<Child *>(p.get()));
}

template <typename Child>
inline void write(file_writer& out, std::unique_ptr<metadata>& p) {
    write(out, *static_cast<Child *>(p.get()));
}

future<> parse(random_access_reader& in, statistics& s) {
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

inline void write(file_writer& out, statistics& s) {
    write(out, s.hash);
    struct kv {
        metadata_type key;
        uint32_t value;
    };
    // sort map by file offset value and store the result into a vector.
    // this is indeed needed because output stream cannot afford random writes.
    auto v = make_shared<std::vector<kv>>();
    v->reserve(s.hash.map.size());
    for (auto val : s.hash.map) {
        kv tmp = { val.first, val.second };
        v->push_back(tmp);
    }
    std::sort(v->begin(), v->end(), [] (kv i, kv j) { return i.value < j.value; });
    for (auto& val: *v) {
        switch (val.key) {
            case metadata_type::Validation:
                write<validation_metadata>(out, s.contents[val.key]);
                break;
            case metadata_type::Compaction:
                write<compaction_metadata>(out, s.contents[val.key]);
                break;
            case metadata_type::Stats:
                write<stats_metadata>(out, s.contents[val.key]);
                break;
            default:
                sstlog.warn("Invalid metadata type at Statistics file: {} ", int(val.key));
                return; // FIXME: should throw
            }
    }
}

future<> parse(random_access_reader& in, estimated_histogram& eh) {
    auto len = std::make_unique<uint32_t>();

    auto f = parse(in, *len);
    return f.then([&in, &eh, len = std::move(len)] {
        uint32_t length = *len;

        if (length == 0) {
            throw malformed_sstable_exception("Estimated histogram with zero size found. Can't continue!");
        }
        eh.bucket_offsets.resize(length - 1);
        eh.buckets.resize(length);

        auto type_size = sizeof(uint64_t) * 2;
        return in.read_exactly(length * type_size).then([&eh, length, type_size] (auto buf) {
            check_buf_size(buf, length * type_size);

            auto *nr = reinterpret_cast<const net::packed<uint64_t> *>(buf.get());
            size_t j = 0;
            for (size_t i = 0; i < length; ++i) {
                eh.bucket_offsets[i == 0 ? 0 : i - 1] = net::ntoh(nr[j++]);
                eh.buckets[i] = net::ntoh(nr[j++]);
            }
            return make_ready_future<>();
        });
    });
}

inline void write(file_writer& out, estimated_histogram& eh) {
    uint32_t len = 0;
    check_truncate_and_assign(len, eh.buckets.size());

    write(out, len);
    struct element {
        uint64_t offsets;
        uint64_t buckets;
    };
    std::vector<element> elements;
    elements.resize(eh.buckets.size());

    auto *offsets_nr = reinterpret_cast<const net::packed<uint64_t> *>(eh.bucket_offsets.data());
    auto *buckets_nr = reinterpret_cast<const net::packed<uint64_t> *>(eh.buckets.data());
    for (size_t i = 0; i < eh.buckets.size(); i++) {
        elements[i].offsets = net::hton(offsets_nr[i == 0 ? 0 : i - 1]);
        elements[i].buckets = net::hton(buckets_nr[i]);
    }

    auto p = reinterpret_cast<const char*>(elements.data());
    auto bytes = elements.size() * sizeof(element);
    out.write(p, bytes).get();
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() {
    if (_components.size()) {
        return make_ready_future<>();
    }

    auto file_path = filename(sstable::component_type::TOC);

    sstlog.debug("Reading TOC file {} ", file_path);

    return open_checked_file_dma(sstable_read_error, file_path, open_flags::ro).then([this] (file f) {
        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
        auto buf = bufptr.get();

        auto fut = f.dma_read(0, buf, 4096);
        return std::move(fut).then([this, f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                try {
                   _components.insert(reverse_map(c, _component_map));
                } catch (std::out_of_range& oor) {
                    throw malformed_sstable_exception("Unrecognized TOC component: " + c);
                }
            }
            if (!_components.size()) {
                throw malformed_sstable_exception("Empty TOC");
            }
            return f.close().finally([f] {});
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

void sstable::generate_toc(compressor c, double filter_fp_chance) {
    // Creating table of components.
    _components.insert(component_type::TOC);
    _components.insert(component_type::Statistics);
    _components.insert(component_type::Digest);
    _components.insert(component_type::Index);
    _components.insert(component_type::Summary);
    _components.insert(component_type::Data);
    if (filter_fp_chance != 1.0) {
        _components.insert(component_type::Filter);
    }
    if (c == compressor::none) {
        _components.insert(component_type::CRC);
    } else {
        _components.insert(component_type::CompressionInfo);
    }
}

void sstable::write_toc(const io_priority_class& pc) {
    auto file_path = filename(sstable::component_type::TemporaryTOC);

    sstlog.debug("Writing TOC file {} ", file_path);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file f = new_sstable_component_file(sstable_write_error, file_path, open_flags::wo | open_flags::create | open_flags::exclusive).get0();

    bool toc_exists = file_exists(filename(sstable::component_type::TOC)).get0();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        f.close().get();
        remove_file(file_path).get();
        throw std::runtime_error(sprint("SSTable write failed due to existence of TOC file for generation %ld of %s.%s", _generation, _ks, _cf));
    }

    file_output_stream_options options;
    options.buffer_size = 4096;
    options.io_priority_class = pc;
    auto w = file_writer(std::move(f), std::move(options));

    for (auto&& key : _components) {
            // new line character is appended to the end of each component name.
        auto value = _component_map[key] + "\n";
        bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
        write(w, b);
    }
    w.flush().get();
    w.close().get();

    // Flushing parent directory to guarantee that temporary TOC file reached
    // the disk.
    file dir_f = open_checked_directory(sstable_write_error, _dir).get0();
    sstable_write_io_check([&] {
        dir_f.flush().get();
        dir_f.close().get();
    });
}

void sstable::seal_sstable() {
    // SSTable sealing is about renaming temporary TOC file after guaranteeing
    // that each component reached the disk safely.

    file dir_f = open_checked_directory(sstable_write_error, _dir).get0();

    sstable_write_io_check([&] {
        // Guarantee that every component of this sstable reached the disk.
        dir_f.flush().get();
        // Rename TOC because it's no longer temporary.
        engine().rename_file(filename(sstable::component_type::TemporaryTOC), filename(sstable::component_type::TOC)).get();
        // Guarantee that the changes above reached the disk.
        dir_f.flush().get();
        dir_f.close().get();
    });
    // If this point was reached, sstable should be safe in disk.
    sstlog.debug("SSTable with generation {} of {}.{} was sealed successfully.", _generation, _ks, _cf);
}

void write_crc(const sstring file_path, checksum& c) {
    sstlog.debug("Writing CRC file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file f = new_sstable_component_file(sstable_write_error, file_path, oflags).get0();

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = file_writer(std::move(f), std::move(options));
    write(w, c);
    w.close().get();
}

// Digest file stores the full checksum of data file converted into a string.
void write_digest(const sstring file_path, uint32_t full_checksum) {
    sstlog.debug("Writing Digest file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    auto f = new_sstable_component_file(sstable_write_error, file_path, oflags).get0();

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = file_writer(std::move(f), std::move(options));

    auto digest = to_sstring<bytes>(full_checksum);
    write(w, digest);
    w.close().get();
}

thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_sample_pattern_cache;
thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_original_index_cache;

future<index_list> sstable::read_indexes(uint64_t summary_idx, const io_priority_class& pc) {
    if (summary_idx >= _summary.header.size) {
        return make_ready_future<index_list>(index_list());
    }

    uint64_t position = _summary.entries[summary_idx].position;
    uint64_t quantity = downsampling::get_effective_index_interval_after_index(summary_idx, _summary.header.sampling_level,
        _summary.header.min_index_interval);

    uint64_t end;
    if (++summary_idx >= _summary.header.size) {
        end = index_size();
    } else {
        end = _summary.entries[summary_idx].position;
    }

    return do_with(index_consumer(quantity), [this, position, end, &pc] (index_consumer& ic) {
        file_input_stream_options options;
        options.buffer_size = sstable_buffer_size;
        options.io_priority_class = pc;
        auto stream = make_file_input_stream(this->_index_file, position, end - position, std::move(options));
        // TODO: it's redundant to constrain the consumer here to stop at
        // index_size()-position, the input stream is already constrained.
        auto ctx = make_lw_shared<index_consume_entry_context<index_consumer>>(ic, std::move(stream), this->index_size() - position);
        return ctx->consume_input(*ctx).then([ctx, &ic] {
            return make_ready_future<index_list>(std::move(ic.indexes));
        });
    });
}

template <sstable::component_type Type, typename T>
future<> sstable::read_simple(T& component, const io_priority_class& pc) {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + _component_map[Type] + " file {} ").c_str(), file_path);
    return open_file_dma(file_path, open_flags::ro).then([this, &component] (file fi) {
        auto f = make_checked_file(sstable_read_error, fi);
        auto r = make_lw_shared<file_random_access_reader>(std::move(f), sstable_buffer_size);
        auto fut = parse(*r, component);
        return fut.finally([r = std::move(r)] {
            return r->close();
        }).then([r] {});
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

template <sstable::component_type Type, typename T>
void sstable::write_simple(T& component, const io_priority_class& pc) {
    auto file_path = filename(Type);
    sstlog.debug(("Writing " + _component_map[Type] + " file {} ").c_str(), file_path);
    file f = new_sstable_component_file(sstable_write_error, file_path, open_flags::wo | open_flags::create | open_flags::exclusive).get0();

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto w = file_writer(std::move(f), std::move(options));
    write(w, component);
    w.flush().get();
    w.close().get();
}

template future<> sstable::read_simple<sstable::component_type::Filter>(sstables::filter& f, const io_priority_class& pc);
template void sstable::write_simple<sstable::component_type::Filter>(sstables::filter& f, const io_priority_class& pc);

future<> sstable::read_compression(const io_priority_class& pc) {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_compression, pc);
}

void sstable::write_compression(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return;
    }

    write_simple<component_type::CompressionInfo>(_compression, pc);
}

future<> sstable::read_statistics(const io_priority_class& pc) {
    return read_simple<component_type::Statistics>(_statistics, pc);
}

void sstable::write_statistics(const io_priority_class& pc) {
    write_simple<component_type::Statistics>(_statistics, pc);
}

future<> sstable::read_summary(const io_priority_class& pc) {
    if (_summary) {
        return make_ready_future<>();
    }

    return read_toc().then([this, &pc] {
        if (has_component(sstable::component_type::Summary)) {
            return read_simple<component_type::Summary>(_summary, pc);
        } else {
           return generate_summary(default_priority_class());
        }
    });
}

future<> sstable::open_data() {
    return when_all(open_checked_file_dma(sstable_read_error, filename(component_type::Index), open_flags::ro),
                    open_checked_file_dma(sstable_read_error, filename(component_type::Data), open_flags::ro))
                    .then([this] (auto files) {
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
        return _data_file.size().then([this] (auto size) {
            if (this->has_component(sstable::component_type::CompressionInfo)) {
                _compression.update(size);
            } else {
                _data_file_size = size;
            }
        }).then([this] {
            return _index_file.size().then([this] (auto size) {
              _index_file_size = size;
            });
        }).then([this] {
            // Get disk usage for this sstable (includes all components).
            _bytes_on_disk = 0;
            return do_for_each(_components, [this] (component_type c) {
                return sstable_write_io_check([&] {
                    return engine().file_size(this->filename(c));
                }).then([this] (uint64_t bytes) {
                    _bytes_on_disk += bytes;
                });
            });
        });

    });
}

future<> sstable::create_data() {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file_open_options opt;
    opt.extent_allocation_size_hint = 32 << 20;
    return when_all(new_sstable_component_file(sstable_write_error, filename(component_type::Index), oflags),
                    new_sstable_component_file(sstable_write_error, filename(component_type::Data), oflags, opt)).then([this] (auto files) {
        // FIXME: If both files could not be created, the first get below will
        // throw an exception, and second get() will not be attempted, and
        // we'll get a warning about the second future being destructed
        // without its exception being examined.
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
    });
}

// This interface is only used during tests, snapshot loading and early initialization.
// No need to set tunable priorities for it.
future<> sstable::load() {
    return read_toc().then([this] {
        return read_statistics(default_priority_class());
    }).then([this] {
        return read_compression(default_priority_class());
    }).then([this] {
        return read_filter(default_priority_class());
    }).then([this] {;
        return read_summary(default_priority_class());
    }).then([this] {
        return open_data();
    });
}

// @clustering_key: it's expected that clustering key is already in its composite form.
// NOTE: empty clustering key means that there is no clustering key.
void sstable::write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite_marker m) {
    // FIXME: min_components and max_components also keep track of clustering
    // prefix, so we must merge clustering_key and column_names somehow and
    // pass the result to the functions below.
    column_name_helper::min_max_components(_c_stats.min_column_names, _c_stats.max_column_names, column_names);

    // was defined in the schema, for example.
    auto c= composite::from_exploded(column_names, m);
    auto ck_bview = bytes_view(clustering_key);

    // The marker is not a component, so if the last component is empty (IOW,
    // only serializes to the marker), then we just replace the key's last byte
    // with the marker. If the component however it is not empty, then the
    // marker should be in the end of it, and we just join them together as we
    // do for any normal component
    if (c.size() == 1) {
        ck_bview.remove_suffix(1);
    }
    size_t sz = ck_bview.size() + c.size();
    if (sz > std::numeric_limits<uint16_t>::max()) {
        throw std::runtime_error(sprint("Column name too large (%d > %d)", sz, std::numeric_limits<uint16_t>::max()));
    }
    uint16_t sz16 = sz;
    write(out, sz16, ck_bview, c);
}

void sstable::write_column_name(file_writer& out, bytes_view column_names) {
    column_name_helper::min_max_components(_c_stats.min_column_names, _c_stats.max_column_names, { column_names });

    size_t sz = column_names.size();
    if (sz > std::numeric_limits<uint16_t>::max()) {
        throw std::runtime_error(sprint("Column name too large (%d > %d)", sz, std::numeric_limits<uint16_t>::max()));
    }
    uint16_t sz16 = sz;
    write(out, sz16, column_names);
}


static inline void update_cell_stats(column_stats& c_stats, uint64_t timestamp) {
    c_stats.update_min_timestamp(timestamp);
    c_stats.update_max_timestamp(timestamp);
    c_stats.column_count++;
}

// Intended to write all cell components that follow column name.
void sstable::write_cell(file_writer& out, atomic_cell_view cell) {
    // FIXME: counter cell isn't supported yet.

    uint64_t timestamp = cell.timestamp();

    update_cell_stats(_c_stats, timestamp);

    if (cell.is_dead(_now)) {
        // tombstone cell

        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = cell.deletion_time().time_since_epoch().count();

        _c_stats.tombstone_histogram.update(deletion_time);

        write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (cell.is_live_and_has_ttl()) {
        // expiring cell

        column_mask mask = column_mask::expiration;
        uint32_t ttl = cell.ttl().count();
        uint32_t expiration = cell.expiry().time_since_epoch().count();
        disk_string_view<uint32_t> cell_value { cell.value() };

        write(out, mask, ttl, expiration, timestamp, cell_value);
    } else {
        // regular cell

        column_mask mask = column_mask::none;
        disk_string_view<uint32_t> cell_value { cell.value() };

        write(out, mask, timestamp, cell_value);
    }
}

void sstable::write_row_marker(file_writer& out, const rows_entry& clustered_row, const composite& clustering_key) {
    const auto& marker = clustered_row.row().marker();
    if (marker.is_missing()) {
        return;
    }

    // Write row mark cell to the beginning of clustered row.
    write_column_name(out, clustering_key, { bytes_view() });
    uint64_t timestamp = marker.timestamp();
    uint32_t value_length = 0;

    update_cell_stats(_c_stats, timestamp);

    if (marker.is_dead(_now)) {
        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = marker.deletion_time().time_since_epoch().count();

        _c_stats.tombstone_histogram.update(deletion_time);

        write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (marker.is_expiring()) {
        column_mask mask = column_mask::expiration;
        uint32_t ttl = marker.ttl().count();
        uint32_t expiration = marker.expiry().time_since_epoch().count();
        write(out, mask, ttl, expiration, timestamp, value_length);
    } else {
        column_mask mask = column_mask::none;
        write(out, mask, timestamp, value_length);
    }
}

void sstable::write_range_tombstone(file_writer& out, const composite& clustering_prefix, std::vector<bytes_view> suffix, const tombstone t) {
    if (!t) {
        return;
    }

    write_column_name(out, clustering_prefix, suffix, composite_marker::start_range);
    column_mask mask = column_mask::range_tombstone;
    write(out, mask);
    write_column_name(out, clustering_prefix, suffix, composite_marker::end_range);
    uint64_t timestamp = t.timestamp;
    uint32_t deletion_time = t.deletion_time.time_since_epoch().count();

    update_cell_stats(_c_stats, timestamp);
    _c_stats.tombstone_histogram.update(deletion_time);

    write(out, deletion_time, timestamp);
}

void sstable::write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection) {

    auto t = static_pointer_cast<const collection_type_impl>(cdef.type);
    auto mview = t->deserialize_mutation_form(collection);
    const bytes& column_name = cdef.name();
    write_range_tombstone(out, clustering_key, { bytes_view(column_name) }, mview.tomb);
    for (auto& cp: mview.cells) {
        write_column_name(out, clustering_key, { column_name, cp.first });
        write_cell(out, cp.second);
    }
}

// write_datafile_clustered_row() is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
void sstable::write_clustered_row(file_writer& out, const schema& schema, const rows_entry& clustered_row) {
    auto clustering_key = composite::from_clustering_element(schema, clustered_row.key());

    if (schema.is_compound() && !schema.is_dense()) {
        write_row_marker(out, clustered_row, clustering_key);
    }
    // Before writing cells, range tombstone must be written if the row has any (deletable_row::t).
    if (clustered_row.row().deleted_at()) {
        write_range_tombstone(out, clustering_key, {}, clustered_row.row().deleted_at());
    }

    // Write all cells of a partition's row.
    clustered_row.row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = schema.regular_column_at(id);
        // non atomic cell isn't supported yet. atomic cell maps to a single trift cell.
        // non atomic cell maps to multiple trift cell, e.g. collection.
        if (!column_definition.is_atomic()) {
            write_collection(out, clustering_key, column_definition, c.as_collection_mutation());
            return;
        }
        assert(column_definition.is_regular());
        atomic_cell_view cell = c.as_atomic_cell();
        const bytes& column_name = column_definition.name();

        if (schema.is_compound()) {
            if (schema.is_dense()) {
                write_column_name(out, bytes_view(clustering_key));
            } else {
                write_column_name(out, clustering_key, { bytes_view(column_name) });
            }
        } else {
            if (schema.is_dense()) {
                write_column_name(out, bytes_view(clustered_row.key().get_component(schema, 0)));
            } else {
                write_column_name(out, bytes_view(column_name));
            }
        }
        write_cell(out, cell);
    });
}

void sstable::write_static_row(file_writer& out, const schema& schema, const row& static_row) {
    static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = schema.static_column_at(id);
        if (!column_definition.is_atomic()) {
            auto sp = composite::static_prefix(schema);
            write_collection(out, sp, column_definition, c.as_collection_mutation());
            return;
        }
        assert(column_definition.is_static());
        atomic_cell_view cell = c.as_atomic_cell();
        auto sp = composite::static_prefix(schema);
        write_column_name(out, sp, { bytes_view(column_definition.name()) });
        write_cell(out, cell);
    });
}

static void write_index_entry(file_writer& out, disk_string_view<uint16_t>& key, uint64_t pos) {
    // FIXME: support promoted indexes.
    uint32_t promoted_index_size = 0;

    write(out, key, pos, promoted_index_size);
}

static void prepare_summary(summary& s, uint64_t expected_partition_count, uint32_t min_index_interval) {
    assert(expected_partition_count >= 1);

    s.header.min_index_interval = min_index_interval;
    s.header.sampling_level = downsampling::BASE_SAMPLING_LEVEL;
    uint64_t max_expected_entries =
            (expected_partition_count / min_index_interval) +
            !!(expected_partition_count % min_index_interval);
    // FIXME: handle case where max_expected_entries is greater than max value stored by uint32_t.
    if (max_expected_entries > std::numeric_limits<uint32_t>::max()) {
        throw malformed_sstable_exception("Current sampling level (" + to_sstring(downsampling::BASE_SAMPLING_LEVEL) + ") not enough to generate summary.");
    }

    s.keys_written = 0;
    s.header.memory_size = 0;
}

static void seal_summary(summary& s,
        std::experimental::optional<key>&& first_key,
        std::experimental::optional<key>&& last_key) {
    s.header.size = s.entries.size();
    s.header.size_at_full_sampling = s.header.size;

    s.header.memory_size = s.header.size * sizeof(uint32_t);
    for (auto& e: s.entries) {
        s.positions.push_back(s.header.memory_size);
        s.header.memory_size += e.key.size() + sizeof(e.position);
    }
    assert(first_key); // assume non-empty sstable
    s.first_key.value = first_key->get_bytes();

    if (last_key) {
        s.last_key.value = last_key->get_bytes();
    } else {
        // An empty last_mutation indicates we had just one partition
        s.last_key.value = s.first_key.value;
    }
}

static void prepare_compression(compression& c, const schema& schema) {
    const auto& cp = schema.get_compressor_params();
    c.set_compressor(cp.get_compressor());
    c.chunk_len = cp.chunk_length();
    c.data_len = 0;
    // FIXME: crc_check_chance can be configured by the user.
    // probability to verify the checksum of a compressed chunk we read.
    // defaults to 1.0.
    c.options.elements.push_back({"crc_check_chance", "1.0"});
    c.init_full_checksum();
}

static void maybe_add_summary_entry(summary& s, bytes_view key, uint64_t offset) {
    // Maybe add summary entry into in-memory representation of summary file.
    if ((s.keys_written++ % s.header.min_index_interval) == 0) {
        s.entries.push_back({ bytes(key.data(), key.size()), offset });
    }
}

// In the beginning of the statistics file, there is a disk_hash used to
// map each metadata type to its correspondent position in the file.
static void seal_statistics(statistics& s, metadata_collector& collector,
        const sstring partitioner, double bloom_filter_fp_chance) {
    static constexpr int METADATA_TYPE_COUNT = 3;

    size_t old_offset, offset = 0;
    // account disk_hash size.
    offset += sizeof(uint32_t);
    // account disk_hash members.
    offset += (METADATA_TYPE_COUNT * (sizeof(metadata_type) + sizeof(uint32_t)));

    validation_metadata validation;
    compaction_metadata compaction;
    stats_metadata stats;

    old_offset = offset;
    validation.partitioner.value = to_bytes(partitioner);
    validation.filter_chance = bloom_filter_fp_chance;
    offset += validation.serialized_size();
    s.contents[metadata_type::Validation] = std::make_unique<validation_metadata>(std::move(validation));
    s.hash.map[metadata_type::Validation] = old_offset;

    old_offset = offset;
    collector.construct_compaction(compaction);
    offset += compaction.serialized_size();
    s.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>(std::move(compaction));
    s.hash.map[metadata_type::Compaction] = old_offset;

    collector.construct_stats(stats);
    // NOTE: method serialized_size of stats_metadata must be implemented for
    // a new type of compaction to get supported.
    s.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
    s.hash.map[metadata_type::Stats] = offset;
}

///
///  @param out holds an output stream to data file.
///
void sstable::do_write_components(::mutation_reader mr,
        uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size, file_writer& out,
        const io_priority_class& pc) {
    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto index = make_shared<file_writer>(_index_file, std::move(options));

    auto filter_fp_chance = schema->bloom_filter_fp_chance();
    _filter = utils::i_filter::get_filter(estimated_partitions, filter_fp_chance);

    prepare_summary(_summary, estimated_partitions, schema->min_index_interval());

    // FIXME: we may need to set repaired_at stats at this point.

    // Remember first and last keys, which we need for the summary file.
    std::experimental::optional<key> first_key, last_key;

    // Iterate through CQL partitions, then CQL rows, then CQL columns.
    // Each mt.all_partitions() entry is a set of clustered rows sharing the same partition key.
    while (out.offset() < max_sstable_size) {
        mutation_opt mut = mr().get0();
        if (!mut) {
            break;
        }

        // Set current index of data to later compute row size.
        _c_stats.start_offset = out.offset();

        auto partition_key = key::from_partition_key(*schema, mut->key());

        maybe_add_summary_entry(_summary, bytes_view(partition_key), index->offset());
        _filter->add(bytes_view(partition_key));
        _collector.add_key(bytes_view(partition_key));

        auto p_key = disk_string_view<uint16_t>();
        p_key.value = bytes_view(partition_key);

        // Write index file entry from partition key into index file.
        write_index_entry(*index, p_key, out.offset());

        // Write partition key into data file.
        write(out, p_key);

        auto tombstone = mut->partition().partition_tombstone();
        deletion_time d;

        if (tombstone) {
            d.local_deletion_time = tombstone.deletion_time.time_since_epoch().count();
            d.marked_for_delete_at = tombstone.timestamp;

            _c_stats.tombstone_histogram.update(d.local_deletion_time);
            _c_stats.update_max_local_deletion_time(d.local_deletion_time);
            _c_stats.update_min_timestamp(d.marked_for_delete_at);
            _c_stats.update_max_timestamp(d.marked_for_delete_at);
        } else {
            // Default values for live, undeleted rows.
            d.local_deletion_time = std::numeric_limits<int32_t>::max();
            d.marked_for_delete_at = std::numeric_limits<int64_t>::min();
        }
        write(out, d);

        auto& partition = mut->partition();
        auto& static_row = partition.static_row();

        write_static_row(out, *schema, static_row);
        for (const auto& rt: partition.row_tombstones()) {
            auto prefix = composite::from_clustering_element(*schema, rt.prefix());
            write_range_tombstone(out, prefix, {}, rt.t());
        }

        // Write all CQL rows from a given mutation partition.
        for (auto& clustered_row: partition.clustered_rows()) {
            write_clustered_row(out, *schema, clustered_row);
        }
        int16_t end_of_row = 0;
        write(out, end_of_row);

        // compute size of the current row.
        _c_stats.row_size = out.offset() - _c_stats.start_offset;
        // update is about merging column_stats with the data being stored by collector.
        _collector.update(std::move(_c_stats));
        _c_stats.reset();

        if (!first_key) {
            first_key = std::move(partition_key);
        } else {
            last_key = std::move(partition_key);
        }

    }
    seal_summary(_summary, std::move(first_key), std::move(last_key));

    index->close().get();
    _index_file = file(); // index->close() closed _index_file

    if (has_component(sstable::component_type::CompressionInfo)) {
        _collector.add_compression_ratio(_compression.compressed_file_length(), _compression.uncompressed_file_length());
    }

    // NOTE: Cassandra gets partition name by calling getClass().getCanonicalName() on
    // partition class.
    seal_statistics(_statistics, _collector, dht::global_partitioner().name(), filter_fp_chance);
}

void sstable::prepare_write_components(::mutation_reader mr, uint64_t estimated_partitions, schema_ptr schema,
        uint64_t max_sstable_size, const io_priority_class& pc) {
    // CRC component must only be present when compression isn't enabled.
    bool checksum_file = has_component(sstable::component_type::CRC);

    if (checksum_file) {
        file_output_stream_options options;
        options.buffer_size = sstable_buffer_size;
        options.io_priority_class = pc;

        auto w = make_shared<checksummed_file_writer>(_data_file, std::move(options), checksum_file);
        this->do_write_components(std::move(mr), estimated_partitions, std::move(schema), max_sstable_size, *w, pc);
        w->close().get();
        _data_file = file(); // w->close() closed _data_file

        write_digest(filename(sstable::component_type::Digest), w->full_checksum());
        write_crc(filename(sstable::component_type::CRC), w->finalize_checksum());
    } else {
        file_output_stream_options options;
        options.io_priority_class = pc;

        prepare_compression(_compression, *schema);
        auto w = make_shared<file_writer>(make_compressed_file_output_stream(_data_file, std::move(options), &_compression));
        this->do_write_components(std::move(mr), estimated_partitions, std::move(schema), max_sstable_size, *w, pc);
        w->close().get();
        _data_file = file(); // w->close() closed _data_file

        write_digest(filename(sstable::component_type::Digest), _compression.full_checksum());
    }
}

future<> sstable::write_components(memtable& mt, bool backup, const io_priority_class& pc) {
    _collector.set_replay_position(mt.replay_position());
    return write_components(mt.make_reader(mt.schema()),
            mt.partition_count(), mt.schema(), std::numeric_limits<uint64_t>::max(), backup, pc);
}

future<> sstable::write_components(::mutation_reader mr,
        uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size, bool backup, const io_priority_class& pc) {
    return seastar::async([this, mr = std::move(mr), estimated_partitions, schema = std::move(schema), max_sstable_size, backup, &pc] () mutable {
        generate_toc(schema->get_compressor_params().get_compressor(), schema->bloom_filter_fp_chance());
        write_toc(pc);
        create_data().get();
        prepare_write_components(std::move(mr), estimated_partitions, std::move(schema), max_sstable_size, pc);
        write_summary(pc);
        write_filter(pc);
        write_statistics(pc);
        // NOTE: write_compression means maybe_write_compression.
        write_compression(pc);
        seal_sstable();

        if (backup) {
            auto dir = get_dir() + "/backups/";
            sstable_write_io_check(touch_directory, dir).get();
            create_links(dir).get();
        }
    });
}

future<> sstable::generate_summary(const io_priority_class& pc) {
    if (_summary) {
        return make_ready_future<>();
    }

    sstlog.info("Summary file {} not found. Generating Summary...", filename(sstable::component_type::Summary));
    class summary_generator {
        summary& _summary;
    public:
        std::experimental::optional<key> first_key, last_key;

        summary_generator(summary& s) : _summary(s) {}
        bool should_continue() {
            return true;
        }
        void consume_entry(index_entry&& ie) {
            maybe_add_summary_entry(_summary, ie.get_key_bytes(), ie.position());
            if (!first_key) {
                first_key = key(to_bytes(ie.get_key_bytes()));
            } else {
                last_key = key(to_bytes(ie.get_key_bytes()));
            }
        }
    };

    return open_checked_file_dma(sstable_read_error, filename(component_type::Index), open_flags::ro).then([this, &pc] (file index_file) {
        return do_with(std::move(index_file), [this, &pc] (file index_file) {
            return index_file.size().then([this, &pc, index_file] (auto size) {
                // an upper bound. Surely to be less than this.
                auto estimated_partitions = size / sizeof(uint64_t);
                // Since we don't have a summary, use a default min_index_interval, and if needed we'll resample
                // later.
                prepare_summary(_summary, estimated_partitions, 0x80);

                file_input_stream_options options;
                options.buffer_size = sstable_buffer_size;
                options.io_priority_class = pc;
                auto stream = make_file_input_stream(index_file, 0, size, std::move(options));
                return do_with(summary_generator(_summary), [this, &pc, stream = std::move(stream), size] (summary_generator& s) mutable {
                    auto ctx = make_lw_shared<index_consume_entry_context<summary_generator>>(s, std::move(stream), size);
                    return ctx->consume_input(*ctx).then([this, ctx, &s] {
                        seal_summary(_summary, std::move(s.first_key), std::move(s.last_key));
                    });
                });
            });
        });
    });
}

uint64_t sstable::data_size() const {
    if (has_component(sstable::component_type::CompressionInfo)) {
        return _compression.data_len;
    }
    return _data_file_size;
}

uint64_t sstable::bytes_on_disk() {
    assert(_bytes_on_disk > 0);
    return _bytes_on_disk;
}

const bool sstable::has_component(component_type f) const {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) const {
    return filename(_dir, _ks, _cf, _version, _generation, _format, f);
}

std::vector<sstring> sstable::component_filenames() const {
    std::vector<sstring> res;
    for (auto c : _component_map | boost::adaptors::map_keys) {
        if (has_component(c)) {
            res.emplace_back(filename(c));
        }
    }
    return res;
}

sstring sstable::toc_filename() const {
    return filename(component_type::TOC);
}

const sstring sstable::filename(sstring dir, sstring ks, sstring cf, version_types version, int64_t generation,
                                format_types format, component_type component) {

    static std::unordered_map<version_types, std::function<sstring (entry_descriptor d)>, enum_hash<version_types>> strmap = {
        { sstable::version_types::ka, [] (entry_descriptor d) {
            return d.ks + "-" + d.cf + "-" + _version_string.at(d.version) + "-" + to_sstring(d.generation) + "-" + _component_map.at(d.component); }
        },
        { sstable::version_types::la, [] (entry_descriptor d) {
            return _version_string.at(d.version) + "-" + to_sstring(d.generation) + "-" + _format_string.at(d.format) + "-" + _component_map.at(d.component); }
        }
    };

    return dir + "/" + strmap[version](entry_descriptor(ks, cf, version, generation, format, component));
}

future<> sstable::create_links(sstring dir, int64_t generation) const {
    // TemporaryTOC is always first, TOC is always last
    auto dst = sstable::filename(dir, _ks, _cf, _version, generation, _format, component_type::TemporaryTOC);
    return sstable_write_io_check(::link_file, filename(component_type::TOC), dst).then([dir] {
        return sstable_write_io_check(sync_directory, dir);
    }).then([this, dir, generation] {
        // FIXME: Should clean already-created links if we failed midway.
        return parallel_for_each(_components, [this, dir, generation] (auto comp) {
            if (comp == component_type::TOC) {
                return make_ready_future<>();
            }
            auto dst = sstable::filename(dir, _ks, _cf, _version, generation, _format, comp);
            return sstable_write_io_check(::link_file, this->filename(comp), dst);
        });
    }).then([dir] {
        return sstable_write_io_check(sync_directory, dir);
    }).then([dir, this, generation] {
        auto src = sstable::filename(dir, _ks, _cf, _version, generation, _format, component_type::TemporaryTOC);
        auto dst = sstable::filename(dir, _ks, _cf, _version, generation, _format, component_type::TOC);
        return sstable_write_io_check([&] {
            return engine().rename_file(src, dst);
        });
    }).then([dir] {
        return sstable_write_io_check(sync_directory, dir);
    });
}

future<> sstable::set_generation(int64_t new_generation) {
    return create_links(_dir, new_generation).then([this] {
        return remove_file(filename(component_type::TOC)).then([this] {
            return sstable_write_io_check(sync_directory, _dir);
        }).then([this] {
            return parallel_for_each(_components, [this] (auto comp) {
                if (comp == component_type::TOC) {
                    return make_ready_future<>();
                }
                return remove_file(this->filename(comp));
            });
        });
    }).then([this, new_generation] {
        return sync_directory(_dir).then([this, new_generation] {
            _generation = new_generation;
        });
    });
}

entry_descriptor entry_descriptor::make_descriptor(sstring fname) {
    static std::regex la("la-(\\d+)-(\\w+)-(.*)");
    static std::regex ka("(\\w+)-(\\w+)-ka-(\\d+)-(.*)");

    std::smatch match;

    sstable::version_types version;

    sstring generation;
    sstring format;
    sstring component;
    sstring ks;
    sstring cf;

    std::string s(fname);
    if (std::regex_match(s, match, la)) {
        sstring ks = "";
        sstring cf = "";
        version = sstable::version_types::la;
        generation = match[1].str();
        format = sstring(match[2].str());
        component = sstring(match[3].str());
    } else if (std::regex_match(s, match, ka)) {
        ks = match[1].str();
        cf = match[2].str();
        version = sstable::version_types::ka;
        format = sstring("big");
        generation = match[3].str();
        component = sstring(match[4].str());
    } else {
        throw malformed_sstable_exception(sprint("invalid version for file %s. Name doesn't match any known version.", fname));
    }
    return entry_descriptor(ks, cf, version, boost::lexical_cast<unsigned long>(generation), sstable::format_from_sstring(format), sstable::component_from_sstring(component));
}

sstable::version_types sstable::version_from_sstring(sstring &s) {
    return reverse_map(s, _version_string);
}

sstable::format_types sstable::format_from_sstring(sstring &s) {
    return reverse_map(s, _format_string);
}

sstable::component_type sstable::component_from_sstring(sstring &s) {
    return reverse_map(s, _component_map);
}

// NOTE: Prefer using data_stream() if you know the byte position at which the
// read will stop. Knowing the end allows data_stream() to use a large a read-
// ahead buffer before reaching the end, but not over-read at the end, so
// data_stream() is more efficient than data_stream_at().
input_stream<char> sstable::data_stream_at(uint64_t pos, uint64_t buf_size, const io_priority_class& pc) {
    file_input_stream_options options;
    options.buffer_size = buf_size;
    options.io_priority_class = pc;
    if (_compression) {
        return make_compressed_file_input_stream(_data_file, &_compression,
                pos, _compression.data_len - pos, std::move(options));
    } else {
        return make_file_input_stream(_data_file, pos, std::move(options));
    }
}

input_stream<char> sstable::data_stream(uint64_t pos, size_t len, const io_priority_class& pc) {
    file_input_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    if (_compression) {
        return make_compressed_file_input_stream(_data_file, &_compression,
                pos, len, std::move(options));
    } else {
        return make_file_input_stream(_data_file, pos, len, std::move(options));
    }
}

future<temporary_buffer<char>> sstable::data_read(uint64_t pos, size_t len, const io_priority_class& pc) {
    return do_with(data_stream(pos, len, pc), [len] (auto& stream) {
        return stream.read_exactly(len);
    });
}

partition_key
sstable::get_first_partition_key(const schema& s) const {
    if (_summary.first_key.value.empty()) {
        throw std::runtime_error("first key of summary is empty");
    }
    return key::from_bytes(_summary.first_key.value).to_partition_key(s);
}

partition_key
sstable::get_last_partition_key(const schema& s) const {
    if (_summary.last_key.value.empty()) {
        throw std::runtime_error("last key of summary is empty");
    }
    return key::from_bytes(_summary.last_key.value).to_partition_key(s);
}

dht::decorated_key sstable::get_first_decorated_key(const schema& s) const {
    // FIXME: we can avoid generating the decorated key over and over again by
    // storing it in the sstable object. The same applies to last().
    auto pk = get_first_partition_key(s);
    return dht::global_partitioner().decorate_key(s, std::move(pk));
}

dht::decorated_key sstable::get_last_decorated_key(const schema& s) const {
    auto pk = get_last_partition_key(s);
    return dht::global_partitioner().decorate_key(s, std::move(pk));
}

int sstable::compare_by_first_key(const schema& s, const sstable& other) const {
    return get_first_decorated_key(s).tri_compare(s, other.get_first_decorated_key(s));
}

double sstable::get_compression_ratio() const {
    if (this->has_component(sstable::component_type::CompressionInfo)) {
        return (double) _compression.compressed_file_length() / _compression.uncompressed_file_length();
    } else {
        return metadata_collector::NO_COMPRESSION_RATIO;
    }
}

future<> sstable::mutate_sstable_level(uint32_t new_level) {
    if (!has_component(component_type::Statistics)) {
        return make_ready_future<>();
    }

    auto entry = _statistics.contents.find(metadata_type::Stats);
    if (entry == _statistics.contents.end()) {
        return make_ready_future<>();
    }

    auto& p = entry->second;
    if (!p) {
        throw std::runtime_error("Statistics is malformed");
    }
    stats_metadata& s = *static_cast<stats_metadata *>(p.get());
    if (s.sstable_level == new_level) {
        return make_ready_future<>();
    }

    s.sstable_level = new_level;
    // Technically we don't have to write the whole file again. But the assumption that
    // we will always write sequentially is a powerful one, and this does not merit an
    // exception.
    return seastar::async([this] {
        // This is not part of the standard memtable flush path, but there is no reason
        // to come up with a class just for that. It is used by the snapshot/restore mechanism
        // which comprises mostly hard link creation and this operation at the end + this operation,
        // and also (eventually) by some compaction strategy. In any of the cases, it won't be high
        // priority enough so we will use the default priority
        write_statistics(default_priority_class());
    });
}

int sstable::compare_by_max_timestamp(const sstable& other) const {
    auto ts1 = get_stats_metadata().max_timestamp;
    auto ts2 = other.get_stats_metadata().max_timestamp;
    return (ts1 > ts2 ? 1 : (ts1 == ts2 ? 0 : -1));
}

sstable::~sstable() {
    if (_index_file) {
        _index_file.close().handle_exception([save = _index_file, op = background_jobs().start()] (auto ep) {
            sstlog.warn("sstable close index_file failed: {}", ep);
            general_disk_error();
        });
    }
    if (_data_file) {
        _data_file.close().handle_exception([save = _data_file, op = background_jobs().start()] (auto ep) {
            sstlog.warn("sstable close data_file failed: {}", ep);
            general_disk_error();
        });
    }

    if (_marked_for_deletion) {
        // We need to delete the on-disk files for this table. Since this is a
        // destructor, we can't wait for this to finish, or return any errors,
        // but just need to do our best. If a deletion fails for some reason we
        // log and ignore this failure, because on startup we'll again try to
        // clean up unused sstables, and because we'll never reuse the same
        // generation number anyway.
        try {
            shared_remove_by_toc_name(filename(component_type::TOC), _shared).handle_exception(
                        [op = background_jobs().start()] (std::exception_ptr eptr) {
                            sstlog.warn("Exception when deleting sstable file: {}", eptr);
                        });
        } catch (...) {
            sstlog.warn("Exception when deleting sstable file: {}", std::current_exception());
        }

    }
}

sstring
dirname(sstring fname) {
    return boost::filesystem::canonical(std::string(fname)).parent_path().string();
}

future<>
sstable::shared_remove_by_toc_name(sstring toc_name, bool shared) {
    if (!shared) {
        return remove_by_toc_name(toc_name);
    } else {
        auto shard = std::hash<sstring>()(toc_name) % smp::count;
        return smp::submit_to(shard, [toc_name, src_shard = engine().cpu_id()] {
            auto& remove_set = _shards_agreeing_to_remove_sstable[toc_name];
            remove_set.insert(src_shard);
            auto counter = remove_set.size();
            if (counter == smp::count) {
                _shards_agreeing_to_remove_sstable.erase(toc_name);
                return remove_by_toc_name(toc_name);
            } else {
                return make_ready_future<>();
            }
        });
    }
}

future<>
fsync_directory(sstring fname) {
    return sstable_write_io_check([&] {
        return open_checked_directory(sstable_write_error ,dirname(fname)).then([] (file f) {
            return do_with(std::move(f), [] (file& f) {
                return f.flush();
            });
        });
    });
}

future<>
remove_by_toc_name(sstring sstable_toc_name) {
    return seastar::async([sstable_toc_name] {
        sstring prefix = sstable_toc_name.substr(0, sstable_toc_name.size() - TOC_SUFFIX.size());
        auto new_toc_name = prefix + TEMPORARY_TOC_SUFFIX;
        sstring dir;

        if (sstable_write_io_check(file_exists, sstable_toc_name).get0()) {
            dir = dirname(sstable_toc_name);
            sstable_write_io_check(rename_file, sstable_toc_name, new_toc_name).get();
            sstable_write_io_check(fsync_directory, dir).get();
        } else {
            dir = dirname(new_toc_name);
        }

        auto toc_file = open_checked_file_dma(sstable_read_error, new_toc_name, open_flags::ro).get0();
        auto in = make_file_input_stream(toc_file);
        auto size = toc_file.size().get0();
        auto text = in.read_exactly(size).get0();
        in.close().get();
        std::vector<sstring> components;
        sstring all(text.begin(), text.end());
        boost::split(components, all, boost::is_any_of("\n"));
        parallel_for_each(components, [prefix] (sstring component) {
            if (component.empty()) {
                // eof
                return make_ready_future<>();
            }
            if (component == TOC_SUFFIX) {
                // already deleted
                return make_ready_future<>();
            }
            auto fname = prefix + component;
            return sstable_write_io_check(remove_file, prefix + component).then_wrapped([fname = std::move(fname)] (future<> f) {
                // forgive ENOENT, since the component may not have been written;
                try {
                    f.get();
                } catch (std::system_error& e) {
                    if (!is_system_error_errno(ENOENT)) {
                        throw;
                    }
                    sstlog.debug("Forgiving ENOENT when deleting file {}", fname);
                }
                return make_ready_future<>();
            });
        }).get();
        sstable_write_io_check(fsync_directory, dir).get();
        sstable_write_io_check(remove_file, new_toc_name).get();
    });
}

future<>
sstable::mark_for_deletion_on_disk() {
    mark_for_deletion();

    auto toc_name = filename(component_type::TOC);
    auto shard = std::hash<sstring>()(toc_name) % smp::count;

    return smp::submit_to(shard, [toc_name] {
        static thread_local std::unordered_set<sstring> renaming;

        if (renaming.count(toc_name) > 0) {
            return make_ready_future<>();
        }

        renaming.emplace(toc_name);

        return seastar::async([toc_name] {
            if (!sstable_write_io_check(file_exists, toc_name).get0()) {
                return; // already gone
            }

            auto dir = dirname(toc_name);
            auto toc_file = open_checked_file_dma(sstable_read_error, toc_name, open_flags::ro).get0();
            sstring prefix = toc_name.substr(0, toc_name.size() - TOC_SUFFIX.size());
            auto new_toc_name = prefix + TEMPORARY_TOC_SUFFIX;
            sstable_write_io_check(rename_file, toc_name, new_toc_name).get();
            sstable_write_io_check(fsync_directory, dir).get();
        }).finally([toc_name] {
            renaming.erase(toc_name);
        });
    });
}

future<>
sstable::remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f) {
    return seastar::async([ks, cf, dir, generation, v, f] {
        auto toc = sstable_write_io_check(file_exists, filename(dir, ks, cf, v, generation, f, component_type::TOC)).get0();
        // assert that toc doesn't exist for sstable with temporary toc.
        assert(toc == false);

        auto tmptoc = sstable_write_io_check(file_exists, filename(dir, ks, cf, v, generation, f, component_type::TemporaryTOC)).get0();
        // assert that temporary toc exists for this sstable.
        assert(tmptoc == true);

        sstlog.warn("Deleting components of sstable from {}.{} of generation {} that has a temporary TOC", ks, cf, generation);

        for (auto& entry : sstable::_component_map) {
            // Skipping TemporaryTOC because it must be the last component to
            // be deleted, and unordered map doesn't guarantee ordering.
            // This is needed because we may end up with a partial delete in
            // event of a power failure.
            // If TemporaryTOC is deleted prematurely and scylla crashes,
            // the subsequent boot would fail because of that generation
            // missing a TOC.
            if (entry.first == component_type::TemporaryTOC) {
                continue;
            }

            auto file_path = filename(dir, ks, cf, v, generation, f, entry.first);
            // Skip component that doesn't exist.
            auto exists = sstable_write_io_check(file_exists, file_path).get0();
            if (!exists) {
                continue;
            }
            sstable_write_io_check(remove_file, file_path).get();
        }
        sstable_write_io_check(fsync_directory, dir).get();
        // Removing temporary
        sstable_write_io_check(remove_file, filename(dir, ks, cf, v, generation, f, component_type::TemporaryTOC)).get();
        // Fsync'ing column family dir to guarantee that deletion completed.
        sstable_write_io_check(fsync_directory, dir).get();
    });
}

future<range<partition_key>>
sstable::get_sstable_key_range(const schema& s) {
    auto fut = read_summary(default_priority_class());
    return std::move(fut).then([this, &s] () mutable {
        auto first = get_first_partition_key(s);
        auto last = get_last_partition_key(s);
        return make_ready_future<range<partition_key>>(range<partition_key>::make(first, last));
    });
}

void sstable::mark_sstable_for_deletion(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f) {
    auto sst = sstable(ks, cf, dir, generation, v, f);
    sst.mark_for_deletion();
}

std::ostream&
operator<<(std::ostream& os, const sstable_to_delete& std) {
    return os << std.name << "(" << (std.shared ? "shared" : "unshared") << ")";
}

using shards_agreeing_to_delete_sstable_type = std::unordered_set<shard_id>;
using sstables_to_delete_atomically_type = std::set<sstring>;
struct pending_deletion {
    sstables_to_delete_atomically_type names;
    std::vector<lw_shared_ptr<promise<>>> completions;
};

static thread_local bool g_atomic_deletions_cancelled = false;
static thread_local std::list<lw_shared_ptr<pending_deletion>> g_atomic_deletion_sets;
static thread_local std::unordered_map<sstring, shards_agreeing_to_delete_sstable_type> g_shards_agreeing_to_delete_sstable;

static logging::logger deletion_logger("sstable-deletion");

static
future<>
do_delete_atomically(std::vector<sstable_to_delete> atomic_deletion_set, unsigned deleting_shard) {
    // runs on shard 0 only
    deletion_logger.debug("shard {} atomically deleting {}", deleting_shard, atomic_deletion_set);

    if (g_atomic_deletions_cancelled) {
        deletion_logger.debug("atomic deletions disabled, erroring out");
        throw std::runtime_error(sprint("atomic deletions disabled; not deleting %s", atomic_deletion_set));
    }

    // Insert atomic_deletion_set into the list of sets pending deletion.  If the new set
    // overlaps with an existing set, merge them (the merged set will be deleted atomically).
    std::list<lw_shared_ptr<pending_deletion>> new_atomic_deletion_sets;
    auto merged_set = make_lw_shared(pending_deletion());
    for (auto&& sst_to_delete : atomic_deletion_set) {
        merged_set->names.insert(sst_to_delete.name);
        if (!sst_to_delete.shared) {
            for (auto shard : boost::irange<shard_id>(0, smp::count)) {
                g_shards_agreeing_to_delete_sstable[sst_to_delete.name].insert(shard);
            }
        }
    }
    merged_set->completions.push_back(make_lw_shared<promise<>>());
    auto ret = merged_set->completions.back()->get_future();
    for (auto&& old_set : g_atomic_deletion_sets) {
         auto intersection = sstables_to_delete_atomically_type();
         boost::set_intersection(merged_set->names, old_set->names, std::inserter(intersection, intersection.end()));
         if (intersection.empty()) {
             // We copy old_set to avoid corrupting g_atomic_deletion_sets if we fail
             // further on.
             new_atomic_deletion_sets.push_back(old_set);
         } else {
             deletion_logger.debug("merging with {}", old_set->names);
             boost::insert(merged_set->names, old_set->names);
             boost::push_back(merged_set->completions, old_set->completions);
         }
    }
    deletion_logger.debug("new atomic set: {}", merged_set->names);
    new_atomic_deletion_sets.push_back(merged_set);
    // can now exception-safely commit:
    g_atomic_deletion_sets = std::move(new_atomic_deletion_sets);

    // Mark each sstable as being deleted from deleting_shard.  We have to do
    // this in a separate pass, so the consideration whether we can delete or not
    // sees all the data from this pass.
    for (auto&& sst : atomic_deletion_set) {
        g_shards_agreeing_to_delete_sstable[sst.name].insert(deleting_shard);
    }

    // Figure out if the (possibly merged) set can be deleted
    for (auto&& sst : merged_set->names) {
        if (g_shards_agreeing_to_delete_sstable[sst].size() != smp::count) {
            // Not everyone agrees, leave the set pending
            deletion_logger.debug("deferring deletion until all shards agree");
            return ret;
        }
    }

    // Cannot recover from a failed deletion
    g_atomic_deletion_sets.pop_back();
    for (auto&& name : merged_set->names) {
        g_shards_agreeing_to_delete_sstable.erase(name);
    }

    // Everyone agrees, let's delete
    // FIXME: this needs to be done atomically (using a log file of sstables we intend to delete)
    parallel_for_each(merged_set->names, [] (sstring name) {
        deletion_logger.debug("deleting {}", name);
        return remove_by_toc_name(name);
    }).then_wrapped([merged_set] (future<> result) {
        deletion_logger.debug("atomic deletion completed: {}", merged_set->names);
        shared_future<> sf(std::move(result));
        for (auto&& comp : merged_set->completions) {
            sf.get_future().forward_to(std::move(*comp));
        }
    });

    return ret;
}

future<>
delete_atomically(std::vector<sstable_to_delete> ssts) {
    auto shard = engine().cpu_id();
    return smp::submit_to(0, [=] {
        return do_delete_atomically(ssts, shard);
    });
}

future<>
delete_atomically(std::vector<shared_sstable> ssts) {
    std::vector<sstable_to_delete> sstables_to_delete_atomically;
    for (auto&& sst : ssts) {
        sstables_to_delete_atomically.push_back({sst->toc_filename(), sst->is_shared()});
    }
    return delete_atomically(std::move(sstables_to_delete_atomically));
}

void
cancel_atomic_deletions() {
    g_atomic_deletions_cancelled = true;
    for (auto&& pd : g_atomic_deletion_sets) {
        for (auto&& c : pd->completions) {
            c->set_exception(std::runtime_error(sprint("Atomic sstable deletions cancelled; not deleting %s", pd->names)));
        }
    }
    g_atomic_deletion_sets.clear();
    g_shards_agreeing_to_delete_sstable.clear();
}

}
