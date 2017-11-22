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
#include <seastar/core/byteorder.hh>
#include <iterator>

#include "types.hh"
#include "sstables.hh"
#include "progress_monitor.hh"
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
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/insert.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/is_sorted.hpp>
#include <regex>
#include <core/align.hh>
#include "utils/phased_barrier.hh"
#include "range_tombstone_list.hh"
#include "counters.hh"
#include "binary_search.hh"
#include "utils/bloom_filter.hh"

#include "checked-file-impl.hh"
#include "integrity_checked_file_impl.hh"
#include "service/storage_service.hh"

thread_local disk_error_signal_type sstable_read_error;
thread_local disk_error_signal_type sstable_write_error;

namespace sstables {

logging::logger sstlog("sstable");

static const db::config& get_config();

seastar::shared_ptr<write_monitor> default_write_monitor() {
    static thread_local seastar::shared_ptr<write_monitor> monitor = seastar::make_shared<noop_write_monitor>();
    return monitor;
}

static future<file> open_sstable_component_file(const io_error_handler& error_handler, sstring name, open_flags flags,
        file_open_options options) {
    if (get_config().enable_sstable_data_integrity_check()) {
        return open_integrity_checked_file_dma(name, flags, options).then([&error_handler] (auto f) {
            return make_checked_file(error_handler, std::move(f));
        });
    }
    return open_checked_file_dma(error_handler, name, flags, options);
}

future<file> new_sstable_component_file(const io_error_handler& error_handler, sstring name, open_flags flags,
        file_open_options options = {}) {
    return open_sstable_component_file(error_handler, name, flags, options).handle_exception([name] (auto ep) {
        sstlog.error("Could not create SSTable component {}. Found exception: {}", name, ep);
        return make_exception_future<file>(ep);
    });
}

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
    std::unique_ptr<input_stream<char>> _in;
    seastar::gate _close_gate;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;
public:
    future<temporary_buffer<char>> read_exactly(size_t n) {
        return _in->read_exactly(n);
    }
    void seek(uint64_t pos) {
        if (_in) {
            seastar::with_gate(_close_gate, [in = std::move(_in)] () mutable {
                auto fut = in->close();
                return fut.then([in = std::move(in)] {});
            });
        }
        _in = std::make_unique<input_stream<char>>(open_at(pos));
    }
    bool eof() { return _in->eof(); }
    virtual future<> close() {
        return _close_gate.close().then([this] {
            return _in->close();
        });
    }
    virtual ~random_access_reader() { }
};

class file_random_access_reader : public random_access_reader {
    file _file;
    uint64_t _file_size;
    size_t _buffer_size;
    unsigned _read_ahead;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        auto len = _file_size - pos;
        file_input_stream_options options;
        options.buffer_size = _buffer_size;
        options.read_ahead = _read_ahead;

        return make_file_input_stream(_file, pos, len, std::move(options));
    }
    explicit file_random_access_reader(file f, uint64_t file_size, size_t buffer_size = 8192, unsigned read_ahead = 4)
        : _file(std::move(f)), _file_size(file_size), _buffer_size(buffer_size), _read_ahead(read_ahead)
    {
        seek(0);
    }
    virtual future<> close() override {
        return random_access_reader::close().finally([this] {
            return _file.close().handle_exception([save = _file] (auto ep) {
                sstlog.warn("sstable close failed: {}", ep);
                general_disk_error();
            });
        });
    }
};

shared_sstable
make_sstable(schema_ptr schema, sstring dir, int64_t generation, sstable_version_types v, sstable_format_types f, gc_clock::time_point now,
            io_error_handler_gen error_handler_gen, size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), std::move(dir), generation, v, f, now, std::move(error_handler_gen), buffer_size);
}

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
    { component_type::Scylla, "Scylla.db" },
    { component_type::TemporaryTOC, TEMPORARY_TOC_SUFFIX },
    { component_type::TemporaryStatistics, "Statistics.db.tmp" },
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

inline void write(file_writer& out, const bytes& s) {
    out.write(s).get();
}

inline void write(file_writer& out, bytes_view s) {
    out.write(reinterpret_cast<const char*>(s.data()), s.size()).get();
}

inline void write(file_writer& out, bytes_ostream s) {
    for (bytes_view fragment : s) {
        write(out, fragment);
    }
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(random_access_reader& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

template<typename First, typename... Rest>
inline void write(file_writer& out, const First& first, Rest&&... rest) {
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
write(file_writer& out, const T& t) {
    // describe_type() is not const correct, so cheat here:
    const_cast<T&>(t).describe_type([&out] (auto&&... what) -> void {
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
inline void write(file_writer& out, const disk_string<Size>& s) {
    Size len = 0;
    check_truncate_and_assign(len, s.value.size());
    write(out, len);
    write(out, s.value);
}

template <typename Size>
inline void write(file_writer& out, const disk_string_view<Size>& s) {
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
parse(random_access_reader& in, Size& len, utils::chunked_vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(random_access_reader& in, Size& len, utils::chunked_vector<Members>& arr) {
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
write(file_writer& out, const utils::chunked_vector<Members>& arr) {
    for (auto& a : arr) {
        write(out, a);
    }
}

template <typename Members>
inline typename std::enable_if_t<std::is_integral<Members>::value, void>
write(file_writer& out, const utils::chunked_vector<Members>& arr) {
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
inline void write(file_writer& out, const disk_array<Size, Members>& arr) {
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
inline void write(file_writer& out, const std::unordered_map<Key, Value>& map) {
    for (auto& val: map) {
        write(out, val.first, val.second);
    };
}

template <typename Size, typename Key, typename Value>
inline void write(file_writer& out, const disk_hash<Size, Key, Value>& h) {
    Size len = 0;
    check_truncate_and_assign(len, h.map.size());
    write(out, len);
    write(out, h.map);
}

// Abstract parser/sizer/writer for a single tagged member of a tagged union
template <typename DiskSetOfTaggedUnion>
struct single_tagged_union_member_serdes {
    using value_type = typename DiskSetOfTaggedUnion::value_type;
    virtual ~single_tagged_union_member_serdes() {}
    virtual future<> do_parse(random_access_reader& in, value_type& v) const = 0;
    virtual uint32_t do_size(const value_type& v) const = 0;
    virtual void do_write(file_writer& out, const value_type& v) const = 0;
};

// Concrete parser for a single member of a tagged union; parses type "Member"
template <typename DiskSetOfTaggedUnion, typename Member>
struct single_tagged_union_member_serdes_for final : single_tagged_union_member_serdes<DiskSetOfTaggedUnion> {
    using base = single_tagged_union_member_serdes<DiskSetOfTaggedUnion>;
    using value_type = typename base::value_type;
    virtual future<> do_parse(random_access_reader& in, value_type& v) const {
        v = Member();
        return parse(in, boost::get<Member>(v).value);
    }
    virtual uint32_t do_size(const value_type& v) const override {
        return serialized_size(boost::get<Member>(v).value);
    }
    virtual void do_write(file_writer& out, const value_type& v) const override {
        write(out, boost::get<Member>(v).value);
    }
};

template <typename TagType, typename... Members>
struct disk_set_of_tagged_union<TagType, Members...>::serdes {
    using disk_set = disk_set_of_tagged_union<TagType, Members...>;
    // We can't use unique_ptr, because we initialize from an std::intializer_list, which is not move compatible.
    using serdes_map_type = std::unordered_map<TagType, shared_ptr<single_tagged_union_member_serdes<disk_set>>, typename disk_set::hash_type>;
    using value_type = typename disk_set::value_type;
    serdes_map_type map = {
        {Members::tag(), make_shared<single_tagged_union_member_serdes_for<disk_set, Members>>()}...
    };
    future<> lookup_and_parse(random_access_reader& in, TagType tag, uint32_t& size, disk_set& s, value_type& value) const {
        auto i = map.find(tag);
        if (i == map.end()) {
            return in.read_exactly(size).discard_result();
        } else {
            return i->second->do_parse(in, value).then([tag, &s, &value] () mutable {
                s.data.emplace(tag, std::move(value));
            });
        }
    }
    uint32_t lookup_and_size(TagType tag, const value_type& value) const {
        return map.at(tag)->do_size(value);
    }
    void lookup_and_write(file_writer& out, TagType tag, const value_type& value) const {
        return map.at(tag)->do_write(out, value);
    }
};

template <typename TagType, typename... Members>
typename disk_set_of_tagged_union<TagType, Members...>::serdes disk_set_of_tagged_union<TagType, Members...>::s_serdes;

template <typename TagType, typename... Members>
future<>
parse(random_access_reader& in, disk_set_of_tagged_union<TagType, Members...>& s) {
    using disk_set = disk_set_of_tagged_union<TagType, Members...>;
    using key_type = typename disk_set::key_type;
    using value_type = typename disk_set::value_type;
    return do_with(0u, 0u, 0u, value_type{}, [&] (key_type& nr_elements, key_type& new_key, unsigned& new_size, value_type& new_value) {
        return parse(in, nr_elements).then([&] {
            auto rng = boost::irange<key_type>(0, nr_elements); // do_for_each doesn't like an rvalue range
            return do_for_each(rng.begin(), rng.end(), [&] (key_type ignore) {
                return parse(in, new_key).then([&] {
                    return parse(in, new_size).then([&] {
                        return disk_set::s_serdes.lookup_and_parse(in, TagType(new_key), new_size, s, new_value);
                    });
                });
            });
        });
    });
}

template <typename TagType, typename... Members>
void write(file_writer& out, const disk_set_of_tagged_union<TagType, Members...>& s) {
    using disk_set = disk_set_of_tagged_union<TagType, Members...>;
    write(out, uint32_t(s.data.size()));
    for (auto&& kv : s.data) {
        auto&& tag = kv.first;
        auto&& value = kv.second;
        write(out, tag);
        write(out, uint32_t(disk_set::s_serdes.lookup_and_size(tag, value)));
        disk_set::s_serdes.lookup_and_write(out, tag, value);
    }
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
            s.positions = utils::chunked_vector<pos_type>(nr, nr + s.header.size);

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
                    entry.token = dht::global_partitioner().get_token(entry.get_key());

                    return make_ready_future<>();
                });
            }).then([&s] {
                // Delete last element which isn't part of the on-disk format.
                s.positions.pop_back();
            });
        });
    });
}

inline void write(file_writer& out, const summary_entry& entry) {
    // FIXME: summary entry is supposedly written in memory order, but that
    // would prevent portability of summary file between machines of different
    // endianness. We can treat it as little endian to preserve portability.
    write(out, entry.key);
    auto p = reinterpret_cast<const char*>(&entry.position);
    out.write(p, sizeof(uint64_t)).get();
}

inline void write(file_writer& out, const summary& s) {
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
    if (i >= (_components->summary.entries.size())) {
        throw std::out_of_range(sprint("Invalid Summary index: %ld", i));
    }

    return make_ready_future<summary_entry&>(_components->summary.entries[i]);
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
inline void write(file_writer& out, const std::unique_ptr<metadata>& p) {
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

inline void write(file_writer& out, const statistics& s) {
    write(out, s.hash);
    auto types = boost::copy_range<std::vector<metadata_type>>(s.hash.map | boost::adaptors::map_keys);
    // use same sort order as seal_statistics
    boost::sort(types);
    for (auto t : types) {
        s.contents.at(t)->write(out);
    }
}

future<> parse(random_access_reader& in, utils::estimated_histogram& eh) {
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

inline void write(file_writer& out, const utils::estimated_histogram& eh) {
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

struct streaming_histogram_element {
    using key_type = typename decltype(utils::streaming_histogram::bin)::key_type;
    using value_type = typename decltype(utils::streaming_histogram::bin)::mapped_type;
    key_type key;
    value_type value;

    template <typename Describer>
    auto describe_type(Describer f) { return f(key, value); }
};

future<> parse(random_access_reader& in, utils::streaming_histogram& sh) {
    auto a = std::make_unique<disk_array<uint32_t, streaming_histogram_element>>();

    auto f = parse(in, sh.max_bin_size, *a);
    return f.then([&sh, a = std::move(a)] {
        auto length = a->elements.size();
        if (length > sh.max_bin_size) {
            throw malformed_sstable_exception("Streaming histogram with more entries than allowed. Can't continue!");
        }

        // Find bad histogram which had incorrect elements merged due to use of
        // unordered map. The keys will be unordered. Histogram which size is
        // less than max allowed will be correct because no entries needed to be
        // merged, so we can avoid discarding those.
        // look for commit with title 'streaming_histogram: fix update' for more details.
        auto possibly_broken_histogram = length == sh.max_bin_size;
        auto less_comp = [] (auto& x, auto& y) { return x.key < y.key; };
        if (possibly_broken_histogram && !boost::is_sorted(a->elements, less_comp)) {
            return make_ready_future<>();
        }

        auto transform = [] (auto element) -> std::pair<streaming_histogram_element::key_type, streaming_histogram_element::value_type> {
            return { element.key, element.value };
        };
        boost::copy(a->elements | boost::adaptors::transformed(transform), std::inserter(sh.bin, sh.bin.end()));

        return make_ready_future<>();
    });
}

inline void write(file_writer& out, const utils::streaming_histogram& sh) {
    uint32_t max_bin_size;
    check_truncate_and_assign(max_bin_size, sh.max_bin_size);

    disk_array<uint32_t, streaming_histogram_element> a;
    a.elements = boost::copy_range<utils::chunked_vector<streaming_histogram_element>>(sh.bin
        | boost::adaptors::transformed([&] (auto& kv) { return streaming_histogram_element{kv.first, kv.second}; }));

    write(out, max_bin_size, a);
}

future<> parse(random_access_reader& in, compression& c) {
    auto data_len_ptr = make_lw_shared<uint64_t>(0);
    auto chunk_len_ptr = make_lw_shared<uint32_t>(0);

    return parse(in, c.name, c.options, *chunk_len_ptr, *data_len_ptr).then([&in, &c, chunk_len_ptr, data_len_ptr] {
        c.set_uncompressed_chunk_length(*chunk_len_ptr);
        c.set_uncompressed_file_length(*data_len_ptr);

        auto len = make_lw_shared<uint32_t>();
        return parse(in, *len).then([&in, &c, len] {
            auto eoarr = [&c, len] { return c.offsets.size() == *len; };

            return do_until(eoarr, [&in, &c, len] {
                auto now = std::min(*len - c.offsets.size(), 100000 / sizeof(uint64_t));
                return in.read_exactly(now * sizeof(uint64_t)).then([&c, len, now] (auto buf) {
                    uint64_t value;
                    for (size_t i = 0; i < now; ++i) {
                        std::copy_n(buf.get() + i * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&value));
                        c.offsets.push_back(net::ntoh(value));
                    }
                });
            });
        });
    });
}

void write(file_writer& out, const compression& c) {
    write(out, c.name, c.options, c.uncompressed_chunk_length(), c.uncompressed_file_length());

    write(out, static_cast<uint32_t>(c.offsets.size()));

    std::vector<uint64_t> tmp;
    const size_t per_loop = 100000 / sizeof(uint64_t);
    tmp.resize(per_loop);
    size_t idx = 0;
    while (idx != c.offsets.size()) {
        auto now = std::min(c.offsets.size() - idx, per_loop);
        // copy offsets into tmp converting each entry into big-endian representation.
        auto nr = c.offsets.begin() + idx;
        for (size_t i = 0; i < now; i++) {
            tmp[i] = net::hton(nr[i]);
        }
        auto p = reinterpret_cast<const char*>(tmp.data());
        auto bytes = now * sizeof(uint64_t);
        out.write(p, bytes).get();
        idx += now;
    }
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() {
    if (_recognized_components.size()) {
        return make_ready_future<>();
    }

    auto file_path = filename(sstable::component_type::TOC);

    sstlog.debug("Reading TOC file {} ", file_path);

    return open_checked_file_dma(_read_error_handler, file_path, open_flags::ro).then([this, file_path] (file f) {
        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
        auto buf = bufptr.get();

        auto fut = f.dma_read(0, buf, 4096);
        return std::move(fut).then([this, f = std::move(f), bufptr = std::move(bufptr), file_path] (size_t size) mutable {
            // This file is supposed to be very small. Theoretically we should check its size,
            // but if we so much as read a whole page from it, there is definitely something fishy
            // going on - and this simplifies the code.
            if (size >= 4096) {
                throw malformed_sstable_exception("SSTable too big: " + to_sstring(size) + " bytes", file_path);
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
                    _recognized_components.insert(reverse_map(c, _component_map));
                } catch (std::out_of_range& oor) {
                    _unrecognized_components.push_back(c);
                    sstlog.info("Unrecognized TOC component was found: {} in sstable {}", c, file_path);
                }
            }
            if (!_recognized_components.size()) {
                throw malformed_sstable_exception("Empty TOC", file_path);
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
            throw;
        }
    });

}

void sstable::generate_toc(compressor c, double filter_fp_chance) {
    // Creating table of components.
    _recognized_components.insert(component_type::TOC);
    _recognized_components.insert(component_type::Statistics);
    _recognized_components.insert(component_type::Digest);
    _recognized_components.insert(component_type::Index);
    _recognized_components.insert(component_type::Summary);
    _recognized_components.insert(component_type::Data);
    if (filter_fp_chance != 1.0) {
        _recognized_components.insert(component_type::Filter);
    }
    if (c == compressor::none) {
        _recognized_components.insert(component_type::CRC);
    } else {
        _recognized_components.insert(component_type::CompressionInfo);
    }
    _recognized_components.insert(component_type::Scylla);
}

void sstable::write_toc(const io_priority_class& pc) {
    auto file_path = filename(sstable::component_type::TemporaryTOC);

    sstlog.debug("Writing TOC file {} ", file_path);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file f = new_sstable_component_file(_write_error_handler, file_path, open_flags::wo | open_flags::create | open_flags::exclusive).get0();

    bool toc_exists = file_exists(filename(sstable::component_type::TOC)).get0();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        f.close().get();
        remove_file(file_path).get();
        throw std::runtime_error(sprint("SSTable write failed due to existence of TOC file for generation %ld of %s.%s", _generation, _schema->ks_name(), _schema->cf_name()));
    }

    file_output_stream_options options;
    options.buffer_size = 4096;
    options.io_priority_class = pc;
    auto w = file_writer(std::move(f), std::move(options));

    for (auto&& key : _recognized_components) {
            // new line character is appended to the end of each component name.
        auto value = _component_map[key] + "\n";
        bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
        write(w, b);
    }
    w.flush().get();
    w.close().get();

    // Flushing parent directory to guarantee that temporary TOC file reached
    // the disk.
    file dir_f = open_checked_directory(_write_error_handler, _dir).get0();
    sstable_write_io_check([&] {
        dir_f.flush().get();
        dir_f.close().get();
    });
}

future<> sstable::seal_sstable() {
    // SSTable sealing is about renaming temporary TOC file after guaranteeing
    // that each component reached the disk safely.
    return open_checked_directory(_write_error_handler, _dir).then([this] (file dir_f) {
        // Guarantee that every component of this sstable reached the disk.
        return sstable_write_io_check([&] { return dir_f.flush(); }).then([this] {
            // Rename TOC because it's no longer temporary.
            return sstable_write_io_check([&] {
                return engine().rename_file(filename(sstable::component_type::TemporaryTOC), filename(sstable::component_type::TOC));
            });
        }).then([this, dir_f] () mutable {
            // Guarantee that the changes above reached the disk.
            return sstable_write_io_check([&] { return dir_f.flush(); });
        }).then([this, dir_f] () mutable {
            return sstable_write_io_check([&] { return dir_f.close(); });
        }).then([this, dir_f] {
            // If this point was reached, sstable should be safe in disk.
            sstlog.debug("SSTable with generation {} of {}.{} was sealed successfully.", _generation, _schema->ks_name(), _schema->cf_name());
        });
    });
}

void write_crc(io_error_handler& error_handler, const sstring file_path, const checksum& c) {
    sstlog.debug("Writing CRC file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file f = new_sstable_component_file(error_handler, file_path, oflags).get0();

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = file_writer(std::move(f), std::move(options));
    write(w, c);
    w.close().get();
}

// Digest file stores the full checksum of data file converted into a string.
void write_digest(io_error_handler& error_handler, const sstring file_path, uint32_t full_checksum) {
    sstlog.debug("Writing Digest file {} ", file_path);

    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    auto f = new_sstable_component_file(error_handler, file_path, oflags).get0();

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = file_writer(std::move(f), std::move(options));

    auto digest = to_sstring<bytes>(full_checksum);
    write(w, digest);
    w.close().get();
}

thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_sample_pattern_cache;
thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_original_index_cache;

std::unique_ptr<index_reader> sstable::get_index_reader(const io_priority_class& pc) {
    return std::make_unique<index_reader>(shared_from_this(), pc);
}

template <sstable::component_type Type, typename T>
future<> sstable::read_simple(T& component, const io_priority_class& pc) {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + _component_map[Type] + " file {} ").c_str(), file_path);
    return open_file_dma(file_path, open_flags::ro).then([this, &component] (file fi) {
        auto fut = fi.size();
        return fut.then([this, &component, fi = std::move(fi)] (uint64_t size) {
            auto f = make_checked_file(_read_error_handler, fi);
            auto r = make_lw_shared<file_random_access_reader>(std::move(f), size, sstable_buffer_size);
            auto fut = parse(*r, component);
            return fut.finally([r] {
                return r->close();
            }).then([r] {});
        });
    }).then_wrapped([this, file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(file_path + ": file not found");
            }
            throw;
        }
    });
}

template <sstable::component_type Type, typename T>
void sstable::write_simple(const T& component, const io_priority_class& pc) {
    auto file_path = filename(Type);
    sstlog.debug(("Writing " + _component_map[Type] + " file {} ").c_str(), file_path);
    file f = new_sstable_component_file(_write_error_handler, file_path, open_flags::wo | open_flags::create | open_flags::exclusive).get0();

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto w = file_writer(std::move(f), std::move(options));
    write(w, component);
    w.flush().get();
    w.close().get();
}

template future<> sstable::read_simple<sstable::component_type::Filter>(sstables::filter& f, const io_priority_class& pc);
template void sstable::write_simple<sstable::component_type::Filter>(const sstables::filter& f, const io_priority_class& pc);

future<> sstable::read_compression(const io_priority_class& pc) {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_components->compression, pc);
}

void sstable::write_compression(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::CompressionInfo)) {
        return;
    }

    write_simple<component_type::CompressionInfo>(_components->compression, pc);
}

void sstable::validate_min_max_metadata() {
    auto entry = _components->statistics.contents.find(metadata_type::Stats);
    if (entry == _components->statistics.contents.end()) {
        throw std::runtime_error("Stats metadata not available");
    }
    auto& p = entry->second;
    if (!p) {
        throw std::runtime_error("Statistics is malformed");
    }

    stats_metadata& s = *static_cast<stats_metadata *>(p.get());
    auto is_composite_valid = [] (const bytes& b) {
        auto v = composite_view(b);
        try {
            size_t s = 0;
            for (auto& c : v.components()) {
                s += c.first.size() + sizeof(composite::size_type) + sizeof(composite::eoc_type);
            }
            return s == b.size();
        } catch (marshal_exception&) {
            return false;
        }
    };
    auto clear_incorrect_min_max_column_names = [&s] {
        s.min_column_names.elements.clear();
        s.max_column_names.elements.clear();
    };
    auto& min_column_names = s.min_column_names.elements;
    auto& max_column_names = s.max_column_names.elements;

    if (min_column_names.empty() && max_column_names.empty()) {
        return;
    }

    // The min/max metadata is wrong if:
    // 1) it's not empty and schema defines no clustering key.
    // 2) their size differ.
    // 3) column name is stored instead of clustering value.
    // 4) clustering component is stored as composite.
    if ((!_schema->clustering_key_size() && (min_column_names.size() || max_column_names.size())) ||
            (min_column_names.size() != max_column_names.size())) {
        clear_incorrect_min_max_column_names();
        return;
    }

    for (auto i = 0U; i < min_column_names.size(); i++) {
        if (_schema->get_column_definition(min_column_names[i].value) || _schema->get_column_definition(max_column_names[i].value)) {
            clear_incorrect_min_max_column_names();
            break;
        }

        if (_schema->is_compound() && _schema->clustering_key_size() > 1 && _schema->is_dense() &&
                (is_composite_valid(min_column_names[i].value) || is_composite_valid(max_column_names[i].value))) {
            clear_incorrect_min_max_column_names();
            break;
        }
    }
}

void sstable::set_clustering_components_ranges() {
    if (!_schema->clustering_key_size()) {
        return;
    }
    auto& min_column_names = get_stats_metadata().min_column_names.elements;
    auto& max_column_names = get_stats_metadata().max_column_names.elements;

    auto s = std::min(min_column_names.size(), max_column_names.size());
    _clustering_components_ranges.reserve(s);
    for (auto i = 0U; i < s; i++) {
        auto r = nonwrapping_range<bytes_view>({{ min_column_names[i].value, true }}, {{ max_column_names[i].value, true }});
        _clustering_components_ranges.push_back(std::move(r));
    }
}

const std::vector<nonwrapping_range<bytes_view>>& sstable::clustering_components_ranges() const {
    return _clustering_components_ranges;
}

double sstable::estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const {
    auto& st = get_stats_metadata();
    auto estimated_count = st.estimated_column_count.mean() * st.estimated_column_count.count();
    if (estimated_count > 0) {
        double droppable = st.estimated_tombstone_drop_time.sum(gc_before.time_since_epoch().count());
        return droppable / estimated_count;
    }
    return 0.0f;
}

future<> sstable::read_statistics(const io_priority_class& pc) {
    return read_simple<component_type::Statistics>(_components->statistics, pc);
}

void sstable::write_statistics(const io_priority_class& pc) {
    write_simple<component_type::Statistics>(_components->statistics, pc);
}

void sstable::rewrite_statistics(const io_priority_class& pc) {
    auto file_path = filename(component_type::TemporaryStatistics);
    sstlog.debug("Rewriting statistics component of sstable {}", get_filename());
    file f = new_sstable_component_file(_write_error_handler, file_path, open_flags::wo | open_flags::create | open_flags::truncate).get0();

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto w = file_writer(std::move(f), std::move(options));
    write(w, _components->statistics);
    w.flush().get();
    w.close().get();
    // rename() guarantees atomicity when renaming a file into place.
    sstable_write_io_check(rename_file, file_path, filename(component_type::Statistics)).get();
}

future<> sstable::read_summary(const io_priority_class& pc) {
    if (_components->summary) {
        return make_ready_future<>();
    }

    return read_toc().then([this, &pc] {
        // We'll try to keep the main code path exception free, but if an exception does happen
        // we can try to regenerate the Summary.
        if (has_component(sstable::component_type::Summary)) {
            return read_simple<component_type::Summary>(_components->summary, pc).handle_exception([this, &pc] (auto ep) {
                sstlog.warn("Couldn't read summary file {}: {}. Recreating it.", this->filename(component_type::Summary), ep);
                return this->generate_summary(pc);
            });
        } else {
            return generate_summary(pc);
        }
    });
}

future<> sstable::open_data() {
    return when_all(open_checked_file_dma(_read_error_handler, filename(component_type::Index), open_flags::ro),
                    open_checked_file_dma(_read_error_handler, filename(component_type::Data), open_flags::ro))
                    .then([this] (auto files) {
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
        return this->update_info_for_opened_data();
    }).then([this] {
        _shards = compute_shards_for_this_sstable();
    });
}

future<> sstable::update_info_for_opened_data() {
    return _data_file.stat().then([this] (struct stat st) {
        if (this->has_component(sstable::component_type::CompressionInfo)) {
            _components->compression.update(st.st_size);
        }
        _data_file_size = st.st_size;
        _data_file_write_time = db_clock::from_time_t(st.st_mtime);
    }).then([this] {
        return _index_file.size().then([this] (auto size) {
            _index_file_size = size;
        });
    }).then([this] {
        if (this->has_component(sstable::component_type::Filter)) {
            return io_check([&] {
                return engine().file_size(this->filename(sstable::component_type::Filter));
            }).then([this] (auto size) {
                _filter_file_size = size;
            });
        }
        return make_ready_future<>();
    }).then([this] {
        this->set_clustering_components_ranges();
        this->set_first_and_last_keys();

        // Get disk usage for this sstable (includes all components).
        _bytes_on_disk = 0;
        return do_for_each(_recognized_components, [this] (component_type c) {
            return this->sstable_write_io_check([&] {
                return engine().file_size(this->filename(c));
            }).then([this] (uint64_t bytes) {
                _bytes_on_disk += bytes;
            });
        });
    });
}

future<> sstable::create_data() {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file_open_options opt;
    opt.extent_allocation_size_hint = 32 << 20;
    opt.sloppy_size = true;
    return when_all(new_sstable_component_file(_write_error_handler, filename(component_type::Index), oflags, opt),
                    new_sstable_component_file(_write_error_handler, filename(component_type::Data), oflags, opt)).then([this] (auto files) {
        // FIXME: If both files could not be created, the first get below will
        // throw an exception, and second get() will not be attempted, and
        // we'll get a warning about the second future being destructed
        // without its exception being examined.
        _index_file = std::get<file>(std::get<0>(files).get());
        _data_file  = std::get<file>(std::get<1>(files).get());
    });
}

future<> sstable::read_filter(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::Filter)) {
        _components->filter = std::make_unique<utils::filter::always_present_filter>();
        return make_ready_future<>();
    }

    return do_with(sstables::filter(), [this, &pc] (auto& filter) {
        return this->read_simple<sstable::component_type::Filter>(filter, pc).then([this, &filter] {
            large_bitset bs(filter.buckets.elements.size() * 64);
            bs.load(filter.buckets.elements.begin(), filter.buckets.elements.end());
            _components->filter = utils::filter::create_filter(filter.hashes, std::move(bs));
        });
    });
}

void sstable::write_filter(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::Filter)) {
        return;
    }

    auto f = static_cast<utils::filter::murmur3_bloom_filter *>(_components->filter.get());

    auto&& bs = f->bits();
    utils::chunked_vector<uint64_t> v(align_up(bs.size(), size_t(64)) / 64);
    bs.save(v.begin());
    auto filter = sstables::filter(f->num_hashes(), std::move(v));
    write_simple<sstable::component_type::Filter>(filter, pc);
}

// This interface is only used during tests, snapshot loading and early initialization.
// No need to set tunable priorities for it.
future<> sstable::load(const io_priority_class& pc) {
    return read_toc().then([this, &pc] {
        return seastar::when_all_succeed(
                read_statistics(pc),
                read_compression(pc),
                read_scylla_metadata(pc),
                read_filter(pc),
                read_summary(pc)).then([this] {
            validate_min_max_metadata();
            set_clustering_components_ranges();
            return open_data();
        });
    });
}

future<> sstable::load(sstables::foreign_sstable_open_info info) {
    return read_toc().then([this, info = std::move(info)] () mutable {
        _components = std::move(info.components);
        _data_file = make_checked_file(_read_error_handler, info.data.to_file());
        _index_file = make_checked_file(_read_error_handler, info.index.to_file());
        _shards = std::move(info.owners);
        validate_min_max_metadata();
        return update_info_for_opened_data();
    });
}

future<sstable_open_info> sstable::load_shared_components(const schema_ptr& s, sstring dir, int generation, version_types v, format_types f,
        const io_priority_class& pc) {
    auto sst = sstables::make_sstable(s, dir, generation, v, f);
    return sst->load(pc).then([sst] () mutable {
        auto info = sstable_open_info{make_lw_shared<shareable_components>(std::move(*sst->_components)),
            std::move(sst->_shards), std::move(sst->_data_file), std::move(sst->_index_file)};
        return make_ready_future<sstable_open_info>(std::move(info));
    });
}

future<foreign_sstable_open_info> sstable::get_open_info() & {
    return _components.copy().then([this] (auto c) mutable {
        return foreign_sstable_open_info{std::move(c), this->get_shards_for_this_sstable(), _data_file.dup(), _index_file.dup(),
            _generation, _version, _format};
    });
}

static composite::eoc bound_kind_to_start_marker(bound_kind start_kind) {
    return start_kind == bound_kind::excl_start
         ? composite::eoc::end
         : composite::eoc::start;
}

static composite::eoc bound_kind_to_end_marker(bound_kind end_kind) {
    return end_kind == bound_kind::excl_end
         ? composite::eoc::start
         : composite::eoc::end;
}

static void output_promoted_index_entry(bytes_ostream& promoted_index,
        const bytes& first_col,
        const bytes& last_col,
        uint64_t offset, uint64_t width) {
    char s[2];
    write_be(s, uint16_t(first_col.size()));
    promoted_index.write(s, 2);
    promoted_index.write(first_col);
    write_be(s, uint16_t(last_col.size()));
    promoted_index.write(s, 2);
    promoted_index.write(last_col);
    char q[8];
    write_be(q, uint64_t(offset));
    promoted_index.write(q, 8);
    write_be(q, uint64_t(width));
    promoted_index.write(q, 8);
}

// FIXME: use this in write_column_name() instead of repeating the code
static bytes serialize_colname(const composite& clustering_key,
        const std::vector<bytes_view>& column_names, composite::eoc marker) {
    auto c = composite::from_exploded(column_names, marker);
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
    bytes colname(bytes::initialized_later(), sz);
    std::copy(ck_bview.begin(), ck_bview.end(), colname.begin());
    std::copy(c.get_bytes().begin(), c.get_bytes().end(), colname.begin() + ck_bview.size());
    return colname;
}

// Call maybe_flush_pi_block() before writing the given sstable atom to the
// output. This may start a new promoted-index block depending on how much
// data we've already written since the start of the current block. Starting
// a new block involves both outputting the range of the old block to the
// index file, and outputting again the currently-open range tombstones to
// the data file.
// TODO: currently, maybe_flush_pi_block serializes the column name on every
// call, saving it in _pi_write.block_last_colname which we need for closing
// each block, as well as for closing the last block. We could instead save
// just the unprocessed arguments, and serialize them only when needed at the
// end of the block. For this we would need this function to take rvalue
// references (so data is moved in), and need not to use vector of byte_view
// (which might be gone later).
void sstable::maybe_flush_pi_block(file_writer& out,
        const composite& clustering_key,
        const std::vector<bytes_view>& column_names,
        composite::eoc marker) {
    bytes colname = serialize_colname(clustering_key, column_names, marker);
    if (_pi_write.block_first_colname.empty()) {
        // This is the first column in the partition, or first column since we
        // closed a promoted-index block. Remember its name and position -
        // we'll need to write it to the promoted index.
        _pi_write.block_start_offset = out.offset();
        _pi_write.block_next_start_offset = out.offset() + _pi_write.desired_block_size;
        _pi_write.block_first_colname = colname;
        _pi_write.block_last_colname = std::move(colname);
    } else if (out.offset() >= _pi_write.block_next_start_offset) {
        // If we wrote enough bytes to the partition since we output a sample
        // to the promoted index, output one now and start a new one.
        output_promoted_index_entry(_pi_write.data,
                _pi_write.block_first_colname,
                _pi_write.block_last_colname,
                _pi_write.block_start_offset - _c_stats.start_offset,
                out.offset() - _pi_write.block_start_offset);
        _pi_write.numblocks++;
        _pi_write.block_start_offset = out.offset();
        // Because the new block can be read without the previous blocks, we
        // need to repeat the range tombstones which are still open.
        // Note that block_start_offset is before outputting those (so the new
        // block includes them), but we set block_next_start_offset after - so
        // even if we wrote a lot of open tombstones, we still get a full
        // block size of new data.
        if (!clustering_key.empty()) {
            auto& rts = _pi_write.tombstone_accumulator->range_tombstones_for_row(
                    clustering_key_prefix::from_range(clustering_key.values()));
            for (const auto& rt : rts) {
                auto start = composite::from_clustering_element(*_pi_write.schemap, rt.start);
                auto end = composite::from_clustering_element(*_pi_write.schemap, rt.end);
                write_range_tombstone(out,
                        start, bound_kind_to_start_marker(rt.start_kind),
                        end, bound_kind_to_end_marker(rt.end_kind),
                        {}, rt.tomb);
            }
        }
        _pi_write.block_next_start_offset = out.offset() + _pi_write.desired_block_size;
        _pi_write.block_first_colname = colname;
        _pi_write.block_last_colname = std::move(colname);
    } else {
        // Keep track of the last column in the partition - we'll need it to close
        // the last block in the promoted index, unfortunately.
        _pi_write.block_last_colname = std::move(colname);
    }
}

void sstable::write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite::eoc marker) {
    // was defined in the schema, for example.
    auto c = composite::from_exploded(column_names, marker);
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
void sstable::write_cell(file_writer& out, atomic_cell_view cell, const column_definition& cdef) {
    uint64_t timestamp = cell.timestamp();

    update_cell_stats(_c_stats, timestamp);

    if (cell.is_dead(_now)) {
        // tombstone cell

        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = cell.deletion_time().time_since_epoch().count();

        _c_stats.update_max_local_deletion_time(deletion_time);
        _c_stats.tombstone_histogram.update(deletion_time);

        write(out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (cdef.is_counter()) {
        // counter cell
        assert(!cell.is_counter_update());

        column_mask mask = column_mask::counter;
        write(out, mask, int64_t(0), timestamp);

        counter_cell_view ccv(cell);
        auto shard_count = ccv.shard_count();

        static constexpr auto header_entry_size = sizeof(int16_t);
        static constexpr auto counter_shard_size = 32u; // counter_id: 16 + clock: 8 + value: 8
        auto total_size = sizeof(int16_t) + shard_count * (header_entry_size + counter_shard_size);

        write(out, int32_t(total_size), int16_t(shard_count));
        for (auto i = 0u; i < shard_count; i++) {
            write<int16_t>(out, std::numeric_limits<int16_t>::min() + i);
        }
        auto write_shard = [&] (auto&& s) {
            auto uuid = s.id().to_uuid();
            write(out, int64_t(uuid.get_most_significant_bits()),
                  int64_t(uuid.get_least_significant_bits()),
                  int64_t(s.logical_clock()), int64_t(s.value()));
        };
        if (service::get_local_storage_service().cluster_supports_correct_counter_order()) {
            for (auto&& s : ccv.shards()) {
                write_shard(s);
            }
        } else {
            for (auto&& s : ccv.shards_compatible_with_1_7_4()) {
                write_shard(s);
            }
        }

        _c_stats.update_max_local_deletion_time(std::numeric_limits<int>::max());
    } else if (cell.is_live_and_has_ttl()) {
        // expiring cell

        column_mask mask = column_mask::expiration;
        uint32_t ttl = cell.ttl().count();
        uint32_t expiration = cell.expiry().time_since_epoch().count();
        disk_string_view<uint32_t> cell_value { cell.value() };

        _c_stats.update_max_local_deletion_time(expiration);
        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        _c_stats.tombstone_histogram.update(expiration);

        write(out, mask, ttl, expiration, timestamp, cell_value);
    } else {
        // regular cell

        column_mask mask = column_mask::none;
        disk_string_view<uint32_t> cell_value { cell.value() };

        _c_stats.update_max_local_deletion_time(std::numeric_limits<int>::max());

        write(out, mask, timestamp, cell_value);
    }
}

void sstable::write_row_marker(file_writer& out, const row_marker& marker, const composite& clustering_key) {
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

void sstable::write_deletion_time(file_writer& out, const tombstone t) {
    uint64_t timestamp = t.timestamp;
    uint32_t deletion_time = t.deletion_time.time_since_epoch().count();

    update_cell_stats(_c_stats, timestamp);
    _c_stats.update_max_local_deletion_time(deletion_time);
    _c_stats.tombstone_histogram.update(deletion_time);

    write(out, deletion_time, timestamp);
}

void sstable::write_row_tombstone(file_writer& out, const composite& key, const row_tombstone t) {
    if (!t) {
        return;
    }

    auto write_tombstone = [&] (tombstone t, column_mask mask) {
        write_column_name(out, key, {}, composite::eoc::start);
        write(out, mask);
        write_column_name(out, key, {}, composite::eoc::end);
        write_deletion_time(out, t);
    };

    write_tombstone(t.regular(), column_mask::range_tombstone);
    if (t.is_shadowable()) {
        write_tombstone(t.shadowable().tomb(), column_mask::shadowable);
    }
}

void sstable::write_range_tombstone(file_writer& out,
        const composite& start,
        composite::eoc start_marker,
        const composite& end,
        composite::eoc end_marker,
        std::vector<bytes_view> suffix,
        const tombstone t) {
    if (!t) {
        return;
    }

    write_column_name(out, start, suffix, start_marker);
    column_mask mask = column_mask::range_tombstone;
    write(out, mask);
    write_column_name(out, end, suffix, end_marker);
    write_deletion_time(out, t);
}

void sstable::write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection) {

    auto t = static_pointer_cast<const collection_type_impl>(cdef.type);
    auto mview = t->deserialize_mutation_form(collection);
    const bytes& column_name = cdef.name();
    write_range_tombstone(out, clustering_key, clustering_key, { bytes_view(column_name) }, mview.tomb);
    for (auto& cp: mview.cells) {
        maybe_flush_pi_block(out, clustering_key, { column_name, cp.first });
        write_column_name(out, clustering_key, { column_name, cp.first });
        write_cell(out, cp.second, cdef);
    }
}

// This function is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
void sstable::write_clustered_row(file_writer& out, const schema& schema, const clustering_row& clustered_row) {
    auto clustering_key = composite::from_clustering_element(schema, clustered_row.key());

    if (schema.is_compound() && !schema.is_dense()) {
        maybe_flush_pi_block(out, clustering_key, { bytes_view() });
        write_row_marker(out, clustered_row.marker(), clustering_key);
    }
    // Before writing cells, range tombstone must be written if the row has any (deletable_row::t).
    if (clustered_row.tomb()) {
        maybe_flush_pi_block(out, clustering_key, {});
        write_row_tombstone(out, clustering_key, clustered_row.tomb());
        // Because we currently may break a partition to promoted-index blocks
        // in the middle of a clustered row, we also need to track the current
        // row's tombstone - not just range tombstones - which may effect the
        // beginning of a new block.
        // TODO: consider starting a new block only between rows, so the
        // following code can be dropped:
        _pi_write.tombstone_accumulator->apply(range_tombstone(
                clustered_row.key(), bound_kind::incl_start,
                clustered_row.key(), bound_kind::incl_end, clustered_row.tomb().tomb()));
    }

    if (schema.clustering_key_size()) {
        column_name_helper::min_max_components(schema, _collector.min_column_names(), _collector.max_column_names(),
            clustered_row.key().components());
    }

    // Write all cells of a partition's row.
    clustered_row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
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
                maybe_flush_pi_block(out, composite(), { bytes_view(clustering_key) });
                write_column_name(out, bytes_view(clustering_key));
            } else {
                maybe_flush_pi_block(out, clustering_key, { bytes_view(column_name) });
                write_column_name(out, clustering_key, { bytes_view(column_name) });
            }
        } else {
            if (schema.is_dense()) {
                maybe_flush_pi_block(out, composite(), { bytes_view(clustered_row.key().get_component(schema, 0)) });
                write_column_name(out, bytes_view(clustered_row.key().get_component(schema, 0)));
            } else {
                maybe_flush_pi_block(out, composite(), { bytes_view(column_name) });
                write_column_name(out, bytes_view(column_name));
            }
        }
        write_cell(out, cell, column_definition);
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
        const auto& column_name = column_definition.name();
        if (schema.is_compound()) {
            auto sp = composite::static_prefix(schema);
            maybe_flush_pi_block(out, sp, { bytes_view(column_name) });
            write_column_name(out, sp, { bytes_view(column_name) });
        } else {
            assert(!schema.is_dense());
            maybe_flush_pi_block(out, composite(), { bytes_view(column_name) });
            write_column_name(out, bytes_view(column_name));
        }
        atomic_cell_view cell = c.as_atomic_cell();
        write_cell(out, cell, column_definition);
    });
}

static void write_index_header(file_writer& out, disk_string_view<uint16_t>& key, uint64_t pos) {
    write(out, key, pos);
}

static void write_index_promoted(file_writer& out, bytes_ostream& promoted_index,
        deletion_time deltime, uint32_t numblocks) {
    uint32_t promoted_index_size = promoted_index.size();
    if (promoted_index_size) {
        promoted_index_size += 16 /* deltime + numblocks */;
        write(out, promoted_index_size, deltime, numblocks, promoted_index);
    } else {
        write(out, promoted_index_size);
    }
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
    c.set_uncompressed_chunk_length(cp.chunk_length());
    // FIXME: crc_check_chance can be configured by the user.
    // probability to verify the checksum of a compressed chunk we read.
    // defaults to 1.0.
    c.options.elements.push_back({"crc_check_chance", "1.0"});
    c.init_full_checksum();
}

static
void
populate_statistics_offsets(statistics& s) {
    // copy into a sorted vector to guarantee consistent order
    auto types = boost::copy_range<std::vector<metadata_type>>(s.contents | boost::adaptors::map_keys);
    boost::sort(types);

    // populate the hash with garbage so we can calculate its size
    for (auto t : types) {
        s.hash.map[t] = -1;
    }

    auto offset = serialized_size(s.hash);
    for (auto t : types) {
        s.hash.map[t] = offset;
        offset += s.contents[t]->serialized_size();
    }
}

static
sharding_metadata
create_sharding_metadata(schema_ptr schema, const dht::decorated_key& first_key, const dht::decorated_key& last_key, shard_id shard) {
    auto prange = dht::partition_range::make(dht::ring_position(first_key), dht::ring_position(last_key));
    auto sm = sharding_metadata();
    for (auto&& range : dht::split_range_to_single_shard(*schema, prange, shard)) {
        if (true) { // keep indentation
            // we know left/right are not infinite
            auto&& left = range.start()->value();
            auto&& right = range.end()->value();
            auto&& left_token = left.token();
            auto left_exclusive = !left.has_key() && left.bound() == dht::ring_position::token_bound::end;
            auto&& right_token = right.token();
            auto right_exclusive = !right.has_key() && right.bound() == dht::ring_position::token_bound::start;
            sm.token_ranges.elements.push_back(disk_token_range{
                {left_exclusive, to_bytes(bytes_view(left_token._data))},
                {right_exclusive, to_bytes(bytes_view(right_token._data))}});
        }
    }
    return sm;
}


// In the beginning of the statistics file, there is a disk_hash used to
// map each metadata type to its correspondent position in the file.
static void seal_statistics(statistics& s, metadata_collector& collector,
        const sstring partitioner, double bloom_filter_fp_chance, schema_ptr schema,
        const dht::decorated_key& first_key, const dht::decorated_key& last_key) {
    validation_metadata validation;
    compaction_metadata compaction;
    stats_metadata stats;

    validation.partitioner.value = to_bytes(partitioner);
    validation.filter_chance = bloom_filter_fp_chance;
    s.contents[metadata_type::Validation] = std::make_unique<validation_metadata>(std::move(validation));

    collector.construct_compaction(compaction);
    s.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>(std::move(compaction));

    collector.construct_stats(stats);
    s.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));

    populate_statistics_offsets(s);
}

void components_writer::maybe_add_summary_entry(summary& s, const dht::token& token, bytes_view key, uint64_t data_offset,
        uint64_t index_offset, uint64_t& next_data_offset_to_write_summary, size_t summary_byte_cost) {
    // generates a summary entry when possible (= keep summary / data size ratio within reasonable limits)
    if (data_offset >= next_data_offset_to_write_summary) {
        auto entry_size = 8 + 2 + key.size();  // offset + key_size.size + key.size
        next_data_offset_to_write_summary += summary_byte_cost * entry_size;
        s.entries.push_back({ token, bytes(key.data(), key.size()), index_offset });
    }
}

void components_writer::maybe_add_summary_entry(const dht::token& token,  bytes_view key) {
    return maybe_add_summary_entry(_sst._components->summary, token, key, get_offset(),
        _index.offset(), _next_data_offset_to_write_summary, _summary_byte_cost);
}

// Returns offset into data component.
uint64_t components_writer::get_offset() const {
    if (_sst.has_component(sstable::component_type::CompressionInfo)) {
        // Variable returned by compressed_file_length() is constantly updated by compressed output stream.
        return _sst._components->compression.compressed_file_length();
    } else {
        return _out.offset();
    }
}

file_writer components_writer::index_file_writer(sstable& sst, const io_priority_class& pc) {
    file_output_stream_options options;
    options.buffer_size = sst.sstable_buffer_size;
    options.io_priority_class = pc;
    options.write_behind = 10;
    return file_writer(std::move(sst._index_file), std::move(options));
}

// Get the currently loaded configuration, or the default configuration in
// case none has been loaded (this happens, for example, in unit tests).
static const db::config& get_config() {
    if (service::get_storage_service().local_is_initialized() &&
            service::get_local_storage_service().db().local_is_initialized()) {
        return service::get_local_storage_service().db().local().get_config();
    } else {
        static db::config default_config;
        return default_config;
    }
}

// Returns the cost for writing a byte to summary such that the ratio of summary
// to data will be 1 to cost by the time sstable is sealed.
static size_t summary_byte_cost() {
    auto summary_ratio = get_config().sstable_summary_ratio();
    return summary_ratio ? (1 / summary_ratio) : components_writer::default_summary_byte_cost;
}

components_writer::components_writer(sstable& sst, const schema& s, file_writer& out,
                                     uint64_t estimated_partitions,
                                     const sstable_writer_config& cfg,
                                     const io_priority_class& pc)
    : _sst(sst)
    , _schema(s)
    , _out(out)
    , _index(index_file_writer(sst, pc))
    , _index_needs_close(true)
    , _max_sstable_size(cfg.max_sstable_size)
    , _tombstone_written(false)
    , _summary_byte_cost(summary_byte_cost())
{
    _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _schema.bloom_filter_fp_chance());
    _sst._pi_write.desired_block_size = cfg.promoted_index_block_size.value_or(get_config().column_index_size_in_kb() * 1024);

    prepare_summary(_sst._components->summary, estimated_partitions, _schema.min_index_interval());

    // FIXME: we may need to set repaired_at stats at this point.
}

void components_writer::consume_new_partition(const dht::decorated_key& dk) {
    // Set current index of data to later compute row size.
    _sst._c_stats.start_offset = _out.offset();

    _partition_key = key::from_partition_key(_schema, dk.key());

    maybe_add_summary_entry(dk.token(), bytes_view(*_partition_key));
    _sst._components->filter->add(bytes_view(*_partition_key));
    _sst._collector.add_key(bytes_view(*_partition_key));

    auto p_key = disk_string_view<uint16_t>();
    p_key.value = bytes_view(*_partition_key);

    // Write index file entry from partition key into index file.
    // Write an index entry minus the "promoted index" (sample of columns)
    // part. We can only write that after processing the entire partition
    // and collecting the sample of columns.
    write_index_header(_index, p_key, _out.offset());
    _sst._pi_write.data = {};
    _sst._pi_write.numblocks = 0;
    _sst._pi_write.deltime.local_deletion_time = std::numeric_limits<int32_t>::max();
    _sst._pi_write.deltime.marked_for_delete_at = std::numeric_limits<int64_t>::min();
    _sst._pi_write.block_start_offset = _out.offset();
    _sst._pi_write.tombstone_accumulator = range_tombstone_accumulator(_schema, false);
    _sst._pi_write.schemap = &_schema; // sadly we need this

    // Write partition key into data file.
    write(_out, p_key);

    _tombstone_written = false;
}

void components_writer::consume(tombstone t) {
    deletion_time d;

    if (t) {
        d.local_deletion_time = t.deletion_time.time_since_epoch().count();
        d.marked_for_delete_at = t.timestamp;

        _sst._c_stats.tombstone_histogram.update(d.local_deletion_time);
        _sst._c_stats.update_max_local_deletion_time(d.local_deletion_time);
        _sst._c_stats.update_min_timestamp(d.marked_for_delete_at);
        _sst._c_stats.update_max_timestamp(d.marked_for_delete_at);
    } else {
        // Default values for live, undeleted rows.
        d.local_deletion_time = std::numeric_limits<int32_t>::max();
        d.marked_for_delete_at = std::numeric_limits<int64_t>::min();
    }
    write(_out, d);
    _tombstone_written = true;
    // TODO: need to verify we don't do this twice?
    _sst._pi_write.deltime = d;
}

stop_iteration components_writer::consume(static_row&& sr) {
    ensure_tombstone_is_written();
    _sst.write_static_row(_out, _schema, sr.cells());
    return stop_iteration::no;
}

stop_iteration components_writer::consume(clustering_row&& cr) {
    ensure_tombstone_is_written();
    _sst.write_clustered_row(_out, _schema, cr);
    return stop_iteration::no;
}

stop_iteration components_writer::consume(range_tombstone&& rt) {
    ensure_tombstone_is_written();
    // Remember the range tombstone so when we need to open a new promoted
    // index block, we can figure out which ranges are still open and need
    // to be repeated in the data file. Note that apply() also drops ranges
    // already closed by rt.start, so the accumulator doesn't grow boundless.
    _sst._pi_write.tombstone_accumulator->apply(rt);
    auto start = composite::from_clustering_element(_schema, std::move(rt.start));
    auto start_marker = bound_kind_to_start_marker(rt.start_kind);
    auto end = composite::from_clustering_element(_schema, std::move(rt.end));
    auto end_marker = bound_kind_to_end_marker(rt.end_kind);
    _sst.maybe_flush_pi_block(_out, start, {}, start_marker);
    _sst.write_range_tombstone(_out, std::move(start), start_marker, std::move(end), end_marker, {}, rt.tomb);
    return stop_iteration::no;
}

stop_iteration components_writer::consume_end_of_partition() {
    // If there is an incomplete block in the promoted index, write it too.
    // However, if the _promoted_index is still empty, don't add a single
    // chunk - better not output a promoted index at all in this case.
    if (!_sst._pi_write.data.empty() && !_sst._pi_write.block_first_colname.empty()) {
        output_promoted_index_entry(_sst._pi_write.data,
            _sst._pi_write.block_first_colname,
            _sst._pi_write.block_last_colname,
            _sst._pi_write.block_start_offset - _sst._c_stats.start_offset,
            _out.offset() - _sst._pi_write.block_start_offset);
        _sst._pi_write.numblocks++;
    }
    write_index_promoted(_index, _sst._pi_write.data, _sst._pi_write.deltime,
            _sst._pi_write.numblocks);
    _sst._pi_write.data = {};
    _sst._pi_write.block_first_colname = {};

    ensure_tombstone_is_written();
    int16_t end_of_row = 0;
    write(_out, end_of_row);

    // compute size of the current row.
    _sst._c_stats.row_size = _out.offset() - _sst._c_stats.start_offset;
    // update is about merging column_stats with the data being stored by collector.
    _sst._collector.update(_schema, std::move(_sst._c_stats));
    _sst._c_stats.reset();

    if (!_first_key) {
        _first_key = *_partition_key;
    }
    _last_key = std::move(*_partition_key);

    return get_offset() < _max_sstable_size ? stop_iteration::no : stop_iteration::yes;
}

void components_writer::consume_end_of_stream() {
    seal_summary(_sst._components->summary, std::move(_first_key), std::move(_last_key)); // what if there is only one partition? what if it is empty?

    _index_needs_close = false;
    _index.close().get();

    if (_sst.has_component(sstable::component_type::CompressionInfo)) {
        _sst._collector.add_compression_ratio(_sst._components->compression.compressed_file_length(), _sst._components->compression.uncompressed_file_length());
    }

    _sst.set_first_and_last_keys();
    seal_statistics(_sst._components->statistics, _sst._collector, dht::global_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key());
}

components_writer::~components_writer() {
    if (_index_needs_close) {
        try {
            _index.close().get();
        } catch (...) {
            sstlog.error("components_writer failed to close file: {}", std::current_exception());
        }
    }
}

future<>
sstable::read_scylla_metadata(const io_priority_class& pc) {
    if (_components->scylla_metadata) {
        return make_ready_future<>();
    }
    return read_toc().then([this, &pc] {
        _components->scylla_metadata.emplace();  // engaged optional means we won't try to re-read this again
        if (!has_component(component_type::Scylla)) {
            return make_ready_future<>();
        }
        return read_simple<component_type::Scylla>(*_components->scylla_metadata, pc);
    });
}

void
sstable::write_scylla_metadata(const io_priority_class& pc, shard_id shard) {
    auto&& first_key = get_first_decorated_key();
    auto&& last_key = get_last_decorated_key();
    auto sm = create_sharding_metadata(_schema, first_key, last_key, shard);
    _components->scylla_metadata.emplace();
    _components->scylla_metadata->data.set<scylla_metadata_type::Sharding>(std::move(sm));

    write_simple<component_type::Scylla>(*_components->scylla_metadata, pc);
}

void sstable_writer::prepare_file_writer()
{
    file_output_stream_options options;
    options.io_priority_class = _pc;
    options.buffer_size = _sst.sstable_buffer_size;
    options.write_behind = 10;

    if (!_compression_enabled) {
        _writer = std::make_unique<checksummed_file_writer>(std::move(_sst._data_file), std::move(options), true);
    } else {
        prepare_compression(_sst._components->compression, _schema);
        _writer = std::make_unique<file_writer>(make_compressed_file_output_stream(std::move(_sst._data_file), std::move(options), &_sst._components->compression));
    }
}

void sstable_writer::finish_file_writer()
{
    auto writer = std::move(_writer);
    writer->close().get();

    if (!_compression_enabled) {
        auto chksum_wr = static_cast<checksummed_file_writer*>(writer.get());
        write_digest(_sst._write_error_handler, _sst.filename(sstable::component_type::Digest), chksum_wr->full_checksum());
        write_crc(_sst._write_error_handler, _sst.filename(sstable::component_type::CRC), chksum_wr->finalize_checksum());
    } else {
        write_digest(_sst._write_error_handler, _sst.filename(sstable::component_type::Digest), _sst._components->compression.full_checksum());
    }
}

sstable_writer::~sstable_writer() {
    if (_writer) {
        try {
            _writer->close().get();
        } catch (...) {
            sstlog.error("sstable_writer failed to close file: {}", std::current_exception());
        }
    }
}

sstable_writer::sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
                               const sstable_writer_config& cfg, const io_priority_class& pc, shard_id shard)
    : _sst(sst)
    , _schema(s)
    , _pc(pc)
    , _backup(cfg.backup)
    , _leave_unsealed(cfg.leave_unsealed)
    , _shard(shard)
    , _monitor(cfg.monitor)
{
    _sst.generate_toc(_schema.get_compressor_params().get_compressor(), _schema.bloom_filter_fp_chance());
    _sst.write_toc(_pc);
    _sst.create_data().get();
    _compression_enabled = !_sst.has_component(sstable::component_type::CRC);
    prepare_file_writer();
    _components_writer.emplace(_sst, _schema, *_writer, estimated_partitions, cfg, _pc);
}

void sstable_writer::consume_end_of_stream()
{
    _components_writer->consume_end_of_stream();
    _components_writer = stdx::nullopt;
    finish_file_writer();
    _sst.write_summary(_pc);
    _sst.write_filter(_pc);
    _sst.write_statistics(_pc);
    _sst.write_compression(_pc);
    _sst.write_scylla_metadata(_pc, _shard);

    _monitor->on_write_completed();

    if (!_leave_unsealed) {
        _sst.seal_sstable(_backup).get();
    }

    _monitor->on_flush_completed();
}

future<> sstable::seal_sstable(bool backup)
{
    return seal_sstable().then([this, backup] {
        if (backup) {
            auto dir = get_dir() + "/backups/";
            return sstable_write_io_check(touch_directory, dir).then([this, dir] {
                return create_links(dir);
            });
        }
        return make_ready_future<>();
    });
}

sstable_writer sstable::get_writer(const schema& s, uint64_t estimated_partitions, const sstable_writer_config& cfg, const io_priority_class& pc, shard_id shard)
{
    return sstable_writer(*this, s, estimated_partitions, cfg, pc, shard);
}

future<> sstable::write_components(
        mutation_reader mr,
        uint64_t estimated_partitions,
        schema_ptr schema,
        const sstable_writer_config& cfg,
        const io_priority_class& pc) {
    if (cfg.replay_position) {
        _collector.set_replay_position(cfg.replay_position.value());
    }
    seastar::thread_attributes attr;
    attr.scheduling_group = cfg.thread_scheduling_group;
    return seastar::async(std::move(attr), [this, mr = std::move(mr), estimated_partitions, schema = std::move(schema), cfg, &pc] () mutable {
        auto wr = get_writer(*schema, estimated_partitions, cfg, pc);
        consume_flattened_in_thread(mr, wr);
    });
}

future<> sstable::generate_summary(const io_priority_class& pc) {
    if (_components->summary) {
        return make_ready_future<>();
    }

    sstlog.info("Summary file {} not found. Generating Summary...", filename(sstable::component_type::Summary));
    class summary_generator {
        summary& _summary;
        uint64_t _data_size;
        size_t _summary_byte_cost;
        uint64_t _next_data_offset_to_write_summary = 0;
    public:
        std::experimental::optional<key> first_key, last_key;

        summary_generator(summary& s, uint64_t data_size) : _summary(s), _data_size(data_size), _summary_byte_cost(summary_byte_cost()) {}
        bool should_continue() {
            return true;
        }
        void consume_entry(index_entry&& ie, uint64_t index_offset) {
            auto token = dht::global_partitioner().get_token(ie.get_key());
            components_writer::maybe_add_summary_entry(_summary, token, ie.get_key_bytes(), _data_size, index_offset,
                _next_data_offset_to_write_summary, _summary_byte_cost);
            if (!first_key) {
                first_key = key(to_bytes(ie.get_key_bytes()));
            } else {
                last_key = key(to_bytes(ie.get_key_bytes()));
            }
        }
    };

    return open_checked_file_dma(_read_error_handler, filename(component_type::Index), open_flags::ro).then([this, &pc] (file index_file) {
        return do_with(std::move(index_file), [this, &pc] (file index_file) {
            return seastar::when_all_succeed(
                    io_check([&] { return engine().file_size(this->filename(sstable::component_type::Data)); }),
                    index_file.size()).then([this, &pc, index_file] (auto data_size, auto index_size) {
                // an upper bound. Surely to be less than this.
                auto estimated_partitions = index_size / sizeof(uint64_t);
                prepare_summary(_components->summary, estimated_partitions, _schema->min_index_interval());

                file_input_stream_options options;
                options.buffer_size = sstable_buffer_size;
                options.io_priority_class = pc;
                auto stream = make_file_input_stream(index_file, 0, index_size, std::move(options));
                return do_with(summary_generator(_components->summary, data_size),
                        [this, &pc, stream = std::move(stream), index_size] (summary_generator& s) mutable {
                    auto ctx = make_lw_shared<index_consume_entry_context<summary_generator>>(s, std::move(stream), 0, index_size);
                    return ctx->consume_input(*ctx).finally([ctx] {
                        return ctx->close();
                    }).then([this, ctx, &s] {
                        seal_summary(_components->summary, std::move(s.first_key), std::move(s.last_key));
                    });
                });
            }).then([index_file] () mutable {
                return index_file.close().handle_exception([] (auto ep) {
                    sstlog.warn("sstable close index_file failed: {}", ep);
                    general_disk_error();
                });
            });
        });
    });
}

uint64_t sstable::data_size() const {
    if (has_component(sstable::component_type::CompressionInfo)) {
        return _components->compression.uncompressed_file_length();
    }
    return _data_file_size;
}

uint64_t sstable::ondisk_data_size() const {
    return _data_file_size;
}

uint64_t sstable::bytes_on_disk() {
    assert(_bytes_on_disk > 0);
    return _bytes_on_disk;
}

const bool sstable::has_component(component_type f) const {
    return _recognized_components.count(f);
}

const sstring sstable::filename(component_type f) const {
    return filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, f);
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

const sstring sstable::filename(sstring dir, sstring ks, sstring cf, version_types version, int64_t generation,
                                format_types format, sstring component) {
    static std::unordered_map<version_types, const char*, enum_hash<version_types>> fmtmap = {
        { sstable::version_types::ka, "{0}-{1}-{2}-{3}-{5}" },
        { sstable::version_types::la, "{2}-{3}-{4}-{5}" }
    };

    return dir + "/" + seastar::format(fmtmap[version], ks, cf, _version_string.at(version), to_sstring(generation), _format_string.at(format), component);
}

std::vector<std::pair<sstable::component_type, sstring>> sstable::all_components() const {
    std::vector<std::pair<component_type, sstring>> all;
    all.reserve(_recognized_components.size() + _unrecognized_components.size());
    for (auto& c : _recognized_components) {
        all.push_back(std::make_pair(c, _component_map.at(c)));
    }
    for (auto& c : _unrecognized_components) {
        all.push_back(std::make_pair(component_type::Unknown, c));
    }
    return all;
}

future<> sstable::create_links(sstring dir, int64_t generation) const {
    // TemporaryTOC is always first, TOC is always last
    auto dst = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, component_type::TemporaryTOC);
    return sstable_write_io_check(::link_file, filename(component_type::TOC), dst).then([this, dir] {
        return sstable_write_io_check(sync_directory, dir);
    }).then([this, dir, generation] {
        // FIXME: Should clean already-created links if we failed midway.
        return parallel_for_each(all_components(), [this, dir, generation] (auto p) {
            if (p.first == component_type::TOC) {
                return make_ready_future<>();
            }
            auto src = sstable::filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, p.second);
            auto dst = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, p.second);
            return this->sstable_write_io_check(::link_file, std::move(src), std::move(dst));
        });
    }).then([this, dir] {
        return sstable_write_io_check(sync_directory, dir);
    }).then([dir, this, generation] {
        auto src = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, component_type::TemporaryTOC);
        auto dst = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, component_type::TOC);
        return sstable_write_io_check([&] {
            return engine().rename_file(src, dst);
        });
    }).then([this, dir] {
        return sstable_write_io_check(sync_directory, dir);
    });
}

future<> sstable::set_generation(int64_t new_generation) {
    return create_links(_dir, new_generation).then([this] {
        return remove_file(filename(component_type::TOC)).then([this] {
            return sstable_write_io_check(sync_directory, _dir);
        }).then([this] {
            return parallel_for_each(all_components(), [this] (auto p) {
                if (p.first == component_type::TOC) {
                    return make_ready_future<>();
                }
                return remove_file(sstable::filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, p.second));
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
    try {
        return reverse_map(s, _component_map);
    } catch (std::out_of_range&) {
        return component_type::Unknown;
    }
}

input_stream<char> sstable::data_stream(uint64_t pos, size_t len, const io_priority_class& pc, reader_resource_tracker resource_tracker, lw_shared_ptr<file_input_stream_history> history) {
    file_input_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = 4;
    options.dynamic_adjustments = std::move(history);

    auto f = resource_tracker.track(_data_file);

    input_stream<char> stream;
    if (_components->compression) {
        return make_compressed_file_input_stream(f, &_components->compression,
                pos, len, std::move(options));

    }

    return make_file_input_stream(f, pos, len, std::move(options));
}

future<temporary_buffer<char>> sstable::data_read(uint64_t pos, size_t len, const io_priority_class& pc) {
    return do_with(data_stream(pos, len, pc, no_resource_tracking(), {}), [len] (auto& stream) {
        return stream.read_exactly(len).finally([&stream] {
            return stream.close();
        });
    });
}

void sstable::set_first_and_last_keys() {
    if (_first && _last) {
        return;
    }
    auto decorate_key = [this] (const char *m, const bytes& value) {
        if (value.empty()) {
            throw std::runtime_error(sprint("%s key of summary of %s is empty", m, get_filename()));
        }
        auto pk = key::from_bytes(value).to_partition_key(*_schema);
        return dht::global_partitioner().decorate_key(*_schema, std::move(pk));
    };
    _first = decorate_key("first", _components->summary.first_key.value);
    _last = decorate_key("last", _components->summary.last_key.value);
}

const partition_key& sstable::get_first_partition_key() const {
    return get_first_decorated_key().key();
 }

const partition_key& sstable::get_last_partition_key() const {
    return get_last_decorated_key().key();
}

const dht::decorated_key& sstable::get_first_decorated_key() const {
    if (!_first) {
        throw std::runtime_error(sprint("first key of %s wasn't set", get_filename()));
    }
    return *_first;
}

const dht::decorated_key& sstable::get_last_decorated_key() const {
    if (!_last) {
        throw std::runtime_error(sprint("last key of %s wasn't set", get_filename()));
    }
    return *_last;
}

int sstable::compare_by_first_key(const sstable& other) const {
    return get_first_decorated_key().tri_compare(*_schema, other.get_first_decorated_key());
}

double sstable::get_compression_ratio() const {
    if (this->has_component(sstable::component_type::CompressionInfo)) {
        return double(_components->compression.compressed_file_length()) / _components->compression.uncompressed_file_length();
    } else {
        return metadata_collector::NO_COMPRESSION_RATIO;
    }
}

std::unordered_set<uint64_t> sstable::ancestors() const {
    const compaction_metadata& cm = get_compaction_metadata();
    return boost::copy_range<std::unordered_set<uint64_t>>(cm.ancestors.elements);
}

void sstable::set_sstable_level(uint32_t new_level) {
    auto entry = _components->statistics.contents.find(metadata_type::Stats);
    if (entry == _components->statistics.contents.end()) {
        return;
    }
    auto& p = entry->second;
    if (!p) {
        throw std::runtime_error("Statistics is malformed");
    }
    stats_metadata& s = *static_cast<stats_metadata *>(p.get());
    sstlog.debug("set level of {} with generation {} from {} to {}", get_filename(), _generation, s.sstable_level, new_level);
    s.sstable_level = new_level;
}

future<> sstable::mutate_sstable_level(uint32_t new_level) {
    if (!has_component(component_type::Statistics)) {
        return make_ready_future<>();
    }

    auto entry = _components->statistics.contents.find(metadata_type::Stats);
    if (entry == _components->statistics.contents.end()) {
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
        rewrite_statistics(default_priority_class());
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
            delete_atomically({sstable_to_delete(filename(component_type::TOC), _shared)}).handle_exception(
                        [op = background_jobs().start()] (std::exception_ptr eptr) {
                            try {
                                std::rethrow_exception(eptr);
                            } catch (atomic_deletion_cancelled&) {
                                sstlog.debug("Exception when deleting sstable file: {}", eptr);
                            } catch (...) {
                                sstlog.warn("Exception when deleting sstable file: {}", eptr);
                            }
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
fsync_directory(const io_error_handler& error_handler, sstring fname) {
    return ::sstable_io_check(error_handler, [&] {
        return open_checked_directory(error_handler, dirname(fname)).then([] (file f) {
            return do_with(std::move(f), [] (file& f) {
                return f.flush().then([&f] {
                    return f.close();
                });
            });
        });
    });
}

future<>
remove_by_toc_name(sstring sstable_toc_name, const io_error_handler& error_handler) {
    return seastar::async([sstable_toc_name, &error_handler] () mutable {
        sstring prefix = sstable_toc_name.substr(0, sstable_toc_name.size() - TOC_SUFFIX.size());
        auto new_toc_name = prefix + TEMPORARY_TOC_SUFFIX;
        sstring dir;

        if (sstable_io_check(error_handler, file_exists, sstable_toc_name).get0()) {
            dir = dirname(sstable_toc_name);
            sstable_io_check(error_handler, rename_file, sstable_toc_name, new_toc_name).get();
            fsync_directory(error_handler, dir).get();
        } else if (sstable_io_check(error_handler, file_exists, new_toc_name).get0()) {
            dir = dirname(new_toc_name);
        } else {
            sstlog.warn("Unable to delete {} because it doesn't exist.", sstable_toc_name);
            return;
        }

        auto toc_file = open_checked_file_dma(error_handler, new_toc_name, open_flags::ro).get0();
        auto in = make_file_input_stream(toc_file);
        auto size = toc_file.size().get0();
        auto text = in.read_exactly(size).get0();
        in.close().get();
        std::vector<sstring> components;
        sstring all(text.begin(), text.end());
        boost::split(components, all, boost::is_any_of("\n"));
        parallel_for_each(components, [prefix, &error_handler] (sstring component) mutable {
            if (component.empty()) {
                // eof
                return make_ready_future<>();
            }
            if (component == TOC_SUFFIX) {
                // already deleted
                return make_ready_future<>();
            }
            auto fname = prefix + component;
            return sstable_io_check(error_handler, remove_file, prefix + component).then_wrapped([fname = std::move(fname)] (future<> f) {
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
        fsync_directory(error_handler, dir).get();
        sstable_io_check(error_handler, remove_file, new_toc_name).get();
    });
}

future<>
sstable::remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f) {
    return seastar::async([ks, cf, dir, generation, v, f] {
        const io_error_handler& error_handler = sstable_write_error_handler;
        auto toc = sstable_io_check(error_handler, file_exists, filename(dir, ks, cf, v, generation, f, component_type::TOC)).get0();
        // assert that toc doesn't exist for sstable with temporary toc.
        assert(toc == false);

        auto tmptoc = sstable_io_check(error_handler, file_exists, filename(dir, ks, cf, v, generation, f, component_type::TemporaryTOC)).get0();
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
            auto exists = sstable_io_check(error_handler, file_exists, file_path).get0();
            if (!exists) {
                continue;
            }
            sstable_io_check(error_handler, remove_file, file_path).get();
        }
        fsync_directory(error_handler, dir).get();
        // Removing temporary
        sstable_io_check(error_handler, remove_file, filename(dir, ks, cf, v, generation, f, component_type::TemporaryTOC)).get();
        // Fsync'ing column family dir to guarantee that deletion completed.
        fsync_directory(error_handler, dir).get();
    });
}

future<range<partition_key>>
sstable::get_sstable_key_range(const schema& s) {
    auto fut = read_summary(default_priority_class());
    return std::move(fut).then([this, &s] () mutable {
        this->set_first_and_last_keys();
        return make_ready_future<range<partition_key>>(range<partition_key>::make(get_first_partition_key(), get_last_partition_key()));
    });
}

/**
 * Returns a pair of positions [p1, p2) in the summary file corresponding to entries
 * covered by the specified range, or a disengaged optional if no such pair exists.
 */
stdx::optional<std::pair<uint64_t, uint64_t>> sstable::get_sample_indexes_for_range(const dht::token_range& range) {
    auto entries_size = _components->summary.entries.size();
    auto search = [this](bool before, const dht::token& token) {
        auto kind = before ? key::kind::before_all_keys : key::kind::after_all_keys;
        key k(kind);
        // Binary search will never returns positive values.
        return uint64_t((binary_search(_components->summary.entries, k, token) + 1) * -1);
    };
    uint64_t left = 0;
    if (range.start()) {
        left = search(range.start()->is_inclusive(), range.start()->value());
        if (left == entries_size) {
            // left is past the end of the sampling.
            return stdx::nullopt;
        }
    }
    uint64_t right = entries_size;
    if (range.end()) {
        right = search(!range.end()->is_inclusive(), range.end()->value());
        if (right == 0) {
            // The first key is strictly greater than right.
            return stdx::nullopt;
        }
    }
    if (left < right) {
        return stdx::optional<std::pair<uint64_t, uint64_t>>(stdx::in_place_t(), left, right);
    }
    return stdx::nullopt;
}

std::vector<dht::decorated_key> sstable::get_key_samples(const schema& s, const dht::token_range& range) {
    auto index_range = get_sample_indexes_for_range(range);
    std::vector<dht::decorated_key> res;
    if (index_range) {
        for (auto idx = index_range->first; idx < index_range->second; ++idx) {
            auto pkey = _components->summary.entries[idx].get_key().to_partition_key(s);
            res.push_back(dht::global_partitioner().decorate_key(s, std::move(pkey)));
        }
    }
    return res;
}

uint64_t sstable::estimated_keys_for_range(const dht::token_range& range) {
    auto sample_index_range = get_sample_indexes_for_range(range);
    uint64_t sample_key_count = sample_index_range ? sample_index_range->second - sample_index_range->first : 0;
    // adjust for the current sampling level
    uint64_t estimated_keys = sample_key_count * ((downsampling::BASE_SAMPLING_LEVEL * _components->summary.header.min_index_interval) / _components->summary.header.sampling_level);
    return std::max(uint64_t(1), estimated_keys);
}

std::vector<unsigned>
sstable::compute_shards_for_this_sstable() const {
    std::unordered_set<unsigned> shards;
    dht::partition_range_vector token_ranges;
    const auto* sm = _components->scylla_metadata
            ? _components->scylla_metadata->data.get<scylla_metadata_type::Sharding, sharding_metadata>()
            : nullptr;
    if (!sm) {
        token_ranges.push_back(dht::partition_range::make(
                dht::ring_position::starting_at(get_first_decorated_key().token()),
                dht::ring_position::ending_at(get_last_decorated_key().token())));
    } else {
        auto disk_token_range_to_ring_position_range = [] (const disk_token_range& dtr) {
            auto t1 = dht::token(dht::token::kind::key, managed_bytes(bytes_view(dtr.left.token)));
            auto t2 = dht::token(dht::token::kind::key, managed_bytes(bytes_view(dtr.right.token)));
            return dht::partition_range::make(
                    (dtr.left.exclusive ? dht::ring_position::ending_at : dht::ring_position::starting_at)(std::move(t1)),
                    (dtr.right.exclusive ? dht::ring_position::starting_at : dht::ring_position::ending_at)(std::move(t2)));
        };
        token_ranges = boost::copy_range<dht::partition_range_vector>(
                sm->token_ranges.elements
                | boost::adaptors::transformed(disk_token_range_to_ring_position_range));
    }
    auto sharder = dht::ring_position_range_vector_sharder(std::move(token_ranges));
    auto rpras = sharder.next(*_schema);
    while (rpras) {
        shards.insert(rpras->shard);
        rpras = sharder.next(*_schema);
    }
    return boost::copy_range<std::vector<unsigned>>(shards);
}

utils::hashed_key sstable::make_hashed_key(const schema& s, const partition_key& key) {
    return utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(s, key)));
}

std::ostream&
operator<<(std::ostream& os, const sstable_to_delete& std) {
    return os << std.name << "(" << (std.shared ? "shared" : "unshared") << ")";
}

future<>
delete_sstables(std::vector<sstring> tocs) {
    // FIXME: this needs to be done atomically (using a log file of sstables we intend to delete)
    return parallel_for_each(tocs, [] (sstring name) {
        return remove_by_toc_name(name);
    });
}

static thread_local atomic_deletion_manager g_atomic_deletion_manager(smp::count, delete_sstables);

future<>
delete_atomically(std::vector<sstable_to_delete> ssts) {
    auto shard = engine().cpu_id();
    return smp::submit_to(0, [=] {
        return g_atomic_deletion_manager.delete_atomically(ssts, shard);
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

void cancel_prior_atomic_deletions() {
    g_atomic_deletion_manager.cancel_prior_atomic_deletions();
}

void cancel_atomic_deletions() {
    g_atomic_deletion_manager.cancel_atomic_deletions();
}

atomic_deletion_cancelled::atomic_deletion_cancelled(std::vector<sstring> names)
        : _msg(sprint("atomic deletions cancelled; not deleting %s", names)) {
}

const char*
atomic_deletion_cancelled::what() const noexcept {
    return _msg.c_str();
}

thread_local shared_index_lists::stats shared_index_lists::_shard_stats;
static thread_local seastar::metrics::metric_groups metrics;

future<> init_metrics() {
  return seastar::smp::invoke_on_all([] {
    namespace sm = seastar::metrics;
    metrics.add_group("sstables", {
        sm::make_derive("index_page_hits", [] { return shared_index_lists::shard_stats().hits; },
            sm::description("Index page requests which could be satisfied without waiting")),
        sm::make_derive("index_page_misses", [] { return shared_index_lists::shard_stats().misses; },
            sm::description("Index page requests which initiated a read from disk")),
        sm::make_derive("index_page_blocks", [] { return shared_index_lists::shard_stats().blocks; },
            sm::description("Index page requests which needed to wait due to page not being loaded yet")),
    });
  });
}

mutation_source sstable::as_mutation_source() {
    return mutation_source([sst = shared_from_this()] (schema_ptr s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_ptr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) mutable {
        // CAVEAT: if as_mutation_source() is called on a single partition
        // we want to optimize and read exactly this partition. As a
        // consequence, fast_forward_to() will *NOT* work on the result,
        // regardless of what the fwd_mr parameter says.
        if (range.is_singular() && range.start()->value().has_key()) {
            return sst->read_row_flat(s, range.start()->value(), slice, pc, no_resource_tracking(), fwd);
        } else {
            return sst->read_range_rows_flat(s, range, slice, pc, no_resource_tracking(), fwd, fwd_mr);
        }
    });
}


}

namespace seastar {

void
lw_shared_ptr_deleter<sstables::sstable>::dispose(sstables::sstable* s) {
    delete s;
}

}
