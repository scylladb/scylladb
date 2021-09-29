/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_ptr_incomplete.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/util/closeable.hh>
#include <iterator>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "dht/sharder.hh"
#include "types.hh"
#include "writer.hh"
#include "m_format_read_helpers.hh"
#include "sstables.hh"
#include "sstable_writer.hh"
#include "metadata_collector.hh"
#include "progress_monitor.hh"
#include "compress.hh"
#include "unimplemented.hh"
#include "index_reader.hh"
#include "memtable.hh"
#include "range.hh"
#include "downsampling.hh"
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/insert.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/is_sorted.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <regex>
#include <seastar/core/align.hh>
#include "range_tombstone_list.hh"
#include "counters.hh"
#include "binary_search.hh"
#include "utils/bloom_filter.hh"
#include "utils/memory_data_sink.hh"
#include "utils/cached_file.hh"
#include "checked-file-impl.hh"
#include "integrity_checked_file_impl.hh"
#include "db/extensions.hh"
#include "unimplemented.hh"
#include "vint-serialization.hh"
#include "db/large_data_handler.hh"
#include "db/config.hh"
#include "sstables/random_access_reader.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/partition_index_cache.hh"
#include "utils/UUID_gen.hh"
#include "database.hh"
#include "sstables_manager.hh"
#include <boost/algorithm/string/predicate.hpp>
#include "tracing/traced_file.hh"
#include "kl/reader.hh"
#include "mx/reader.hh"
#include "utils/bit_cast.hh"
#include "utils/cached_file.hh"

thread_local disk_error_signal_type sstable_read_error;
thread_local disk_error_signal_type sstable_write_error;

namespace sstables {

logging::logger sstlog("sstable");

namespace bi = boost::intrusive;

class sstable_tracker {
    bi::list<sstable,
        bi::member_hook<sstable, sstable::tracker_link_type, &sstable::_tracker_link>,
        bi::constant_time_size<false>> _sstables;
public:
    void add(sstable& sst) {
        _sstables.push_back(sst);
    }
};

static thread_local sstable_tracker tracker;

// Because this is a noop and won't hold any state, it is better to use a global than a
// thread_local. It will be faster, specially on non-x86.
struct noop_write_monitor final : public write_monitor {
    virtual void on_write_started(const writer_offset_tracker&) override { };
    virtual void on_data_write_completed() override { }
};
static noop_write_monitor default_noop_write_monitor;
write_monitor& default_write_monitor() {
    return default_noop_write_monitor;
}

static noop_read_monitor default_noop_read_monitor;
read_monitor& default_read_monitor() {
    return default_noop_read_monitor;
}

static no_read_monitoring noop_read_monitor_generator;
read_monitor_generator& default_read_monitor_generator() {
    return noop_read_monitor_generator;
}

static future<file> open_sstable_component_file_non_checked(std::string_view name, open_flags flags, file_open_options options,
        bool check_integrity) noexcept {
    if (flags != open_flags::ro && check_integrity) {
        return open_integrity_checked_file_dma(name, flags, options);
    }
    return open_file_dma(name, flags, options);
}

future<> sstable::rename_new_sstable_component_file(sstring from_name, sstring to_name) {
    return sstable_write_io_check(rename_file, from_name, to_name).handle_exception([from_name, to_name] (std::exception_ptr ep) {
        sstlog.error("Could not rename SSTable component {} to {}. Found exception: {}", from_name, to_name, ep);
        return make_exception_future<>(ep);
    });
}

future<file> sstable::new_sstable_component_file(const io_error_handler& error_handler, component_type type, open_flags flags, file_open_options options) noexcept {
  try {
    auto create_flags = open_flags::create | open_flags::exclusive;
    auto readonly = (flags & create_flags) != create_flags;
    auto name = !readonly && _temp_dir ? temp_filename(type) : filename(type);

    auto f = open_sstable_component_file_non_checked(name, flags, options,
                    _manager.config().enable_sstable_data_integrity_check());

    if (type != component_type::TOC && type != component_type::TemporaryTOC) {
        for (auto * ext : _manager.config().extensions().sstable_file_io_extensions()) {
            f = with_file_close_on_failure(std::move(f), [ext, this, type, flags] (file f) {
               return ext->wrap_file(*this, type, f, flags).then([f](file nf) mutable {
                   return nf ? nf : std::move(f);
               });
            });
        }
    }

    f = with_file_close_on_failure(std::move(f), [&error_handler] (file f) {
        return make_checked_file(error_handler, std::move(f));
    });

    if (!readonly) {
        f = with_file_close_on_failure(std::move(f).handle_exception([name] (auto ep) {
            sstlog.error("Could not create SSTable component {}. Found exception: {}", name, ep);
            return make_exception_future<file>(ep);
        }), [this, type, name = std::move(name)] (file fd) mutable {
            return rename_new_sstable_component_file(name, filename(type)).then([fd = std::move(fd)] () mutable {
                return make_ready_future<file>(std::move(fd));
            });
        });
    }
    return f;
  } catch (...) {
      return current_exception_as_future<file>();
  }
}

std::unordered_map<sstable::version_types, sstring, enum_hash<sstable::version_types>> sstable::_version_string = {
    { sstable::version_types::ka , "ka" },
    { sstable::version_types::la , "la" },
    { sstable::version_types::mc , "mc" },
    { sstable::version_types::md , "md" },
};

std::unordered_map<sstable::format_types, sstring, enum_hash<sstable::format_types>> sstable::_format_string = {
    { sstable::format_types::big , "big" }
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

template <typename T>
requires std::is_integral_v<T>
future<> parse(const schema&, sstable_version_types v, random_access_reader& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));
        i = net::ntoh(read_unaligned<T>(buf.get()));
        return make_ready_future<>();
    });
}

template <typename T>
requires std::is_enum_v<T>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));
        i = static_cast<T>(net::ntoh(read_unaligned<std::underlying_type_t<T>>(buf.get())));
        return make_ready_future<>();
    });
}

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, bool& i) {
    return parse(s, v, in, reinterpret_cast<uint8_t&>(i));
}

future<> parse(const schema&, sstable_version_types, random_access_reader& in, double& d) {
    return in.read_exactly(sizeof(double)).then([&d] (auto buf) {
        check_buf_size(buf, sizeof(double));
        unsigned long nr = read_unaligned<unsigned long>(buf.get());
        d = bit_cast<double>(net::ntoh(nr));
        return make_ready_future<>();
    });
}

template <typename T>
future<> parse(const schema&, sstable_version_types, random_access_reader& in, T& len, bytes& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        // Likely a different type of char. Most bufs are unsigned, whereas the bytes type is signed.
        s = bytes(reinterpret_cast<const bytes::value_type *>(buf.get()), len);
    });
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, First& first, Rest&&... rest) {
    return parse(s, v, in, first).then([v, &s, &in, &rest...] {
        return parse(s, v, in, std::forward<Rest>(rest)...);
    });
}

// Intended to be used for a type that describes itself through describe_type().
template <self_describing T>
future<>
parse(const schema& s, sstable_version_types v, random_access_reader& in, T& t) {
    return t.describe_type(v, [v, &s, &in] (auto&&... what) -> future<> {
        return parse(s, v, in, what...);
    });
}

template <class T>
future<> parse(const schema&, sstable_version_types v, random_access_reader& in, vint<T>& t) {
    return read_vint(in, t.value);
}

future<> parse(const schema&, sstable_version_types, random_access_reader& in, utils::UUID& uuid) {
    return in.read_exactly(uuid.serialized_size()).then([&uuid] (temporary_buffer<char> buf) {
        check_buf_size(buf, utils::UUID::serialized_size());

        uuid = utils::UUID_gen::get_UUID(const_cast<int8_t*>(reinterpret_cast<const int8_t*>(buf.get())));
    });
}

inline void write(sstable_version_types v, file_writer& out, const utils::UUID& uuid) {
    out.write(uuid.serialize());
}

// For all types that take a size, we provide a template that takes the type
// alone, and another, separate one, that takes a size parameter as well, of
// type Size. This is because although most of the time the size and the data
// are contiguous, it is not always the case. So we want to have the
// flexibility of parsing them separately.
template <typename Size>
future<> parse(const schema& schema, sstable_version_types v, random_access_reader& in, disk_string<Size>& s) {
    auto len = std::make_unique<Size>();
    auto f = parse(schema, v, in, *len);
    return f.then([v, &schema, &in, &s, len = std::move(len)] {
        return parse(schema, v, in, *len, s.value);
    });
}

future<> parse(const schema& schema, sstable_version_types v, random_access_reader& in, disk_string_vint_size& s) {
    auto len = std::make_unique<uint64_t>();
    auto f = read_vint(in, *len);
    return f.then([v, &schema, &in, &s, len = std::move(len)] {
        return parse(schema, v, in, *len, s.value);
    });
}

template <typename Members>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, disk_array_vint_size<Members>& arr) {
    auto len = std::make_unique<uint64_t>();
    auto f = read_vint(in, *len);
    return f.then([v, &s, &in, &arr, len = std::move(len)] {
        return parse(s, v, in, *len, arr.elements);
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
future<>
parse(const schema& s, sstable_version_types v, random_access_reader& in, Size& len, utils::chunked_vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [v, &s, count, &in, &arr] {
        arr.emplace_back();
        (*count)++;
        return parse(s, v, in, arr.back());
    });
}

template <typename Size, std::integral Members>
future<>
parse(const schema&, sstable_version_types, random_access_reader& in, Size& len, utils::chunked_vector<Members>& arr) {
    auto done = make_lw_shared<size_t>(0);
    return repeat([&in, &len, &arr, done]  {
        auto now = std::min(len - *done, 100000 / sizeof(Members));
        return in.read_exactly(now * sizeof(Members)).then([&arr, len, now, done] (auto buf) {
            check_buf_size(buf, now * sizeof(Members));

            for (size_t i = 0; i < now; ++i) {
                arr.push_back(net::ntoh(read_unaligned<Members>(buf.get() + i * sizeof(Members))));
            }
            *done += now;
            return make_ready_future<stop_iteration>(*done == len ? stop_iteration::yes : stop_iteration::no);
        });
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, disk_array<Size, Members>& arr) {
    auto len = make_lw_shared<Size>();
    auto f = parse(s, v, in, *len);
    return f.then([v, &s, &in, &arr, len] {
        arr.elements.reserve(*len);
        return parse(s, v, in, *len, arr.elements);
    }).finally([len] {});
}

template <typename Size, typename Key, typename Value>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, Size& len, std::unordered_map<Key, Value>& map) {
    return do_with(Size(), [v, &s, &in, len, &map] (Size& count) {
        auto eos = [len, &count] { return len == count++; };
        return do_until(eos, [v, &s, len, &in, &map] {
            struct kv {
                Key key;
                Value value;
            };

            return do_with(kv(), [v, &s, &in, &map] (auto& el) {
                return parse(s, v, in, el.key, el.value).then([&el, &map] {
                    map.emplace(el.key, el.value);
                });
            });
        });
    });
}

template <typename First, typename Second>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, std::pair<First, Second>& p) {
    return parse(s, v, in, p.first, p.second);
}

template <typename Size, typename Key, typename Value>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, disk_hash<Size, Key, Value>& h) {
    auto w = std::make_unique<Size>();
    auto f = parse(s, v, in, *w);
    return f.then([v, &s, &in, &h, w = std::move(w)] {
        return parse(s, v, in, *w, h.map);
    });
}

// Abstract parser/sizer/writer for a single tagged member of a tagged union
template <typename DiskSetOfTaggedUnion>
struct single_tagged_union_member_serdes {
    using value_type = typename DiskSetOfTaggedUnion::value_type;
    virtual ~single_tagged_union_member_serdes() {}
    virtual future<> do_parse(const schema& s, sstable_version_types version, random_access_reader& in, value_type& v) const = 0;
    virtual uint32_t do_size(sstable_version_types version, const value_type& v) const = 0;
    virtual void do_write(sstable_version_types version, file_writer& out, const value_type& v) const = 0;
};

// Concrete parser for a single member of a tagged union; parses type "Member"
template <typename DiskSetOfTaggedUnion, typename Member>
struct single_tagged_union_member_serdes_for final : single_tagged_union_member_serdes<DiskSetOfTaggedUnion> {
    using base = single_tagged_union_member_serdes<DiskSetOfTaggedUnion>;
    using value_type = typename base::value_type;
    virtual future<> do_parse(const schema& s, sstable_version_types version, random_access_reader& in, value_type& v) const override {
        v = Member();
        return parse(s, version, in, boost::get<Member>(v).value);
    }
    virtual uint32_t do_size(sstable_version_types version, const value_type& v) const override {
        return serialized_size(version, boost::get<Member>(v).value);
    }
    virtual void do_write(sstable_version_types version, file_writer& out, const value_type& v) const override {
        write(version, out, boost::get<Member>(v).value);
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
    future<> lookup_and_parse(const schema& schema, sstable_version_types v, random_access_reader& in, TagType tag, uint32_t& size, disk_set& s, value_type& value) const {
        auto i = map.find(tag);
        if (i == map.end()) {
            return in.read_exactly(size).discard_result();
        } else {
            return i->second->do_parse(schema, v, in, value).then([tag, &s, &value] () mutable {
                s.data.emplace(tag, std::move(value));
            });
        }
    }
    uint32_t lookup_and_size(sstable_version_types v, TagType tag, const value_type& value) const {
        return map.at(tag)->do_size(v, value);
    }
    void lookup_and_write(sstable_version_types v, file_writer& out, TagType tag, const value_type& value) const {
        return map.at(tag)->do_write(v, out, value);
    }
};

template <typename TagType, typename... Members>
typename disk_set_of_tagged_union<TagType, Members...>::serdes disk_set_of_tagged_union<TagType, Members...>::s_serdes;

template <typename TagType, typename... Members>
future<>
parse(const schema& schema, sstable_version_types v, random_access_reader& in, disk_set_of_tagged_union<TagType, Members...>& s) {
    using disk_set = disk_set_of_tagged_union<TagType, Members...>;
    using key_type = typename disk_set::key_type;
    using value_type = typename disk_set::value_type;
    return do_with(0u, 0u, 0u, value_type{}, [&] (key_type& nr_elements, key_type& new_key, unsigned& new_size, value_type& new_value) {
        return parse(schema, v, in, nr_elements).then([&, v] {
            auto rng = boost::irange<key_type>(0, nr_elements); // do_for_each doesn't like an rvalue range
            return do_for_each(rng.begin(), rng.end(), [&, v] (key_type ignore) {
                return parse(schema, v, in, new_key).then([&, v] {
                    return parse(schema, v, in, new_size).then([&, v] {
                        return disk_set::s_serdes.lookup_and_parse(schema, v, in, TagType(new_key), new_size, s, new_value);
                    });
                });
            });
        });
    });
}

template <typename TagType, typename... Members>
void write(sstable_version_types v, file_writer& out, const disk_set_of_tagged_union<TagType, Members...>& s) {
    using disk_set = disk_set_of_tagged_union<TagType, Members...>;
    write(v, out, uint32_t(s.data.size()));
    for (auto&& kv : s.data) {
        auto&& tag = kv.first;
        auto&& value = kv.second;
        write(v, out, tag);
        write(v, out, uint32_t(disk_set::s_serdes.lookup_and_size(v, tag, value)));
        disk_set::s_serdes.lookup_and_write(v, out, tag, value);
    }
}

future<> parse(const schema& schema, sstable_version_types v, random_access_reader& in, summary& s) {
    using pos_type = typename decltype(summary::positions)::value_type;

    co_await parse(schema, v, in, s.header.min_index_interval,
                     s.header.size,
                     s.header.memory_size,
                     s.header.sampling_level,
                     s.header.size_at_full_sampling);
    auto buf = co_await in.read_exactly(s.header.size * sizeof(pos_type));
    auto len = s.header.size * sizeof(pos_type);
    check_buf_size(buf, len);

    // Positions are encoded in little-endian.
    auto b = buf.get();
    s.positions = utils::chunked_vector<pos_type>();
    while (s.positions.size() != s.header.size) {
        s.positions.push_back(seastar::read_le<pos_type>(b));
        b += sizeof(pos_type);
        co_await coroutine::maybe_yield();
    }
    // Since the keys in the index are not sized, we need to calculate
    // the start position of the index i+1 to determine the boundaries
    // of index i. The "memory_size" field in the header determines the
    // total memory used by the map, so if we push it to the vector, we
    // can guarantee that no conditionals are used, and we can always
    // query the position of the "next" index.
    s.positions.push_back(s.header.memory_size);

    co_await in.seek(sizeof(summary::header) + s.header.memory_size);
    co_await parse(schema, v, in, s.first_key, s.last_key);
    co_await in.seek(s.positions[0] + sizeof(summary::header));

    s.entries.reserve(s.header.size);

    int idx = 0;
    while (s.entries.size() != s.header.size) {
        auto pos = s.positions[idx++];
        auto next = s.positions[idx];

        auto entrysize = next - pos;
        auto buf = co_await in.read_exactly(entrysize);
        check_buf_size(buf, entrysize);

        auto keysize = entrysize - 8;
        auto key_data = s.add_summary_data(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), keysize));
        buf.trim_front(keysize);

        // position is little-endian encoded
        auto position = seastar::read_le<uint64_t>(buf.get());
        auto token = schema.get_partitioner().get_token(key_view(key_data));
        s.add_summary_data(token.data());
        s.entries.push_back({ token, key_data, position });
    }
    // Delete last element which isn't part of the on-disk format.
    s.positions.pop_back();
}

inline void write(sstable_version_types v, file_writer& out, const summary_entry& entry) {
    // FIXME: summary entry is supposedly written in memory order, but that
    // would prevent portability of summary file between machines of different
    // endianness. We can treat it as little endian to preserve portability.
    write(v, out, entry.key);
    auto p = seastar::cpu_to_le<uint64_t>(entry.position);
    out.write(reinterpret_cast<const char*>(&p), sizeof(p));
}

inline void write(sstable_version_types v, file_writer& out, const summary& s) {
    // NOTE: positions and entries must be stored in LITTLE-ENDIAN.
    write(v, out, s.header.min_index_interval,
                  s.header.size,
                  s.header.memory_size,
                  s.header.sampling_level,
                  s.header.size_at_full_sampling);
    for (auto&& e : s.positions) {
        auto p = seastar::cpu_to_le(e);
        out.write(reinterpret_cast<const char*>(&p), sizeof(p));
    }
    write(v, out, s.entries);
    write(v, out, s.first_key, s.last_key);
}

future<summary_entry&> sstable::read_summary_entry(size_t i) {
    // The last one is the boundary marker
    if (i >= (_components->summary.entries.size())) {
        throw std::out_of_range(format("Invalid Summary index: {:d}", i));
    }

    return make_ready_future<summary_entry&>(_components->summary.entries[i]);
}

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, deletion_time& d) {
    return parse(s, v, in, d.local_deletion_time, d.marked_for_delete_at);
}

template <typename Child>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, std::unique_ptr<metadata>& p) {
    p.reset(new Child);
    return parse(s, v, in, *static_cast<Child *>(p.get()));
}

template <typename Child>
inline void write(sstable_version_types v, file_writer& out, const std::unique_ptr<metadata>& p) {
    write(v, out, *static_cast<Child *>(p.get()));
}

future<> parse(const schema& schema, sstable_version_types v, random_access_reader& in, statistics& s) {
    try {
        co_await parse(schema, v, in, s.offsets);
        // Old versions of Scylla do not respect the order.
        // See https://github.com/scylladb/scylla/issues/3937
        boost::sort(s.offsets.elements, [] (auto&& e1, auto&& e2) { return e1.first < e2.first; });
        for (auto val : s.offsets.elements) {
            auto type = val.first;
            co_await in.seek(val.second);
            switch (type) {
            case metadata_type::Validation:
                co_await parse<validation_metadata>(schema, v, in, s.contents[type]);
                break;
            case metadata_type::Compaction:
                co_await parse<compaction_metadata>(schema, v, in, s.contents[type]);
                break;
            case metadata_type::Stats:
                co_await parse<stats_metadata>(schema, v, in, s.contents[type]);
                break;
            case metadata_type::Serialization:
                if (v < sstable_version_types::mc) {
                    throw malformed_sstable_exception(
                        "Statistics is malformed: SSTable is in 2.x format but contains serialization header.");
                } else {
                    co_await parse<serialization_header>(schema, v, in, s.contents[type]);
                }
                break;
            default:
                throw malformed_sstable_exception(fmt::format("Invalid metadata type at Statistics file: {} ", int(type)));
            }
        }
    } catch (const malformed_sstable_exception&) {
        throw;
    } catch (...) {
        throw malformed_sstable_exception(fmt::format("Statistics file is malformed: {}", std::current_exception()));
    }
}

inline void write(sstable_version_types v, file_writer& out, const statistics& s) {
    write(v, out, s.offsets);
    for (auto&& e : s.offsets.elements) {
        s.contents.at(e.first)->write(v, out);
    }
}

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, utils::estimated_histogram& eh) {
    auto len = std::make_unique<uint32_t>();

    co_await parse(s, v, in, *len);
    uint32_t length = *len;

    if (length == 0) {
        throw malformed_sstable_exception("Estimated histogram with zero size found. Can't continue!");
    }

    // Arrays are potentially pre-initialized by the estimated_histogram constructor.
    eh.bucket_offsets.clear();
    eh.buckets.clear();

    eh.bucket_offsets.reserve(length - 1);
    eh.buckets.reserve(length);

    auto type_size = sizeof(uint64_t) * 2;
    auto buf = co_await in.read_exactly(length * type_size);
    check_buf_size(buf, length * type_size);

    size_t j = 0;
    while (eh.buckets.size() != length) {
        auto offset = net::ntoh(read_unaligned<uint64_t>(buf.get() + (j++) * sizeof(uint64_t)));
        auto bucket = net::ntoh(read_unaligned<uint64_t>(buf.get() + (j++) * sizeof(uint64_t)));
        if (!eh.buckets.empty()) {
            eh.bucket_offsets.push_back(offset);
        }
        eh.buckets.push_back(bucket);
        co_await coroutine::maybe_yield();
    }
}

void write(sstable_version_types v, file_writer& out, const utils::estimated_histogram& eh) {
    uint32_t len = 0;
    check_truncate_and_assign(len, eh.buckets.size());

    write(v, out, len);
    struct element {
        int64_t offsets;
        int64_t buckets;
    };
    std::vector<element> elements;
    elements.reserve(eh.buckets.size());

    const int64_t* offsets_nr = eh.bucket_offsets.data();
    const int64_t* buckets_nr = eh.buckets.data();
    for (size_t i = 0; i < eh.buckets.size(); i++) {
        auto offsets = net::hton(offsets_nr[i == 0 ? 0 : i - 1]);
        auto buckets = net::hton(buckets_nr[i]);
        elements.emplace_back(element{offsets, buckets});
        if (need_preempt()) {
            seastar::thread::yield();
        }
    }

    auto p = reinterpret_cast<const char*>(elements.data());
    auto bytes = elements.size() * sizeof(element);
    out.write(p, bytes);
}

struct streaming_histogram_element {
    using key_type = typename decltype(utils::streaming_histogram::bin)::key_type;
    using value_type = typename decltype(utils::streaming_histogram::bin)::mapped_type;
    key_type key;
    value_type value;

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) { return f(key, value); }
};

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, utils::streaming_histogram& sh) {
    auto a = disk_array<uint32_t, streaming_histogram_element>();

    co_await parse(s, v, in, sh.max_bin_size, a);
    auto length = a.elements.size();
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
    if (possibly_broken_histogram && !boost::is_sorted(a.elements, less_comp)) {
        co_return;
    }

    auto transform = [] (auto element) -> std::pair<streaming_histogram_element::key_type, streaming_histogram_element::value_type> {
        return { element.key, element.value };
    };
    boost::copy(a.elements | boost::adaptors::transformed(transform), std::inserter(sh.bin, sh.bin.end()));
}

void write(sstable_version_types v, file_writer& out, const utils::streaming_histogram& sh) {
    uint32_t max_bin_size;
    check_truncate_and_assign(max_bin_size, sh.max_bin_size);

    disk_array<uint32_t, streaming_histogram_element> a;
    a.elements = boost::copy_range<utils::chunked_vector<streaming_histogram_element>>(sh.bin
        | boost::adaptors::transformed([&] (auto& kv) { return streaming_histogram_element{kv.first, kv.second}; }));

    write(v, out, max_bin_size, a);
}

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, commitlog_interval& ci) {
    co_await parse(s, v, in, ci.start);
    co_await parse(s, v, in, ci.end);
}

void write(sstable_version_types v, file_writer& out, const commitlog_interval& ci) {
    write(v, out, ci.start);
    write(v, out, ci.end);
}

future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, compression& c) {
    uint64_t data_len = 0;
    uint32_t chunk_len = 0;

    co_await parse(s, v, in, c.name, c.options, chunk_len, data_len);
    c.set_uncompressed_chunk_length(chunk_len);
    c.set_uncompressed_file_length(data_len);

    uint32_t len = 0;
    compression::segmented_offsets::writer offsets = c.offsets.get_writer();
    co_await parse(s, v, in, len);
    auto eoarr = [&c, &len] { return c.offsets.size() == len; };

    while (!eoarr()) {
        auto now = std::min(len - c.offsets.size(), 100000 / sizeof(uint64_t));
        auto buf = co_await in.read_exactly(now * sizeof(uint64_t));
        for (size_t i = 0; i < now; ++i) {
            uint64_t value = read_unaligned<uint64_t>(buf.get() + i * sizeof(uint64_t));
            offsets.push_back(net::ntoh(value));
        }
    }
}

void write(sstable_version_types v, file_writer& out, const compression& c) {
    write(v, out, c.name, c.options, c.uncompressed_chunk_length(), c.uncompressed_file_length());

    write(v, out, static_cast<uint32_t>(c.offsets.size()));

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
        out.write(p, bytes);
        idx += now;
    }
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() noexcept {
    if (_recognized_components.size()) {
        return make_ready_future<>();
    }

    sstlog.debug("Reading TOC file {}", filename(component_type::TOC));

    return with_file(new_sstable_component_file(_read_error_handler, component_type::TOC, open_flags::ro), [this] (file f) {
        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
        auto buf = bufptr.get();

        auto fut = f.dma_read(0, buf, 4096);
        return std::move(fut).then([this, f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
            // This file is supposed to be very small. Theoretically we should check its size,
            // but if we so much as read a whole page from it, there is definitely something fishy
            // going on - and this simplifies the code.
            if (size >= 4096) {
                throw malformed_sstable_exception("SSTable TOC too big: " + to_sstring(size) + " bytes", filename(component_type::TOC));
            }

            std::string_view buf(bufptr.get(), size);
            std::vector<sstring> comps;

            boost::split(comps , buf, boost::is_any_of("\n"));

            for (auto& c: comps) {
                // accept trailing newlines
                if (c == "") {
                    continue;
                }
                try {
                    _recognized_components.insert(reverse_map(c, sstable_version_constants::get_component_map(_version)));
                } catch (std::out_of_range& oor) {
                    _unrecognized_components.push_back(c);
                    sstlog.info("Unrecognized TOC component was found: {} in sstable {}", c, filename(component_type::TOC));
                }
            }
            if (!_recognized_components.size()) {
                throw malformed_sstable_exception("Empty TOC", filename(component_type::TOC));
            }
        });
    }).then_wrapped([this] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw malformed_sstable_exception(filename(component_type::TOC) + ": file not found");
            }
            throw;
        }
    });

}

void sstable::generate_toc(compressor_ptr c, double filter_fp_chance) {
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
    if (c == nullptr) {
        _recognized_components.insert(component_type::CRC);
    } else {
        _recognized_components.insert(component_type::CompressionInfo);
    }
    _recognized_components.insert(component_type::Scylla);
}

file_writer::~file_writer() {
    if (_closed) {
        return;
    }
    try {
        // close() should be called by the owner of the file_writer.
        // However it may not be called on exception handling paths
        // so auto-close the output_stream so it won't be destructed while open.
        _out.close().get();
    } catch (...) {
        sstlog.warn("Error while auto-closing {}: {}. Ignored.", get_filename(), std::current_exception());
    }
}

void file_writer::close() {
    assert(!_closed && "file_writer already closed");
    try {
        _closed = true;
        _out.close().get();
    } catch (...) {
        auto e = std::current_exception();
        sstlog.error("Error while closing {}: {}", get_filename(), e);
        std::rethrow_exception(e);
    }
}

const char* file_writer::get_filename() const noexcept {
    return _filename ? _filename->c_str() : "<anonymous output_stream>";
}

future<file_writer> sstable::make_component_file_writer(component_type c, file_output_stream_options options, open_flags oflags) noexcept {
    // Note: file_writer::make closes the file if file_writer creation fails
    // so we don't need to use with_file_close_on_failure here.
    return futurize_invoke([this, c] { return filename(c); }).then([this, c, options = std::move(options), oflags] (sstring filename) mutable {
        return new_sstable_component_file(_write_error_handler, c, oflags).then([options = std::move(options), filename = std::move(filename)] (file f) mutable {
            return file_writer::make(std::move(f), std::move(options), std::move(filename));
        });
    });
}

void sstable::write_toc(const io_priority_class& pc) {
    touch_temp_dir().get0();
    auto file_path = filename(component_type::TemporaryTOC);

    sstlog.debug("Writing TOC file {} ", file_path);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file_output_stream_options options;
    options.buffer_size = 4096;
    options.io_priority_class = pc;
    auto w = make_component_file_writer(component_type::TemporaryTOC, std::move(options)).get0();

    bool toc_exists = file_exists(filename(component_type::TOC)).get0();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        w.close();
        remove_file(file_path).get();
        throw std::runtime_error(format("SSTable write failed due to existence of TOC file for generation {:d} of {}.{}", _generation, _schema->ks_name(), _schema->cf_name()));
    }

    for (auto&& key : _recognized_components) {
            // new line character is appended to the end of each component name.
        auto value = sstable_version_constants::get_component_map(_version).at(key) + "\n";
        bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
        write(_version, w, b);
    }
    w.flush();
    w.close();

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
    return remove_temp_dir().then([this] {
        return open_checked_directory(_write_error_handler, _dir).then([this] (file dir_f) {
            // Guarantee that every component of this sstable reached the disk.
            return sstable_write_io_check([&] { return dir_f.flush(); }).then([this] {
                // Rename TOC because it's no longer temporary.
                return sstable_write_io_check([&] {
                    return rename_file(filename(component_type::TemporaryTOC), filename(component_type::TOC));
                });
            }).then([this, dir_f] () mutable {
                // Guarantee that the changes above reached the disk.
                return sstable_write_io_check([&] { return dir_f.flush(); });
            }).then([this, dir_f] () mutable {
                return sstable_write_io_check([&] { return dir_f.close(); });
            }).then([this, dir_f] {
                if (_marked_for_deletion == mark_for_deletion::implicit) {
                    _marked_for_deletion = mark_for_deletion::none;
                }
                // If this point was reached, sstable should be safe in disk.
                sstlog.debug("SSTable with generation {} of {}.{} was sealed successfully.", _generation, _schema->ks_name(), _schema->cf_name());
            });
        });
    });
}

void sstable::write_crc(const checksum& c) {
    auto file_path = filename(component_type::CRC);
    sstlog.debug("Writing CRC file {} ", file_path);

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = make_component_file_writer(component_type::CRC, std::move(options)).get0();
    write(get_version(), w, c);
    w.close();
}

// Digest file stores the full checksum of data file converted into a string.
void sstable::write_digest(uint32_t full_checksum) {
    auto file_path = filename(component_type::Digest);
    sstlog.debug("Writing Digest file {} ", file_path);

    file_output_stream_options options;
    options.buffer_size = 4096;
    auto w = make_component_file_writer(component_type::Digest, std::move(options)).get0();

    auto digest = to_sstring<bytes>(full_checksum);
    write(get_version(), w, digest);
    w.close();
}

thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_sample_pattern_cache;
thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_original_index_cache;


template <component_type Type, typename T>
future<> sstable::read_simple(T& component, const io_priority_class& pc) {

    auto file_path = filename(Type);
    sstlog.debug(("Reading " + sstable_version_constants::get_component_map(_version).at(Type) + " file {} ").c_str(), file_path);
    return new_sstable_component_file(_read_error_handler, Type, open_flags::ro).then([this, &component] (file fi) {
        auto fut = fi.size();
        return fut.then([this, &component, fi = std::move(fi)] (uint64_t size) {
            auto r = make_lw_shared<file_random_access_reader>(std::move(fi), size, sstable_buffer_size);
            auto fut = parse(*_schema, _version, *r, component);
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
        } catch (malformed_sstable_exception &e) {
            throw malformed_sstable_exception(e.what(), file_path);
        }
    });
}

void sstable::do_write_simple(component_type type, const io_priority_class& pc,
        noncopyable_function<void (version_types version, file_writer& writer)> write_component) {
    auto file_path = filename(type);
    sstlog.debug(("Writing " + sstable_version_constants::get_component_map(_version).at(type) + " file {} ").c_str(), file_path);

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto w = make_component_file_writer(type, std::move(options)).get0();
    std::exception_ptr eptr;
    try {
        write_component(_version, w);
        w.flush();
    } catch (...) {
        eptr = std::current_exception();
    }
    try {
        w.close();
    } catch (...) {
        std::exception_ptr close_eptr = std::current_exception();
        sstlog.warn("failed to close file_writer: {}", close_eptr);
        // If write succeeded but close failed, we rethrow close's exception.
        if (!eptr) {
            eptr = close_eptr;
        }
    }
    if (eptr) {
        std::rethrow_exception(eptr);
    }
}

template <component_type Type, typename T>
void sstable::write_simple(const T& component, const io_priority_class& pc) {
    do_write_simple(Type, pc, [&component] (version_types v, file_writer& w) {
        write(v, w, component);
    });
}

template future<> sstable::read_simple<component_type::Filter>(sstables::filter& f, const io_priority_class& pc);
template void sstable::write_simple<component_type::Filter>(const sstables::filter& f, const io_priority_class& pc);

template void sstable::write_simple<component_type::Summary>(const sstables::summary_ka&, const io_priority_class&);

future<> sstable::read_compression(const io_priority_class& pc) {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_components->compression, pc);
}

void sstable::write_compression(const io_priority_class& pc) {
    if (!has_component(component_type::CompressionInfo)) {
        return;
    }

    write_simple<component_type::CompressionInfo>(_components->compression, pc);
}

void sstable::validate_partitioner() {
    auto entry = _components->statistics.contents.find(metadata_type::Validation);
    if (entry == _components->statistics.contents.end()) {
        throw std::runtime_error("Validation metadata not available");
    }
    auto& p = entry->second;
    if (!p) {
        throw std::runtime_error("Validation is malformed");
    }

    validation_metadata& v = *static_cast<validation_metadata *>(p.get());
    if (v.partitioner.value != to_bytes(_schema->get_partitioner().name())) {
        throw std::runtime_error(
                fmt::format(FMT_STRING("SSTable {} uses {} partitioner which is different than {} partitioner used by the database"),
                            get_filename(),
                            sstring(reinterpret_cast<char*>(v.partitioner.value.data()), v.partitioner.value.size()),
                            _schema->get_partitioner().name()));
    }

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
    // - it's not empty and schema defines no clustering key.
    //
    // Notes:
    // - we are going to rely on min/max column names for
    //   clustering filtering only from md-format sstables,
    //   see sstable::may_contain_rows().
    //   We choose not to clear_incorrect_min_max_column_names
    //   from older versions here as this disturbs sstable unit tests.
    //
    // - now that we store min/max metadata for range tombstones,
    //   their size may differ.
    if (!_schema->clustering_key_size()) {
        clear_incorrect_min_max_column_names();
        return;
    }
}

void sstable::validate_max_local_deletion_time() {
    if (!has_correct_max_deletion_time()) {
        auto& entry = _components->statistics.contents[metadata_type::Stats];
        auto& s = *static_cast<stats_metadata*>(entry.get());
        s.max_local_deletion_time = std::numeric_limits<int32_t>::max();
    }
}

void sstable::set_position_range() {
    if (!_schema->clustering_key_size()) {
        return;
    }

    auto& min_elements = get_stats_metadata().min_column_names.elements;
    auto& max_elements = get_stats_metadata().max_column_names.elements;

    if (min_elements.empty() && max_elements.empty()) {
        return;
    }

    auto pip = [] (const utils::chunked_vector<disk_string<uint16_t>>& column_names, bound_kind kind) {
        std::vector<bytes> key_bytes;
        key_bytes.reserve(column_names.size());
        for (auto& value : column_names) {
            key_bytes.emplace_back(bytes_view(value));
        }
        auto ckp = clustering_key_prefix(std::move(key_bytes));
        return position_in_partition(position_in_partition::range_tag_t(), kind, std::move(ckp));
    };

    _position_range = position_range(pip(min_elements, bound_kind::incl_start), pip(max_elements, bound_kind::incl_end));
}

double sstable::estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const {
    auto& st = get_stats_metadata();
    auto estimated_count = st.estimated_cells_count.mean() * st.estimated_cells_count.count();
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

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    auto w = make_component_file_writer(component_type::TemporaryStatistics, std::move(options),
            open_flags::wo | open_flags::create | open_flags::truncate).get0();
    write(_version, w, _components->statistics);
    w.flush();
    w.close();
    // rename() guarantees atomicity when renaming a file into place.
    sstable_write_io_check(rename_file, file_path, filename(component_type::Statistics)).get();
}

future<> sstable::read_summary(const io_priority_class& pc) noexcept {
    if (_components->summary) {
        return make_ready_future<>();
    }

    return read_toc().then([this, &pc] {
        // We'll try to keep the main code path exception free, but if an exception does happen
        // we can try to regenerate the Summary.
        if (has_component(component_type::Summary)) {
            return read_simple<component_type::Summary>(_components->summary, pc).handle_exception([this, &pc] (auto ep) {
                sstlog.warn("Couldn't read summary file {}: {}. Recreating it.", this->filename(component_type::Summary), ep);
                return this->generate_summary(pc);
            });
        } else {
            return generate_summary(pc);
        }
    });
}

future<file> sstable::open_file(component_type type, open_flags flags, file_open_options opts) noexcept {
    return new_sstable_component_file(_read_error_handler, type, flags, opts);
}

future<> sstable::open_or_create_data(open_flags oflags, file_open_options options) noexcept {
    return when_all_succeed(
        open_file(component_type::Index, oflags, options).then([this] (file f) { _index_file = std::move(f); }),
        open_file(component_type::Data, oflags, options).then([this] (file f) { _data_file = std::move(f); })
    ).discard_result();
}

future<> sstable::open_data() noexcept {
    return open_or_create_data(open_flags::ro).then([this] {
        return this->update_info_for_opened_data();
    }).then([this] {
        if (_shards.empty()) {
            _shards = compute_shards_for_this_sstable();
        }
        auto* sm = _components->scylla_metadata->data.get<scylla_metadata_type::Sharding, sharding_metadata>();
        if (!sm) {
            return make_ready_future<>();
        }
        auto c = &sm->token_ranges.elements;
        // Sharding information uses a lot of memory and once we're doing with this computation we will no longer use it.
        return do_until([c] { return c->empty(); }, [c] {
            c->pop_back();
            return make_ready_future<>();
        }).then([this, c] () mutable {
            *c = {};
            return make_ready_future<>();
        });
    }).then([this] {
        auto* ld_stats = _components->scylla_metadata->data.get<scylla_metadata_type::LargeDataStats, scylla_metadata::large_data_stats>();
        if (ld_stats) {
            _large_data_stats.emplace(*ld_stats);
        }
        auto* origin = _components->scylla_metadata->data.get<scylla_metadata_type::SSTableOrigin, scylla_metadata::sstable_origin>();
        if (origin) {
            _origin = sstring(to_sstring_view(bytes_view(origin->value)));
        }
    }).then([this] {
        _open_mode.emplace(open_flags::ro);
        _stats.on_open_for_reading();
    });
}

future<> sstable::update_info_for_opened_data() {
    return _data_file.stat().then([this] (struct stat st) {
        if (this->has_component(component_type::CompressionInfo)) {
            _components->compression.update(st.st_size);
        }
        _data_file_size = st.st_size;
        _data_file_write_time = db_clock::from_time_t(st.st_mtime);
    }).then([this] {
        return _index_file.size().then([this] (auto size) {
            _index_file_size = size;
            assert(!_cached_index_file);
            _cached_index_file = seastar::make_shared<cached_file>(_index_file,
                                                                   index_page_cache_metrics,
                                                                   _manager.get_cache_tracker().get_lru(),
                                                                   _manager.get_cache_tracker().region(),
                                                                   _index_file_size);
            _index_file = make_cached_seastar_file(*_cached_index_file);
        });
    }).then([this] {
        if (this->has_component(component_type::Filter)) {
            return io_check([&] {
                return file_size(this->filename(component_type::Filter));
            }).then([this] (auto size) {
                _filter_file_size = size;
            });
        }
        return make_ready_future<>();
    }).then([this] {
        this->set_position_range();
        this->set_first_and_last_keys();
        _run_identifier = _components->scylla_metadata->get_optional_run_identifier().value_or(utils::make_random_uuid());

        // Get disk usage for this sstable (includes all components).
        _bytes_on_disk = 0;
        return do_for_each(_recognized_components, [this] (component_type c) {
            return this->sstable_write_io_check([&, c] {
                return file_exists(this->filename(c)).then([this, c] (bool exists) {
                    // ignore summary that isn't present in disk but was previously generated by read_summary().
                    if (!exists && c == component_type::Summary && _components->summary.memory_footprint()) {
                        return make_ready_future<uint64_t>(0);
                    }
                    return file_size(this->filename(c));
                });
            }).then([this] (uint64_t bytes) {
                _bytes_on_disk += bytes;
            });
        });
    });
}

future<> sstable::create_data() noexcept {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file_open_options opt;
    opt.extent_allocation_size_hint = 32 << 20;
    opt.sloppy_size = true;
    return open_or_create_data(oflags, std::move(opt)).then([this, oflags] {
        _open_mode.emplace(oflags);
        _stats.on_open_for_writing();
    });
}

future<> sstable::drop_caches() {
    return _cached_index_file->evict_gently().then([this] {
        return _index_cache->evict_gently();
    });
}

future<> sstable::read_filter(const io_priority_class& pc) {
    if (!has_component(component_type::Filter)) {
        _components->filter = std::make_unique<utils::filter::always_present_filter>();
        return make_ready_future<>();
    }

    return seastar::async([this, &pc] () mutable {
        sstables::filter filter;
        read_simple<component_type::Filter>(filter, pc).get();
        auto nr_bits = filter.buckets.elements.size() * std::numeric_limits<typename decltype(filter.buckets.elements)::value_type>::digits;
        large_bitset bs(nr_bits, std::move(filter.buckets.elements));
        utils::filter_format format = (_version >= sstable_version_types::mc)
                                      ? utils::filter_format::m_format
                                      : utils::filter_format::k_l_format;
        _components->filter = utils::filter::create_filter(filter.hashes, std::move(bs), format);
    });
}

void sstable::write_filter(const io_priority_class& pc) {
    if (!has_component(component_type::Filter)) {
        return;
    }

    auto f = static_cast<utils::filter::murmur3_bloom_filter *>(_components->filter.get());

    auto&& bs = f->bits();
    auto filter_ref = sstables::filter_ref(f->num_hashes(), bs.get_storage());
    write_simple<component_type::Filter>(filter_ref, pc);
}

// This interface is only used during tests, snapshot loading and early initialization.
// No need to set tunable priorities for it.
future<> sstable::load(const io_priority_class& pc) noexcept {
    return read_toc().then([this, &pc] {
        // read scylla-meta after toc. Might need it to parse
        // rest (hint extensions)
        return read_scylla_metadata(pc).then([this, &pc] {
            // Read statistics ahead of others - if summary is missing
            // we'll attempt to re-generate it and we need statistics for that
            return read_statistics(pc).then([this, &pc] {
                return seastar::when_all_succeed(
                        read_compression(pc),
                        read_filter(pc),
                        read_summary(pc)).then_unpack([this] {
                            validate_min_max_metadata();
                            validate_max_local_deletion_time();
                            validate_partitioner();
                            return open_data();
                        });
            });
        });
    });
}

future<> sstable::load(sstables::foreign_sstable_open_info info) noexcept {
    static_assert(std::is_nothrow_move_constructible_v<sstables::foreign_sstable_open_info>);
    return read_toc().then([this, info = std::move(info)] () mutable {
        _components = std::move(info.components);
        _data_file = make_checked_file(_read_error_handler, info.data.to_file());
        _index_file = make_checked_file(_read_error_handler, info.index.to_file());
        _shards = std::move(info.owners);
        validate_min_max_metadata();
        validate_max_local_deletion_time();
        validate_partitioner();
        return update_info_for_opened_data();
    });
}

future<foreign_sstable_open_info> sstable::get_open_info() & {
    return _components.copy().then([this] (auto c) mutable {
        return foreign_sstable_open_info{std::move(c), this->get_shards_for_this_sstable(), _data_file.dup(), _index_file.dup(),
            _generation, _version, _format, data_size()};
    });
}

void prepare_summary(summary& s, uint64_t expected_partition_count, uint32_t min_index_interval) {
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

future<> seal_summary(summary& s,
        std::optional<key>&& first_key,
        std::optional<key>&& last_key,
        const index_sampling_state& state) {
    s.header.size = s.entries.size();
    s.header.size_at_full_sampling = sstable::get_size_at_full_sampling(state.partition_count, s.header.min_index_interval);

    assert(first_key); // assume non-empty sstable
    s.first_key.value = first_key->get_bytes();

    if (last_key) {
        s.last_key.value = last_key->get_bytes();
    } else {
        // An empty last_mutation indicates we had just one partition
        s.last_key.value = s.first_key.value;
    }

    s.header.memory_size = s.header.size * sizeof(uint32_t);
    s.positions.reserve(s.entries.size());
    return do_for_each(s.entries, [&s] (summary_entry& e) {
        s.positions.push_back(s.header.memory_size);
        s.header.memory_size += e.key.size() + sizeof(e.position);
    });
}

static
void
populate_statistics_offsets(sstable_version_types v, statistics& s) {
    // copy into a sorted vector to guarantee consistent order
    auto types = boost::copy_range<std::vector<metadata_type>>(s.contents | boost::adaptors::map_keys);
    boost::sort(types);

    // populate the hash with garbage so we can calculate its size
    for (auto t : types) {
        s.offsets.elements.emplace_back(t, -1);
    }

    auto offset = serialized_size(v, s.offsets);
    s.offsets.elements.clear();
    for (auto t : types) {
        s.offsets.elements.emplace_back(t, offset);
        offset += s.contents[t]->serialized_size(v);
    }
}

static
sharding_metadata
create_sharding_metadata(schema_ptr schema, const dht::decorated_key& first_key, const dht::decorated_key& last_key, shard_id shard) {
    auto prange = dht::partition_range::make(dht::ring_position(first_key), dht::ring_position(last_key));
    auto sm = sharding_metadata();
    auto&& ranges = dht::split_range_to_single_shard(*schema, prange, shard).get0();
    if (ranges.empty()) {
        auto split_ranges_all_shards = dht::split_range_to_shards(prange, *schema);
        sstlog.warn("create_sharding_metadata: range={} has no intersection with shard={} first_key={} last_key={} ranges_single_shard={} ranges_all_shards={}",
                prange, shard, first_key, last_key, ranges, split_ranges_all_shards);
    }
    sm.token_ranges.elements.reserve(ranges.size());
    for (auto&& range : std::move(ranges)) {
        if (true) { // keep indentation
            // we know left/right are not infinite
            auto&& left = range.start()->value();
            auto&& right = range.end()->value();
            auto&& left_token = left.token();
            auto left_exclusive = !left.has_key() && left.bound() == dht::ring_position::token_bound::end;
            auto&& right_token = right.token();
            auto right_exclusive = !right.has_key() && right.bound() == dht::ring_position::token_bound::start;
            sm.token_ranges.elements.push_back(disk_token_range{
                {left_exclusive, left_token.data()},
                {right_exclusive, right_token.data()}});
        }
    }
    return sm;
}

// In the beginning of the statistics file, there is a disk_hash used to
// map each metadata type to its correspondent position in the file.
void seal_statistics(sstable_version_types v, statistics& s, metadata_collector& collector, const std::set<int>& _compaction_ancestors,
        const sstring partitioner, double bloom_filter_fp_chance, schema_ptr schema,
        const dht::decorated_key& first_key, const dht::decorated_key& last_key, const encoding_stats& enc_stats) {
    validation_metadata validation;
    compaction_metadata compaction;
    stats_metadata stats;

    validation.partitioner.value = to_bytes(partitioner);
    validation.filter_chance = bloom_filter_fp_chance;
    s.contents[metadata_type::Validation] = std::make_unique<validation_metadata>(std::move(validation));

    collector.construct_compaction(compaction);
    if (v < sstable_version_types::mc && !_compaction_ancestors.empty()) {
        compaction.ancestors.elements = utils::chunked_vector<uint32_t>(_compaction_ancestors.begin(), _compaction_ancestors.end());
    }
    s.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>(std::move(compaction));

    collector.construct_stats(stats);
    s.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));

    populate_statistics_offsets(v, s);
}

void maybe_add_summary_entry(summary& s, const dht::token& token, bytes_view key, uint64_t data_offset,
        uint64_t index_offset, index_sampling_state& state) {
    state.partition_count++;
    // generates a summary entry when possible (= keep summary / data size ratio within reasonable limits)
    if (data_offset >= state.next_data_offset_to_write_summary) {
        auto entry_size = 8 + 2 + key.size();  // offset + key_size.size + key.size
        state.next_data_offset_to_write_summary += state.summary_byte_cost * entry_size;
        s.add_summary_data(token.data());
        auto key_data = s.add_summary_data(key);
        s.entries.push_back({ token, key_data, index_offset });
    }
}

// Returns the cost for writing a byte to summary such that the ratio of summary
// to data will be 1 to cost by the time sstable is sealed.
size_t summary_byte_cost(double summary_ratio) {
    return summary_ratio ? (1 / summary_ratio) : index_sampling_state::default_summary_byte_cost;
}

future<>
sstable::read_scylla_metadata(const io_priority_class& pc) noexcept {
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
sstable::write_scylla_metadata(const io_priority_class& pc, shard_id shard, sstable_enabled_features features, struct run_identifier identifier,
        std::optional<scylla_metadata::large_data_stats> ld_stats, sstring origin) {
    auto&& first_key = get_first_decorated_key();
    auto&& last_key = get_last_decorated_key();
    auto sm = create_sharding_metadata(_schema, first_key, last_key, shard);

    // sstable write may fail to generate empty metadata if mutation source has only data from other shard.
    // see https://github.com/scylladb/scylla/issues/2932 for details on how it can happen.
    if (sm.token_ranges.elements.empty()) {
        throw std::runtime_error(format("Failed to generate sharding metadata for {}", get_filename()));
    }

    if (!_components->scylla_metadata) {
        _components->scylla_metadata.emplace();
    }

    _components->scylla_metadata->data.set<scylla_metadata_type::Sharding>(std::move(sm));
    _components->scylla_metadata->data.set<scylla_metadata_type::Features>(std::move(features));
    _components->scylla_metadata->data.set<scylla_metadata_type::RunIdentifier>(std::move(identifier));
    if (ld_stats) {
        _components->scylla_metadata->data.set<scylla_metadata_type::LargeDataStats>(std::move(*ld_stats));
    }
    if (!origin.empty()) {
        scylla_metadata::sstable_origin o;
        o.value = bytes(to_bytes_view(sstring_view(origin)));
        _components->scylla_metadata->data.set<scylla_metadata_type::SSTableOrigin>(std::move(o));
    }

    write_simple<component_type::Scylla>(*_components->scylla_metadata, pc);
}

bool sstable::may_contain_rows(const query::clustering_row_ranges& ranges) const {
    if (_version < sstables::sstable_version_types::md) {
        return true;
    }

    // Include sstables with tombstones that are not scylla's since
    // they may contain partition tombstones that are not taken into
    // account in min/max coloumn names metadata.
    // We clear min/max metadata for partition tombstones so they
    // will match as containing the rows we're looking for.
    if (!has_scylla_component()) {
        if (get_stats_metadata().estimated_tombstone_drop_time.bin.size()) {
            return true;
        }
    }

    return std::ranges::any_of(ranges, [this] (const query::clustering_range& range) {
        return _position_range.overlaps(*_schema,
            position_in_partition_view::for_range_start(range),
            position_in_partition_view::for_range_end(range));
    });
}

future<> sstable::seal_sstable(bool backup)
{
    return seal_sstable().then([this, backup] {
        if (backup) {
            auto dir = get_dir() + "/backups/";
            auto fut = sstable_touch_directory_io_check(dir);
            return fut.then([this, dir = std::move(dir)] () mutable {
                return create_links(std::move(dir));
            });
        }
        return make_ready_future<>();
    });
}

sstable_writer sstable::get_writer(const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, const io_priority_class& pc, shard_id shard)
{
    // Mark sstable for implicit deletion if destructed before it is sealed.
    _marked_for_deletion = mark_for_deletion::implicit;
    return sstable_writer(*this, s, estimated_partitions, cfg, enc_stats, pc, shard);
}

// Encoding stats for compaction are based on the sstable's stats metadata
// since, in contract to the mc-format encoding_stats that are evaluated
// before the sstable data is written, the stats metadata is updated during
// writing so it provides actual minimum values of the written timestamps.
encoding_stats sstable::get_encoding_stats_for_compaction() const {
    encoding_stats enc_stats;

    auto& stats = get_stats_metadata();
    enc_stats.min_timestamp = stats.min_timestamp;
    enc_stats.min_local_deletion_time = gc_clock::time_point(gc_clock::duration(stats.min_local_deletion_time));
    enc_stats.min_ttl = gc_clock::duration(stats.min_ttl);

    return enc_stats;
}

void sstable::assert_large_data_handler_is_running() {
    if (!get_large_data_handler().running()) {
        on_internal_error(sstlog, "The large data handler is not running");
    }
}

future<> sstable::write_components(
        flat_mutation_reader mr,
        uint64_t estimated_partitions,
        schema_ptr schema,
        const sstable_writer_config& cfg,
        encoding_stats stats,
        const io_priority_class& pc) {
    assert_large_data_handler_is_running();
    return seastar::async([this, mr = std::move(mr), estimated_partitions, schema = std::move(schema), cfg, stats, &pc] () mutable {
        auto close_mr = deferred_close(mr);
        auto wr = get_writer(*schema, estimated_partitions, cfg, stats, pc);
        mr.consume_in_thread(std::move(wr));
    }).finally([this] {
        assert_large_data_handler_is_running();
    });
}

future<> sstable::generate_summary(const io_priority_class& pc) {
    if (_components->summary) {
        co_return;
    }

    sstlog.info("Summary file {} not found. Generating Summary...", filename(component_type::Summary));
    class summary_generator {
        const dht::i_partitioner& _partitioner;
        summary& _summary;
        index_sampling_state _state;
    public:
        std::optional<key> first_key, last_key;

        summary_generator(const dht::i_partitioner& p, summary& s, double summary_ratio) : _partitioner(p), _summary(s) {
            _state.summary_byte_cost = summary_byte_cost(summary_ratio);
        }
        bool should_continue() {
            return true;
        }
        void consume_entry(parsed_partition_index_entry&& e) {
            auto token = _partitioner.get_token(key_view(to_bytes_view(e.key)));
            maybe_add_summary_entry(_summary, token, to_bytes_view(e.key), e.data_file_offset, e.index_offset, _state);
            if (!first_key) {
                first_key = key(to_bytes(to_bytes_view(e.key)));
            } else {
                last_key = key(to_bytes(to_bytes_view(e.key)));
            }
        }
        const index_sampling_state& state() const {
            return _state;
        }
    };

    auto index_file = co_await new_sstable_component_file(_read_error_handler, component_type::Index, open_flags::ro);
    auto sem = reader_concurrency_semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::generate_summary()");

    std::exception_ptr ex;

    try {
        auto index_size = co_await index_file.size();
        // an upper bound. Surely to be less than this.
        auto estimated_partitions = std::max<uint64_t>(index_size / sizeof(uint64_t), 1);
        prepare_summary(_components->summary, estimated_partitions, _schema->min_index_interval());

        file_input_stream_options options;
        options.buffer_size = sstable_buffer_size;
        options.io_priority_class = pc;

        auto s = summary_generator(_schema->get_partitioner(), _components->summary, _manager.config().sstable_summary_ratio());
            auto ctx = make_lw_shared<index_consume_entry_context<summary_generator>>(
                    sem.make_tracking_only_permit(_schema.get(), "generate-summary", db::no_timeout), s, trust_promoted_index::yes, *_schema, index_file, std::move(options), 0, index_size,
                    (_version >= sstable_version_types::mc
                        ? std::make_optional(get_clustering_values_fixed_lengths(get_serialization_header()))
                        : std::optional<column_values_fixed_lengths>{}));

        try {
            co_await ctx->consume_input();
        } catch (...) {
            ex = std::current_exception();
        }

        co_await ctx->close();

        if (ex) {
            std::rethrow_exception(std::exchange(ex, {}));
        }

        co_await seal_summary(_components->summary, std::move(s.first_key), std::move(s.last_key), s.state());
    } catch (...) {
        ex = std::current_exception();
    }

    co_await sem.stop();

    try {
        co_await index_file.close();
    } catch (...) {
        sstlog.warn("sstable close index_file failed: {}", std::current_exception());
        general_disk_error();
    }

    if (ex) {
        std::rethrow_exception(ex);
    }
}

bool sstable::is_shared() const {
    if (_shards.empty()) {
        on_internal_error(sstlog, format("Shards weren't computed for SSTable: {}", get_filename()));
    }
    return _shards.size() > 1;
}

uint64_t sstable::data_size() const {
    if (has_component(component_type::CompressionInfo)) {
        return _components->compression.uncompressed_file_length();
    }
    return _data_file_size;
}

uint64_t sstable::ondisk_data_size() const {
    return _data_file_size;
}

uint64_t sstable::bytes_on_disk() const {
    assert(_bytes_on_disk > 0);
    return _bytes_on_disk;
}

const bool sstable::has_component(component_type f) const {
    return _recognized_components.contains(f);
}

future<> sstable::touch_temp_dir() {
    if (_temp_dir) {
        return make_ready_future<>();
    }
    auto temp_dir = get_temp_dir();
    sstlog.debug("Touching temp_dir={}", temp_dir);
    auto fut = sstable_touch_directory_io_check(temp_dir);
    return fut.then([this, temp_dir = std::move(temp_dir)] () mutable {
        _temp_dir = std::move(temp_dir);
    });
}

future<> sstable::remove_temp_dir() {
    if (!_temp_dir) {
        return make_ready_future<>();
    }
    sstlog.debug("Removing temp_dir={}", _temp_dir);
    return remove_file(*_temp_dir).then_wrapped([this] (future<> f) {
        if (!f.failed()) {
            _temp_dir.reset();
            return make_ready_future<>();
        }
        auto ep = f.get_exception();
        sstlog.error("Could not remove temporary directory: {}", ep);
        return make_exception_future<>(ep);
    });
}

std::vector<sstring> sstable::component_filenames() const {
    std::vector<sstring> res;
    for (auto c : sstable_version_constants::get_component_map(_version) | boost::adaptors::map_keys) {
        if (has_component(c)) {
            res.emplace_back(filename(c));
        }
    }
    return res;
}

bool sstable::requires_view_building() const {
    return boost::algorithm::ends_with(_dir, "staging");
}

sstring sstable::component_basename(const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                                    format_types format, sstring component) {
    sstring v = _version_string.at(version);
    sstring g = to_sstring(generation);
    sstring f = _format_string.at(format);
    switch (version) {
    case sstable::version_types::ka:
        return ks + "-" + cf + "-" + v + "-" + g + "-" + component;
    case sstable::version_types::la:
        return v + "-" + g + "-" + f + "-" + component;
    case sstable::version_types::mc:
    case sstable::version_types::md:
        return v + "-" + g + "-" + f + "-" + component;
    }
    assert(0 && "invalid version");
}

sstring sstable::component_basename(const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                          format_types format, component_type component) {
    return component_basename(ks, cf, version, generation, format,
            sstable_version_constants::get_component_map(version).at(component));
}

sstring sstable::filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                          format_types format, component_type component) {
    return dir + "/" + component_basename(ks, cf, version, generation, format, component);
}

sstring sstable::filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                          format_types format, sstring component) {
    return dir + "/" + component_basename(ks, cf, version, generation, format, component);
}

std::vector<std::pair<component_type, sstring>> sstable::all_components() const {
    std::vector<std::pair<component_type, sstring>> all;
    all.reserve(_recognized_components.size() + _unrecognized_components.size());
    for (auto& c : _recognized_components) {
        all.push_back(std::make_pair(c, sstable_version_constants::get_component_map(_version).at(c)));
    }
    for (auto& c : _unrecognized_components) {
        all.push_back(std::make_pair(component_type::Unknown, c));
    }
    return all;
}

static bool is_same_file(const seastar::stat_data& sd1, const seastar::stat_data& sd2) noexcept {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

future<bool> same_file(sstring path1, sstring path2) noexcept {
    return when_all_succeed(file_stat(std::move(path1)), file_stat(std::move(path2))).then_unpack([] (seastar::stat_data sd1, seastar::stat_data sd2) {
        return is_same_file(sd1, sd2);
    });
}

// support replay of link by considering link_file EEXIST error as successful when the newpath is hard linked to oldpath.
future<> idempotent_link_file(sstring oldpath, sstring newpath) noexcept {
    return do_with(std::move(oldpath), std::move(newpath), [] (const sstring& oldpath, const sstring& newpath) {
        return link_file(oldpath, newpath).handle_exception([&] (std::exception_ptr eptr) mutable {
            try {
                std::rethrow_exception(eptr);
            } catch (const std::system_error& ex) {
                if (ex.code().value() != EEXIST) {
                    throw;
                }
            }
            return same_file(oldpath, newpath).then_wrapped([eptr = std::move(eptr)] (future<bool> fut) mutable {
                if (!fut.failed()) {
                    auto same = fut.get0();
                    if (same) {
                        return make_ready_future<>();
                    }
                }
                return make_exception_future<>(eptr);
            });
        });
    });
}

// Check is the operation is replayed, possibly when moving sstables
// from staging to the base dir, for example, right after create_links completes,
// and right before deleting the source links.
// We end up in two valid sstables in this case, so make create_links idempotent.
future<> sstable::check_create_links_replay(const sstring& dst_dir, int64_t dst_gen,
        const std::vector<std::pair<sstables::component_type, sstring>>& comps) const {
    return parallel_for_each(comps, [this, &dst_dir, dst_gen] (const auto& p) mutable {
        auto comp = p.second;
        auto src = sstable::filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, comp);
        auto dst = sstable::filename(dst_dir, _schema->ks_name(), _schema->cf_name(), _version, dst_gen, _format, comp);
        return do_with(std::move(src), std::move(dst), [this] (const sstring& src, const sstring& dst) mutable {
            return file_exists(dst).then([&, this] (bool exists) mutable {
                if (!exists) {
                    return make_ready_future<>();
                }
                return same_file(src, dst).then_wrapped([&, this] (future<bool> fut) {
                    if (fut.failed()) {
                        auto eptr = fut.get_exception();
                        sstlog.error("Error while linking SSTable: {} to {}: {}", src, dst, eptr);
                        return make_exception_future<>(eptr);
                    }
                    auto same = fut.get0();
                    if (!same) {
                        auto msg = format("Error while linking SSTable: {} to {}: File exists", src, dst);
                        sstlog.error("{}", msg);
                        throw malformed_sstable_exception(msg, _dir);
                    }
                    return make_ready_future<>();
                });
            });
        });
    });
}

/// create_links_common links all component files from the sstable directory to
/// the given destination directory, using the provided generation.
///
/// It first checks if this is a replay of a previous
/// create_links call, by testing if the destination names already
/// exist, and if so, if they point to the same inodes as the
/// source names.  Otherwise, we return an error.
/// This is an indication that something went wrong.
///
/// Creating the links is done by:
/// First, linking the source TOC component to the destination TemporaryTOC,
/// to mark the destination for rollback, in case we crash mid-way.
/// Then, all components are linked.
///
/// Note that if scylla crashes at this point, the destination SSTable
/// will have both a TemporaryTOC file and a regular TOC file.
/// It should be deleted on restart, thus rolling the operation backwards.
///
/// Eventually, if \c mark_for_removal is unset, the detination
/// TemporaryTOC is removed, to "commit" the destination sstable;
///
/// Otherwise, if \c mark_for_removal is set, the TemporaryTOC at the destination
/// is moved to the source directory to mark the source sstable for removal,
/// thus atomically toggling crash recovery from roll-back to roll-forward.
///
/// Similar to the scenario described above, crashing at this point
/// would leave the source sstable marked for removal, possibly
/// having both a TemporaryTOC file and a regular TOC file, and
/// then the source sstable should be deleted on restart, rolling the
/// operation forward.
///
/// Note that idempotent versions of link_file and rename_file
/// are used.  These versions handle EEXIST errors that may happen
/// when the respective operations are replayed.
///
/// \param dir - the destination directory.
/// \param generation - the generation of the destination sstable
/// \param mark_for_removal - mark the sstable for removal after linking it to the destination dir
future<> sstable::create_links_common(const sstring& dir, int64_t generation, bool mark_for_removal) const {
    sstlog.trace("create_links: {} -> {} generation={} mark_for_removal={}", get_filename(), dir, generation, mark_for_removal);
    return do_with(dir, all_components(), [this, generation, mark_for_removal] (const sstring& dir, auto& comps) {
        return check_create_links_replay(dir, generation, comps).then([this, &dir, generation, &comps, mark_for_removal] {
            // TemporaryTOC is always first, TOC is always last
            auto dst = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, component_type::TemporaryTOC);
            return sstable_write_io_check(idempotent_link_file, filename(component_type::TOC), std::move(dst)).then([this, &dir] {
                return sstable_write_io_check(sync_directory, dir);
            }).then([this, &dir, generation, &comps] {
                return parallel_for_each(comps, [this, &dir, generation] (auto p) {
                    auto src = sstable::filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, p.second);
                    auto dst = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, p.second);
                    return sstable_write_io_check(idempotent_link_file, std::move(src), std::move(dst));
                });
            }).then([this, &dir] {
                return sstable_write_io_check(sync_directory, dir);
            });
        }).then([this, &dir, generation, mark_for_removal] {
            auto dst_temp_toc = sstable::filename(dir, _schema->ks_name(), _schema->cf_name(), _version, generation, _format, component_type::TemporaryTOC);
            if (mark_for_removal) {
                // Now that the source sstable is linked to new_dir, mark the source links for
                // deletion by leaving a TemporaryTOC file in the source directory.
                auto src_temp_toc = sstable::filename(_dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, component_type::TemporaryTOC);
                return sstable_write_io_check(rename_file, std::move(dst_temp_toc), std::move(src_temp_toc)).then([this] {
                    return sstable_write_io_check(sync_directory, _dir);
                });
            } else {
                // Now that the source sstable is linked to dir, remove
                // the TemporaryTOC file at the destination.
                return sstable_write_io_check(remove_file, std::move(dst_temp_toc));
            }
        }).then([this, &dir] {
            return sstable_write_io_check(sync_directory, dir);
        }).then([this, &dir, generation] {
            sstlog.trace("create_links: {} -> {} generation={}: done", get_filename(), dir, generation);
        });
    });
}

future<> sstable::create_links(const sstring& dir, int64_t generation) const {
    return create_links_common(dir, generation, false /* mark_for_removal */);
}

future<> sstable::create_links_and_mark_for_removal(const sstring& dir, int64_t generation) const {
    return create_links_common(dir, generation, true /* mark_for_removal */);
}

future<> sstable::set_generation(int64_t new_generation) {
    sstlog.debug("Setting generation for {} to generation={}", get_filename(), new_generation);
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

future<> sstable::move_to_new_dir(sstring new_dir, int64_t new_generation, bool do_sync_dirs) {
    sstring old_dir = get_dir();
    sstlog.debug("Moving {} old_generation={} to {} new_generation={} do_sync_dirs={}",
            get_filename(), old_dir, _generation, new_dir, new_generation, do_sync_dirs);
    return create_links_and_mark_for_removal(new_dir, new_generation).then([this, old_dir, new_dir, new_generation] {
        _dir = new_dir;
        int64_t old_generation = std::exchange(_generation, new_generation);
        return parallel_for_each(all_components(), [this, old_generation, old_dir] (auto p) {
            return sstable_write_io_check(remove_file, sstable::filename(old_dir, _schema->ks_name(), _schema->cf_name(), _version, old_generation, _format, p.second));
        }).then([this, old_dir, old_generation] {
            auto temp_toc = sstable_version_constants::get_component_map(_version).at(component_type::TemporaryTOC);
            return sstable_write_io_check(remove_file, sstable::filename(old_dir, _schema->ks_name(), _schema->cf_name(), _version, old_generation, _format, temp_toc));
        });
    }).then([this, old_dir, new_dir, do_sync_dirs] {
        if (!do_sync_dirs) {
            return make_ready_future<>();
        }
        return when_all_succeed(sync_directory(old_dir), sync_directory(new_dir)).discard_result();
    });
}

flat_mutation_reader_v2
sstable::make_reader(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& mon) {
    if (_version >= version_types::mc) {
        return mx::make_reader(shared_from_this(), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, mon);
    }
    return upgrade_to_v2(kl::make_reader(shared_from_this(), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, mon));
}

flat_mutation_reader
sstable::make_reader_v1(
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& mon) {
    const auto reversed = slice.options.contains(query::partition_slice::option::reversed);
    auto fwd_sm = fwd;
    if (reversed) {
        fwd_sm = streamed_mutation::forwarding::no;
        schema = schema->make_reversed();
    }

    flat_mutation_reader rd(nullptr);
    if (_version >= version_types::mc) {
        rd = downgrade_to_v1(mx::make_reader(shared_from_this(), std::move(schema), permit, range, slice, pc, std::move(trace_state), fwd_sm, fwd_mr, mon));
    } else {
        rd = kl::make_reader(shared_from_this(), std::move(schema), permit, range, slice, pc, std::move(trace_state), fwd_sm, fwd_mr, mon);
    }

    if (reversed) {
        rd = make_reversing_reader(std::move(rd), permit.max_result_size());
        if (fwd) {
            rd = make_forwardable(std::move(rd));
        }
    }
    return rd;
}

flat_mutation_reader_v2
sstable::make_crawling_reader(
        schema_ptr schema,
        reader_permit permit,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor) {
    if (_version >= version_types::mc) {
        return mx::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), pc, std::move(trace_state), monitor);
    }
    return upgrade_to_v2(kl::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), pc, std::move(trace_state), monitor));
}

flat_mutation_reader
sstable::make_crawling_reader_v1(
        schema_ptr schema,
        reader_permit permit,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor) {
    if (_version >= version_types::mc) {
        return downgrade_to_v1(mx::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), pc, std::move(trace_state), monitor));
    }
    return kl::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), pc, std::move(trace_state), monitor);
}

entry_descriptor entry_descriptor::make_descriptor(sstring sstdir, sstring fname) {
    static std::regex la_mx("(la|m[cd])-(\\d+)-(\\w+)-(.*)");
    static std::regex ka("(\\w+)-(\\w+)-ka-(\\d+)-(.*)");

    static std::regex dir(".*/([^/]*)/([^/]+)-[\\da-fA-F]+(?:/staging|/upload|/snapshots/[^/]+)?/?");

    std::smatch match;

    sstable::version_types version;

    sstring generation;
    sstring format;
    sstring component;
    sstring ks;
    sstring cf;

    sstlog.debug("Make descriptor sstdir: {}; fname: {}", sstdir, fname);
    std::string s(fname);
    if (std::regex_match(s, match, la_mx)) {
        std::string sdir(sstdir);
        std::smatch dirmatch;
        if (std::regex_match(sdir, dirmatch, dir)) {
            ks = dirmatch[1].str();
            cf = dirmatch[2].str();
        } else {
            throw malformed_sstable_exception(seastar::format("invalid path for file {}: {}. Path doesn't match known pattern.", fname, sstdir));
        }
        version = from_string(match[1].str());
        generation = match[2].str();
        format = sstring(match[3].str());
        component = sstring(match[4].str());
    } else if (std::regex_match(s, match, ka)) {
        ks = match[1].str();
        cf = match[2].str();
        version = sstable::version_types::ka;
        format = sstring("big");
        generation = match[3].str();
        component = sstring(match[4].str());
    } else {
        throw malformed_sstable_exception(seastar::format("invalid version for file {}. Name doesn't match any known version.", fname));
    }
    return entry_descriptor(sstdir, ks, cf, boost::lexical_cast<unsigned long>(generation), version, sstable::format_from_sstring(format), sstable::component_from_sstring(version, component));
}

sstable::version_types sstable::version_from_sstring(sstring &s) {
    try {
        return reverse_map(s, _version_string);
    } catch (std::out_of_range&) {
        throw std::out_of_range(seastar::format("Unknown sstable version: {}", s.c_str()));
    }
}

sstable::format_types sstable::format_from_sstring(sstring &s) {
    try {
        return reverse_map(s, _format_string);
    } catch (std::out_of_range&) {
        throw std::out_of_range(seastar::format("Unknown sstable format: {}", s.c_str()));
    }
}

component_type sstable::component_from_sstring(version_types v, sstring &s) {
    try {
        return reverse_map(s, sstable_version_constants::get_component_map(v));
    } catch (std::out_of_range&) {
        return component_type::Unknown;
    }
}

input_stream<char> sstable::data_stream(uint64_t pos, size_t len, const io_priority_class& pc,
        reader_permit permit, tracing::trace_state_ptr trace_state, lw_shared_ptr<file_input_stream_history> history) {
    file_input_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = 4;
    options.dynamic_adjustments = std::move(history);

    file f = make_tracked_file(_data_file, std::move(permit));
    if (trace_state) {
        f = tracing::make_traced_file(std::move(f), std::move(trace_state), format("{}:", get_filename()));
    }

    input_stream<char> stream;
    if (_components->compression) {
        if (_version >= sstable_version_types::mc) {
             return make_compressed_file_m_format_input_stream(f, &_components->compression,
                pos, len, std::move(options));
        } else {
            return make_compressed_file_k_l_format_input_stream(f, &_components->compression,
                pos, len, std::move(options));
        }
    }

    return make_file_input_stream(f, pos, len, std::move(options));
}

future<temporary_buffer<char>> sstable::data_read(uint64_t pos, size_t len, const io_priority_class& pc, reader_permit permit) {
    return do_with(data_stream(pos, len, pc, std::move(permit), tracing::trace_state_ptr(), {}), [len] (auto& stream) {
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
            throw malformed_sstable_exception(format("{} key of summary of {} is empty", m, get_filename()));
        }
        auto pk = key::from_bytes(value).to_partition_key(*_schema);
        return dht::decorate_key(*_schema, std::move(pk));
    };
    auto first = decorate_key("first", _components->summary.first_key.value);
    auto last = decorate_key("last", _components->summary.last_key.value);
    if (first.tri_compare(*_schema, last) > 0) {
        throw malformed_sstable_exception(format("{}: first and last keys of summary are misordered: first={} > last={}", get_filename(), first, last));
    }
    _first = std::move(first);
    _last = std::move(last);
}

const partition_key& sstable::get_first_partition_key() const {
    return get_first_decorated_key().key();
 }

const partition_key& sstable::get_last_partition_key() const {
    return get_last_decorated_key().key();
}

const dht::decorated_key& sstable::get_first_decorated_key() const {
    if (!_first) {
        throw std::runtime_error(format("first key of {} wasn't set", get_filename()));
    }
    return *_first;
}

const dht::decorated_key& sstable::get_last_decorated_key() const {
    if (!_last) {
        throw std::runtime_error(format("last key of {} wasn't set", get_filename()));
    }
    return *_last;
}

std::strong_ordering sstable::compare_by_first_key(const sstable& other) const {
    return get_first_decorated_key().tri_compare(*_schema, other.get_first_decorated_key());
}

double sstable::get_compression_ratio() const {
    if (this->has_component(component_type::CompressionInfo)) {
        return double(_components->compression.compressed_file_length()) / _components->compression.uncompressed_file_length();
    } else {
        return metadata_collector::NO_COMPRESSION_RATIO;
    }
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

future<> sstable::close_files() {
    auto index_closed = make_ready_future<>();
    if (_index_file) {
        index_closed = _index_file.close().handle_exception([me = shared_from_this()] (auto ep) {
            sstlog.warn("sstable close index_file failed: {}", ep);
            general_disk_error();
        });
    }
    auto data_closed = make_ready_future<>();
    if (_data_file) {
        data_closed = _data_file.close().handle_exception([me = shared_from_this()] (auto ep) {
            sstlog.warn("sstable close data_file failed: {}", ep);
            general_disk_error();
        });
    }

    auto unlinked = make_ready_future<>();
    if (_marked_for_deletion != mark_for_deletion::none) {
        // If a deletion fails for some reason we
        // log and ignore this failure, because on startup we'll again try to
        // clean up unused sstables, and because we'll never reuse the same
        // generation number anyway.
        sstlog.debug("Deleting sstable that is {}marked for deletion", _marked_for_deletion == mark_for_deletion::implicit ? "implicitly " : "");
        try {
            unlinked = unlink().handle_exception(
                        [me = shared_from_this()] (std::exception_ptr eptr) {
                            try {
                                std::rethrow_exception(eptr);
                            } catch (...) {
                                sstlog.warn("Exception when deleting sstable file: {}", eptr);
                            }
                        });
        } catch (...) {
            sstlog.warn("Exception when deleting sstable file: {}", std::current_exception());
        }

    }

    _on_closed(*this);

    return when_all_succeed(std::move(index_closed), std::move(data_closed), std::move(unlinked)).discard_result().then([this] {
        if (_open_mode) {
            if (_open_mode.value() == open_flags::ro) {
                _stats.on_close_for_reading();
            } else {
                _stats.on_close_for_writing();
            }
        }
        _open_mode.reset();
    });
}

static inline sstring dirname(const sstring& fname) {
    return fs::canonical(fs::path(fname)).parent_path().string();
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

static future<>
remove_by_toc_name(std::string_view sstable_toc_strview) noexcept {
    sstring sstable_toc_name, prefix, new_toc_name, dir;
    try {
        sstable_toc_name = sstring(sstable_toc_strview);
        prefix = sstable_toc_name.substr(0, sstable_toc_name.size() - sstable_version_constants::TOC_SUFFIX.size());
        new_toc_name = prefix + sstable_version_constants::TEMPORARY_TOC_SUFFIX;
    } catch (...) {
        return current_exception_as_future();
    }

    return do_with(std::move(sstable_toc_name), std::move(prefix), std::move(new_toc_name), sstring(),
            [] (sstring& sstable_toc_name, sstring& prefix, sstring& new_toc_name, sstring& dir) {
        sstlog.debug("Removing by TOC name: {}", sstable_toc_name);
        return sstable_io_check(sstable_write_error_handler, file_exists, sstable_toc_name).then([&] (bool toc_exists) {
            if (toc_exists) {
                dir = dirname(sstable_toc_name);
                // If new_toc_name exists it will be atomically replaced.  See rename(2)
                return sstable_io_check(sstable_write_error_handler, rename_file, sstable_toc_name, new_toc_name).then([&dir] {
                    return fsync_directory(sstable_write_error_handler, dir);
                }).then([] {
                    return make_ready_future<bool>(true);
                });
            } else {
                return sstable_io_check(sstable_write_error_handler, file_exists, new_toc_name);
            }
        }).then([&] (bool exists) {
            if (!exists) {
                sstlog.warn("Unable to delete {} because it doesn't exist.", sstable_toc_name);
                return make_ready_future<>();
            } else {
                dir = dirname(new_toc_name);
            }
            return with_file(open_checked_file_dma(sstable_write_error_handler, new_toc_name, open_flags::ro), [&] (file& toc_file) {
                return toc_file.size().then([&] (size_t size) {
                    return do_with(make_file_input_stream(toc_file), [&, size] (input_stream<char>& in) {
                        return in.read_exactly(size).then([&] (temporary_buffer<char> text) {
                            std::vector<sstring> components;
                            sstring all(text.begin(), text.end());
                            boost::split(components, all, boost::is_any_of("\n"));
                            return parallel_for_each(components, [&prefix] (sstring component) {
                                if (component.empty()) {
                                    // eof
                                    return make_ready_future<>();
                                }
                                if (component == sstable_version_constants::TOC_SUFFIX) {
                                    // already renamed
                                    return make_ready_future<>();
                                }
                                auto fname = prefix + component;
                                return sstable_io_check(sstable_write_error_handler, remove_file, fname).handle_exception([fname = std::move(fname)] (std::exception_ptr eptr) {
                                    // forgive ENOENT, since the component may not have been written;
                                    try {
                                        std::rethrow_exception(eptr);
                                    } catch (const std::system_error& e) {
                                        if (!is_system_error_errno(ENOENT)) {
                                            return make_exception_future<>(eptr);
                                        }
                                        sstlog.debug("Forgiving ENOENT when deleting file {}", fname);
                                        return make_ready_future<>();
                                    }
                                    __builtin_unreachable();
                                });
                            }).then([&dir] {
                                return fsync_directory(sstable_write_error_handler, dir);
                            }).then([&new_toc_name] {
                                return sstable_io_check(sstable_write_error_handler, remove_file, new_toc_name);
                            });
                        }).finally([&in] () mutable {
                            return in.close();
                        });
                    });
                });
            });
        });
    });
}

future<>
sstable::remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f) {
    return seastar::async([ks, cf, dir, generation, v, f] {
        const io_error_handler& error_handler = sstable_write_error_handler;
        auto toc = sstable_io_check(error_handler, file_exists, filename(dir, ks, cf, v, generation, f, component_type::TOC)).get0();

        sstlog.warn("Deleting components of sstable from {}.{} of generation {} that has a temporary TOC", ks, cf, generation);

        // assert that toc doesn't exist for sstable with temporary toc.
        assert(toc == false);

        auto tmptoc = sstable_io_check(error_handler, file_exists, filename(dir, ks, cf, v, generation, f, component_type::TemporaryTOC)).get0();
        // assert that temporary toc exists for this sstable.
        assert(tmptoc == true);

        for (auto& entry : sstable_version_constants::get_component_map(v)) {
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

/**
 * Returns a pair of positions [p1, p2) in the summary file corresponding to entries
 * covered by the specified range, or a disengaged optional if no such pair exists.
 */
std::optional<std::pair<uint64_t, uint64_t>> sstable::get_sample_indexes_for_range(const dht::token_range& range) {
    auto entries_size = _components->summary.entries.size();
    auto search = [this](bool before, const dht::token& token) {
        auto kind = before ? key::kind::before_all_keys : key::kind::after_all_keys;
        key k(kind);
        // Binary search will never returns positive values.
        return uint64_t((binary_search(_schema->get_partitioner(), _components->summary.entries, k, token) + 1) * -1);
    };
    uint64_t left = 0;
    if (range.start()) {
        left = search(range.start()->is_inclusive(), range.start()->value());
        if (left == entries_size) {
            // left is past the end of the sampling.
            return std::nullopt;
        }
    }
    uint64_t right = entries_size;
    if (range.end()) {
        right = search(!range.end()->is_inclusive(), range.end()->value());
        if (right == 0) {
            // The first key is strictly greater than right.
            return std::nullopt;
        }
    }
    if (left < right) {
        return std::optional<std::pair<uint64_t, uint64_t>>(std::in_place_t(), left, right);
    }
    return std::nullopt;
}

/**
 * Returns a pair of positions [p1, p2) in the summary file corresponding to
 * pages which may include keys covered by the specified range, or a disengaged
 * optional if the sstable does not include any keys from the range.
 */
std::optional<std::pair<uint64_t, uint64_t>> sstable::get_index_pages_for_range(const dht::token_range& range) {
    const auto& entries = _components->summary.entries;
    auto entries_size = entries.size();
    index_comparator cmp(*_schema);
    dht::ring_position_comparator rp_cmp(*_schema);
    uint64_t left = 0;
    if (range.start()) {
        dht::ring_position_view pos = range.start()->is_inclusive()
            ? dht::ring_position_view::starting_at(range.start()->value())
            : dht::ring_position_view::ending_at(range.start()->value());

        // There is no summary entry for the last key, so in order to determine
        // if pos overlaps with the sstable or not we have to compare with the
        // last key.
        if (rp_cmp(pos, get_last_decorated_key()) > 0) {
            // left is past the end of the sampling.
            return std::nullopt;
        }

        left = std::distance(std::begin(entries),
            std::lower_bound(entries.begin(), entries.end(), pos, cmp));

        if (left) {
            --left;
        }
    }
    uint64_t right = entries_size;
    if (range.end()) {
        dht::ring_position_view pos = range.end()->is_inclusive()
                                      ? dht::ring_position_view::ending_at(range.end()->value())
                                      : dht::ring_position_view::starting_at(range.end()->value());

        right = std::distance(std::begin(entries),
            std::lower_bound(entries.begin(), entries.end(), pos, cmp));
        if (right == 0) {
            // The first key is strictly greater than right.
            return std::nullopt;
        }
    }
    if (left < right) {
        return std::optional<std::pair<uint64_t, uint64_t>>(std::in_place_t(), left, right);
    }
    return std::nullopt;
}

std::vector<dht::decorated_key> sstable::get_key_samples(const schema& s, const dht::token_range& range) {
    auto index_range = get_sample_indexes_for_range(range);
    std::vector<dht::decorated_key> res;
    if (index_range) {
        for (auto idx = index_range->first; idx < index_range->second; ++idx) {
            auto pkey = _components->summary.entries[idx].get_key().to_partition_key(s);
            res.push_back(dht::decorate_key(s, std::move(pkey)));
        }
    }
    return res;
}

uint64_t sstable::estimated_keys_for_range(const dht::token_range& range) {
    auto page_range = get_index_pages_for_range(range);
    if (!page_range) {
        return 0;
    }
    using uint128_t = unsigned __int128;
    uint64_t range_pages = page_range->second - page_range->first;
    auto total_keys = get_estimated_key_count();
    auto total_pages = _components->summary.entries.size();
    uint64_t estimated_keys = (uint128_t)range_pages * total_keys / total_pages;
    return std::max(uint64_t(1), estimated_keys);
}

std::vector<unsigned>
sstable::compute_shards_for_this_sstable() const {
    std::unordered_set<unsigned> shards;
    dht::partition_range_vector token_ranges;
    const auto* sm = _components->scylla_metadata
            ? _components->scylla_metadata->data.get<scylla_metadata_type::Sharding, sharding_metadata>()
            : nullptr;
    if (!sm || sm->token_ranges.elements.empty()) {
        token_ranges.push_back(dht::partition_range::make(
                dht::ring_position::starting_at(get_first_decorated_key().token()),
                dht::ring_position::ending_at(get_last_decorated_key().token())));
    } else {
        auto disk_token_range_to_ring_position_range = [] (const disk_token_range& dtr) {
            auto t1 = dht::token(dht::token::kind::key, bytes_view(dtr.left.token));
            auto t2 = dht::token(dht::token::kind::key, bytes_view(dtr.right.token));
            return dht::partition_range::make(
                    (dtr.left.exclusive ? dht::ring_position::ending_at : dht::ring_position::starting_at)(std::move(t1)),
                    (dtr.right.exclusive ? dht::ring_position::starting_at : dht::ring_position::ending_at)(std::move(t2)));
        };
        token_ranges = boost::copy_range<dht::partition_range_vector>(
                sm->token_ranges.elements
                | boost::adaptors::transformed(disk_token_range_to_ring_position_range));
    }
    auto sharder = dht::ring_position_range_vector_sharder(_schema->get_sharder(), std::move(token_ranges));
    auto rpras = sharder.next(*_schema);
    while (rpras) {
        shards.insert(rpras->shard);
        rpras = sharder.next(*_schema);
    }
    return boost::copy_range<std::vector<unsigned>>(shards);
}

future<bool> sstable::has_partition_key(const utils::hashed_key& hk, const dht::decorated_key& dk) {
    shared_sstable s = shared_from_this();
    if (!filter_has_key(hk)) {
        co_return false;
    }
    bool present;
    std::exception_ptr ex;
    auto sem = reader_concurrency_semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::has_partition_key()");
    try {
        auto lh_index_ptr = std::make_unique<sstables::index_reader>(s, sem.make_tracking_only_permit(_schema.get(), s->get_filename(), db::no_timeout), default_priority_class(), tracing::trace_state_ptr(), use_caching::yes);
        present = co_await lh_index_ptr->advance_lower_and_check_if_present(dk);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await sem.stop();
    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }
    co_return present;
}

utils::hashed_key sstable::make_hashed_key(const schema& s, const partition_key& key) {
    return utils::make_hashed_key(static_cast<bytes_view>(key::from_partition_key(s, key)));
}

future<>
delete_sstables(std::vector<sstring> tocs) {
    return parallel_for_each(tocs, [] (const sstring& name) {
        return remove_by_toc_name(name);
    });
}

future<>
sstable::unlink() noexcept {
    // We must be able to generate toc_filename()
    // in order to delete the sstable.
    // Running out of memory here will terminate.
    auto name = [this] () noexcept {
        memory::scoped_critical_alloc_section _;
        return toc_filename();
    }();

    // remove_by_toc_name doesn't throw
    auto fut = remove_by_toc_name(name);
    // remove_fut never fails
    auto remove_fut = fut.then_wrapped([&name] (future<> f) {
        if (f.failed()) {
            // Log and ignore the failure since there is nothing much we can do about it at this point.
            // a. Compaction will retry deleting the sstable in the next pass, and
            // b. in the future sstables_manager is planned to handle sstables deletion.
            // c. Eventually we may want to record these failures in a system table
            //    and notify the administrator about that for manual handling (rather than aborting).
            sstlog.warn("Failed to delete {}: {}. Ignoring.", name, f.get_exception());
        }
        return make_ready_future<>();
    });

    try {
        co_await get_large_data_handler().maybe_delete_large_data_entries(shared_from_this());
    } catch (...) {
        // Just log and ignore failures to delete large data entries.
        // They are not critical to the operation of the database.
        sstlog.warn("Failed to delete large data entry for {}: {}. Ignoring.", name, std::current_exception());
    }

    co_await std::move(remove_fut);
    _stats.on_delete();
}

future<>
delete_atomically(std::vector<shared_sstable> ssts) {
    if (ssts.empty()) {
        return make_ready_future<>();
    }
    return seastar::async([ssts = std::move(ssts)] {
        sstring sstdir;
        min_max_tracker<int64_t> gen_tracker;

        for (const auto& sst : ssts) {
            gen_tracker.update(sst->generation());

            if (sstdir.empty()) {
                sstdir = sst->get_dir();
            } else {
                // All sstables are assumed to be in the same column_family, hence
                // sharing their base directory.
                assert (sstdir == sst->get_dir());
            }
        }

        sstring pending_delete_dir = sstdir + "/" + sstable::pending_delete_dir_basename();
        sstring pending_delete_log = format("{}/sstables-{}-{}.log", pending_delete_dir, gen_tracker.min(), gen_tracker.max());
        sstring tmp_pending_delete_log = pending_delete_log + ".tmp";
        sstlog.trace("Writing {}", tmp_pending_delete_log);
        try {
            touch_directory(pending_delete_dir).get();
            auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
            // Create temporary pending_delete log file.
            auto f = open_file_dma(tmp_pending_delete_log, oflags).get0();
            // Write all toc names into the log file.
            file_output_stream_options options;
            options.buffer_size = 4096;
            auto w = file_writer::make(std::move(f), options, tmp_pending_delete_log).get0();

            for (const auto& sst : ssts) {
                auto toc = sst->component_basename(component_type::TOC);
                w.write(toc.c_str(), toc.size());
                w.write("\n", 1);
            }

            w.flush();
            w.close();

            auto dir_f = open_directory(pending_delete_dir).get0();
            // Once flushed and closed, the temporary log file can be renamed.
            rename_file(tmp_pending_delete_log, pending_delete_log).get();

            // Guarantee that the changes above reached the disk.
            dir_f.flush().get();
            dir_f.close().get();
            sstlog.debug("{} written successfully.", pending_delete_log);
        } catch (...) {
            sstlog.warn("Error while writing {}: {}. Ignoring.", pending_delete_log, std::current_exception());
        }

        parallel_for_each(ssts, [] (shared_sstable sst) {
            return sst->unlink();
        }).get();

        // Once all sstables are deleted, the log file can be removed.
        // Note: the log file will be removed also if unlink failed to remove
        // any sstable and ignored the error.
        try {
            remove_file(pending_delete_log).get();
            sstlog.debug("{} removed.", pending_delete_log);
        } catch (...) {
            sstlog.warn("Error removing {}: {}. Ignoring.", pending_delete_log, std::current_exception());
        }
    });
}

// FIXME: Go through maybe_delete_large_partitions_entry on recovery
// since this is an indication we crashed in the middle of delete_atomically
future<> replay_pending_delete_log(sstring pending_delete_log) {
    sstlog.debug("Reading pending_deletes log file {}", pending_delete_log);
    return seastar::async([pending_delete_log = std::move(pending_delete_log)] {
        sstring pending_delete_dir = dirname(pending_delete_log);
        assert(sstable::is_pending_delete_dir(fs::path(pending_delete_dir)));
        try {
            auto sstdir = dirname(pending_delete_dir);
            auto f = open_file_dma(pending_delete_log, open_flags::ro).get0();
            auto size = f.size().get0();
            auto in = make_file_input_stream(f);
            auto text = in.read_exactly(size).get0();
            in.close().get();
            f.close().get();

            sstring all(text.begin(), text.end());
            std::vector<sstring> basenames;
            boost::split(basenames, all, boost::is_any_of("\n"), boost::token_compress_on);
            auto tocs = boost::copy_range<std::vector<sstring>>(basenames
                    | boost::adaptors::filtered([] (auto&& basename) { return !basename.empty(); })
                    | boost::adaptors::transformed([&sstdir] (auto&& basename) { return sstdir + "/" + basename; }));
            delete_sstables(tocs).get();
        } catch (...) {
            sstlog.warn("Error replaying {}: {}. Ignoring.", pending_delete_log, std::current_exception());
        }
    });
}

thread_local sstables_stats::stats sstables_stats::_shard_stats;
thread_local partition_index_cache::stats partition_index_cache::_shard_stats;
thread_local cached_file::metrics index_page_cache_metrics;
thread_local mc::cached_promoted_index::metrics promoted_index_cache_metrics;
static thread_local seastar::metrics::metric_groups metrics;

future<> init_metrics() {
  return seastar::smp::invoke_on_all([] {
    namespace sm = seastar::metrics;
    metrics.add_group("sstables", {
        sm::make_derive("index_page_hits", [] { return partition_index_cache::shard_stats().hits; },
            sm::description("Index page requests which could be satisfied without waiting")),
        sm::make_derive("index_page_misses", [] { return partition_index_cache::shard_stats().misses; },
            sm::description("Index page requests which initiated a read from disk")),
        sm::make_derive("index_page_blocks", [] { return partition_index_cache::shard_stats().blocks; },
            sm::description("Index page requests which needed to wait due to page not being loaded yet")),
        sm::make_derive("index_page_evictions", [] { return partition_index_cache::shard_stats().evictions; },
            sm::description("Index pages which got evicted from memory")),
        sm::make_derive("index_page_populations", [] { return partition_index_cache::shard_stats().populations; },
            sm::description("Index pages which got populated into memory")),
        sm::make_gauge("index_page_used_bytes", [] { return partition_index_cache::shard_stats().used_bytes; },
            sm::description("Amount of bytes used by index pages in memory")),

        sm::make_derive("index_page_cache_hits", [] { return index_page_cache_metrics.page_hits; },
            sm::description("Index page cache requests which were served from cache")),
        sm::make_derive("index_page_cache_misses", [] { return index_page_cache_metrics.page_misses; },
            sm::description("Index page cache requests which had to perform I/O")),
        sm::make_derive("index_page_cache_evictions", [] { return index_page_cache_metrics.page_evictions; },
            sm::description("Total number of index page cache pages which have been evicted")),
        sm::make_derive("index_page_cache_populations", [] { return index_page_cache_metrics.page_populations; },
            sm::description("Total number of index page cache pages which were inserted into the cache")),
        sm::make_gauge("index_page_cache_bytes", [] { return index_page_cache_metrics.cached_bytes; },
            sm::description("Total number of bytes cached in the index page cache")),
        sm::make_gauge("index_page_cache_bytes_in_std", [] { return index_page_cache_metrics.bytes_in_std; },
            sm::description("Total number of bytes in temporary buffers which live in the std allocator")),

        sm::make_derive("pi_cache_hits_l0", [] { return promoted_index_cache_metrics.hits_l0; },
            sm::description("Number of requests for promoted index block in state l0 which didn't have to go to the page cache")),
        sm::make_derive("pi_cache_hits_l1", [] { return promoted_index_cache_metrics.hits_l1; },
            sm::description("Number of requests for promoted index block in state l1 which didn't have to go to the page cache")),
        sm::make_derive("pi_cache_hits_l2", [] { return promoted_index_cache_metrics.hits_l2; },
            sm::description("Number of requests for promoted index block in state l2 which didn't have to go to the page cache")),
        sm::make_derive("pi_cache_misses_l0", [] { return promoted_index_cache_metrics.misses_l0; },
            sm::description("Number of requests for promoted index block in state l0 which had to go to the page cache")),
        sm::make_derive("pi_cache_misses_l1", [] { return promoted_index_cache_metrics.misses_l1; },
            sm::description("Number of requests for promoted index block in state l1 which had to go to the page cache")),
        sm::make_derive("pi_cache_misses_l2", [] { return promoted_index_cache_metrics.misses_l2; },
            sm::description("Number of requests for promoted index block in state l2 which had to go to the page cache")),
        sm::make_derive("pi_cache_populations", [] { return promoted_index_cache_metrics.populations; },
            sm::description("Number of promoted index blocks which got inserted")),
        sm::make_derive("pi_cache_evictions", [] { return promoted_index_cache_metrics.evictions; },
            sm::description("Number of promoted index blocks which got evicted")),
        sm::make_gauge("pi_cache_bytes", [] { return promoted_index_cache_metrics.used_bytes; },
            sm::description("Number of bytes currently used by cached promoted index blocks")),
        sm::make_gauge("pi_cache_block_count", [] { return promoted_index_cache_metrics.block_count; },
            sm::description("Number of promoted index blocks currently cached")),

        sm::make_derive("partition_writes", [] { return sstables_stats::get_shard_stats().partition_writes; },
            sm::description("Number of partitions written")),
        sm::make_derive("static_row_writes", [] { return sstables_stats::get_shard_stats().static_row_writes; },
            sm::description("Number of static rows written")),
        sm::make_derive("row_writes", [] { return sstables_stats::get_shard_stats().row_writes; },
            sm::description("Number of clustering rows written")),
        sm::make_derive("cell_writes", [] { return sstables_stats::get_shard_stats().cell_writes; },
            sm::description("Number of cells written")),
        sm::make_derive("tombstone_writes", [] { return sstables_stats::get_shard_stats().tombstone_writes; },
            sm::description("Number of tombstones written")),
        sm::make_derive("range_tombstone_writes", [] { return sstables_stats::get_shard_stats().range_tombstone_writes; },
            sm::description("Number of range tombstones written")),
        sm::make_derive("range_tombstone_reads", [] { return sstables_stats::get_shard_stats().range_tombstone_reads; },
            sm::description("Number of range tombstones read")),
        sm::make_derive("row_tombstone_reads", [] { return sstables_stats::get_shard_stats().row_tombstone_reads; },
            sm::description("Number of row tombstones read")),
        sm::make_derive("cell_tombstone_writes", [] { return sstables_stats::get_shard_stats().cell_tombstone_writes; },
            sm::description("Number of cell tombstones written")),
        sm::make_derive("single_partition_reads", [] { return sstables_stats::get_shard_stats().single_partition_reads; },
            sm::description("Number of single partition flat mutation reads")),
        sm::make_derive("range_partition_reads", [] { return sstables_stats::get_shard_stats().range_partition_reads; },
            sm::description("Number of partition range flat mutation reads")),
        sm::make_derive("partition_reads", [] { return sstables_stats::get_shard_stats().partition_reads; },
            sm::description("Number of partitions read")),
        sm::make_derive("partition_seeks", [] { return sstables_stats::get_shard_stats().partition_seeks; },
            sm::description("Number of partitions seeked")),
        sm::make_derive("row_reads", [] { return sstables_stats::get_shard_stats().row_reads; },
            sm::description("Number of rows read")),

        sm::make_counter("capped_local_deletion_time", [] { return sstables_stats::get_shard_stats().capped_local_deletion_time; },
            sm::description("Was local deletion time capped at maximum allowed value in Statistics")),
        sm::make_counter("capped_tombstone_deletion_time", [] { return sstables_stats::get_shard_stats().capped_tombstone_deletion_time; },
            sm::description("Was partition tombstone deletion time capped at maximum allowed value")),

        sm::make_derive("total_open_for_reading", [] { return sstables_stats::get_shard_stats().open_for_reading; },
            sm::description("Counter of sstables open for reading")),
        sm::make_derive("total_open_for_writing", [] { return sstables_stats::get_shard_stats().open_for_writing; },
            sm::description("Counter of sstables open for writing")),

        sm::make_gauge("currently_open_for_reading", [] {
            return sstables_stats::get_shard_stats().open_for_reading -
                   sstables_stats::get_shard_stats().closed_for_reading;
        }, sm::description("Number of sstables currently open for reading")),
        sm::make_gauge("currently_open_for_writing", [] {
            return sstables_stats::get_shard_stats().open_for_writing -
                   sstables_stats::get_shard_stats().closed_for_writing;
        }, sm::description("Number of sstables currently open for writing")),

        sm::make_derive("total_deleted", [] { return sstables_stats::get_shard_stats().deleted; },
            sm::description("Counter of deleted sstables")),

        sm::make_gauge("bloom_filter_memory_size", [] { return utils::filter::bloom_filter::get_shard_stats().memory_size; },
            sm::description("Bloom filter memory usage in bytes.")),
    });
  });
}

mutation_source sstable::as_mutation_source() {
    return mutation_source([sst = shared_from_this()] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) mutable {
        return sst->make_reader_v1(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    });
}

sstable::sstable(schema_ptr schema,
        sstring dir,
        int64_t generation,
        version_types v,
        format_types f,
        db::large_data_handler& large_data_handler,
        sstables_manager& manager,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size)
    : sstable_buffer_size(buffer_size)
    , _schema(std::move(schema))
    , _dir(std::move(dir))
    , _generation(generation)
    , _version(v)
    , _format(f)
    , _index_cache(std::make_unique<partition_index_cache>(
            manager.get_cache_tracker().get_lru(), manager.get_cache_tracker().region()))
    , _now(now)
    , _read_error_handler(error_handler_gen(sstable_read_error))
    , _write_error_handler(error_handler_gen(sstable_write_error))
    , _large_data_handler(large_data_handler)
    , _manager(manager)
{
    tracker.add(*this);
    manager.add(this);
}

file sstable::uncached_index_file() {
    return _cached_index_file->get_file();
}

void sstable::unused() {
    if (_active) {
        _active = false;
        _manager.deactivate(this);
    } else {
        _manager.remove(this);
    }
}

future<> sstable::destroy() {
    return close_files().finally([this] {
        return _index_cache->evict_gently().then([this] {
            if (_cached_index_file) {
                return _cached_index_file->evict_gently();
            } else {
                return make_ready_future<>();
            }
        });
    });
}

future<file_writer> file_writer::make(file f, file_output_stream_options options, sstring filename) noexcept {
    // note: make_file_output_stream closes the file if the stream creation fails
    return make_file_output_stream(std::move(f), std::move(options))
        .then([filename = std::move(filename)] (output_stream<char>&& out) {
            return file_writer(std::move(out), std::move(filename));
        });
}

std::ostream& operator<<(std::ostream& out, const deletion_time& dt) {
    return out << "{timestamp=" << dt.marked_for_delete_at << ", deletion_time=" << dt.marked_for_delete_at << "}";
}

std::ostream& operator<<(std::ostream& out, const sstables::component_type& comp_type) {
    using ct = sstables::component_type;
    switch (comp_type) {
    case ct::Index: out << "Index"; break;
    case ct::CompressionInfo: out << "CompressionInfo"; break;
    case ct::Data: out << "Data"; break;
    case ct::TOC: out << "TOC"; break;
    case ct::Summary: out << "Summary"; break;
    case ct::Digest: out << "Digest"; break;
    case ct::CRC: out << "CRC"; break;
    case ct::Filter: out << "Filter"; break;
    case ct::Statistics: out << "Statistics"; break;
    case ct::TemporaryTOC: out << "TemporaryTOC"; break;
    case ct::TemporaryStatistics: out << "TemporaryStatistics"; break;
    case ct::Scylla: out << "Scylla"; break;
    case ct::Unknown: out << "Unknown"; break;
    }
    return out;
}

std::optional<large_data_stats_entry> sstable::get_large_data_stat(large_data_type t) const noexcept {
    if (_large_data_stats) {
        auto it = _large_data_stats->map.find(t);
        if (it != _large_data_stats->map.end()) {
            return std::make_optional<large_data_stats_entry>(it->second);
        }
    }
    return std::make_optional<large_data_stats_entry>();
}

}

namespace seastar {

void
lw_shared_ptr_deleter<sstables::sstable>::dispose(sstables::sstable* s) {
    s->unused();
}


template
sstables::sstable*
seastar::internal::lw_shared_ptr_accessors<sstables::sstable, void>::to_value(seastar::lw_shared_ptr_counter_base*);

}
