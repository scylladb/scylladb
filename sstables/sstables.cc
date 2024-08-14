/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include <concepts>
#include <vector>
#include <limits>
#include <fmt/ranges.h>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_ptr_incomplete.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/file.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/short_streams.hh>
#include <iterator>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>

#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/to_string.hh"
#include "data_dictionary/storage_options.hh"
#include "dht/sharder.hh"
#include "writer.hh"
#include "m_format_read_helpers.hh"
#include "open_info.hh"
#include "sstables.hh"
#include "sstable_writer.hh"
#include "sstable_version.hh"
#include "metadata_collector.hh"
#include "progress_monitor.hh"
#include "compress.hh"
#include "index_reader.hh"
#include "downsampling.hh"
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/is_sorted.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/regex.hpp>
#include <seastar/core/align.hh>
#include "mutation/range_tombstone_list.hh"
#include "binary_search.hh"
#include "utils/bloom_filter.hh"
#include "utils/cached_file.hh"
#include "utils/stall_free.hh"
#include "checked-file-impl.hh"
#include "db/extensions.hh"
#include "sstables/partition_index_cache.hh"
#include "db/large_data_handler.hh"
#include "db/config.hh"
#include "sstables/random_access_reader.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/partition_index_cache.hh"
#include "utils/UUID_gen.hh"
#include "sstables_manager.hh"
#include "tracing/traced_file.hh"
#include "kl/reader.hh"
#include "mx/reader.hh"
#include "utils/bit_cast.hh"
#include "utils/cached_file.hh"
#include "tombstone_gc.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/mutation_source.hh"
#include "readers/reversing_v2.hh"
#include "readers/forwardable_v2.hh"

#include "release.hh"
#include "utils/build_id.hh"

thread_local disk_error_signal_type sstable_read_error;
thread_local disk_error_signal_type sstable_write_error;

namespace sstables {

// The below flag governs the mode of index file page caching used by the index
// reader.
//
// If set to true, the reader will read and/or populate a common global cache,
// which shares its capacity with the row cache. If false, the reader will use
// BYPASS CACHE semantics for index caching.
//
// This flag is intended to be a temporary hack. The goal is to eventually
// solve index caching problems via a smart cache replacement policy.
//
thread_local utils::updateable_value<bool> global_cache_index_pages(true);

logging::logger sstlog("sstable");

template <typename T>
const char* nullsafe_typename(T* x) noexcept {
    try {
        return typeid(*x).name();
    } catch (const std::bad_typeid&) {
        return "nullptr";
    }
}

// dynamic_cast, but calls on_internal_error on failure.
template <typename Derived, typename Base>
Derived* downcast_ptr(Base* x) {
    if (auto casted = dynamic_cast<Derived*>(x)) {
        return casted;
    } else {
        on_internal_error(sstlog, fmt::format("Bad downcast: expected {}, but got {}", typeid(Derived*).name(), nullsafe_typename(x)));
    }
}

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

future<file> sstable::new_sstable_component_file(const io_error_handler& error_handler, component_type type, open_flags flags, file_open_options options) const noexcept {
  try {
    auto f = _storage->open_component(*this, type, flags, options, _manager.config().enable_sstable_data_integrity_check());

    if (type != component_type::TOC && type != component_type::TemporaryTOC) {
        for (auto * ext : _manager.config().extensions().sstable_file_io_extensions()) {
            f = with_file_close_on_failure(std::move(f), [ext, this, type, flags] (file f) {
               return ext->wrap_file(const_cast<sstable&>(*this), type, f, flags).then([f](file nf) mutable {
                   return nf ? nf : std::move(f);
               });
            });
        }
    }

    f = with_file_close_on_failure(std::move(f), [&error_handler] (file f) {
        return make_checked_file(error_handler, std::move(f));
    });

    return f.handle_exception([this, type, &error_handler] (auto ep) {
        sstlog.error("Could not create SSTable component {}. Found exception: {}", filename(type), ep);
        try {
            error_handler(ep);
        } catch (...) {
            ep = std::current_exception();
        }
        return make_exception_future<file>(ep);
    });
  } catch (...) {
      return current_exception_as_future<file>();
  }
}

const std::unordered_map<sstable_version_types, sstring, enum_hash<sstable_version_types>> version_string = {
    { sstable_version_types::ka , "ka" },
    { sstable_version_types::la , "la" },
    { sstable_version_types::mc , "mc" },
    { sstable_version_types::md , "md" },
    { sstable_version_types::me , "me" },
};

const std::unordered_map<sstable_format_types, sstring, enum_hash<sstable_format_types>> format_string = {
    { sstable_format_types::big , "big" }
};

// This assumes that the mappings are small enough, and called unfrequent
// enough.  If that changes, it would be advisable to create a full static
// reverse mapping, even if it is done at runtime.
template <typename Map, std::equality_comparable_with<typename Map::mapped_type> Value>
static typename Map::key_type reverse_map(const Value& v, const Map& map) {
    for (auto& [key, value]: map) {
        if (value == v) {
            return key;
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
        d = std::bit_cast<double>(net::ntoh(nr));
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

template <typename Tag>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, utils::tagged_uuid<Tag>& id) {
    // Read directly into the tagged_uuid `id` member
    // This is ugly, but save an allocation or reimplementation
    // of parse(..., utils::UUID&)
    utils::UUID& uuid = *const_cast<utils::UUID*>(&id.uuid());
    return parse(s, v, in, uuid);
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
    for (auto count = len; count; count--) {
        arr.emplace_back();
        co_await parse(s, v, in, arr.back());
    }
}

template <typename Size, std::integral Members>
future<>
parse(const schema&, sstable_version_types, random_access_reader& in, Size& len, utils::chunked_vector<Members>& arr) {
    Size now = arr.max_chunk_capacity();
    for (auto count = len; count; count -= now) {
        if (now > count) {
            now = count;
        }
        auto buf = co_await in.read_exactly(now * sizeof(Members));
        check_buf_size(buf, now * sizeof(Members));
        for (size_t i = 0; i < now; ++i) {
            arr.push_back(net::ntoh(read_unaligned<Members>(buf.get() + i * sizeof(Members))));
        }
    }
}

template <typename Contents>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, std::optional<Contents>& opt) {
    bool engaged;
    co_await parse(s, v, in, engaged);
    if (engaged) {
        opt.emplace();
        co_await parse(s, v, in, *opt);
    } else {
        opt.reset();
    }
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, disk_array<Size, Members>& arr) {
    Size len;
    co_await parse(s, v, in, len);
    arr.elements.reserve(len);
    co_await parse(s, v, in, len, arr.elements);
}

template <typename Size, typename Key, typename Value>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, Size& len, std::unordered_map<Key, Value>& map) {
    for (auto count = len; count; count--) {
        Key key;
        Value value;
        co_await parse(s, v, in, key, value);
        map.emplace(key, value);
    }
}

template <typename First, typename Second>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, std::pair<First, Second>& p) {
    return parse(s, v, in, p.first, p.second);
}

template <typename Size, typename Key, typename Value>
future<> parse(const schema& s, sstable_version_types v, random_access_reader& in, disk_hash<Size, Key, Value>& h) {
    Size w;
    co_await parse(s, v, in, w);
    co_await parse(s, v, in, w, h.map);
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

    key_type nr_elements;
    co_await parse(schema, v, in, nr_elements);
    for ([[maybe_unused]] auto _ : boost::irange<key_type>(0, nr_elements)) {
        key_type new_key;
        unsigned new_size;
        co_await parse(schema, v, in, new_key);
        co_await parse(schema, v, in, new_size);
        value_type new_value;
        co_await disk_set::s_serdes.lookup_and_parse(schema, v, in, TagType(new_key), new_size, s, new_value);
    }
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
        s.entries.push_back(summary_entry{ token, key_data, position });
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
        return make_exception_future<summary_entry&>(std::out_of_range(format("Invalid Summary index: {:d}", i)));
    }

    return make_ready_future<summary_entry&>(_components->summary.entries[i]);
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
        co_await coroutine::return_exception(malformed_sstable_exception("Estimated histogram with zero size found. Can't continue!"));
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
        co_await coroutine::return_exception(malformed_sstable_exception("Streaming histogram with more entries than allowed. Can't continue!"));
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
    if (chunk_len == 0) {
        throw malformed_sstable_exception("CompressionInfo is malformed: zero chunk_len");
    }
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

static inline sstring parent_path(const sstring& fname) {
    return fs::canonical(fs::path(fname)).parent_path().string();
}

future<std::vector<sstring>> sstable::read_and_parse_toc(file f) {
    return with_closeable(make_file_input_stream(f), [] (input_stream<char>& in) -> future<std::vector<sstring>> {
        std::vector<sstring> components;
        auto all = co_await util::read_entire_stream_contiguous(in);
        boost::split(components, all, boost::is_any_of("\n"));
        co_return components;
    });
}

// This is small enough, and well-defined. Easier to just read it all
// at once
future<> sstable::read_toc() noexcept {
    if (_recognized_components.size()) {
        co_return;
    }

    try {
        co_await do_read_simple(component_type::TOC, [&] (version_types v, file f) -> future<> {
            auto comps = co_await read_and_parse_toc(f);
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
    } catch (std::system_error& e) {
        if (e.code() == std::error_code(ENOENT, std::system_category())) {
            throw malformed_sstable_exception(filename(component_type::TOC) + ": file not found");
        }
        throw;
    }
}

void sstable::generate_toc() {
    // Creating table of components.
    _recognized_components.insert(component_type::TOC);
    _recognized_components.insert(component_type::Statistics);
    _recognized_components.insert(component_type::Digest);
    _recognized_components.insert(component_type::Index);
    _recognized_components.insert(component_type::Summary);
    _recognized_components.insert(component_type::Data);
    if (_schema->bloom_filter_fp_chance() != 1.0) {
        _recognized_components.insert(component_type::Filter);
    }
    if (_schema->get_compressor_params().get_compressor() == nullptr) {
        _recognized_components.insert(component_type::CRC);
    } else {
        _recognized_components.insert(component_type::CompressionInfo);
    }
    _recognized_components.insert(component_type::Scylla);
}

future<std::unordered_map<component_type, file>> sstable::readable_file_for_all_components() const {
    std::unordered_map<component_type, file> files;
    for (auto c : _recognized_components) {
        files.emplace(c, co_await open_file(c, open_flags::ro));
    }
    co_return std::move(files);
}

future<entry_descriptor> sstable::clone(generation_type new_generation) const {
    co_await _storage->snapshot(*this, _storage->prefix(), storage::absolute_path::yes, new_generation);
    co_return entry_descriptor(new_generation, _version, _format, component_type::TOC, _state);
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
    // Writing into sstable component output stream should be done with care.
    // In particular -- flushing can happen only once right before closing
    // the stream. Flushing the stream in between several writes is not going
    // to work, because file stream would step on unaligned IO and S3 upload
    // stream would send completion message to the server and would lose any
    // subsequent write.
    SCYLLA_ASSERT(!_closed && "file_writer already closed");
    std::exception_ptr ex;
    try {
        _out.flush().get();
    } catch (...) {
        ex = std::current_exception();
    }
    try {
        _closed = true;
        _out.close().get();
    } catch (...) {
        auto e = std::current_exception();
        sstlog.error("Error while closing {}: {}", get_filename(), e);
        if (!ex) {
            ex = std::move(e);
        }
    }
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
}

const char* file_writer::get_filename() const noexcept {
    return _filename ? _filename->c_str() : "<anonymous output_stream>";
}

future<file_writer> sstable::make_component_file_writer(component_type c, file_output_stream_options options, open_flags oflags) noexcept {
    // Note: file_writer::make closes the file if file_writer creation fails
    // so we don't need to use with_file_close_on_failure here.
    return futurize_invoke([this, c] { return filename(c); }).then([this, c, options = std::move(options), oflags] (sstring filename) mutable {
        return _storage->make_component_sink(*this, c, oflags, std::move(options)).then([filename = std::move(filename)] (data_sink sink) mutable {
            return file_writer(output_stream<char>(std::move(sink)), std::move(filename));
        });
    });
}

void sstable::open_sstable(const sstring& origin) {
    _origin = origin;
    generate_toc();
    _storage->open(*this);
}

void sstable::write_toc(file_writer w) {
    sstlog.debug("Writing TOC file {} ", toc_filename());

    do_write_simple(std::move(w), [&] (version_types v, file_writer& w) {
        for (auto&& key : _recognized_components) {
            // new line character is appended to the end of each component name.
            auto value = sstable_version_constants::get_component_map(v).at(key) + "\n";
            bytes b = bytes(reinterpret_cast<const bytes::value_type *>(value.c_str()), value.size());
            write(v, w, b);
        }
    });
}

void sstable::write_crc(const checksum& c) {
    unsigned buffer_size = 4096;
    do_write_simple(component_type::CRC, [&] (version_types v, file_writer& w) {
        write(v, w, c);
    }, buffer_size);
}

// Digest file stores the full checksum of data file converted into a string.
void sstable::write_digest(uint32_t full_checksum) {
    unsigned buffer_size = 4096;
    do_write_simple(component_type::Digest, [&] (version_types v, file_writer& w) {
        auto digest = to_sstring<bytes>(full_checksum);
        write(v, w, digest);
    }, buffer_size);
}

thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_sample_pattern_cache;
thread_local std::array<std::vector<int>, downsampling::BASE_SAMPLING_LEVEL> downsampling::_original_index_cache;

future<> sstable::do_read_simple(component_type type,
                                 noncopyable_function<future<> (version_types, file&&, uint64_t sz)> read_component) {
    auto file_path = filename(type);
    sstlog.debug("Reading {} file {}", sstable_version_constants::get_component_map(_version).at(type), file_path);
    try {
        file fi = co_await new_sstable_component_file(_read_error_handler, type, open_flags::ro);
        uint64_t size = co_await fi.size();

        co_await read_component(_version, std::move(fi), size);
        _metadata_size_on_disk += size;
    }  catch (std::system_error& e) {
        if (e.code() == std::error_code(ENOENT, std::system_category())) {
            throw malformed_sstable_exception(file_path + ": file not found");
        }
        throw;
    } catch (malformed_sstable_exception& e) {
        throw malformed_sstable_exception(e.what(), file_path);
    }
}

future<> sstable::do_read_simple(component_type type,
                                 noncopyable_function<future<> (version_types, file)> read_component) {
    return do_read_simple(type, [read_component = std::move(read_component)] (version_types v, file&& f, uint64_t) -> future<> {
        std::exception_ptr ex;
        try {
            co_await read_component(v, f);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await f.close();

        maybe_rethrow_exception(std::move(ex));
    });
}

template <component_type Type, typename T>
future<> sstable::read_simple(T& component) {
    return do_read_simple(Type, [&] (version_types v, file&& f, uint64_t size) -> future<> {
        std::exception_ptr ex;
        auto r = file_random_access_reader(std::move(f), size, sstable_buffer_size);
        try {
            co_await parse(*_schema, v, r, component);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await r.close();

        maybe_rethrow_exception(std::move(ex));
    });
}

void sstable::do_write_simple(file_writer&& writer,
                              noncopyable_function<void (version_types, file_writer&)> write_component) {
    write_component(_version, writer);
    _metadata_size_on_disk += writer.offset();
    writer.close();
}

void sstable::do_write_simple(component_type type,
        noncopyable_function<void (version_types version, file_writer& writer)> write_component, unsigned buffer_size) {
    auto file_path = filename(type);
    sstlog.debug("Writing {} file {}", sstable_version_constants::get_component_map(_version).at(type), file_path);

    file_output_stream_options options;
    options.buffer_size = buffer_size;
    auto w = make_component_file_writer(type, std::move(options)).get();
    do_write_simple(std::move(w), std::move(write_component));
}

template <component_type Type, typename T>
void sstable::write_simple(const T& component) {
    do_write_simple(Type, [&component] (version_types v, file_writer& w) {
        write(v, w, component);
    }, sstable_buffer_size);
}

template future<> sstable::read_simple<component_type::Filter>(sstables::filter& f);
template void sstable::write_simple<component_type::Filter>(const sstables::filter& f);

template void sstable::write_simple<component_type::Summary>(const sstables::summary_ka&);

future<> sstable::read_compression() {
     // FIXME: If there is no compression, we should expect a CRC file to be present.
    if (!has_component(component_type::CompressionInfo)) {
        return make_ready_future<>();
    }

    return read_simple<component_type::CompressionInfo>(_components->compression);
}

void sstable::write_compression() {
    if (!has_component(component_type::CompressionInfo)) {
        return;
    }

    write_simple<component_type::CompressionInfo>(_components->compression);
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

void sstable::set_min_max_position_range() {
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

    _min_max_position_range = position_range(pip(min_elements, bound_kind::incl_start), pip(max_elements, bound_kind::incl_end));
}

future<std::optional<position_in_partition>>
sstable::find_first_position_in_partition(reader_permit permit, const dht::decorated_key& key, bool reversed) {
    using position_in_partition_opt = std::optional<position_in_partition>;
    class position_finder {
        position_in_partition_opt& _pos;
        bool _reversed;
    private:
        // If consuming in reversed mode, range_tombstone_change or clustering_row will have
        // its bound weight reversed, so we need to revert it here so the returned position
        // can be correctly used to mark the end bound.
        void on_position_found(position_in_partition&& pos, bool reverse_pos = false) {
            _pos = reverse_pos ? std::move(pos).reversed() : std::move(pos);
        }
    public:
        position_finder(position_in_partition_opt& pos, bool reversed) noexcept : _pos(pos), _reversed(reversed) {}

        void consume_new_partition(const dht::decorated_key& dk) {}

        stop_iteration consume(tombstone t) {
            // Handle case partition contains only a partition_tombstone, so position_in_partition
            // for this key should be before all rows.
            on_position_found(position_in_partition::before_all_clustered_rows());
            return stop_iteration::no;
        }

        stop_iteration consume(range_tombstone_change&& rt) {
            on_position_found(std::move(std::move(rt)).position(), _reversed);
            return stop_iteration::yes;
        }

        stop_iteration consume(clustering_row&& cr) {
            on_position_found(position_in_partition::for_key(std::move(cr.key())), _reversed);
            return stop_iteration::yes;
        }

        stop_iteration consume(static_row&& sr) {
            on_position_found(position_in_partition(sr.position()));
            // If reversed == true, we shouldn't stop at static row as we want to find the last row.
            // We don't want to ignore its position, to handle the case where partition contains only static row.
            return stop_iteration(!_reversed);
        }

        stop_iteration consume_end_of_partition() {
            // Handle case where partition has no rows.
            if (!_pos) {
                on_position_found(_reversed ? position_in_partition::after_all_clustered_rows() : position_in_partition::before_all_clustered_rows());
            }
            return stop_iteration::yes;
        }

        mutation_opt consume_end_of_stream() {
            return std::nullopt;
        }
    };

    auto pr = dht::partition_range::make_singular(key);
    auto s = get_schema();
    auto full_slice = s->full_slice();
    if (reversed) {
        s = s->make_reversed();
        full_slice.options.set(query::partition_slice::option::reversed);
    }
    auto r = make_reader(s, std::move(permit), pr, full_slice, {}, streamed_mutation::forwarding::no,
                         mutation_reader::forwarding::no /* to avoid reading past the partition end */);

    position_in_partition_opt ret = std::nullopt;
    position_finder finder(ret, reversed);
    std::exception_ptr ex;
    try {
        co_await r.consume(finder);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await r.close();
    if (ex) {
        co_await coroutine::exception(std::move(ex));
    }
    co_return std::move(ret);
}

future<> sstable::load_first_and_last_position_in_partition() {
    if (!_schema->clustering_key_size()) {
        co_return;
    }

    auto& sem = _manager.sstable_metadata_concurrency_sem();
    reader_permit permit = co_await sem.obtain_permit(_schema, "sstable::load_first_and_last_position_range", sstable_buffer_size, db::no_timeout, {});
    auto first_pos_opt = co_await find_first_position_in_partition(permit, get_first_decorated_key(), false);
    auto last_pos_opt = co_await find_first_position_in_partition(permit, get_last_decorated_key(), true);

    // Allow loading to proceed even if we were unable to load this metadata as the lack of it
    // will not affect correctness.
    if (!first_pos_opt || !last_pos_opt) {
        sstlog.warn("Unable to retrieve metadata for first and last keys of {}. Not a critical error.", get_filename());
        co_return;
    }

    _first_partition_first_position = std::move(*first_pos_opt);
    _last_partition_last_position = std::move(*last_pos_opt);
}

double sstable::estimate_droppable_tombstone_ratio(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const {
    auto gc_before = get_gc_before_for_drop_estimation(compaction_time, gc_state, s);

    auto& st = get_stats_metadata();
    auto estimated_count = st.estimated_cells_count.mean() * st.estimated_cells_count.count();
    if (estimated_count > 0) {
        double droppable = st.estimated_tombstone_drop_time.sum(gc_before.time_since_epoch().count());
        return droppable / estimated_count;
    }
    return 0.0f;
}

future<> sstable::read_statistics() {
    return read_simple<component_type::Statistics>(_components->statistics);
}

void sstable::write_statistics() {
    write_simple<component_type::Statistics>(_components->statistics);
}

void sstable::rewrite_statistics() {
    auto file_path = filename(component_type::TemporaryStatistics);
    sstlog.debug("Rewriting statistics component of sstable {}", get_filename());

    file_output_stream_options options;
    options.buffer_size = sstable_buffer_size;
    auto w = make_component_file_writer(component_type::TemporaryStatistics, std::move(options),
            open_flags::wo | open_flags::create | open_flags::truncate).get();
    write(_version, w, _components->statistics);
    w.close();
    // rename() guarantees atomicity when renaming a file into place.
    sstable_write_io_check(rename_file, file_path, filename(component_type::Statistics)).get();
}

future<> sstable::read_summary() noexcept {
    if (_components->summary) {
        return make_ready_future<>();
    }

    return read_toc().then([this] {
        // We'll try to keep the main code path exception free, but if an exception does happen
        // we can try to regenerate the Summary.
        if (has_component(component_type::Summary)) {
            return read_simple<component_type::Summary>(_components->summary).handle_exception([this] (auto ep) {
                sstlog.warn("Couldn't read summary file {}: {}. Recreating it.", this->filename(component_type::Summary), ep);
                return this->generate_summary();
            });
        } else {
            return generate_summary();
        }
    });
}

future<file> sstable::open_file(component_type type, open_flags flags, file_open_options opts) const noexcept {
    return new_sstable_component_file(_read_error_handler, type, flags, opts);
}

future<> sstable::open_or_create_data(open_flags oflags, file_open_options options) noexcept {
    return when_all_succeed(
        open_file(component_type::Index, oflags, options).then([this] (file f) { _index_file = std::move(f); }),
        open_file(component_type::Data, oflags, options).then([this] (file f) { _data_file = std::move(f); })
    ).discard_result();
}

future<> sstable::open_data(sstable_open_config cfg) noexcept {
    co_await open_or_create_data(open_flags::ro);
    co_await update_info_for_opened_data(cfg);
    SCYLLA_ASSERT(!_shards.empty());
    auto* sm = _components->scylla_metadata->data.get<scylla_metadata_type::Sharding, sharding_metadata>();
    if (sm) {
        // Sharding information uses a lot of memory and once we're doing with this computation we will no longer use it.
        co_await utils::clear_gently(sm->token_ranges.elements);
        sm->token_ranges.elements = {};
    }
    auto* ld_stats = _components->scylla_metadata->data.get<scylla_metadata_type::LargeDataStats, scylla_metadata::large_data_stats>();
    if (ld_stats) {
        _large_data_stats.emplace(*ld_stats);
    }
    auto* origin = _components->scylla_metadata->data.get<scylla_metadata_type::SSTableOrigin, scylla_metadata::sstable_origin>();
    if (origin) {
        _origin = sstring(to_sstring_view(bytes_view(origin->value)));
    }
    _open_mode.emplace(open_flags::ro);
    _stats.on_open_for_reading();

    _total_reclaimable_memory.reset();
    _manager.increment_total_reclaimable_memory_and_maybe_reclaim(this);
}

future<> sstable::update_info_for_opened_data(sstable_open_config cfg) {
    struct stat st = co_await _data_file.stat();

    if (this->has_component(component_type::CompressionInfo)) {
        _components->compression.update(st.st_size);
    }
    _data_file_size = st.st_size;
    _data_file_write_time = db_clock::from_time_t(st.st_mtime);

    auto size = co_await _index_file.size();
    _index_file_size = size;
    SCYLLA_ASSERT(!_cached_index_file);
    _cached_index_file = seastar::make_shared<cached_file>(_index_file,
                                                            _manager.get_cache_tracker().get_index_cached_file_stats(),
                                                            _manager.get_cache_tracker().get_lru(),
                                                            _manager.get_cache_tracker().region(),
                                                            _index_file_size);
    _index_file = make_cached_seastar_file(*_cached_index_file);

    this->set_min_max_position_range();
    this->set_first_and_last_keys();
    _run_identifier = _components->scylla_metadata->get_optional_run_identifier().value_or(run_id::create_random_id());
    if (cfg.load_first_and_last_position_metadata) {
        co_await load_first_and_last_position_in_partition();
    }
}

future<> sstable::create_data() noexcept {
    auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
    file_open_options opt;
    opt.extent_allocation_size_hint = 32 << 20;
    opt.sloppy_size = true;
    return open_or_create_data(oflags, std::move(opt)).then([this, oflags] {
        _open_mode.emplace(oflags);
    });
}

future<> sstable::drop_caches() {
    co_await _cached_index_file->evict_gently();
    co_await _index_cache->evict_gently();
}

// Return the filter format for the given sstable version
static inline utils::filter_format get_filter_format(sstable_version_types version) {
    return (version >= sstable_version_types::mc)
               ? utils::filter_format::m_format
               : utils::filter_format::k_l_format;
}

future<> sstable::read_filter(sstable_open_config cfg) {
    if (!cfg.load_bloom_filter || !has_component(component_type::Filter)) {
        _components->filter = std::make_unique<utils::filter::always_present_filter>();
        return make_ready_future<>();
    }

    return seastar::async([this] () mutable {
        sstables::filter filter;
        read_simple<component_type::Filter>(filter).get();
        auto nr_bits = filter.buckets.elements.size() * std::numeric_limits<typename decltype(filter.buckets.elements)::value_type>::digits;
        large_bitset bs(nr_bits, std::move(filter.buckets.elements));
        _components->filter = utils::filter::create_filter(filter.hashes, std::move(bs), get_filter_format(_version));
    });
}

void sstable::write_filter() {
    if (!has_component(component_type::Filter)) {
        return;
    }

    auto f = downcast_ptr<utils::filter::murmur3_bloom_filter>(_components->filter.get());

    auto&& bs = f->bits();
    auto filter_ref = sstables::filter_ref(f->num_hashes(), bs.get_storage());
    write_simple<component_type::Filter>(filter_ref);
}

void sstable::maybe_rebuild_filter_from_index(uint64_t num_partitions) {
    if (!has_component(component_type::Filter)) {
        return;
    }

    // Skip rebuilding the bloom filter if the false positive rate based
    // on the current bitset size is within 75% to 125% of the configured
    // false positive rate.
    auto curr_bitset_size = downcast_ptr<utils::filter::bloom_filter>(_components->filter.get())->bits().memory_size();
    auto bitset_size_lower_bound = utils::i_filter::get_filter_size(num_partitions,
                                                                    _schema->bloom_filter_fp_chance() * 1.25);
    auto bitset_size_upper_bound = utils::i_filter::get_filter_size(num_partitions,
                                                                    _schema->bloom_filter_fp_chance() * 0.75);
    if (bitset_size_lower_bound <= curr_bitset_size && curr_bitset_size <= bitset_size_upper_bound) {
        return;
    }

    // Consumer that adds the keys from index entries to the given bloom filter
    class bloom_filter_builder {
        utils::filter_ptr& _filter;
    public:
        bloom_filter_builder(utils::filter_ptr &filter) : _filter(filter) {}
        void consume_entry(parsed_partition_index_entry&& e) {
            _filter->add(to_bytes_view(e.key));
        }
    };

    // Create a new filter that can optimally represent the given num_partitions.
    auto optimal_filter = utils::i_filter::get_filter(num_partitions, _schema->bloom_filter_fp_chance(), get_filter_format(_version));
    sstlog.info("Rebuilding bloom filter {}: resizing bitset from {} bytes to {} bytes. sstable origin: {}", filename(component_type::Filter), curr_bitset_size,
                downcast_ptr<utils::filter::bloom_filter>(optimal_filter.get())->bits().memory_size(), _origin);

    auto index_file = open_file(component_type::Index, open_flags::ro).get();
    auto index_file_closer = deferred_action([&index_file] {
        try {
            index_file.close().get();
        } catch (...) {
            sstlog.warn("sstable close index_file failed: {}", std::current_exception());
            general_disk_error();
        }
    });
    auto index_file_size = index_file.size().get();

    auto sem = reader_concurrency_semaphore(reader_concurrency_semaphore::no_limits{}, "sstable::rebuild_filter_from_index",
            reader_concurrency_semaphore::register_metrics::no);
    auto sem_stopper = deferred_stop(sem);

    // rebuild the filter using index_consume_entry_context
    bloom_filter_builder bfb_consumer(optimal_filter);
    index_consume_entry_context<bloom_filter_builder> consumer_ctx(
            *this, sem.make_tracking_only_permit(_schema, "rebuild_filter_from_index", db::no_timeout, {}), bfb_consumer, trust_promoted_index::no,
            make_file_input_stream(index_file, 0, index_file_size, {.buffer_size = sstable_buffer_size}), 0, index_file_size,
            (_version >= sstable_version_types::mc
                ? std::make_optional(get_clustering_values_fixed_lengths(get_serialization_header()))
                : std::optional<column_values_fixed_lengths>{}), _manager._abort);
    auto consumer_ctx_closer = deferred_close(consumer_ctx);
    try {
        consumer_ctx.consume_input().get();
    } catch (...) {
        sstlog.warn("Failed to rebuild bloom filter {} : {}. Existing bloom filter will be written to disk.", filename(component_type::Filter), std::current_exception());
        return;
    }

    // Replace the existing filter with the new optimal filter.
    _components->filter.swap(optimal_filter);
}

size_t sstable::total_reclaimable_memory_size() const {
    if (!_total_reclaimable_memory) {
        _total_reclaimable_memory = _components->filter ? _components->filter->memory_size() : 0;
    }

    return _total_reclaimable_memory.value();
}

size_t sstable::reclaim_memory_from_components() {
    size_t memory_reclaimed_this_iteration = 0;

    if (_components->filter) {
        auto filter_memory_size = _components->filter->memory_size();
        if (filter_memory_size > 0) {
            // Discard it from memory by replacing it with an always present variant.
            // No need to remove it from _recognized_components as the filter is still in disk.
            _components->filter = std::make_unique<utils::filter::always_present_filter>();
            memory_reclaimed_this_iteration += filter_memory_size;
        }
    }

    _total_reclaimable_memory.reset();
    _total_memory_reclaimed += memory_reclaimed_this_iteration;
    return memory_reclaimed_this_iteration;
}

size_t sstable::total_memory_reclaimed() const {
    return _total_memory_reclaimed;
}

future<> sstable::reload_reclaimed_components() {
    if (_total_memory_reclaimed == 0) {
        // nothing to reload
        co_return;
    }

    co_await utils::get_local_injector().inject("reload_reclaimed_components/pause", [] (auto& handler) {
        sstlog.info("reload_reclaimed_components/pause init");
        auto ret = handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::seconds{5});
        sstlog.info("reload_reclaimed_components/pause done");
        return ret;
    });

    co_await read_filter();
    _total_reclaimable_memory.reset();
    _total_memory_reclaimed -= _components->filter->memory_size();
    sstlog.info("Reloaded bloom filter of {}", get_filename());
}

future<> sstable::load_metadata(sstable_open_config cfg, bool validate) noexcept {
    co_await read_toc();
    // read scylla-meta after toc. Might need it to parse
    // rest (hint extensions)
    co_await read_scylla_metadata();
    // Read statistics ahead of others - if summary is missing
    // we'll attempt to re-generate it and we need statistics for that
    co_await read_statistics();
    co_await coroutine::all(
            [&] { return read_compression(); },
            [&] { return read_filter(cfg); },
            [&] { return read_summary(); });
    if (validate) {
        validate_min_max_metadata();
        validate_max_local_deletion_time();
        validate_partitioner();
    }
}

// This interface is only used during tests, snapshot loading and early initialization.
// No need to set tunable priorities for it.
future<> sstable::load(const dht::sharder& sharder, sstable_open_config cfg) noexcept {
    co_await load_metadata(cfg, true);
    if (_shards.empty()) {
        set_first_and_last_keys();
        _shards = cfg.current_shard_as_sstable_owner ?
                std::vector<unsigned>{this_shard_id()} : compute_shards_for_this_sstable(sharder);
    }
    co_await open_data(cfg);
}

future<> sstable::load(sstables::foreign_sstable_open_info info) noexcept {
    static_assert(std::is_nothrow_move_constructible_v<sstables::foreign_sstable_open_info>);
    return read_toc().then([this, info = std::move(info)] () mutable {
        _components = std::move(info.components);
        _data_file = make_checked_file(_read_error_handler, info.data.to_file());
        _index_file = make_checked_file(_read_error_handler, info.index.to_file());
        _shards = std::move(info.owners);
        _metadata_size_on_disk = info.metadata_size_on_disk;
        validate_min_max_metadata();
        validate_max_local_deletion_time();
        validate_partitioner();
        return update_info_for_opened_data().then([this]() {
            _total_reclaimable_memory.reset();
            _manager.increment_total_reclaimable_memory_and_maybe_reclaim(this);
        });
    });
}

future<foreign_sstable_open_info> sstable::get_open_info() & {
    return _components.copy().then([this] (auto c) mutable {
        return foreign_sstable_open_info{std::move(c), this->get_shards_for_this_sstable(), _data_file.dup(), _index_file.dup(),
            _generation, _version, _format, data_size(), _metadata_size_on_disk};
    });
}

entry_descriptor sstable::get_descriptor(component_type c) const {
    return entry_descriptor(_generation, _version, _format, c);
}

future<>
sstable::load_owner_shards(const dht::sharder& sharder) {
    if (!_shards.empty()) {
        sstlog.trace("{}: shards={}", get_filename(), _shards);
        co_return;
    }
    co_await read_scylla_metadata();

    auto has_valid_sharding_metadata = std::invoke([this] {
        if (!has_component(component_type::Scylla)) {
            return false;
        }

        auto& scylla_metadata = _components->scylla_metadata;
        const auto* sm = scylla_metadata
                ? scylla_metadata->data.get<scylla_metadata_type::Sharding, sharding_metadata>()
                : nullptr;
        return sm && sm->token_ranges.elements.size();
    });
    // Statistics is needed for SSTable loading validation and possible Summary regeneration.
    co_await read_statistics();

    // If sharding metadata is not available, we must load first and last keys from summary
    // for sstable::compute_shards_for_this_sstable() to operate on them.
    if (!has_valid_sharding_metadata) {
        sstlog.warn("Sharding metadata not available for {}, so Summary will be read to allow Scylla to compute shards owning the SSTable.", get_filename());
        co_await read_summary();
        set_first_and_last_keys();
    }

    _shards = compute_shards_for_this_sstable(sharder);
    sstlog.trace("{}: shards={}", get_filename(), _shards);
}

void prepare_summary(summary& s, uint64_t expected_partition_count, uint32_t min_index_interval) {
    SCYLLA_ASSERT(expected_partition_count >= 1);

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

    SCYLLA_ASSERT(first_key); // assume non-empty sstable
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
create_sharding_metadata(utils::chunked_vector<dht::partition_range> ranges) {
    auto sm = sharding_metadata();
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
                {left_exclusive, {left_token.data()}},
                {right_exclusive, {right_token.data()}}});
        }
    }
    return sm;
}

static
sharding_metadata
create_sharding_metadata(schema_ptr schema, const dht::static_sharder& sharder, const dht::decorated_key& first_key, const dht::decorated_key& last_key, shard_id shard) {
    auto prange = dht::partition_range::make(dht::ring_position(first_key), dht::ring_position(last_key));
    auto ranges = dht::split_range_to_single_shard(*schema, sharder, prange, shard).get();
    if (ranges.empty()) {
        auto split_ranges_all_shards = dht::split_range_to_shards(prange, *schema, sharder);
        sstlog.warn("create_sharding_metadata: range={} has no intersection with shard={} first_key={} last_key={} ranges_single_shard={} ranges_all_shards={}",
                    prange, shard, first_key, last_key, ranges, split_ranges_all_shards);
    }
    return create_sharding_metadata(std::move(ranges));
}

static
sharding_metadata
create_sharding_metadata(schema_ptr schema, const dht::decorated_key& first_key, const dht::decorated_key& last_key, shard_id shard) {
    auto* sharder_opt = schema->try_get_static_sharder();
    if (sharder_opt) {
        return create_sharding_metadata(std::move(schema), *sharder_opt, first_key, last_key, shard);
    }
    utils::chunked_vector<dht::partition_range> r;
    r.push_back(dht::partition_range::make(dht::ring_position(first_key), dht::ring_position(last_key)));
    return create_sharding_metadata(std::move(r));
}

// In the beginning of the statistics file, there is a disk_hash used to
// map each metadata type to its correspondent position in the file.
void seal_statistics(sstable_version_types v, statistics& s, metadata_collector& collector,
        const sstring partitioner, double bloom_filter_fp_chance, schema_ptr schema,
        const dht::decorated_key& first_key, const dht::decorated_key& last_key,
        const encoding_stats& enc_stats, const std::set<int>& compaction_ancestors) {
    validation_metadata validation;
    compaction_metadata compaction;
    stats_metadata stats;

    validation.partitioner.value = to_bytes(partitioner);
    validation.filter_chance = bloom_filter_fp_chance;
    s.contents[metadata_type::Validation] = std::make_unique<validation_metadata>(std::move(validation));

    collector.construct_compaction(compaction);
    if (v < sstable_version_types::mc && !compaction_ancestors.empty()) {
        compaction.ancestors.elements = utils::chunked_vector<uint32_t>(compaction_ancestors.begin(), compaction_ancestors.end());
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
        auto key_data = s.add_summary_data(key);
        s.entries.push_back(summary_entry{ token, key_data, index_offset });
    }
}

// Returns the cost for writing a byte to summary such that the ratio of summary
// to data will be 1 to cost by the time sstable is sealed.
size_t summary_byte_cost(double summary_ratio) {
    return summary_ratio ? (1 / summary_ratio) : index_sampling_state::default_summary_byte_cost;
}

future<>
sstable::read_scylla_metadata() noexcept {
    if (_components->scylla_metadata) {
        return make_ready_future<>();
    }
    return read_toc().then([this] {
        _components->scylla_metadata.emplace();  // engaged optional means we won't try to re-read this again
        if (!has_component(component_type::Scylla)) {
            return make_ready_future<>();
        }
        return read_simple<component_type::Scylla>(*_components->scylla_metadata);
    });
}

void
sstable::write_scylla_metadata(shard_id shard, sstable_enabled_features features, struct run_identifier identifier,
        std::optional<scylla_metadata::large_data_stats> ld_stats) {
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
    if (!_origin.empty()) {
        scylla_metadata::sstable_origin o;
        o.value = bytes(to_bytes_view(sstring_view(_origin)));
        _components->scylla_metadata->data.set<scylla_metadata_type::SSTableOrigin>(std::move(o));
    }

    scylla_metadata::scylla_version version;
    version.value = bytes(to_bytes_view(sstring_view(scylla_version())));
    _components->scylla_metadata->data.set<scylla_metadata_type::ScyllaVersion>(std::move(version));
    scylla_metadata::scylla_build_id build_id;
    build_id.value = bytes(to_bytes_view(sstring_view(get_build_id())));
    _components->scylla_metadata->data.set<scylla_metadata_type::ScyllaBuildId>(std::move(build_id));

    write_simple<component_type::Scylla>(*_components->scylla_metadata);
}

bool sstable::may_contain_rows(const query::clustering_row_ranges& ranges) const {
    if (_version < sstables::sstable_version_types::md) {
        return true;
    }

    // Include sstables with tombstones that are not scylla's since
    // they may contain partition tombstones that are not taken into
    // account in min/max column names metadata.
    // We clear min/max metadata for partition tombstones so they
    // will match as containing the rows we're looking for.
    if (!has_scylla_component()) {
        if (get_stats_metadata().estimated_tombstone_drop_time.bin.size()) {
            return true;
        }
    }

    return std::ranges::any_of(ranges, [this] (const query::clustering_range& range) {
        return _min_max_position_range.overlaps(*_schema,
            position_in_partition_view::for_range_start(range),
            position_in_partition_view::for_range_end(range));
    });
}

future<> sstable::seal_sstable(bool backup)
{
    return _storage->seal(*this).then([this, backup] {
        if (_marked_for_deletion == mark_for_deletion::implicit) {
            _marked_for_deletion = mark_for_deletion::none;
        }
        if (backup) {
            return _storage->snapshot(*this, "backups", storage::absolute_path::no);
        }
        return make_ready_future<>();
    });
}

sstable_writer sstable::get_writer(const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, shard_id shard)
{
    // Mark sstable for implicit deletion if destructed before it is sealed.
    _marked_for_deletion = mark_for_deletion::implicit;
    return sstable_writer(*this, s, estimated_partitions, cfg, enc_stats, shard);
}

future<uint64_t> sstable::validate(reader_permit permit, abort_source& abort,
        std::function<void(sstring)> error_handler, sstables::read_monitor& monitor) {
    if (_version >= sstable_version_types::mc) {
        co_return co_await mx::validate(shared_from_this(), std::move(permit), abort, std::move(error_handler), monitor);
    }

    auto reader = make_crawling_reader(_schema, permit, nullptr, monitor);

    uint64_t errors = 0;
    std::exception_ptr ex;

    try {
        auto validator = mutation_fragment_stream_validator(*_schema);

        while (auto mf_opt = co_await reader()) {
            if (abort.abort_requested()) [[unlikely]] {
                break;
            }

            const auto& mf = *mf_opt;

            if (auto res = validator(mf); !res) {
                error_handler(res.what());
                validator.reset(mf);
                ++errors;
            }

            if (mf.is_partition_start()) {
                const auto& ps = mf.as_partition_start();
                if (auto res = validator(ps.key()); !res) {
                    error_handler(res.what());
                    validator.reset(ps.key());
                    ++errors;
                }
            }
        }
        if (auto res = validator.on_end_of_stream(); !res) {
            error_handler(res.what());
            ++errors;
        }
    } catch (const malformed_sstable_exception& e) {
        try {
            error_handler(format("unrecoverable error: {}", e));
            ++errors;
        } catch (...) {
            ex = std::current_exception();
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await reader.close();

    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }

    co_return errors;
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
        mutation_reader mr,
        uint64_t estimated_partitions,
        schema_ptr schema,
        const sstable_writer_config& cfg,
        encoding_stats stats) {
    assert_large_data_handler_is_running();
    return seastar::async([this, mr = std::move(mr), estimated_partitions, schema = std::move(schema), cfg, stats] () mutable {
        auto close_mr = deferred_close(mr);
        auto wr = get_writer(*schema, estimated_partitions, cfg, stats);
        mr.consume_in_thread(std::move(wr));
    }).finally([this] {
        assert_large_data_handler_is_running();
    });
}

future<> sstable::generate_summary() {
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
    auto sem = reader_concurrency_semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::generate_summary()",
            reader_concurrency_semaphore::register_metrics::no);

    std::exception_ptr ex;

    try {
        auto index_size = co_await index_file.size();
        // an upper bound. Surely to be less than this.
        auto estimated_partitions = std::max<uint64_t>(index_size / sizeof(uint64_t), 1);
        prepare_summary(_components->summary, estimated_partitions, _schema->min_index_interval());

        file_input_stream_options options;
        options.buffer_size = sstable_buffer_size;

        auto s = summary_generator(_schema->get_partitioner(), _components->summary, _manager.config().sstable_summary_ratio());
            auto ctx = make_lw_shared<index_consume_entry_context<summary_generator>>(
                    *this, sem.make_tracking_only_permit(_schema, "generate-summary", db::no_timeout, {}), s, trust_promoted_index::yes,
                    make_file_input_stream(index_file, 0, index_size, std::move(options)), 0, index_size,
                    (_version >= sstable_version_types::mc
                        ? std::make_optional(get_clustering_values_fixed_lengths(get_serialization_header()))
                        : std::optional<column_values_fixed_lengths>{}), _manager._abort);

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
    if (!_metadata_size_on_disk) {
        on_internal_error(sstlog, "On-disk size of sstable metadata was not set");
    }
    if (!_data_file_size) {
        on_internal_error(sstlog, "On-disk size of sstable data was not set");
    }
    if (!_index_file_size) {
        on_internal_error(sstlog, "On-disk size of sstable index was not set");
    }
    return _metadata_size_on_disk + _data_file_size + _index_file_size;
}

uint64_t sstable::filter_size() const {
    return _components->filter->memory_size();
}

bool sstable::has_component(component_type f) const {
    return _recognized_components.contains(f);
}

std::optional<bool> sstable::originated_on_this_node() const {
    if (_version < version_types::me) {
        // earlier formats do not store originating host id
        return std::nullopt;
    }

    auto originating_host_id = get_stats_metadata().originating_host_id;
    if (!originating_host_id) {
        // Scylla always fills in originating host id when writing
        // sstables, so an ME-and-up sstable that does not have it is
        // invalid
        on_internal_error(sstlog, format("No originating host id in SSTable: {}. Load foreign SSTables via the upload dir instead.", get_filename()));
    }

    auto local_host_id = _manager.get_local_host_id();
    if (!local_host_id) {
        // we don't know the local host id before it is loaded from
        // (or generated and written to) system.local, but some system
        // sstable reads must happen before the bootstrap process gets
        // there (like in the resharding case)
        return std::nullopt;
    }

    return *originating_host_id == local_host_id;
}

void sstable::validate_originating_host_id() const {
    if (_version < version_types::me) {
        // earlier formats do not store originating host id
        return;
    }

    auto originating_host_id = get_stats_metadata().originating_host_id;
    if (!originating_host_id) {
        // Scylla always fills in originating host id when writing
        // sstables, so an ME-and-up sstable that does not have it is
        // invalid
        throw std::runtime_error(format("No originating host id in SSTable: {}. Load foreign SSTables via the upload dir instead.", get_filename()));
    }

    auto local_host_id = _manager.get_local_host_id();
    if (!local_host_id) {
        // we don't know the local host id before it is loaded from
        // (or generated and written to) system.local, but some system
        // sstable reads must happen before the bootstrap process gets
        // there, if that's not the case, it's a sign of bug.
        auto msg = format("Unknown local host id while validating SSTable: {}", get_filename());
        if (is_system_keyspace(_schema->ks_name())) {
            sstlog.trace("{}", msg);
        } else {
            on_internal_error(sstlog, msg);
        }
        return;
    }

    if (*originating_host_id != local_host_id) {
        // FIXME refrain from throwing an exception because of #10148
        sstlog.warn("Host id {} does not match local host id {} while validating SSTable: {}. Load foreign SSTables via the upload dir instead.", *originating_host_id, local_host_id, get_filename());
    }
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

sstring sstable::component_basename(const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                                    format_types format, sstring component) {
    sstring v = fmt::to_string(version);
    sstring g = to_sstring(generation);
    sstring f = fmt::to_string(format);
    switch (version) {
    case sstable::version_types::ka:
        return ks + "-" + cf + "-" + v + "-" + g + "-" + component;
    case sstable::version_types::la:
        return v + "-" + g + "-" + f + "-" + component;
    case sstable::version_types::mc:
    case sstable::version_types::md:
    case sstable::version_types::me:
        return v + "-" + g + "-" + f + "-" + component;
    }
    SCYLLA_ASSERT(0 && "invalid version");
}

sstring sstable::component_basename(const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                          format_types format, component_type component) {
    return component_basename(ks, cf, version, generation, format,
            sstable_version_constants::get_component_map(version).at(component));
}

sstring sstable::filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                          format_types format, component_type component) {
    return dir + "/" + component_basename(ks, cf, version, generation, format, component);
}

sstring sstable::filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, generation_type generation,
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

future<> sstable::snapshot(const sstring& dir) const {
    return _storage->snapshot(*this, dir, storage::absolute_path::yes);
}

future<> sstable::change_state(sstable_state to, delayed_commit_changes* delay_commit) {
    co_await _storage->change_state(*this, to, _generation, delay_commit);
    _state = to;
}

future<> sstable::pick_up_from_upload(sstable_state to, generation_type new_generation) {
    co_await _storage->change_state(*this, to, new_generation, nullptr);
    _generation = std::move(new_generation);
    _state = to;
}

future<> delayed_commit_changes::commit() {
    return parallel_for_each(_dirs, [] (sstring dir) {
        return sync_directory(dir);
    });
}

mutation_reader
sstable::make_reader(
        schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& mon) {
    const auto reversed = slice.is_reversed();
    if (_version >= version_types::mc && (!reversed || range.is_singular())) {
        return mx::make_reader(shared_from_this(), std::move(query_schema), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr, mon);
    }

    // Multi-partition reversed queries are not yet supported natively in the mx reader.
    // Therefore in this case we use `make_reversing_reader` over the forward reader.
    // FIXME: remove this workaround eventually.
    auto max_result_size = permit.max_result_size();

    if (_version >= version_types::mc) {
        // The only mx case falling through here is reversed multi-partition reader
        auto rd = make_reversing_reader(mx::make_reader(shared_from_this(), query_schema->make_reversed(), std::move(permit),
                range, reverse_slice(*query_schema, slice), std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr, mon),
            max_result_size);
        if (fwd) {
            rd = make_forwardable(std::move(rd));
        }
        return rd;
    }

    if (reversed) {
        // The kl reader does not support reversed queries at all.
        // Perform a forward query on it, then reverse the result.
        auto rd = make_reversing_reader(kl::make_reader(shared_from_this(), query_schema->make_reversed(), std::move(permit),
                    range, reverse_slice(*query_schema, slice), std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr, mon), max_result_size);
        if (fwd) {
            rd = make_forwardable(std::move(rd));
        }
        return rd;
    }

    return kl::make_reader(shared_from_this(), query_schema, std::move(permit),
                range, slice, std::move(trace_state), fwd, fwd_mr, mon);
}

mutation_reader
sstable::make_crawling_reader(
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor) {
    if (_version >= version_types::mc) {
        return mx::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), std::move(trace_state), monitor);
    }
    return kl::make_crawling_reader(shared_from_this(), std::move(schema), std::move(permit), std::move(trace_state), monitor);
}

static std::tuple<entry_descriptor, sstring, sstring> make_entry_descriptor(const std::filesystem::path& sst_path, sstring* const provided_ks, sstring* const provided_cf) {
    // examples of fname look like
    //   la-42-big-Data.db
    //   ka-42-big-Data.db
    //   me-3g8w_00qf_4pbog2i7h2c7am0uoe-big-Data.db
    static boost::regex la_mx("(la|m[cde])-([^-]+)-(\\w+)-(.*)");
    static boost::regex ka("(\\w+)-(\\w+)-ka-(\\d+)-(.*)");

    static boost::regex dir(format(".*/([^/]*)/([^/]+)-[\\da-fA-F]+(?:/({}|{}|{}|{})(?:/[^/]+)?)?/?",
            sstables::staging_dir, sstables::quarantine_dir, sstables::upload_dir, sstables::snapshots_dir).c_str());

    boost::smatch match;

    sstable::version_types version;

    const auto ks_cf_provided = provided_ks && provided_cf;

    sstring generation;
    sstring format;
    sstring component;
    sstring ks;
    sstring cf;
    if (ks_cf_provided) {
        ks = std::move(*provided_ks);
        cf = std::move(*provided_cf);
    }

    const auto sstdir = sst_path.parent_path();
    const auto fname = sst_path.filename();
    sstlog.debug("Make descriptor sstdir: {}; fname: {}", sstdir, fname);
    std::string s(fname);
    if (boost::regex_match(s, match, la_mx)) {
        std::string sdir(sstdir);
        boost::smatch dirmatch;
        if (!ks_cf_provided) {
            if (boost::regex_match(sdir, dirmatch, dir)) {
                ks = dirmatch[1].str();
                cf = dirmatch[2].str();
            } else {
                throw malformed_sstable_exception(seastar::format("invalid path for file {}: {}. Path doesn't match known pattern.", fname, sstdir));
            }
        }
        version = version_from_string(match[1].str());
        generation = match[2].str();
        format = sstring(match[3].str());
        component = sstring(match[4].str());
    } else if (boost::regex_match(s, match, ka)) {
        if (!ks_cf_provided) {
            ks = match[1].str();
            cf = match[2].str();
        }
        version = sstable::version_types::ka;
        format = sstring("big");
        generation = match[3].str();
        component = sstring(match[4].str());
    } else {
        throw malformed_sstable_exception(seastar::format("invalid version for file {}. Name doesn't match any known version.", fname));
    }
    return std::make_tuple(entry_descriptor(generation_type::from_string(generation), version, format_from_string(format), sstable::component_from_sstring(version, component)), ks, cf);
}

std::tuple<entry_descriptor, sstring, sstring> parse_path(const std::filesystem::path& sst_path) {
    return make_entry_descriptor(sst_path, nullptr, nullptr);
}

entry_descriptor parse_path(const std::filesystem::path& sst_path, sstring ks, sstring cf) {
    auto full = make_entry_descriptor(sst_path, &ks, &cf);
    return std::get<0>(full);
}

sstable_version_types version_from_string(std::string_view s) {
    try {
        return reverse_map(s, version_string);
    } catch (std::out_of_range&) {
        throw std::out_of_range(seastar::format("Unknown sstable version: {}", s));
    }
}

sstable_format_types format_from_string(std::string_view s) {
    try {
        return reverse_map(s, format_string);
    } catch (std::out_of_range&) {
        throw std::out_of_range(seastar::format("Unknown sstable format: {}", s));
    }
}

component_type sstable::component_from_sstring(version_types v, const sstring &s) {
    try {
        return reverse_map(s, sstable_version_constants::get_component_map(v));
    } catch (std::out_of_range&) {
        return component_type::Unknown;
    }
}

input_stream<char> sstable::data_stream(uint64_t pos, size_t len,
        reader_permit permit, tracing::trace_state_ptr trace_state, lw_shared_ptr<file_input_stream_history> history, raw_stream raw) {
    file_input_stream_options options;
    options.buffer_size = sstable_buffer_size;
    options.read_ahead = 4;
    options.dynamic_adjustments = std::move(history);

    file f = make_tracked_file(_data_file, permit);
    if (trace_state) {
        f = tracing::make_traced_file(std::move(f), std::move(trace_state), format("{}:", get_filename()));
    }

    input_stream<char> stream;
    if (_components->compression && raw == raw_stream::no) {
        if (_version >= sstable_version_types::mc) {
             return make_compressed_file_m_format_input_stream(f, &_components->compression,
                pos, len, std::move(options), permit);
        } else {
            return make_compressed_file_k_l_format_input_stream(f, &_components->compression,
                pos, len, std::move(options), permit);
        }
    }

    return make_file_input_stream(f, pos, len, std::move(options));
}

future<temporary_buffer<char>> sstable::data_read(uint64_t pos, size_t len, reader_permit permit) {
    return do_with(data_stream(pos, len, std::move(permit), tracing::trace_state_ptr(), {}), [len] (auto& stream) {
        return stream.read_exactly(len).finally([&stream] {
            return stream.close();
        });
    });
}

template <typename ChecksumType>
static future<bool> do_validate_compressed(input_stream<char>& stream, const sstables::compression& c, bool checksum_all, uint32_t expected_digest) {
    bool valid = true;
    uint64_t offset = 0;
    uint32_t actual_full_checksum = ChecksumType::init_checksum();

    auto accessor = c.offsets.get_accessor();
    for (size_t i = 0; i < c.offsets.size(); ++i) {
        auto current_pos = accessor.at(i);
        auto next_pos = i + 1 == c.offsets.size() ? c.compressed_file_length() : accessor.at(i + 1);
        auto chunk_len = next_pos - current_pos;
        auto buf = co_await stream.read_exactly(chunk_len);

        if (!chunk_len) {
            sstlog.error("Found unexpected chunk of length 0 at offset {}", offset);
            valid = false;
            break;
        }

        if (buf.size() < chunk_len) {
            sstlog.error("Truncated file at offset {}: expected to get chunk of size {}, got {}", offset, chunk_len, buf.size());
            valid = false;
            break;
        }

        auto compressed_len = chunk_len - 4;
        auto expected_checksum = read_be<uint32_t>(buf.get() + compressed_len);
        auto actual_checksum = ChecksumType::checksum(buf.get(), compressed_len);
        if (actual_checksum != expected_checksum) {
            sstlog.error("Compressed chunk checksum mismatch at offset {}, for chunk #{} of size {}: expected={}, actual={}", offset, i, chunk_len, expected_checksum, actual_checksum);
            valid = false;
        }

        actual_full_checksum = checksum_combine_or_feed<ChecksumType>(actual_full_checksum, actual_checksum, buf.get(), compressed_len);
        if (checksum_all) {
            uint32_t be_actual_checksum = cpu_to_be(actual_checksum);
            actual_full_checksum = ChecksumType::checksum(actual_full_checksum,
                    reinterpret_cast<const char*>(&be_actual_checksum), sizeof(be_actual_checksum));
        }

        offset += chunk_len;
    }

    if (actual_full_checksum != expected_digest) {
        sstlog.error("Full checksum mismatch: expected={}, actual={}", expected_digest, actual_full_checksum);
        valid = false;
    }

    co_return valid;
}

template <typename ChecksumType>
static future<bool> do_validate_uncompressed(input_stream<char>& stream, const checksum& checksum, uint32_t expected_digest) {
    bool valid = true;
    uint64_t offset = 0;
    uint32_t actual_full_checksum = ChecksumType::init_checksum();

    for (size_t i = 0; i < checksum.checksums.size(); ++i) {
        const auto expected_checksum = checksum.checksums[i];
        auto buf = co_await stream.read_exactly(checksum.chunk_size);

        if (buf.empty()) {
            sstlog.error("Chunk count mismatch between CRC.db and Data.db at offset {}: expected {} chunks but data file has less", offset, checksum.checksums.size());
            valid = false;
            break;
        }

        auto actual_checksum = ChecksumType::checksum(buf.get(), buf.size());

        if (actual_checksum != expected_checksum) {
            sstlog.error("Chunk checksum mismatch at offset {}, for chunk #{} of size {}: expected={}, actual={}", offset, i, checksum.chunk_size, expected_checksum, actual_checksum);
            valid = false;
        }

        actual_full_checksum = checksum_combine_or_feed<ChecksumType>(actual_full_checksum, actual_checksum, buf.begin(), buf.size());

        offset += buf.size();
    }

    {
        // We should be at EOF here, but the flag might not be set yet. To ensure
        // it is set, try to read some more. This should return an empty buffer.
        auto buf = co_await stream.read();
        if (!buf.empty()) {
            sstlog.error("Chunk count mismatch between CRC.db and Data.db at offset {}: expected {} chunks but data file has more", offset, checksum.checksums.size());
            valid = false;
        }
    }

    if (actual_full_checksum != expected_digest) {
        sstlog.error("Full checksum mismatch: expected={}, actual={}", expected_digest, actual_full_checksum);
        valid = false;
    }

    co_return valid;
}

future<uint32_t> sstable::read_digest() {
    sstring digest_str;

    co_await do_read_simple(component_type::Digest, [&] (version_types v, file digest_file) -> future<> {
        file_input_stream_options options;
        options.buffer_size = 4096;

        auto digest_stream = make_file_input_stream(std::move(digest_file), options);

        std::exception_ptr ex;

        try {
            digest_str = co_await util::read_entire_stream_contiguous(digest_stream);
        } catch (...) {
            ex = std::current_exception();
        }

        co_await digest_stream.close();
        maybe_rethrow_exception(std::move(ex));
    });

    co_return boost::lexical_cast<uint32_t>(digest_str);
}

future<checksum> sstable::read_checksum() {
    sstables::checksum checksum;

    co_await do_read_simple(component_type::CRC, [&] (version_types v, file crc_file) -> future<> {
        file_input_stream_options options;
        options.buffer_size = 4096;

        auto crc_stream = make_file_input_stream(std::move(crc_file), options);

        std::exception_ptr ex;

        try {
            const auto size = sizeof(uint32_t);

            auto buf = co_await crc_stream.read_exactly(size);
            check_buf_size(buf, size);
            checksum.chunk_size = net::ntoh(read_unaligned<uint32_t>(buf.get()));

            buf = co_await crc_stream.read_exactly(size);
            while (!buf.empty()) {
                check_buf_size(buf, size);
                checksum.checksums.push_back(net::ntoh(read_unaligned<uint32_t>(buf.get())));
                buf = co_await crc_stream.read_exactly(size);
            }
        } catch (...) {
            ex = std::current_exception();
        }

        co_await crc_stream.close();
        maybe_rethrow_exception(std::move(ex));
    });

    co_return checksum;
}

future<bool> validate_checksums(shared_sstable sst, reader_permit permit) {
    const auto digest = co_await sst->read_digest();

    auto data_stream = sst->data_stream(0, sst->ondisk_data_size(), permit, nullptr, nullptr, sstable::raw_stream::yes);

    auto valid = true;
    std::exception_ptr ex;

    try {
        if (sst->get_compression()) {
            if (sst->get_version() >= sstable_version_types::mc) {
                valid = co_await do_validate_compressed<crc32_utils>(data_stream, sst->get_compression(), true, digest);
            } else {
                valid = co_await do_validate_compressed<adler32_utils>(data_stream, sst->get_compression(), false, digest);
            }
        } else {
            auto checksum = co_await sst->read_checksum();
            if (sst->get_version() >= sstable_version_types::mc) {
                valid = co_await do_validate_uncompressed<crc32_utils>(data_stream, checksum, digest);
            } else {
                valid = co_await do_validate_uncompressed<adler32_utils>(data_stream, checksum, digest);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await data_stream.close();
    maybe_rethrow_exception(std::move(ex));

    co_return valid;
}

void sstable::set_first_and_last_keys() {
    if (_first && _last) {
        return;
    }
    auto decorate_key = [this] (const char *m, const bytes& value) {
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
        return make_exception_future<>(std::runtime_error("Statistics is malformed"));
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
        rewrite_statistics();
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

    return when_all_succeed(std::move(index_closed), std::move(data_closed), std::move(unlinked)).discard_result().then([this, me = shared_from_this()] {
        if (_open_mode) {
            if (_open_mode.value() == open_flags::ro) {
                _stats.on_close_for_reading();
            }
        }
        _open_mode.reset();
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

future<sstring> make_toc_temporary(sstring sstable_toc_name, storage::sync_dir sync) {
    sstring prefix = sstable_toc_name.substr(0, sstable_toc_name.size() - sstable_version_constants::TOC_SUFFIX.size());
    sstring new_toc_name = prefix + sstable_version_constants::TEMPORARY_TOC_SUFFIX;

    sstlog.debug("Removing by TOC name: {}", sstable_toc_name);
    if (co_await sstable_io_check(sstable_write_error_handler, file_exists, sstable_toc_name)) {
        // If new_toc_name exists it will be atomically replaced.  See rename(2)
        co_await sstable_io_check(sstable_write_error_handler, rename_file, sstable_toc_name, new_toc_name);
        if (sync) {
            co_await sstable_io_check(sstable_write_error_handler, sync_directory, parent_path(new_toc_name));
        }
    } else {
        if (!co_await sstable_io_check(sstable_write_error_handler, file_exists, new_toc_name)) {
            sstlog.warn("Unable to delete {} because it doesn't exist.", sstable_toc_name);
            co_return "";
        }
    }
    co_return new_toc_name;
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
sstable::compute_shards_for_this_sstable(const dht::sharder& sharder_) const {
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
    sstlog.trace("{}: token_ranges={}", get_filename(), token_ranges);
    auto sharder = dht::ring_position_range_vector_sharder(sharder_, std::move(token_ranges));
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
    auto sem = reader_concurrency_semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::has_partition_key()",
            reader_concurrency_semaphore::register_metrics::no);
    std::unique_ptr<sstables::index_reader> lh_index_ptr = nullptr;
    try {
        lh_index_ptr = std::make_unique<sstables::index_reader>(s, sem.make_tracking_only_permit(_schema, s->get_filename(), db::no_timeout, {}));
        present = co_await lh_index_ptr->advance_lower_and_check_if_present(dk);
    } catch (...) {
        ex = std::current_exception();
    }
    if (auto lhi_ptr = std::move(lh_index_ptr)) {
        co_await lhi_ptr->close();
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
sstable::unlink(storage::sync_dir sync) noexcept {
    _on_delete(*this);

    auto remove_fut = _storage->wipe(*this, sync);

    try {
        co_await get_large_data_handler().maybe_delete_large_data_entries(shared_from_this());
    } catch (...) {
        memory::scoped_critical_alloc_section _;
        // Just log and ignore failures to delete large data entries.
        // They are not critical to the operation of the database.
        sstlog.warn("Failed to delete large data entry for {}: {}. Ignoring.", toc_filename(), std::current_exception());
    }

    co_await std::move(remove_fut);
    _stats.on_delete();
    _manager.on_unlink(this);
}

thread_local sstables_stats::stats sstables_stats::_shard_stats;
thread_local mc::cached_promoted_index::metrics promoted_index_cache_metrics;
static thread_local seastar::metrics::metric_groups metrics;

void register_index_page_cache_metrics(seastar::metrics::metric_groups& metrics, cached_file_stats& m) {
    namespace sm = seastar::metrics;
    metrics.add_group("sstables", {
        sm::make_counter("index_page_cache_hits", [&m] { return m.page_hits; },
            sm::description("Index page cache requests which were served from cache")),
        sm::make_counter("index_page_cache_misses", [&m] { return m.page_misses; },
            sm::description("Index page cache requests which had to perform I/O")),
        sm::make_counter("index_page_cache_evictions", [&m] { return m.page_evictions; },
            sm::description("Total number of index page cache pages which have been evicted")),
        sm::make_counter("index_page_cache_populations", [&m] { return m.page_populations; },
            sm::description("Total number of index page cache pages which were inserted into the cache")),
        sm::make_gauge("index_page_cache_bytes", [&m] { return m.cached_bytes; },
            sm::description("Total number of bytes cached in the index page cache")),
        sm::make_gauge("index_page_cache_bytes_in_std", [&m] { return m.bytes_in_std; },
            sm::description("Total number of bytes in temporary buffers which live in the std allocator")),
    });
}

void register_index_page_metrics(seastar::metrics::metric_groups& metrics, partition_index_cache_stats& m) {
    namespace sm = seastar::metrics;
    metrics.add_group("sstables", {
        sm::make_counter("index_page_hits", [&m] { return m.hits; },
            sm::description("Index page requests which could be satisfied without waiting")),
        sm::make_counter("index_page_misses", [&m] { return m.misses; },
            sm::description("Index page requests which initiated a read from disk")),
        sm::make_counter("index_page_blocks", [&m] { return m.blocks; },
            sm::description("Index page requests which needed to wait due to page not being loaded yet")),
        sm::make_counter("index_page_evictions", [&m] { return m.evictions; },
            sm::description("Index pages which got evicted from memory")),
        sm::make_counter("index_page_populations", [&m] { return m.populations; },
            sm::description("Index pages which got populated into memory")),
        sm::make_gauge("index_page_used_bytes", [&m] { return m.used_bytes; },
            sm::description("Amount of bytes used by index pages in memory")),

    });
}

future<> init_metrics() {
  return seastar::smp::invoke_on_all([] {
    namespace sm = seastar::metrics;
    metrics.add_group("sstables", {
        sm::make_counter("pi_cache_hits_l0", [] { return promoted_index_cache_metrics.hits_l0; },
            sm::description("Number of requests for promoted index block in state l0 which didn't have to go to the page cache")),
        sm::make_counter("pi_cache_hits_l1", [] { return promoted_index_cache_metrics.hits_l1; },
            sm::description("Number of requests for promoted index block in state l1 which didn't have to go to the page cache")),
        sm::make_counter("pi_cache_hits_l2", [] { return promoted_index_cache_metrics.hits_l2; },
            sm::description("Number of requests for promoted index block in state l2 which didn't have to go to the page cache")),
        sm::make_counter("pi_cache_misses_l0", [] { return promoted_index_cache_metrics.misses_l0; },
            sm::description("Number of requests for promoted index block in state l0 which had to go to the page cache")),
        sm::make_counter("pi_cache_misses_l1", [] { return promoted_index_cache_metrics.misses_l1; },
            sm::description("Number of requests for promoted index block in state l1 which had to go to the page cache")),
        sm::make_counter("pi_cache_misses_l2", [] { return promoted_index_cache_metrics.misses_l2; },
            sm::description("Number of requests for promoted index block in state l2 which had to go to the page cache")),
        sm::make_counter("pi_cache_populations", [] { return promoted_index_cache_metrics.populations; },
            sm::description("Number of promoted index blocks which got inserted")),
        sm::make_counter("pi_cache_evictions", [] { return promoted_index_cache_metrics.evictions; },
            sm::description("Number of promoted index blocks which got evicted")),
        sm::make_gauge("pi_cache_bytes", [] { return promoted_index_cache_metrics.used_bytes; },
            sm::description("Number of bytes currently used by cached promoted index blocks")),
        sm::make_gauge("pi_cache_block_count", [] { return promoted_index_cache_metrics.block_count; },
            sm::description("Number of promoted index blocks currently cached")),

        sm::make_counter("partition_writes", [] { return sstables_stats::get_shard_stats().partition_writes; },
            sm::description("Number of partitions written")),
        sm::make_counter("static_row_writes", [] { return sstables_stats::get_shard_stats().static_row_writes; },
            sm::description("Number of static rows written")),
        sm::make_counter("row_writes", [] { return sstables_stats::get_shard_stats().row_writes; },
            sm::description("Number of clustering rows written")),
        sm::make_counter("cell_writes", [] { return sstables_stats::get_shard_stats().cell_writes; },
            sm::description("Number of cells written")),
        sm::make_counter("tombstone_writes", [] { return sstables_stats::get_shard_stats().tombstone_writes; },
            sm::description("Number of tombstones written")),
        sm::make_counter("range_tombstone_writes", [] { return sstables_stats::get_shard_stats().range_tombstone_writes; },
            sm::description("Number of range tombstones written")),
        sm::make_counter("pi_auto_scale_events", [] { return sstables_stats::get_shard_stats().promoted_index_auto_scale_events; },
            sm::description("Number of promoted index auto-scaling events")),

        sm::make_counter("range_tombstone_reads", [] { return sstables_stats::get_shard_stats().range_tombstone_reads; },
            sm::description("Number of range tombstones read")),
        sm::make_counter("row_tombstone_reads", [] { return sstables_stats::get_shard_stats().row_tombstone_reads; },
            sm::description("Number of row tombstones read")),
        sm::make_counter("cell_tombstone_writes", [] { return sstables_stats::get_shard_stats().cell_tombstone_writes; },
            sm::description("Number of cell tombstones written")),
        sm::make_counter("single_partition_reads", [] { return sstables_stats::get_shard_stats().single_partition_reads; },
            sm::description("Number of single partition flat mutation reads")),
        sm::make_counter("range_partition_reads", [] { return sstables_stats::get_shard_stats().range_partition_reads; },
            sm::description("Number of partition range flat mutation reads")),
        sm::make_counter("partition_reads", [] { return sstables_stats::get_shard_stats().partition_reads; },
            sm::description("Number of partitions read")),
        sm::make_counter("partition_seeks", [] { return sstables_stats::get_shard_stats().partition_seeks; },
            sm::description("Number of partitions seeked")),
        sm::make_counter("row_reads", [] { return sstables_stats::get_shard_stats().row_reads; },
            sm::description("Number of rows read")),

        sm::make_counter("capped_local_deletion_time", [] { return sstables_stats::get_shard_stats().capped_local_deletion_time; },
            sm::description("Number of SStables with tombstones whose local deletion time was capped at the maximum allowed value in Statistics")),
        sm::make_counter("capped_tombstone_deletion_time", [] { return sstables_stats::get_shard_stats().capped_tombstone_deletion_time; },
            sm::description("Number of tombstones whose local deletion time was capped at the maximum allowed value")),

        sm::make_counter("total_open_for_reading", [] { return sstables_stats::get_shard_stats().open_for_reading; },
            sm::description("Counter of sstables open for reading")),
        sm::make_counter("total_open_for_writing", [] { return sstables_stats::get_shard_stats().open_for_writing; },
            sm::description("Counter of sstables open for writing")),

        sm::make_gauge("currently_open_for_reading", [] {
            return sstables_stats::get_shard_stats().open_for_reading -
                   sstables_stats::get_shard_stats().closed_for_reading;
        }, sm::description("Number of sstables currently open for reading")),
        sm::make_gauge("currently_open_for_writing", [] {
            return sstables_stats::get_shard_stats().open_for_writing -
                   sstables_stats::get_shard_stats().closed_for_writing;
        }, sm::description("Number of sstables currently open for writing")),

        sm::make_counter("total_deleted", [] { return sstables_stats::get_shard_stats().deleted; },
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
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) mutable {
        return sst->make_reader(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
    });
}

sstable::sstable(schema_ptr schema,
        sstring table_dir,
        const data_dictionary::storage_options& storage,
        generation_type generation,
        sstable_state state,
        version_types v,
        format_types f,
        db::large_data_handler& large_data_handler,
        sstables_manager& manager,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size)
    : sstable_buffer_size(buffer_size)
    , _schema(std::move(schema))
    , _generation(generation)
    , _state(state)
    , _storage(make_storage(manager, storage, std::move(table_dir), _state))
    , _version(v)
    , _format(f)
    , _index_cache(std::make_unique<partition_index_cache>(
            manager.get_cache_tracker().get_lru(), manager.get_cache_tracker().region(), manager.get_cache_tracker().get_partition_index_cache_stats()))
    , _now(now)
    , _read_error_handler(error_handler_gen(sstable_read_error))
    , _write_error_handler(error_handler_gen(sstable_write_error))
    , _large_data_handler(large_data_handler)
    , _manager(manager)
{
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
    std::exception_ptr ex;
    try {
        co_await close_files();
    } catch (...) {
        ex = std::current_exception();
    }

    co_await _index_cache->evict_gently();
    if (_cached_index_file) {
        co_await _cached_index_file->evict_gently();
    }
    co_await _storage->destroy(*this);

    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
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

// The gc_before returned by the function can only be used to estimate if the
// sstable is worth dropping some tombstones. We only return the maximum
// gc_before for all the partitions that have record in repair history map. It
// is fine that some of the partitions inside the sstable does not have a
// record.
gc_clock::time_point sstable::get_gc_before_for_drop_estimation(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const {
    auto start = get_first_decorated_key().token();
    auto end = get_last_decorated_key().token();
    auto range = dht::token_range(dht::token_range::bound(start, true), dht::token_range::bound(end, true));
    sstlog.trace("sstable={}, ks={}, cf={}, range={}, gc_state={}, estimate", get_filename(), s->ks_name(), s->cf_name(), range, bool(gc_state));
    return gc_state.get_gc_before_for_range(s, range, compaction_time).max_gc_before;
}

// If the sstable contains any regular live cells, we can not drop the sstable.
// We do not even bother to query the gc_before. Return
// gc_clock::time_point::min() as gc_before.
//
// If the token range of the sstable contains tokens that do not have a record
// in the repair history map, we can not drop the sstable, in such case we
// return gc_clock::time_point::min() as gc_before. Otherwise, return the
// gc_before from the repair history map.
gc_clock::time_point sstable::get_gc_before_for_fully_expire(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const {
    auto deletion_time = get_max_local_deletion_time();
    // No need to query gc_before for the sstable if the max_deletion_time is max()
    if (deletion_time == gc_clock::time_point(gc_clock::duration(std::numeric_limits<int>::max()))) {
        sstlog.trace("sstable={}, ks={}, cf={}, get_max_local_deletion_time={}, min_timestamp={}, gc_grace_seconds={}, shortcut",
                get_filename(), s->ks_name(), s->cf_name(), deletion_time, get_stats_metadata().min_timestamp, s->gc_grace_seconds().count());
        return gc_clock::time_point::min();
    }
    auto start = get_first_decorated_key().token();
    auto end = get_last_decorated_key().token();
    auto range = dht::token_range(dht::token_range::bound(start, true), dht::token_range::bound(end, true));
    sstlog.trace("sstable={}, ks={}, cf={}, range={}, get_max_local_deletion_time={}, min_timestamp={}, gc_grace_seconds={}, gc_state={}, query",
            get_filename(), s->ks_name(), s->cf_name(), range, deletion_time, get_stats_metadata().min_timestamp, s->gc_grace_seconds().count(), bool(gc_state));
    auto res = gc_state.get_gc_before_for_range(s, range, compaction_time);
    return res.knows_entire_range ? res.min_gc_before : gc_clock::time_point::min();
}

// Returns error code, 0 is success
static future<int> remove_dir(fs::path dir, bool recursive) {
    std::exception_ptr ex;
    int error_code;
    try {
        co_await (recursive ? recursive_remove_directory(dir) : remove_file(dir.native()));
        co_return 0;
    } catch (const std::system_error& e) {
        ex = std::current_exception();
        error_code = e.code().value();
        if (error_code == ENOENT) {
            // Ignore missing directories
            co_return 0;
        }
        if ((error_code == EEXIST || error_code == ENOTEMPTY) && !recursive) {
            // Just return failure if the directory is not empty
            // Let the caller decide what to do about it.
            co_return error_code;
        }
    } catch (...) {
        ex = std::current_exception();
        error_code = -1;
    }
    sstlog.warn("Could not remove table directory {}: {}. Ignored.", dir, ex);
    co_return error_code;
}

future<> remove_table_directory_if_has_no_snapshots(fs::path table_dir) {
    // Be paranoid about risky paths
    if (table_dir == "" || table_dir == "/") {
        on_internal_error_noexcept(sstlog, format("Invalid table directory for removal: {}", table_dir));
        abort();
    }

    int error = 0;
    for (auto subdir : sstables::table_subdirectories) {
        // Remove the snapshot directory only if empty
        // while other subdirectories are removed recusresively.
        auto ec = co_await remove_dir(table_dir / subdir, subdir != sstables::snapshots_dir);
        if (subdir == sstables::snapshots_dir && ec == EEXIST) {
            sstlog.info("Leaving table directory {} behind as it has snapshots", table_dir);
        }
        if (!error) {
            error = ec;
        }
    }

    if (!error) {
        // Remove the table directory recursively
        // since it may still hold leftover temporary
        // sstable files and directories
        co_await remove_dir(table_dir, true);
    }
}

std::string to_string(const shared_sstable& sst, bool include_origin) {
    return include_origin ?
        fmt::format("{}:level={:d}:origin={}", sst->get_filename(), sst->get_sstable_level(), sst->get_origin()) :
        fmt::format("{}:level={:d}", sst->get_filename(), sst->get_sstable_level());
}

} // namespace sstables

namespace seastar {

void
lw_shared_ptr_deleter<sstables::sstable>::dispose(sstables::sstable* s) {
    s->unused();
}


template
sstables::sstable*
seastar::internal::lw_shared_ptr_accessors<sstables::sstable, void>::to_value(seastar::lw_shared_ptr_counter_base*);

}
