/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "utils/chunked_vector.hh"
#include "utils/on_internal_error.hh"
#include "types/collection.hh"
#include "types/user.hh"
#include "types/concrete_types.hh"
#include "mutation/mutation_partition.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "combine.hh"
#include "idl/mutation.dist.impl.hh"

#include "collection_mutation.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

collection_mutation::collection_mutation() : _data(managed_bytes::initialized_later{}, sizeof(uint8_t) + sizeof(int32_t)) {
    auto out = managed_bytes_mutable_view(_data);
    write<uint8_t>(out, uint8_t(false)); // No tombstone
    write<int32_t>(out, 0); // No cells
}

collection_mutation::collection_mutation(collection_mutation_view v)
    : _data(v.data) {}

collection_mutation::collection_mutation(managed_bytes data)
    : _data(std::move(data)) {}

collection_mutation::operator collection_mutation_view() const
{
    return collection_mutation_view{managed_bytes_view(_data)};
}

collection_mutation_view atomic_cell_or_collection::as_collection_mutation() const {
    return collection_mutation_view{managed_bytes_view(_data)};
}

using collection_mutation_detail::read_collection_tombstone;
using collection_mutation_detail::read_collection_size;

bool collection_mutation_view::empty() const {
    auto v = data;
    auto tomb = read_collection_tombstone(v);
    return !tomb && read_collection_size(v) == 0;
}

bool collection_mutation_view::is_any_live(const abstract_type& type, tombstone tomb, gc_clock::time_point now) const {
    tomb.apply(this->tomb());
    for (auto& [key, value] : *this) {
        if (value.is_live(tomb, now, false)) {
            return true;
        }
    }

    return false;
}

api::timestamp_type collection_mutation_view::last_update(const abstract_type& type) const {
    api::timestamp_type max = tomb().timestamp;
    for_each_element([&] (managed_bytes_view, atomic_cell_view value) {
        max = std::max(value.timestamp(), max);
        return true;
    });
    return max;
}

tombstone collection_mutation_view::tomb() const {
    auto v = data;
    return read_collection_tombstone(v);
}

uint32_t collection_mutation_view::size() const {
    auto v = data;
    read_collection_tombstone(v); // skip tombstone if present
    return read_collection_size(v);
}

collection_mutation_view::iterator::iterator(managed_bytes_view data) {
    read_collection_tombstone(data); // skip tombstone if present
    _remaining_count = read_collection_size(data);
    _remaining = data;
    ++*this;
}

void collection_mutation_view::iterator::advance() {
    auto [key, value] = collection_mutation_detail::read_element(_remaining);
    _current = value_type{key, atomic_cell_view::from_bytes(value)};
}

collection_mutation_view::iterator collection_mutation_view::begin() const {
    return iterator(data);
}

collection_mutation_view::iterator collection_mutation_view::end() const {
    return iterator{};
}

auto fmt::formatter<collection_mutation_view::printer>::format(const collection_mutation_view::printer& cmvp, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{collection_mutation_view ");
    const auto& cmv = cmvp._cmv;
    bool first = true;
    out = fmt::format_to(out, "tombstone {}", cmv.tomb());
    visit(cmvp._type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            auto&& key_type = ctype.name_comparator();
            auto&& value_type = ctype.value_comparator();
            out = fmt::format_to(out, " collection cells {{");
            for (auto&& [key, value] : cmv) {
                if (!first) {
                    out = fmt::format_to(out, ", ");
                }
                fmt::format_to(out, "{}: {}", key_type->to_string(key.linearize()), atomic_cell_view::printer(*value_type, value));
                first = false;
            }
            out = fmt::format_to(out, "}}");
        },
        [&] (const user_type_impl& utype) {
            out = fmt::format_to(out, " user-type cells {{");
            for (auto&& [raw_idx, value] : cmv) {
                if (first) {
                    out = fmt::format_to(out, " ");
                } else {
                    out = fmt::format_to(out, ", ");
                }
                auto idx = deserialize_field_index(raw_idx);
                out = fmt::format_to(out, "{}: {}", utype.field_name_as_string(idx), atomic_cell_view::printer(*utype.type(idx), value));
                first = false;
            }
            out = fmt::format_to(out, "}}");
        },
        [&] (const abstract_type& o) {
            // Not throwing exception in this likely-to-be debug context
            out = fmt::format_to(out, " attempted to pretty-print collection_mutation_view with type {}", o.name());
        }
    ));
    return fmt::format_to(out, "}}");
}

collection_mutation_compact_and_expire_if_changed_result compact_and_expire_if_changed(
        collection_mutation_view cmv,
        column_id id,
        const abstract_type& type,
        row_tombstone base_tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        compaction_garbage_collector* collector) {
    compact_and_expire_result res{};
    auto tomb = cmv.tomb();
    if (tomb) {
        res.collection_tombstones++;
    }
    auto t = tomb;
    tombstone purged_tomb;
    if (tomb <= base_tomb.regular()) {
        tomb = tombstone();
    } else if (tomb.deletion_time < gc_before && can_gc(tomb, is_shadowable::no)) { // The collection tombstone is never shadowable
        purged_tomb = tomb;
        tomb = tombstone();
    }
    t.apply(base_tomb.regular());
    collection_mutation_writer survivors(tomb);
    auto rest = cmv.data;
    auto n = collection_mutation_detail::skip_header(rest);
    // Usually nothing is dropped, expired or purged: then return no copy, and otherwise
    // copy the leading run of unchanged elements verbatim instead of element by element.
    if (tomb == cmv.tomb()) {
        const auto shadowable = base_tomb.shadowable().tomb();
        const auto first = rest;
        uint32_t kept = 0;
        for (; n; --n, ++kept) {
            auto next = rest;
            auto cell = atomic_cell_view::from_bytes(collection_mutation_detail::read_element(next).second);
            if (!cell.is_live() || cell.has_expired(query_time) || cell.is_covered_by(t, false) || cell.is_covered_by(shadowable, false)) {
                break;
            }
            rest = next;
        }
        res.live_cells += kept;
        if (!n) {
            if (collector) {
                collector->collect(id, collection_mutation());
            }
            return {std::nullopt, std::move(res)};
        }
        survivors.append_serialized(first.prefix(first.size_bytes() - rest.size_bytes()), kept);
    }
    collection_mutation_writer losers(purged_tomb);
    for (; n; --n) {
        auto [key, value] = collection_mutation_detail::read_element(rest);
        auto cell = atomic_cell_view::from_bytes(value);
        auto cannot_erase_cell = [&] {
            // Only row tombstones can be shadowable, (collection) cell tombstones aren't
            return cell.deletion_time() >= gc_before || !can_gc(tombstone(cell.timestamp(), cell.deletion_time()), is_shadowable::no);
        };

        if (cell.is_covered_by(t, false) || cell.is_covered_by(base_tomb.shadowable().tomb(), false)) {
            res.dead_cells++;
            continue;
        }
        if (cell.has_expired(query_time)) {
            auto dead = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
            if (cannot_erase_cell()) {
                survivors.push_back(std::move(key), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time()));
            } else if (collector) {
                losers.push_back(std::move(key), std::move(dead));
            }
            res.dead_cells++;
        } else if (!cell.is_live()) {
            if (cannot_erase_cell()) {
                survivors.push_back(std::move(key), atomic_cell(type, cell));
            } else if (collector) {
                losers.push_back(key, atomic_cell(type, cell));
            }
            res.dead_cells++;
        } else {
            survivors.push_back(key, atomic_cell(type, cell));
            res.live_cells++;
        }
    }
    if (collector) {
        collector->collect(id, std::move(losers).finish());
    }
    return {std::move(survivors).finish(), std::move(res)};
}

collection_mutation_compact_and_expire_result compact_and_expire(
        collection_mutation_view cmv,
        column_id id,
        const abstract_type& type,
        row_tombstone base_tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        compaction_garbage_collector* collector) {
    auto r = compact_and_expire_if_changed(cmv, id, type, base_tomb, query_time, can_gc, gc_before, collector);
    return {r.collection ? std::move(*r.collection) : collection_mutation(cmv), r.result};
}

namespace {

// A collection_element as laid out by idl/mutation.idl.hh, parsed from one
// contiguous frame. Only leading fields are read, so appended fields are ignored.
struct frozen_collection_element {
    // Variant alternatives of collection_element::value, in IDL order.
    enum kind_type : uint32_t { live, expiring, dead };
    bytes_view key;
    kind_type kind;
    api::timestamp_type timestamp;
    bytes_view value;
    gc_clock::time_point time; // expiry or deletion time
    gc_clock::duration ttl;

    static bytes_view read_bytes(seastar::simple_memory_input_stream& in) {
        auto len = ser::deserialize(in, std::type_identity<uint32_t>());
        auto p = in.begin();
        in.skip(len);
        return bytes_view(reinterpret_cast<const int8_t*>(p), len);
    }

    explicit frozen_collection_element(bytes_view frame) {
        seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(frame.data()), frame.size());
        in.skip(sizeof(uint32_t)); // element frame
        key = read_bytes(in);
        in.skip(sizeof(uint32_t)); // variant frame
        kind = kind_type(ser::deserialize(in, std::type_identity<uint32_t>()));
        switch (kind) {
        case expiring:
            in.skip(sizeof(uint32_t)); // expiring_cell frame
            ttl = ser::deserialize(in, std::type_identity<gc_clock::duration>());
            time = ser::deserialize(in, std::type_identity<gc_clock::time_point>());
            [[fallthrough]];
        case live:
            in.skip(sizeof(uint32_t)); // live_cell frame
            timestamp = ser::deserialize(in, std::type_identity<api::timestamp_type>());
            value = read_bytes(in);
            break;
        case dead:
            in.skip(sizeof(uint32_t)); // tombstone frame; dead_cell itself is final
            timestamp = ser::deserialize(in, std::type_identity<api::timestamp_type>());
            time = ser::deserialize(in, std::type_identity<gc_clock::time_point>());
            break;
        default:
            utils::on_internal_error("Trying to deserialize cell in unknown state");
        }
    }

    size_t cell_size() const {
        switch (kind) {
        case live: return atomic_cell_type::live_serialized_size(value.size());
        case expiring: return atomic_cell_type::live_expiring_serialized_size(value.size());
        case dead: return atomic_cell_type::dead_serialized_size();
        }
        __builtin_unreachable();
    }

    void write_to(managed_bytes_mutable_view& out) const {
        write<int32_t>(out, key.size());
        write_fragmented(out, single_fragmented_view(key));
        const auto size = cell_size();
        write<int32_t>(out, size);
        switch (kind) {
        case live: atomic_cell_type::write_live(out, timestamp, single_fragment_range(value)); break;
        case expiring: atomic_cell_type::write_live(out, timestamp, single_fragment_range(value), time, ttl); break;
        case dead: atomic_cell_type::write_dead(out, timestamp, time); break;
        }
        out.remove_prefix(size);
    }
};

struct serialized_cell_adaptor {
    static size_t key_size(const ser::collection_element_view& v) {
        return v.key().view().size_bytes();
    }

    static size_t value_size(const ser::collection_element_view& v) {
        struct collection_cell_visitor {
            size_t operator()(const ser::live_cell_view& lcv) const { return atomic_cell_type::live_serialized_size(lcv.value().view().size_bytes()); }
            size_t operator()(const ser::expiring_cell_view& ecv) const { return atomic_cell_type::live_expiring_serialized_size(ecv.c().value().view().size_bytes()); }
            size_t operator()(const ser::dead_cell_view& dcv) const { return atomic_cell_type::dead_serialized_size(); }
            size_t operator()(const ser::counter_cell_view& ccv) const { utils::on_internal_error("Trying to deserialize counter cell from collection"); }
            size_t operator()(const ser::unknown_variant_type&) const { utils::on_internal_error("Trying to deserialize cell in unknown state"); };
        };
        return boost::apply_visitor(collection_cell_visitor{}, v.value());
    }

    static void write_key(const ser::collection_element_view& v, managed_bytes_mutable_view& out) {
        write_fragmented(out, v.key().view());
    }

    static void write_value(const ser::collection_element_view& v, managed_bytes_mutable_view& out) {
        struct collection_cell_visitor {
            managed_bytes_mutable_view& out;

            void operator()(const ser::live_cell_view& lcv) const {
                const auto v = lcv.value().view();
                atomic_cell_type::write_live(out, lcv.created_at(), v);
                out.remove_prefix(atomic_cell_type::live_serialized_size(v.size_bytes()));
            }
            void operator()(const ser::expiring_cell_view& ecv) const {
                const auto v = ecv.c().value().view();
                atomic_cell_type::write_live(out, ecv.c().created_at(), v, ecv.expiry(), ecv.ttl());
                out.remove_prefix(atomic_cell_type::live_expiring_serialized_size(v.size_bytes()));
            }
            void operator()(const ser::dead_cell_view& dcv) const {
                atomic_cell_type::write_dead(out, dcv.tomb().timestamp(), dcv.tomb().deletion_time());
                out.remove_prefix(atomic_cell_type::dead_serialized_size());
            }
            void operator()(const ser::counter_cell_view& ccv) const {
                utils::on_internal_error("Trying to deserialize counter cell from collection");
            }
            void operator()(const ser::unknown_variant_type&) const {
                utils::on_internal_error("Trying to deserialize cell in unknown state");
            }
        };
        boost::apply_visitor(collection_cell_visitor{out}, v.value());
    }
};

size_t element_size(const frozen_collection_element& e) {
    return 2 * sizeof(int32_t) + e.key.size() + e.cell_size();
}

void write_element(managed_bytes_mutable_view& out, const frozen_collection_element& e) {
    e.write_to(out);
}

size_t element_size(const ser::collection_element_view& e) {
    return 2 * sizeof(int32_t) + serialized_cell_adaptor::key_size(e) + serialized_cell_adaptor::value_size(e);
}

void write_element(managed_bytes_mutable_view& out, const ser::collection_element_view& e) {
    write<int32_t>(out, serialized_cell_adaptor::key_size(e));
    serialized_cell_adaptor::write_key(e, out);
    write<int32_t>(out, serialized_cell_adaptor::value_size(e));
    serialized_cell_adaptor::write_value(e, out);
}

// Calls fn on each of the `remaining` elements of `in`, advancing both; a preemptible call may stop early.
// Frames within one fragment are parsed in place, as the generic IDL views cost thousands of instructions.
template <typename Fn>
void for_each_frozen_element(utils::input_stream& in, uint32_t& remaining, bool preemptible, Fn&& fn) {
    seastar::with_serialized_stream(in, [&] (auto& s) {
        for (uint32_t i = 0; remaining; --remaining, ++i) {
            // Checked per element, as elements can be large; i > 0 guarantees progress.
            if (preemptible && i && seastar::need_preempt()) {
                return;
            }
            bytes_view frag;
            if constexpr (requires { s.first_fragment_data(); }) {
                // A substream shares its parent's fragment, which can extend past the stream.
                frag = bytes_view(reinterpret_cast<const int8_t*>(s.first_fragment_data()), std::min(s.first_fragment_size(), s.size()));
            } else {
                frag = bytes_view(reinterpret_cast<const int8_t*>(s.begin()), s.size());
            }
            if (frag.size() >= sizeof(uint32_t)) {
                const auto frame = seastar::read_le<uint32_t>(reinterpret_cast<const char*>(frag.data()));
                if (frame <= frag.size()) {
                    fn(frozen_collection_element(frag.substr(0, frame)));
                    s.skip(frame);
                    continue;
                }
            }
            fn(ser::deserialize(s, std::type_identity<ser::collection_element_view>()));
        }
    });
}

// Sizes the output exactly in a first pass, then writes it: this also runs under LSA
// (memtable apply), where slack would be garbage.
struct frozen_collection_decoder {
    tombstone tomb;
    utils::input_stream in;
    uint32_t count;
    utils::input_stream sizing_in;
    uint32_t sizing_left;
    uint32_t left;
    size_t size;
    std::optional<managed_bytes> buf;
    managed_bytes_mutable_view out;

    // Positioned at the element count.
    static utils::input_stream elements(const ser::collection_cell_view& collection) {
        auto in = collection.v;
        ser::skip(in, std::type_identity<uint32_t>());
        ser::skip(in, std::type_identity<ser::tombstone_view>());
        return in;
    }

    explicit frozen_collection_decoder(const ser::collection_cell_view& collection)
            : tomb(collection.tomb())
            , in(elements(collection))
            , count(ser::deserialize(in, std::type_identity<uint32_t>()))
            , sizing_in(in)
            , sizing_left(count)
            , left(count)
            , size(sizeof(uint8_t) + (tomb ? 2 * sizeof(int64_t) : 0) + sizeof(int32_t)) {
    }

    // Returns true when done.
    bool decode(bool preemptible) {
        if (!buf) {
            for_each_frozen_element(sizing_in, sizing_left, preemptible, [this] (const auto& e) { size += element_size(e); });
            if (sizing_left) {
                return false;
            }
            buf.emplace(managed_bytes::initialized_later(), size);
            out = managed_bytes_mutable_view(*buf);
            write<uint8_t>(out, uint8_t(bool(tomb)));
            if (tomb) {
                write<int64_t>(out, tomb.timestamp);
                write<int64_t>(out, tomb.deletion_time.time_since_epoch().count());
            }
            // FIXME: overflow?
            write<int32_t>(out, count);
        }
        for_each_frozen_element(in, left, preemptible, [this] (const auto& e) { write_element(out, e); });
        return !left;
    }

    collection_mutation finish() && {
        return collection_mutation(std::move(*buf));
    }
};

}

collection_mutation read_from_collection_cell_view(const abstract_type& type, const ser::collection_cell_view& collection) {
    frozen_collection_decoder d(collection);
    d.decode(false);
    return std::move(d).finish();
}

future<collection_mutation> read_from_collection_cell_view_gently(const ser::collection_cell_view& collection) {
    frozen_collection_decoder d(collection);
    while (!d.decode(true)) {
        co_await coroutine::maybe_yield();
    }
    co_return std::move(d).finish();
}

collection_mutation_writer::collection_mutation_writer(::tombstone tomb) : _tomb(tomb) {
    auto oit = _out.write_begin();
    write<uint8_t>(oit, uint8_t(bool(tomb)));
    if (tomb) {
        write<int64_t>(oit, tomb.timestamp);
        write<int64_t>(oit, tomb.deletion_time.time_since_epoch().count());
    }
    _size_buffer = _out.write_place_holder<int32_t>().ptr;
}

void collection_mutation_writer::push_back(managed_bytes_view key, atomic_cell_view value) {
    {
        auto oit = _out.write_begin();
        write<int32_t>(oit, key.size());
        _out.write(key);
    }

    {
        auto oit = _out.write_begin();
        const auto value_bytes = value.serialize();
        write<int32_t>(oit, value_bytes.size());
        _out.write(value_bytes);
    }

    ++_size;
}

void collection_mutation_writer::append_serialized(managed_bytes_view elements, uint32_t count) {
    for (bytes_view frag : fragment_range(elements)) {
        _out.write(frag);
    }
    _size += count;
}

collection_mutation collection_mutation_writer::finish() && {
    if (empty()) {
        return {};
    }

    write<int32_t>(_size_buffer, _size);
    // Force a copy of the serialized collection, for 2 reasons:
    // * Shrink the allocated memory to actually needed size.
    // * Ensure we are using the correct allocator: bytes_ostream uses the
    //   standard allocator, but we may be in an LSA context here.
    auto& outer_allocator = current_allocator();
    return with_allocator(standard_allocator(), [&] {
        // bytes_ostream uses malloc(), so we need to force standard
        // allocator context here when the buffer is destroyed.
        const auto tmp = std::move(_out).to_managed_bytes();
        return with_allocator(outer_allocator, [&] {
            return collection_mutation(tmp);
        });
    });
}

thread_local size_t collection_fast_path_min_size = 64 * 1024;

// Copies the large side's unchanged runs verbatim; byte-identical to the general merge.
// Requires small.tomb() to be empty, so nothing on the large side is filtered out.
template <typename C>
static collection_mutation
merge_into_large(collection_mutation_view large, collection_mutation_view small, bool large_is_a, C&& key_type) {
    struct element {
        managed_bytes_view key;
        atomic_cell_view value;
    };
    const auto tomb = large.tomb();
    utils::chunked_vector<element> ins;
    for (auto&& [key, value] : small) {
        // Same filter as the general merge: a tombstone wins over an equal timestamp.
        if (!tomb || tomb.timestamp < value.timestamp()) {
            ins.push_back({key, value});
        }
    }

    managed_bytes_view body = large.data;
    auto remaining = collection_mutation_detail::skip_header(body);
    const size_t header_size = large.data.size_bytes() - body.size_bytes() - sizeof(int32_t);
    uint32_t count = remaining;

    // Replace `replaced` bytes at `offset` of the large side's elements with `e`.
    struct splice {
        size_t offset;
        size_t replaced;
        element e;
    };
    utils::chunked_vector<splice> splices;
    splices.reserve(ins.size());
    size_t out_size = large.data.size_bytes();
    auto insert_at = [&] (size_t offset, size_t replaced, element e) {
        out_size += 2 * sizeof(int32_t) + e.key.size_bytes() + e.value.serialize().size_bytes() - replaced;
        splices.push_back({offset, replaced, e});
    };

    auto next = ins.begin();
    managed_bytes_view cur = body;
    while (next != ins.end() && remaining) {
        const size_t offset = body.size_bytes() - cur.size_bytes();
        auto [key, value_bytes] = collection_mutation_detail::read_element(cur);
        auto value = atomic_cell_view::from_bytes(value_bytes);
        --remaining;
        const size_t size = body.size_bytes() - cur.size_bytes() - offset;
        std::strong_ordering cmp = std::strong_ordering::less;
        while (next != ins.end() && (cmp = key_type.compare(next->key, key)) < 0) {
            insert_at(offset, 0, *next++);
            ++count;
        }
        if (cmp == 0) {
            // Same rule as the general merge: a's key, a's cell only if it compares greater.
            const element s = *next++;
            const bool a_wins = large_is_a ? compare_atomic_cell_for_merge(value, s.value) > 0 : compare_atomic_cell_for_merge(s.value, value) > 0;
            const bool large_wins = a_wins == large_is_a;
            // The result takes a's key, so a large b element stays as is when the keys are byte-equal.
            if (!large_wins || (!large_is_a && s.key != key)) {
                insert_at(offset, size, element{large_is_a ? key : s.key, large_wins ? value : s.value});
            }
        }
    }
    for (; next != ins.end(); ++next) {
        insert_at(body.size_bytes(), 0, *next);
        ++count;
    }

    managed_bytes ret(managed_bytes::initialized_later(), out_size);
    managed_bytes_mutable_view out(ret);
    write_fragmented(out, large.data.prefix(header_size));
    write<int32_t>(out, count);
    size_t copied = 0;
    for (auto& sp : splices) {
        write_fragmented(out, body.prefix(sp.offset - copied));
        body.remove_prefix(sp.offset - copied + sp.replaced);
        copied = sp.offset + sp.replaced;
        write<int32_t>(out, sp.e.key.size_bytes());
        write_fragmented(out, sp.e.key);
        auto v = sp.e.value.serialize();
        write<int32_t>(out, v.size_bytes());
        write_fragmented(out, v);
    }
    write_fragmented(out, body);
    return collection_mutation(std::move(ret));
}

template <typename C>
requires std::is_base_of_v<abstract_type, std::remove_reference_t<C>>
static collection_mutation
merge(collection_mutation_view a, collection_mutation_view b, C&& key_type) {
    using element_type = collection_mutation_view::iterator::value_type;

    auto compare = [&] (const element_type& e1, const element_type& e2) {
        return e1.first.with_linearized([&] (bytes_view k1) {
            return e2.first.with_linearized([&] (bytes_view k2) {
                return key_type.less(k1, k2);
            });
        });
    };

    auto merge = [] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };

    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto filter_cells = [&] (const collection_mutation_view& v, tombstone t) {
        return v | std::views::filter([t] (const element_type& e) {
            if (!t) {
                return true;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t.timestamp < e.second.timestamp()) {
                return true;
            }
            return false;
            // FIXME: should we consider TTLs too?
        });
    };

    // Heuristic thresholds: below them re-serializing both sides is cheap enough.
    const auto min_size = collection_fast_path_min_size;
    if (a.data.size_bytes() >= min_size && b.data.size_bytes() * 8 <= a.data.size_bytes() && !b.tomb()) {
        return merge_into_large(a, b, true, key_type);
    }
    if (b.data.size_bytes() >= min_size && a.data.size_bytes() * 8 <= b.data.size_bytes() && !a.tomb()) {
        return merge_into_large(b, a, false, key_type);
    }

    collection_mutation_writer merged(std::max(a.tomb(), b.tomb()));

    auto a_filtered = filter_cells(a, b.tomb());
    auto b_filtered = filter_cells(b, a.tomb());

    combine(
            a_filtered.begin(), a_filtered.end(),
            b_filtered.begin(), b_filtered.end(),
            std::back_inserter(merged),
            compare,
            merge);

    return std::move(merged).finish();
}

collection_mutation merge(const abstract_type& type, collection_mutation_view a, collection_mutation_view b) {
    return visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            return merge(std::move(a), std::move(b), *ctype.name_comparator());
        },
        [&] (const user_type_impl& utype) {
            return merge(std::move(a), std::move(b), *short_type);
        },
        [] (const abstract_type& o) -> collection_mutation {
            throw std::runtime_error(format("collection_mutation merge: unknown type: {}", o.name()));
        }
    ));
}

template <typename C>
requires std::is_base_of_v<abstract_type, std::remove_reference_t<C>>
static collection_mutation
difference(collection_mutation_view a, collection_mutation_view b, C&& key_type)
{
    using element_type = collection_mutation_view::iterator::value_type;

    tombstone diff_tomb;
    if (a.tomb() > b.tomb()) {
        diff_tomb = a.tomb();
    }
    collection_mutation_writer diff(diff_tomb);

    auto less = [&] (const element_type& e1, const element_type& e2) {
        return e1.first.with_linearized([&] (bytes_view k1) {
            return e2.first.with_linearized([&] (bytes_view k2) {
                return key_type.less(k1, k2);
            });
        });
    };

    auto it = b.begin();
    for (auto&& c : a) {
        while (it != b.end() && less(*it, c)) {
            ++it;
        }
        if (it == b.end() || !key_type.equal(it->first, c.first)
            || compare_atomic_cell_for_merge(c.second, it->second) > 0) {

            diff.push_back(c.first, c.second);
        }
    }

    return std::move(diff).finish();
}

collection_mutation difference(const abstract_type& type, collection_mutation_view a, collection_mutation_view b)
{
    return visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            return difference(std::move(a), std::move(b), *ctype.name_comparator());
        },
        [&] (const user_type_impl& utype) {
            return difference(std::move(a), std::move(b), *short_type);
        },
        [] (const abstract_type& o) -> collection_mutation {
            throw std::runtime_error(format("collection_mutation difference: unknown type: {}", o.name()));
        }
    ));
}
