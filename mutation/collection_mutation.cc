/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "utils/on_internal_error.hh"
#include "types/collection.hh"
#include "types/user.hh"
#include "types/concrete_types.hh"
#include "mutation/mutation_partition.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "combine.hh"
#include "idl/mutation.dist.impl.hh"

#include "collection_mutation.hh"

bytes_view collection_mutation_input_stream::read_linearized(size_t n) {
    managed_bytes_view mbv = ::read_simple_bytes(_src, n);
    if (mbv.is_linearized()) {
        return mbv.current_fragment();
    } else {
        return _linearized.emplace_front(linearized(mbv));
    }
}
managed_bytes_view collection_mutation_input_stream::read_fragmented(size_t n) {
    return ::read_simple_bytes(_src, n);
}
bool collection_mutation_input_stream::empty() const {
    return _src.empty();
}

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

bool collection_mutation_view::is_empty() const {
    auto in = collection_mutation_input_stream(data);
    auto has_tomb = in.read_trivial<uint8_t>();
    return !has_tomb && in.read_trivial<uint32_t>() == 0;
}

bool collection_mutation_view::is_any_live(const abstract_type& type, tombstone tomb, gc_clock::time_point now) const {
    auto in = collection_mutation_input_stream(data);
    auto has_tomb = in.read_trivial<uint8_t>();
    if (has_tomb) {
        auto ts = in.read_trivial<api::timestamp_type>();
        auto ttl = in.read_trivial<gc_clock::duration::rep>();
        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }

    auto nr = in.read_trivial<uint32_t>();
    for (uint32_t i = 0; i != nr; ++i) {
        auto key_size = in.read_trivial<uint32_t>();
        in.read_fragmented(key_size); // Skip
        auto vsize = in.read_trivial<uint32_t>();
        auto value = atomic_cell_view::from_bytes(in.read_fragmented(vsize));
        if (value.is_live(tomb, now, false)) {
            return true;
        }
    }

    return false;
}

api::timestamp_type collection_mutation_view::last_update(const abstract_type& type) const {
    auto in = collection_mutation_input_stream(data);
    api::timestamp_type max = api::missing_timestamp;
    auto has_tomb = in.read_trivial<uint8_t>();
    if (has_tomb) {
        max = std::max(max, in.read_trivial<api::timestamp_type>());
        (void)in.read_trivial<gc_clock::duration::rep>();
    }

    auto nr = in.read_trivial<uint32_t>();
    for (uint32_t i = 0; i != nr; ++i) {
        const auto key_size = in.read_trivial<uint32_t>();
        in.read_fragmented(key_size); // Skip
        auto vsize = in.read_trivial<uint32_t>();
        auto value = atomic_cell_view::from_bytes(in.read_fragmented(vsize));
        max = std::max(value.timestamp(), max);
    }

    return max;
}

auto fmt::formatter<collection_mutation_view::printer>::format(const collection_mutation_view::printer& cmvp, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{collection_mutation_view ");
    cmvp._cmv.with_deserialized(cmvp._type, [&out, &type = cmvp._type] (const collection_mutation_view_description& cmvd) {
        bool first = true;
        out = fmt::format_to(out, "tombstone {}", cmvd.tomb);
        visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            auto&& key_type = ctype.name_comparator();
            auto&& value_type = ctype.value_comparator();
            out = fmt::format_to(out, " collection cells {{");
            for (auto&& [key, value] : cmvd.cells) {
                if (!first) {
                    out = fmt::format_to(out, ", ");
                }
                fmt::format_to(out, "{}: {}", key_type->to_string(key), atomic_cell_view::printer(*value_type, value));
                first = false;
            }
            out = fmt::format_to(out, "}}");
        },
        [&] (const user_type_impl& utype) {
            out = fmt::format_to(out, " user-type cells {{");
            for (auto&& [raw_idx, value] : cmvd.cells) {
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
            out = fmt::format_to(out, " attempted to pretty-print collection_mutation_view_description with type {}", o.name());
        }
        ));
    });
    return fmt::format_to(out, "}}");
}


collection_mutation_description
collection_mutation_view_description::materialize(const abstract_type& type) const {
    collection_mutation_description m;
    m.tomb = tomb;
    m.cells.reserve(cells.size());

    visit(type, make_visitor(
    [&] (const collection_type_impl& ctype) {
        auto& value_type = *ctype.value_comparator();
        for (auto&& e : cells) {
            m.cells.emplace_back(to_bytes(e.first), atomic_cell(value_type, e.second));
        }
    },
    [&] (const user_type_impl& utype) {
        for (auto&& e : cells) {
            m.cells.emplace_back(to_bytes(e.first), atomic_cell(*utype.type(deserialize_field_index(e.first)), e.second));
        }
    },
    [&] (const abstract_type& o) {
        throw std::runtime_error(format("attempted to materialize collection_mutation_view_description with type {}", o.name()));
    }
    ));

    return m;
}

compact_and_expire_result collection_mutation_description::compact_and_expire(column_id id, row_tombstone base_tomb, gc_clock::time_point query_time,
    can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector)
{
    compact_and_expire_result res{};
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
    utils::chunked_vector<std::pair<bytes, atomic_cell>> survivors;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> losers;
    for (auto&& name_and_cell : cells) {
        atomic_cell& cell = name_and_cell.second;
        auto cannot_erase_cell = [&] {
            // Only row tombstones can be shadowable, (collection) cell tombstones aren't
            return cell.deletion_time() >= gc_before || !can_gc(tombstone(cell.timestamp(), cell.deletion_time()), is_shadowable::no);
        };

        if (cell.is_covered_by(t, false) || cell.is_covered_by(base_tomb.shadowable().tomb(), false)) {
            res.dead_cells++;
            continue;
        }
        if (cell.has_expired(query_time)) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::make_pair(
                    std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            } else if (collector) {
                losers.emplace_back(std::pair(
                        std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            }
            res.dead_cells++;
        } else if (!cell.is_live()) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::move(name_and_cell));
            } else if (collector) {
                losers.emplace_back(std::move(name_and_cell));
            }
            res.dead_cells++;
        } else {
            survivors.emplace_back(std::move(name_and_cell));
            res.live_cells++;
        }
    }
    if (collector) {
        collector->collect(id, collection_mutation_description{purged_tomb, std::move(losers)});
    }
    cells = std::move(survivors);
    return res;
}

/// A CollectionMutationAdaptor is a static interface that adapts a collection
/// element (an iterator value type) to the serialization requirements of
/// serialize_collection_mutation(). It provides static methods to measure the
/// serialized sizes and to write the key and value of each element into a buffer.
template <typename Adaptor, typename Element>
concept CollectionMutationAdaptor = requires(const Element& e, managed_bytes_mutable_view& out) {
    { Adaptor::key_size(e) } -> std::convertible_to<size_t>;
    { Adaptor::value_size(e) } -> std::convertible_to<size_t>;
    { Adaptor::write_key(e, out) };
    { Adaptor::write_value(e, out) };
};

template <typename Adaptor, typename Iterator>
    requires CollectionMutationAdaptor<Adaptor, std::iter_value_t<Iterator>>
static collection_mutation serialize_collection_mutation(
        const tombstone& tomb,
        std::ranges::subrange<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + Adaptor::key_size(e) + Adaptor::value_size(e);
    };
    auto size = std::ranges::fold_left(cells, (size_t)4, element_size);
    size += 1;
    if (tomb) {
        size += sizeof(int64_t) + sizeof(int64_t);
    }
    managed_bytes ret(managed_bytes::initialized_later(), size);
    managed_bytes_mutable_view out(ret);
    write<uint8_t>(out, uint8_t(bool(tomb)));
    if (tomb) {
        write<int64_t>(out, tomb.timestamp);
        write<int64_t>(out, tomb.deletion_time.time_since_epoch().count());
    }
    auto writek = [&out] (auto& kv) {
        write<int32_t>(out, Adaptor::key_size(kv));
        Adaptor::write_key(kv, out);
    };
    auto writev = [&out] (auto& kv) {
        write<int32_t>(out, Adaptor::value_size(kv));
        Adaptor::write_value(kv, out);
    };
    // FIXME: overflow?
    write<int32_t>(out, std::ranges::distance(cells));
    for (auto&& kv : cells) {
        writek(kv);
        writev(kv);
    }
    return collection_mutation(std::move(ret));
}

namespace {

/// A key-value pair where the key is bytes-like and the value is an atomic_cell-like type
/// with a serialize() method returning managed_bytes_view.
template <typename T>
concept AtomicCellKV = requires(const T& kv) {
    { kv.first.size() } -> std::convertible_to<size_t>;
    { kv.second.serialize() } -> std::convertible_to<managed_bytes_view>;
};

struct atomic_cell_adaptor {
    static size_t key_size(const AtomicCellKV auto& v) { return v.first.size(); }
    static size_t value_size(const AtomicCellKV auto& v) { return v.second.serialize().size(); }

    static void write_key(const AtomicCellKV auto& v, managed_bytes_mutable_view& out) {
        write_fragmented(out, single_fragmented_view(v.first));
    }
    static void write_value(const AtomicCellKV auto& v, managed_bytes_mutable_view& out) {
        write_fragmented(out, v.second.serialize());
    }
};

}

collection_mutation collection_mutation_description::serialize() const {
    return serialize_collection_mutation<atomic_cell_adaptor>(tomb, std::ranges::subrange(cells.begin(), cells.end()));
}

collection_mutation collection_mutation_view_description::serialize() const {
    return serialize_collection_mutation<atomic_cell_adaptor>(tomb, std::ranges::subrange(cells.begin(), cells.end()));
}

namespace {

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

}

collection_mutation read_from_collection_cell_view(const abstract_type& type, const ser::collection_cell_view& collection) {
    auto tomb = collection.tomb();
    auto cells = collection.elements();
    return serialize_collection_mutation<serialized_cell_adaptor>(tomb, std::ranges::subrange(cells.begin(), cells.end()));
}

template <typename C>
requires std::is_base_of_v<abstract_type, std::remove_reference_t<C>>
static collection_mutation_view_description
merge(collection_mutation_view_description a, collection_mutation_view_description b, C&& key_type) {
    using element_type = std::pair<bytes_view, atomic_cell_view>;

    auto compare = [&] (const element_type& e1, const element_type& e2) {
        return key_type.less(e1.first, e2.first);
    };

    auto merge = [] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };

    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto cell_killed = [] (const std::optional<tombstone>& t) {
        return [&t] (const element_type& e) {
            if (!t) {
                return false;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t->timestamp < e.second.timestamp()) {
                return false;
            }
            return true;
            // FIXME: should we consider TTLs too?
        };
    };

    collection_mutation_view_description merged;
    merged.cells.reserve(a.cells.size() + b.cells.size());

    combine(a.cells.begin(), std::remove_if(a.cells.begin(), a.cells.end(), cell_killed(b.tomb)),
            b.cells.begin(), std::remove_if(b.cells.begin(), b.cells.end(), cell_killed(a.tomb)),
            std::back_inserter(merged.cells),
            compare,
            merge);
    merged.tomb = std::max(a.tomb, b.tomb);

    return merged;
}

collection_mutation merge(const abstract_type& type, collection_mutation_view a, collection_mutation_view b) {
    return a.with_deserialized(type, [&] (collection_mutation_view_description a_view) {
        return b.with_deserialized(type, [&] (collection_mutation_view_description b_view) {
            return visit(type, make_visitor(
            [&] (const collection_type_impl& ctype) {
                return merge(std::move(a_view), std::move(b_view), *ctype.name_comparator());
            },
            [&] (const user_type_impl& utype) {
                return merge(std::move(a_view), std::move(b_view), *short_type);
            },
            [] (const abstract_type& o) -> collection_mutation_view_description {
                throw std::runtime_error(format("collection_mutation merge: unknown type: {}", o.name()));
            }
            )).serialize();
        });
    });
}

template <typename C>
requires std::is_base_of_v<abstract_type, std::remove_reference_t<C>>
static collection_mutation_view_description
difference(collection_mutation_view_description a, collection_mutation_view_description b, C&& key_type)
{
    collection_mutation_view_description diff;
    diff.cells.reserve(std::max(a.cells.size(), b.cells.size()));

    auto it = b.cells.begin();
    for (auto&& c : a.cells) {
        while (it != b.cells.end() && key_type.less(it->first, c.first)) {
            ++it;
        }
        if (it == b.cells.end() || !key_type.equal(it->first, c.first)
            || compare_atomic_cell_for_merge(c.second, it->second) > 0) {

            auto cell = std::make_pair(c.first, c.second);
            diff.cells.emplace_back(std::move(cell));
        }
    }
    if (a.tomb > b.tomb) {
        diff.tomb = a.tomb;
    }

    return diff;
}

collection_mutation difference(const abstract_type& type, collection_mutation_view a, collection_mutation_view b)
{
    return a.with_deserialized(type, [&] (collection_mutation_view_description a_view) {
        return b.with_deserialized(type, [&] (collection_mutation_view_description b_view) {
            return visit(type, make_visitor(
            [&] (const collection_type_impl& ctype) {
                return difference(std::move(a_view), std::move(b_view), *ctype.name_comparator());
            },
            [&] (const user_type_impl& utype) {
                return difference(std::move(a_view), std::move(b_view), *short_type);
            },
            [] (const abstract_type& o) -> collection_mutation_view_description {
                throw std::runtime_error(format("collection_mutation difference: unknown type: {}", o.name()));
            }
            )).serialize();
        });
    });
}

template <typename F>
requires std::is_invocable_r_v<std::pair<bytes_view, atomic_cell_view>, F, collection_mutation_input_stream&>
static collection_mutation_view_description
deserialize_collection_mutation(collection_mutation_input_stream& in, F&& read_kv) {
    collection_mutation_view_description ret;

    auto has_tomb = in.read_trivial<uint8_t>();
    if (has_tomb) {
        auto ts = in.read_trivial<api::timestamp_type>();
        auto ttl = in.read_trivial<gc_clock::duration::rep>();
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }

    auto nr = in.read_trivial<uint32_t>();
    ret.cells.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        ret.cells.push_back(read_kv(in));
    }

    SCYLLA_ASSERT(in.empty());
    return ret;
}

collection_mutation_view_description
deserialize_collection_mutation(const abstract_type& type, collection_mutation_input_stream& in) {
    return visit(type, make_visitor(
    [&] (const collection_type_impl& ctype) {
        // value_comparator(), ugh
        return deserialize_collection_mutation(in, [] (collection_mutation_input_stream& in) {
            // FIXME: we could probably avoid the need for size
            auto ksize = in.read_trivial<uint32_t>();
            auto key = in.read_linearized(ksize);
            auto vsize = in.read_trivial<uint32_t>();
            auto value = atomic_cell_view::from_bytes(in.read_fragmented(vsize));
            return std::make_pair(key, value);
        });
    },
    [&] (const user_type_impl& utype) {
        return deserialize_collection_mutation(in, [] (collection_mutation_input_stream& in) {
            // FIXME: we could probably avoid the need for size
            auto ksize = in.read_trivial<uint32_t>();
            auto key = in.read_linearized(ksize);
            auto vsize = in.read_trivial<uint32_t>();
            auto value = atomic_cell_view::from_bytes(in.read_fragmented(vsize));
            return std::make_pair(key, value);
        });
    },
    [&] (const abstract_type& o) -> collection_mutation_view_description {
        throw std::runtime_error(format("deserialize_collection_mutation: unknown type {}", o.name()));
    }
    ));
}
