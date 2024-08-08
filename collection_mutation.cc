/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "types/collection.hh"
#include "types/user.hh"
#include "concrete_types.hh"
#include "mutation/mutation_partition.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "combine.hh"

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

collection_mutation::collection_mutation(const abstract_type& type, collection_mutation_view v)
    : _data(v.data) {}

collection_mutation::collection_mutation(const abstract_type& type, managed_bytes data)
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
        auto value = atomic_cell_view::from_bytes(type, in.read_fragmented(vsize));
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
        auto value = atomic_cell_view::from_bytes(type, in.read_fragmented(vsize));
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
    } else if (tomb.deletion_time < gc_before && can_gc(tomb)) {
        purged_tomb = tomb;
        tomb = tombstone();
    }
    t.apply(base_tomb.regular());
    utils::chunked_vector<std::pair<bytes, atomic_cell>> survivors;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> losers;
    for (auto&& name_and_cell : cells) {
        atomic_cell& cell = name_and_cell.second;
        auto cannot_erase_cell = [&] {
            return cell.deletion_time() >= gc_before || !can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
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

template <typename Iterator>
static collection_mutation serialize_collection_mutation(
        const abstract_type& type,
        const tombstone& tomb,
        boost::iterator_range<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + e.first.size() + e.second.serialize().size();
    };
    auto size = accumulate(cells, (size_t)4, element_size);
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
    auto writek = [&out] (bytes_view v) {
        write<int32_t>(out, v.size());
        write_fragmented(out, single_fragmented_view(v));
    };
    auto writev = [&out] (managed_bytes_view v) {
        write<int32_t>(out, v.size());
        write_fragmented(out, v);
    };
    // FIXME: overflow?
    write<int32_t>(out, boost::distance(cells));
    for (auto&& kv : cells) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        writek(k);

        writev(v.serialize());
    }
    return collection_mutation(type, ret);
}

collection_mutation collection_mutation_description::serialize(const abstract_type& type) const {
    return serialize_collection_mutation(type, tomb, boost::make_iterator_range(cells.begin(), cells.end()));
}

collection_mutation collection_mutation_view_description::serialize(const abstract_type& type) const {
    return serialize_collection_mutation(type, tomb, boost::make_iterator_range(cells.begin(), cells.end()));
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
            )).serialize(type);
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
            )).serialize(type);
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
        return deserialize_collection_mutation(in, [&ctype] (collection_mutation_input_stream& in) {
            // FIXME: we could probably avoid the need for size
            auto ksize = in.read_trivial<uint32_t>();
            auto key = in.read_linearized(ksize);
            auto vsize = in.read_trivial<uint32_t>();
            auto value = atomic_cell_view::from_bytes(*ctype.value_comparator(), in.read_fragmented(vsize));
            return std::make_pair(key, value);
        });
    },
    [&] (const user_type_impl& utype) {
        return deserialize_collection_mutation(in, [&utype] (collection_mutation_input_stream& in) {
            // FIXME: we could probably avoid the need for size
            auto ksize = in.read_trivial<uint32_t>();
            auto key = in.read_linearized(ksize);
            auto vsize = in.read_trivial<uint32_t>();
            auto value = atomic_cell_view::from_bytes(*utype.type(deserialize_field_index(key)), in.read_fragmented(vsize));
            return std::make_pair(key, value);
        });
    },
    [&] (const abstract_type& o) -> collection_mutation_view_description {
        throw std::runtime_error(format("deserialize_collection_mutation: unknown type {}", o.name()));
    }
    ));
}
