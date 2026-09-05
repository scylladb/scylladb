/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "schema_mapping.hh"

#include <algorithm>
#include <stdexcept>
#include <unordered_map>

namespace sstables::parquet {

const char* to_string(folding_level l) {
    switch (l) {
    case folding_level::verbatim:   return "L0";
    case folding_level::row_folded: return "L1";
    case folding_level::uniform:    return "L2";
    case folding_level::logical:    return "L3";
    }
    return "?";
}

namespace {

// Timestamp deltas, computed so they cannot overflow.
//
// Cell timestamps legitimately span the whole int64 range -- the conformance
// corpus deliberately uses values next to both ends -- and `a - b` on two of
// those is signed overflow, which is undefined behaviour rather than a large
// number. Doing the arithmetic unsigned makes the wraparound defined, and adding
// it back the same way recovers the original exactly. mx relies on the same trick
// for its own delta encoding.
inline int64_t ts_delta(int64_t a, int64_t b) {
    return int64_t(uint64_t(a) - uint64_t(b));
}
inline int64_t ts_undelta(int64_t base, int64_t delta) {
    return int64_t(uint64_t(base) + uint64_t(delta));
}

// The timestamp a row folds to: the one most of its live cells agree on.
// Choosing the mode (rather than, say, the max) minimises how many cells need
// an exception entry.
std::optional<int64_t> modal_timestamp(const row& r) {
    std::unordered_map<int64_t, int> freq;
    for (const auto& [i, c] : r.cells) {
        if (c.v || !c.live) { ++freq[c.timestamp]; }
    }
    if (freq.empty()) { return std::nullopt; }
    int64_t best = freq.begin()->first;
    int bestn = 0;
    for (auto& [ts, n] : freq) {
        if (n > bestn || (n == bestn && ts < best)) { best = ts; bestn = n; }
    }
    return best;
}

// The two ways a cell can carry a local_deletion_time, told apart by whether it has a
// value. They are disjoint, which is what lets the deletion time be folded while the
// expiry stays per column.
//
//   dead cell   -- a tombstone. No value, and an ldt that is the deletion time.
//   live expiry -- a live cell with a TTL, whose ldt is when it will expire.
//
// `is_dead_cell` is deliberately exactly the set the per-column layout round-tripped as
// dead: reassemble() decided deadness by "no value, but `__ldt_<col>` has one". So the
// fold preserves that set precisely rather than widening it. A cell with no value and no
// deletion time is not representable at L1 in either layout -- a tombstone with no
// deletion time has nothing to store and no meaning -- and the write path cannot produce
// one: writer_impl sets local_deletion_time on every dead cell it builds.
inline bool is_dead_cell(const cell& c) {
    return !c.v.has_value() && c.local_deletion_time.has_value();
}
inline bool is_live_expiry(const cell& c) {
    return c.v.has_value() && c.local_deletion_time.has_value();
}

// The deletion time a row folds to: the one most of its dead cells agree on. Same choice
// as modal_timestamp() and for the same reason -- the mode minimises how many cells need
// an exception entry. A bind-NULL INSERT is one statement, so all of its tombstones share
// one deletion time, the mode is the only value present, and the exception channel stays
// empty. Ties break to the smaller value so the choice is deterministic.
std::optional<int32_t> modal_deletion_time(const row& r) {
    std::unordered_map<int32_t, int> freq;
    for (const auto& [i, c] : r.cells) {
        if (is_dead_cell(c)) { ++freq[*c.local_deletion_time]; }
    }
    if (freq.empty()) { return std::nullopt; }
    int32_t best = freq.begin()->first;
    int bestn = 0;
    for (auto& [ldt, n] : freq) {
        if (n > bestn || (n == bestn && ldt < best)) { best = ldt; bestn = n; }
    }
    return best;
}

void push_value(column_data& cd, phys_type pt, const value& v) {
    switch (pt) {
    case phys_type::int32:      cd.i32.push_back(std::get<int32_t>(v)); break;
    case phys_type::int64:      cd.i64.push_back(std::get<int64_t>(v)); break;
    case phys_type::dbl:        cd.f64.push_back(std::get<double>(v)); break;
    case phys_type::byte_array: cd.str.push_back(std::get<std::string>(v)); break;
    default: throw std::runtime_error("shred: unsupported physical type");
    }
}

void push_absent(column_data& cd, phys_type pt) {
    switch (pt) {
    case phys_type::int32:      cd.i32.push_back(0); break;
    case phys_type::int64:      cd.i64.push_back(0); break;
    case phys_type::dbl:        cd.f64.push_back(0.0); break;
    case phys_type::byte_array: cd.str.emplace_back(); break;
    default: throw std::runtime_error("shred: unsupported physical type");
    }
}

value read_value(const column_data& cd, phys_type pt, size_t i) {
    switch (pt) {
    case phys_type::int32:      return cd.i32[i];
    case phys_type::int64:      return cd.i64[i];
    case phys_type::dbl:        return cd.f64[i];
    case phys_type::byte_array: return cd.str[i];
    default: throw std::runtime_error("reassemble: unsupported physical type");
    }
}

// Zigzag varint, used for the exception deltas.
void put_zigzag(std::string& o, int64_t v) {
    uint64_t u = (uint64_t(v) << 1) ^ uint64_t(v >> 63);
    while (u >= 0x80) { o.push_back(char(uint8_t(u) | 0x80)); u >>= 7; }
    o.push_back(char(uint8_t(u)));
}
int64_t get_zigzag(const std::string& s, size_t& i) {
    uint64_t u = 0; int shift = 0;
    while (i < s.size()) {
        uint8_t b = uint8_t(s[i++]);
        u |= uint64_t(b & 0x7F) << shift;
        if (!(b & 0x80)) { break; }
        shift += 7;
    }
    return int64_t(u >> 1) ^ -int64_t(u & 1);
}

} // namespace

// The layer decisions map_schema derives from the data. Split out so that the
// reader can recover the same flags from a file's leaf names and rebuild an
// identical mapped_schema through the one builder below -- two ways in, one
// layout, so writer and reader cannot drift apart.
schema_flags scan_rows(const std::vector<cql_column>& cols, const std::vector<row>& rows,
                       leaf_set ls) {
    schema_flags f;
    size_t n_regular = 0;
    for (const auto& c : cols) { if (c.kind == column_kind::regular) { ++n_regular; } }
    f.col_diverges.assign(n_regular, false);

    for (const auto& r : rows) {
        auto mt = modal_timestamp(r);
        if (mt) {
            if (!f.single_ts) { f.single_ts = mt; }
            else if (*f.single_ts != *mt) { f.all_same_ts = false; }
        }
        if (r.marker) {
            f.any_marker = true;
            if (r.marker->ttl) { f.any_marker_ttl = true; }
        }
        if (r.row_del)  { f.any_row_del = true; }
        if (r.part_del) { f.any_part_del = true; }
        if (r.no_ck)    { f.any_no_ck = true; }
        if (r.rtc)      { f.any_rtc = true; }
        for (const auto& [ci, c] : r.cells) {
            if (c.ttl) { f.any_ttl = true; }
            if (is_live_expiry(c)) { f.any_live_expiry = true; }
            if (is_dead_cell(c))   { f.any_dead_cell = true; }
            if (mt && c.timestamp != *mt) {
                f.col_diverges[ci] = true;
                f.all_same_ts = false;
            }
        }
    }
    if (ls == leaf_set::conservative) {
        // Every optional metadata leaf, whether the rows need it or not. An incremental
        // writer has to fix the leaf set before its first row group, and by then it has
        // seen only a prefix of the rows -- so anything a later row might need has to
        // exist already. Unused leaves are all-null and cost a fixed ~225 B each.
        f.any_ttl = f.any_live_expiry = f.any_dead_cell = true;
        f.any_marker = f.any_marker_ttl = true;
        f.any_row_del = f.any_part_del = f.any_rtc = true;
        // Forces the divergence channel on: without it a later row whose cell timestamp
        // differs from its row timestamp would have nowhere to record the difference.
        f.all_same_ts = false;
        for (size_t k = 0; k < f.col_diverges.size(); ++k) { f.col_diverges[k] = true; }
    }

    return f;
}

// The tree a flat leaf list implies: a root and one child per leaf. Collections
// will insert groups here instead of a bare leaf.
// Build the tree the leaves imply, inserting a MAP group wherever a regular
// column is a non-frozen collection. This is the only place that knows the
// nesting, so it is also where the leaves' Dremel levels come from -- they are
// filled in afterwards by walk_leaves(), the same function the reader uses.
static void build_tree(mapped_schema& ms, const std::vector<cql_column>& cols) {
    std::vector<size_t> reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].kind == column_kind::regular) { reg_idx.push_back(i); }
    }
    // Which leaf starts each collection group, and what the group is called.
    std::map<size_t, std::string> group_at;
    std::map<size_t, size_t> group_leaves;      // five leaves, or six for a counter
    for (size_t k = 0; k < ms.value_is_collection.size(); ++k) {
        if (!ms.value_is_collection[k]) { continue; }
        group_at[ms.value_leaf[k]] = cols[reg_idx[k]].name;
        group_leaves[ms.value_leaf[k]] = ms.value_is_counter[k] ? 6 : 5;
    }

    ms.tree.clear();
    std::vector<format::schema_element> body;
    int32_t top_children = 0;
    for (size_t i = 0; i < ms.columns.size(); ) {
        auto g = group_at.find(i);
        if (g != group_at.end()) {
            format::schema_element map_el;
            map_el.name = g->second;
            map_el.repetition_type = repetition::optional;
            // Deliberately NOT annotated as a Parquet MAP. A MAP's key_value group must have one
            // or two children and ours has five -- key, value, __ts, __ttl, __ldt -- or six for a
            // counter, because each element carries its own cell metadata. parquet-cpp enforces
            // that arity and refuses to open the whole file: "Key-value map node must have 1 or 2
            // child elements. Found: 5". So the annotation made every sstable containing a
            // non-frozen collection or a counter unreadable by any parquet-cpp-based reader,
            // which is the opposite of what it was for.
            //
            // Without it the group is an ordinary nested structure -- an optional group holding a
            // repeated group of typed fields -- which is valid Parquet and which readers resolve
            // as a list of structs. The Scylla side is unaffected: our reader works from the tree
            // and the levels, not from the annotation.
            map_el.num_children = 1;
            body.push_back(map_el);

            const size_t nleaves = group_leaves.at(i);
            format::schema_element kv;
            kv.name = "key_value";
            kv.repetition_type = repetition::repeated;
            kv.num_children = int32_t(nleaves);
            body.push_back(kv);

            for (size_t j = 0; j < nleaves; ++j) {
                const auto& c = ms.columns[i + j];
                format::schema_element e;
                e.type = c.type;
                e.repetition_type = c.rep;
                e.name = c.name;
                body.push_back(e);
            }
            ++top_children;
            i += nleaves;
            continue;
        }
        const auto& c = ms.columns[i];
        format::schema_element e;
        e.type = c.type;
        e.repetition_type = c.rep;
        e.name = c.name;
        e.converted_type = c.converted_type;
        body.push_back(e);
        ++top_children;
        ++i;
    }

    format::schema_element root;
    root.name = "schema";
    root.num_children = top_children;
    ms.tree.push_back(root);
    ms.tree.insert(ms.tree.end(), body.begin(), body.end());

    // Levels and paths, from the tree that was just built.
    format::file_metadata probe;
    probe.schema = ms.tree;
    auto leaves = format::walk_leaves(probe);
    if (leaves.size() != ms.columns.size()) {
        throw std::runtime_error("schema tree produced " + std::to_string(leaves.size()) +
                                 " leaves but the mapping has " + std::to_string(ms.columns.size()));
    }
    for (size_t i = 0; i < leaves.size(); ++i) {
        // Position is the only correspondence between the mapping's leaf list and
        // the tree's, and everything downstream relies on it: the levels below, and
        // the per-leaf encoding hints write_rows() hands the writer. A count match
        // does not prove an order match, so check the names too -- a silent
        // mismatch would attach one column's levels and encoding to another.
        if (!leaves[i].path.empty() && leaves[i].path.back() != ms.columns[i].name) {
            throw std::runtime_error("leaf " + std::to_string(i) + " is '" +
                                     leaves[i].path.back() + "' but the mapping calls it '" +
                                     ms.columns[i].name + "'");
        }
        ms.columns[i].max_def = leaves[i].max_def;
        ms.columns[i].max_rep = leaves[i].max_rep;
        ms.columns[i].path    = leaves[i].path;
    }
}

mapped_schema build_mapped_schema(const std::vector<cql_column>& cols,
                                  folding_level requested,
                                  const schema_flags& flags,
                                  exception_encoding exc,
                                  const std::map<std::string, format::encoding>& encoding_overrides) {
    mapped_schema ms;
    ms.level = requested;
    ms.exc_encoding = exc;

    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }
    ms.n_key = key_idx.size();
    ms.n_regular = reg_idx.size();
    ms.ts_exc_index.assign(reg_idx.size(), std::nullopt);
    ms.meta_base_index.assign(reg_idx.size(), std::nullopt);
    ms.value_leaf.assign(reg_idx.size(), 0);
    ms.value_is_collection.assign(reg_idx.size(), false);
    ms.value_is_counter.assign(reg_idx.size(), false);
    ms.ct_ts_index.assign(reg_idx.size(), std::nullopt);
    ms.ct_ldt_index.assign(reg_idx.size(), std::nullopt);
    ms.l1_ttl_index.assign(reg_idx.size(), std::nullopt);
    ms.l1_ldt_index.assign(reg_idx.size(), std::nullopt);

    const bool any_ttl = flags.any_ttl;
    const bool any_live_expiry = flags.any_live_expiry, any_dead_cell = flags.any_dead_cell;
    const std::vector<bool>& col_diverges = flags.col_diverges;
    const std::optional<int64_t>& single_ts = flags.single_ts;

    // L2 keeps one timestamp for the whole row group and no per-cell metadata at all, so
    // *either* kind of deletion time breaks its precondition -- as before, when the two
    // were one flag.
    if (requested == folding_level::uniform &&
        !(flags.all_same_ts && !any_ttl && !any_live_expiry && !any_dead_cell &&
          !flags.any_marker && !flags.any_row_del && !flags.any_part_del &&
          !flags.any_no_ck && !flags.any_rtc)) {
        // Precondition broken -- fall back rather than lose information.
        ms.level = folding_level::row_folded;
    }

    // ---- key columns, always required and always present
    //
    // Encoding hints here are structural, not type-based, which is the distinction §10.3f is about.
    // Both rules below rest on a property of *key order*, not on a guess about what a type usually
    // looks like:
    //
    //   * bigint and timestamp keys get DELTA_BINARY_PACKED because a clustering key ascends.
    //   * a **text or blob clustering key** gets DELTA_BYTE_ARRAY, because rows arrive in clustering
    //     order, so within a partition the values are sorted and adjacent ones share leading bytes.
    //     Front coding stores the shared run once. Measured on the encoder: 35 % of PLAIN on sorted
    //     keys, and still 70 % when nothing is shared, because PLAIN spends a fixed four bytes per
    //     value on lengths where the delta stream packs them.
    //
    // Deliberately *not* applied to a text partition key. Partitions are stored in token order,
    // which is not key order, so consecutive partition-key values share nothing systematic and the
    // gain would be down to luck. That is exactly the kind of type-based guess that lost in §10.3f.
    //
    // The dictionary still wins wherever values repeat -- it stores each distinct value once, where
    // this stores every occurrence -- and the writer's own repeat-ratio check continues to take
    // precedence over this hint. This is for the case a dictionary handles badly: a key with many
    // distinct values that happen to be ordered.
    for (size_t i : key_idx) {
        std::optional<encoding> hint;
        if (cols[i].type == cql_type::bigint || cols[i].type == cql_type::timestamp) {
            hint = encoding::delta_binary_packed;
        } else if (cols[i].kind == column_kind::clustering_key
                   && (cols[i].type == cql_type::text || cols[i].type == cql_type::blob)) {
            hint = encoding::delta_byte_array;
        }
        if (auto it = encoding_overrides.find(cols[i].name); it != encoding_overrides.end()) {
            hint = it->second;
        }
        ms.columns.push_back(column_spec{cols[i].name, phys_of(cols[i].type),
                                         repetition::required, converted_of(cols[i].type), hint});
    }

    // ---- regular columns
    for (size_t k = 0; k < reg_idx.size(); ++k) {
        const auto& c = cols[reg_idx[k]];
        // Regular columns default to PLAIN and let zstd do the work.
        //
        // Both of the obvious type-based rules were tried on real data and both
        // LOST (docs/dev/parquet-storage-format.md section 10.3f):
        //   BYTE_STREAM_SPLIT on doubles      2 562 753 -> 3 968 805 bytes
        //   DELTA_BINARY_PACKED on bigints    2 562 753 -> 2 569 567 bytes
        // Transposing or delta-ing destroys the whole-value repetition that zstd
        // was already exploiting -- money-shaped doubles and low-cardinality ids
        // repeat exactly, and those repeats compress better than any residual.
        //
        // The key columns and __ts keep DELTA_BINARY_PACKED (set elsewhere)
        // because they are monotonic by construction, where it demonstrably wins.
        // Choosing per column from the data is the real answer; see open
        // question 9.
        ms.value_leaf[k] = ms.columns.size();
        if (c.multi_cell) {
            // A non-frozen collection becomes five leaves under a MAP group; the
            // levels are filled in by the tree builder, which is the only place
            // that knows the nesting.
            ms.value_is_collection[k] = true;
            ms.value_is_counter[k] = c.counter;
            ms.columns.push_back(column_spec{"key", phys_type::byte_array,
                                             repetition::required, std::nullopt, std::nullopt});
            // A counter shard's value is not an opaque blob: it is a count and a logical clock.
            // Emitting them as two INT64 leaves makes the column interpretable by any Parquet
            // reader instead of only by something that knows Scylla's packing (design doc 5.2).
            ms.columns.push_back(column_spec{"value",
                                             c.counter ? phys_type::int64 : phys_type::byte_array,
                                             repetition::optional, std::nullopt, std::nullopt});
            ms.columns.push_back(column_spec{"__ts", phys_type::int64,
                                             repetition::required, std::nullopt, std::nullopt});
            ms.columns.push_back(column_spec{"__ttl", phys_type::int32,
                                             repetition::optional, std::nullopt, std::nullopt});
            ms.columns.push_back(column_spec{"__ldt", phys_type::int32,
                                             repetition::optional, std::nullopt, std::nullopt});
            if (c.counter) {
                // Appended rather than placed after `value` so the vcol+N offsets used by the
                // shred and reassemble paths keep their meaning; see mapped_schema.
                ms.columns.push_back(column_spec{"clock", phys_type::int64,
                                                 repetition::optional, std::nullopt, std::nullopt});
            }
            continue;
        }
        // Regular columns default to PLAIN (see the note above on why type-based rules lost), but an
        // operator override applies here too -- this is the case it is most useful for, since a
        // scan-only table may want front coding or a forced dictionary on a payload column that the
        // structural rules deliberately leave alone.
        std::optional<encoding> reg_hint;
        if (auto it = encoding_overrides.find(c.name); it != encoding_overrides.end()) {
            reg_hint = it->second;
        }
        ms.columns.push_back(column_spec{c.name, phys_of(c.type), repetition::optional,
                                         converted_of(c.type), reg_hint});
    }

    for (size_t k = 0; k < reg_idx.size(); ++k) {
        if (!ms.value_is_collection[k]) { continue; }
        // The collection-wide tombstone belongs to the row, not to an element,
        // so it cannot live inside the repeated group.
        const auto& nm = cols[reg_idx[k]].name;
        ms.ct_ts_index[k] = ms.columns.size();
        ms.columns.push_back(column_spec{"__ct_ts_" + nm, phys_type::int64,
                                         repetition::optional, std::nullopt, std::nullopt});
        ms.ct_ldt_index[k] = ms.columns.size();
        ms.columns.push_back(column_spec{"__ct_ldt_" + nm, phys_type::int32,
                                         repetition::optional, std::nullopt, std::nullopt});
    }

    if (ms.level == folding_level::logical) {
        // Nothing beyond the user's own columns. Deliberately no __ts: the point
        // of L3 is a file an analytics reader sees as the plain CQL table.
        build_tree(ms, cols);
        return ms;
    }

    if (ms.level == folding_level::verbatim) {
        // Four metadata leaves per regular column, unconditionally. This is the
        // 2020 mapping and it is here to be measured against, not used.
        for (size_t k = 0; k < reg_idx.size(); ++k) {
            if (ms.value_is_collection[k]) { continue; }   // metadata is per element
            ms.meta_base_index[k] = ms.columns.size();
            const auto& nm = cols[reg_idx[k]].name;
            ms.columns.push_back({"__live_" + nm, phys_type::int32, repetition::required, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ts_"   + nm, phys_type::int64, repetition::required, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ttl_"  + nm, phys_type::int32, repetition::optional, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ldt_"  + nm, phys_type::int32, repetition::optional, std::nullopt, std::nullopt});
        }
    } else if (ms.level == folding_level::row_folded) {
        ms.ts_index = ms.columns.size();
        // PLAIN, deliberately, even though a folded row timestamp is exactly the
        // monotonic-ish shape DELTA_BINARY_PACKED is for.
        //
        // Asking for delta here makes test_pq_corpus_shaped_schema fail: cell write
        // timestamps near int64's minimum come back with the top bit relocated
        // (-2**63 + 74 reads back as 2**57 + 74), so the low bits survive and the
        // high ones do not. That is *not* the codec -- it round-trips int64 extremes
        // exactly and UBSan-clean, single values and 200-value runs alike, and the
        // mapping's leaf order is now asserted against the tree's -- so something in
        // the interaction is still unaccounted for. Enabling it would trade a real
        // correctness failure for about 4 KB.
        //
        // The measured win from encoding hints is elsewhere and unaffected: on a
        // time-series table the *clustering key* goes from 912 882 to 37 445 bytes,
        // 25 % of the whole file, where this column is 3 772. See
        // docs/dev/parquet-storage-format.md section 10.1g and open question 13.
        ms.columns.push_back({"__ts", phys_type::int64, repetition::required, std::nullopt,
                              std::nullopt});
        bool any_divergence = false;
        for (bool b : col_diverges) { any_divergence = any_divergence || b; }
        if (any_divergence) {
            if (ms.exc_encoding == exception_encoding::sparse) {
                // Two leaves, independent of table width: a bitmap of which
                // columns diverge in this row, and the packed deltas.
                ms.tsx_mask_index = ms.columns.size();
                ms.columns.push_back({"__tsx_mask", phys_type::byte_array,
                                      repetition::optional, std::nullopt, std::nullopt});
                ms.tsx_vals_index = ms.columns.size();
                ms.columns.push_back({"__tsx_vals", phys_type::byte_array,
                                      repetition::optional, std::nullopt, std::nullopt});
            } else {
                // One leaf per diverging column -- the shape that measured badly.
                for (size_t k = 0; k < reg_idx.size(); ++k) {
                    if (col_diverges[k]) {
                        ms.ts_exc_index[k] = ms.columns.size();
                        ms.columns.push_back({"__tsx_" + cols[reg_idx[k]].name, phys_type::int64,
                                              repetition::optional, std::nullopt, std::nullopt});
                    }
                }
            }
        }
        if (any_ttl) {
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (ms.value_is_collection[k]) { continue; }
                ms.l1_ttl_index[k] = ms.columns.size();
                ms.columns.push_back({"__ttl_" + cols[reg_idx[k]].name, phys_type::int32,
                                      repetition::optional, std::nullopt, std::nullopt});
            }
        }
        // Per column, and only for the expiry of a *live* cell. A TTL is a property of the
        // individual cell -- two columns of one row can expire at different times -- so
        // there is nothing row-shaped to fold here.
        if (any_live_expiry) {
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (ms.value_is_collection[k]) { continue; }
                ms.l1_ldt_index[k] = ms.columns.size();
                ms.columns.push_back({"__ldt_" + cols[reg_idx[k]].name, phys_type::int32,
                                      repetition::optional, std::nullopt, std::nullopt});
            }
        }
        // The folded deletion channel: three leaves for the whole table, where the
        // per-column layout spent one leaf per column. See mapped_schema for what each
        // carries and why __dmask cannot be dropped.
        if (any_dead_cell) {
            ms.ldt_index = ms.columns.size();
            ms.columns.push_back({"__ldt", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.dmask_index = ms.columns.size();
            ms.columns.push_back({"__dmask", phys_type::byte_array, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.ldtx_mask_index = ms.columns.size();
            ms.columns.push_back({"__ldtx_mask", phys_type::byte_array, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.ldtx_vals_index = ms.columns.size();
            ms.columns.push_back({"__ldtx_vals", phys_type::byte_array, repetition::optional,
                                  std::nullopt, std::nullopt});
        }
        // Row marker, row tombstone, partition tombstone. Each group appears only
        // if some row uses it, so a table that never deletes pays nothing.
        if (flags.any_marker) {
            ms.rm_index = ms.columns.size();
            ms.columns.push_back({"__rm", phys_type::int64, repetition::optional,
                                  std::nullopt, std::nullopt});
            if (flags.any_marker_ttl) {
                ms.rm_ttl_index = ms.columns.size();
                ms.columns.push_back({"__rm_ttl", phys_type::int32, repetition::optional,
                                      std::nullopt, std::nullopt});
                ms.rm_ldt_index = ms.columns.size();
                ms.columns.push_back({"__rm_ldt", phys_type::int32, repetition::optional,
                                      std::nullopt, std::nullopt});
            }
        }
        if (flags.any_row_del) {
            ms.rt_ts_index = ms.columns.size();
            ms.columns.push_back({"__rt_ts", phys_type::int64, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rt_ldt_index = ms.columns.size();
            ms.columns.push_back({"__rt_ldt", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
            // The regular half, which is usually equal to the shadowable one and
            // therefore compresses away, but is not always.
            ms.rtr_ts_index = ms.columns.size();
            ms.columns.push_back({"__rtr_ts", phys_type::int64, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rtr_ldt_index = ms.columns.size();
            ms.columns.push_back({"__rtr_ldt", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
        }
        if (flags.any_no_ck) {
            ms.no_ck_index = ms.columns.size();
            ms.columns.push_back({"__no_ck", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
        }
        if (flags.any_rtc) {
            // Presence of __rtc_w is what marks a row as a range tombstone
            // change; the weight itself is legitimately 0 for an exact bound.
            ms.rtc_w_index = ms.columns.size();
            ms.columns.push_back({"__rtc_w", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rtc_reg_index = ms.columns.size();
            ms.columns.push_back({"__rtc_reg", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rtc_len_index = ms.columns.size();
            ms.columns.push_back({"__rtc_len", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rtc_ts_index = ms.columns.size();
            ms.columns.push_back({"__rtc_ts", phys_type::int64, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.rtc_ldt_index = ms.columns.size();
            ms.columns.push_back({"__rtc_ldt", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
        }
        if (flags.any_part_del) {
            ms.pt_ts_index = ms.columns.size();
            ms.columns.push_back({"__pt_ts", phys_type::int64, repetition::optional,
                                  std::nullopt, std::nullopt});
            ms.pt_ldt_index = ms.columns.size();
            ms.columns.push_back({"__pt_ldt", phys_type::int32, repetition::optional,
                                  std::nullopt, std::nullopt});
        }

        ms.uniform_ts = std::nullopt;
        // Record which optional groups exist so reassemble() can invert.
        ms.meta_base_index.assign(reg_idx.size(), std::nullopt);
    } else {
        ms.uniform_ts = single_ts.value_or(0);
    }
    build_tree(ms, cols);
    return ms;
}

mapped_schema map_schema(const std::vector<cql_column>& cols,
                         folding_level requested,
                         const std::vector<row>& rows,
                         exception_encoding exc,
                         leaf_set ls,
                         const std::map<std::string, format::encoding>& encoding_overrides) {
    return build_mapped_schema(cols, requested, scan_rows(cols, rows, ls), exc, encoding_overrides);
}

mapped_schema recover_mapped_schema(const file_metadata& fm,
                                    const std::vector<cql_column>& cols) {
    // Leaf names as the file itself declares them.
    std::vector<std::string> leaves;
    for (size_t i = 1; i < fm.schema.size(); ++i) {
        if (fm.schema[i].is_leaf()) { leaves.push_back(fm.schema[i].name); }
    }
    auto has = [&] (const std::string& n) {
        return std::find(leaves.begin(), leaves.end(), n) != leaves.end();
    };

    const std::string* lvl = fm.kv("scylla.folding_level");
    if (!lvl) {
        throw std::runtime_error("parquet: file has no scylla.folding_level; "
                                 "not written by this mapping");
    }
    folding_level level;
    if      (*lvl == "L0") { level = folding_level::verbatim; }
    else if (*lvl == "L1") { level = folding_level::row_folded; }
    else if (*lvl == "L2") { level = folding_level::uniform; }
    else if (*lvl == "L3") { level = folding_level::logical; }
    else { throw std::runtime_error("parquet: unknown folding level '" + *lvl + "'"); }

    if (level == folding_level::logical) {
        // L3 discards write times, TTLs and deletions at write time. There is
        // nothing to reassemble; refusing here rather than returning plausible
        // rows with invented metadata.
        throw std::runtime_error("parquet: L3 files are export-only and cannot be read back");
    }

    std::vector<size_t> reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].kind == column_kind::regular) { reg_idx.push_back(i); }
    }

    schema_flags f;
    f.col_diverges.assign(reg_idx.size(), false);
    auto exc = exception_encoding::sparse;

    if (level == folding_level::row_folded) {
        if (has("__tsx_mask")) {
            exc = exception_encoding::sparse;
            // The sparse channel is two leaves for the whole table, so the file
            // cannot say which columns diverge -- and does not need to: the
            // per-row bitmap carries that. Any non-empty col_diverges makes the
            // builder emit the pair, which is all that matters for the layout.
            f.col_diverges.assign(reg_idx.size(), true);
        } else {
            bool any = false;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (has("__tsx_" + cols[reg_idx[k]].name)) { f.col_diverges[k] = true; any = true; }
            }
            if (any) { exc = exception_encoding::per_column; }
        }
        // The __ttl_/__ldt_ groups are all-or-nothing: the builder emits one
        // leaf per regular column or none at all, so testing the first is
        // enough, and the leaf-count check below catches any disagreement.
        // Probe a *scalar* regular column: a collection never gets these leaves
        // (its per-element metadata lives inside the group), so probing the first
        // regular column infers "no TTLs" for any table whose first regular column
        // happens to be a collection -- and then the recovered tree has the wrong
        // number of leaves.
        for (size_t k = 0; k < reg_idx.size(); ++k) {
            if (cols[reg_idx[k]].multi_cell) { continue; }
            f.any_ttl = has("__ttl_" + cols[reg_idx[k]].name);
            // `__ldt_<col>` means different things in the two layouts, and the file says
            // which by whether the folded channel is there:
            //
            //   __dmask present  -- written with the fold. __ldt_<col>, if it exists at
            //                       all, holds only live cells' expiry times.
            //   __dmask absent   -- written before the fold. __ldt_<col> holds both, and
            //                       reassemble()'s per-column clause reads the dead ones.
            //
            // Either way the flag reproduces the same leaf *layout*, which is all the
            // builder needs; the leaf-name check at the end of this function is what
            // proves the reproduction was exact.
            f.any_live_expiry = has("__ldt_" + cols[reg_idx[k]].name);
            break;
        }
        f.any_dead_cell  = has("__dmask");
        f.any_marker     = has("__rm");
        f.any_marker_ttl = has("__rm_ttl");
        f.any_row_del    = has("__rt_ts");
        f.any_part_del   = has("__pt_ts");
        f.any_no_ck      = has("__no_ck");
        f.any_rtc        = has("__rtc_w");
    } else if (level == folding_level::uniform) {
        f.all_same_ts = true;
        const std::string* u = fm.kv("scylla.uniform_timestamp");
        if (!u) { throw std::runtime_error("parquet: L2 file without scylla.uniform_timestamp"); }
        f.single_ts = int64_t(std::stoll(*u));
    }

    auto ms = build_mapped_schema(cols, level, f, exc);

    // The recovered layout must reproduce the file exactly. If it does not, the
    // schema we were handed is not the schema this file was written with, and
    // decoding would silently read the wrong column into the wrong field.
    if (ms.columns.size() != leaves.size()) {
        throw std::runtime_error("parquet: recovered " + std::to_string(ms.columns.size()) +
                                 " leaves but file has " + std::to_string(leaves.size()));
    }
    for (size_t i = 0; i < leaves.size(); ++i) {
        if (ms.columns[i].name != leaves[i]) {
            throw std::runtime_error("parquet: leaf " + std::to_string(i) + " is '" + leaves[i] +
                                     "' but the schema says '" + ms.columns[i].name + "'");
        }
    }
    return ms;
}

// One row's worth of a collection column: five leaves under a repeated group, so
// a variable number of slots rather than one per row. The definition levels are
// per leaf, because `key` and `__ts` are required inside the group (max_def 2)
// while `value`, `__ttl` and `__ldt` are optional (max_def 3):
//
//   0  the whole collection is absent from this row
//   1  present but with no elements
//   2  an element exists; for an optional leaf, its own value is null
//   3  an optional leaf's value is present
static void shred_collection(std::vector<column_data>& out, const mapped_schema& ms,
                             size_t k, size_t vcol, const row& r) {
    auto& l_key = out[vcol];
    auto& l_val = out[vcol + 1];
    auto& l_ts  = out[vcol + 2];
    auto& l_ttl = out[vcol + 3];
    auto& l_ldt = out[vcol + 4];
    // Counters carry a sixth leaf; see mapped_schema::value_is_counter.
    const bool counter = ms.value_is_counter[k];
    column_data* l_clock = counter ? &out[vcol + 5] : nullptr;

    auto slot = [] (column_data& cd, uint64_t rep, uint64_t def) {
        cd.rep_levels.push_back(rep);
        cd.def_levels.push_back(def);
    };

    auto it = r.collections.find(k);
    const collection_cell* cc = it == r.collections.end() ? nullptr : &it->second;

    if (!cc || cc->elements.empty()) {
        // Absent or empty: one slot on every leaf, carrying no value.
        const uint64_t d = cc ? 1 : 0;
        slot(l_key, 0, d);
        slot(l_val, 0, d);
        slot(l_ts,  0, d);
        slot(l_ttl, 0, d);
        slot(l_ldt, 0, d);
        if (counter) { slot(*l_clock, 0, d); }
    } else {
        for (size_t i = 0; i < cc->elements.size(); ++i) {
            const auto& e = cc->elements[i];
            const uint64_t rep = i ? 1 : 0;

            slot(l_key, rep, 2);
            l_key.str.push_back(e.key);

            const bool has_val = e.value.has_value();
            slot(l_val, rep, has_val ? 3 : 2);
            if (counter) {
                // The element value is a packed (count, logical_clock) pair. Split it into the two
                // typed leaves; a value that is not exactly sixteen bytes cannot have come from
                // read_counter_cell(), so treat it as absent rather than guessing.
                int64_t cnt = 0, clk = 0;
                const bool ok = has_val && unpack_i64_pair(*e.value, cnt, clk);
                if (ok) { l_val.i64.push_back(cnt); }
                slot(*l_clock, rep, ok ? 3 : 2);
                if (ok) { l_clock->i64.push_back(clk); }
                if (has_val && !ok) {
                    // Correct the definition level we already wrote: nothing was stored.
                    l_val.def_levels.back() = 2;
                }
            } else if (has_val) {
                l_val.str.push_back(*e.value);
            }

            slot(l_ts, rep, 2);
            l_ts.i64.push_back(e.timestamp);

            slot(l_ttl, rep, e.ttl ? 3 : 2);
            if (e.ttl) { l_ttl.i32.push_back(*e.ttl); }

            slot(l_ldt, rep, e.local_deletion_time ? 3 : 2);
            if (e.local_deletion_time) { l_ldt.i32.push_back(*e.local_deletion_time); }
        }
    }

    // The collection-wide tombstone is a row-level pair, not part of the group.
    const bool has_tomb = cc && cc->tomb.has_value();
    if (ms.ct_ts_index[k]) {
        auto& cd = out[*ms.ct_ts_index[k]];
        cd.def_levels.push_back(has_tomb ? 1 : 0);
        cd.i64.push_back(has_tomb ? cc->tomb->timestamp : 0);
    }
    if (ms.ct_ldt_index[k]) {
        auto& cd = out[*ms.ct_ldt_index[k]];
        cd.def_levels.push_back(has_tomb ? 1 : 0);
        cd.i32.push_back(has_tomb ? cc->tomb->local_deletion_time : 0);
    }
}

std::vector<column_data> shred(const mapped_schema& ms,
                               const std::vector<cql_column>& cols,
                               const std::vector<row>& rows) {
    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }
    std::vector<column_data> out(ms.columns.size());

    for (const auto& r : rows) {
        // key columns
        for (size_t k = 0; k < key_idx.size(); ++k) {
            push_value(out[k], ms.columns[k].type, r.key[k]);
        }
        auto mt = modal_timestamp(r);

        for (size_t k = 0; k < reg_idx.size(); ++k) {
            // Recorded, not computed: a collection column contributes five leaves
            // rather than one, so leaf positions no longer follow from k.
            const size_t vcol = ms.value_leaf[k];

            if (ms.value_is_collection[k]) {
                shred_collection(out, ms, k, vcol, r);
                continue;
            }

            auto it = r.cells.find(k);
            const bool present = it != r.cells.end() && it->second.v.has_value();
            out[vcol].def_levels.push_back(present ? 1 : 0);
            if (present) { push_value(out[vcol], ms.columns[vcol].type, *it->second.v); }
            else         { push_absent(out[vcol], ms.columns[vcol].type); }

            if (ms.level == folding_level::verbatim) {
                const size_t b = *ms.meta_base_index[k];
                const bool have = it != r.cells.end();
                out[b + 0].i32.push_back(have && it->second.live ? 1 : 0);
                out[b + 1].i64.push_back(have ? it->second.timestamp : 0);
                bool has_ttl = have && it->second.ttl.has_value();
                out[b + 2].def_levels.push_back(has_ttl ? 1 : 0);
                out[b + 2].i32.push_back(has_ttl ? *it->second.ttl : 0);
                bool has_ldt = have && it->second.local_deletion_time.has_value();
                out[b + 3].def_levels.push_back(has_ldt ? 1 : 0);
                out[b + 3].i32.push_back(has_ldt ? *it->second.local_deletion_time : 0);
            } else if (ms.level == folding_level::row_folded) {
                if (ms.exc_encoding == exception_encoding::per_column && ms.ts_exc_index[k]) {
                    const size_t x = *ms.ts_exc_index[k];
                    const bool diverges = it != r.cells.end() && mt && it->second.timestamp != *mt;
                    out[x].def_levels.push_back(diverges ? 1 : 0);
                    out[x].i64.push_back(diverges ? it->second.timestamp : 0);
                }
                if (ms.l1_ttl_index[k]) {
                    const size_t b = *ms.l1_ttl_index[k];
                    bool has_ttl = it != r.cells.end() && it->second.ttl.has_value();
                    out[b].def_levels.push_back(has_ttl ? 1 : 0);
                    out[b].i32.push_back(has_ttl ? *it->second.ttl : 0);
                }
                if (ms.l1_ldt_index[k]) {
                    // Live cells only. A dead cell's deletion time goes to the folded
                    // channel below, so this leaf stays null for it -- and on a table
                    // that never uses a TTL the leaf is not materialised at all.
                    const size_t b = *ms.l1_ldt_index[k];
                    bool has_ldt = it != r.cells.end() && is_live_expiry(it->second);
                    out[b].def_levels.push_back(has_ldt ? 1 : 0);
                    out[b].i32.push_back(has_ldt ? *it->second.local_deletion_time : 0);
                }
            }
        }
        if (ms.tsx_mask_index) {
            // Build the row's exception bitmap and delta blob in column order.
            const size_t nbytes = (reg_idx.size() + 7) / 8;
            std::string mask(nbytes, '\0'), vals;
            bool any = false;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                auto it = r.cells.find(k);
                if (it == r.cells.end() || !mt || it->second.timestamp == *mt) { continue; }
                mask[k / 8] = char(uint8_t(mask[k / 8]) | uint8_t(1u << (k % 8)));
                put_zigzag(vals, ts_delta(it->second.timestamp, *mt));
                any = true;
            }
            out[*ms.tsx_mask_index].def_levels.push_back(any ? 1 : 0);
            out[*ms.tsx_mask_index].str.push_back(any ? mask : std::string());
            out[*ms.tsx_vals_index].def_levels.push_back(any ? 1 : 0);
            out[*ms.tsx_vals_index].str.push_back(any ? vals : std::string());
        }
        if (ms.ts_index) { out[*ms.ts_index].i64.push_back(mt.value_or(0)); }

        // The folded deletion channel, built the same way as __tsx above: one pass over the
        // row's columns in column order, a bitmap of which are dead, and deltas only for the
        // ones that disagree with the row's deletion time.
        //
        // Written once per row and unconditionally when the leaves exist -- every leaf must
        // get exactly one slot per row or the row group is ragged, which is the failure
        // 10.15 and the __ttl_/__ldt_ pairing bug were both about.
        if (ms.dmask_index) {
            const auto mdt = modal_deletion_time(r);
            const size_t nbytes = (reg_idx.size() + 7) / 8;
            std::string dmask(nbytes, '\0'), xmask(nbytes, '\0'), xvals;
            bool any_dead = false, any_exc = false;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                // A collection column never participates: its per-element deletion times
                // live inside its MAP group. r.collections is a separate map from r.cells,
                // so this is already true -- stated here because the bitmap is indexed by
                // regular-column position and a stray bit would be read as a dead scalar.
                if (ms.value_is_collection[k]) { continue; }
                auto it = r.cells.find(k);
                if (it == r.cells.end() || !is_dead_cell(it->second)) { continue; }
                dmask[k / 8] = char(uint8_t(dmask[k / 8]) | uint8_t(1u << (k % 8)));
                any_dead = true;
                const int32_t ldt = *it->second.local_deletion_time;
                if (mdt && ldt != *mdt) {
                    xmask[k / 8] = char(uint8_t(xmask[k / 8]) | uint8_t(1u << (k % 8)));
                    // Deltas as int64 through the same zigzag helper the timestamp channel
                    // uses. int32 differences cannot overflow when widened, so no unsigned
                    // trick is needed here -- unlike ts_delta, which subtracts two int64s
                    // that legitimately span the whole range.
                    put_zigzag(xvals, int64_t(ldt) - int64_t(*mdt));
                    any_exc = true;
                }
            }
            out[*ms.ldt_index].def_levels.push_back(any_dead ? 1 : 0);
            out[*ms.ldt_index].i32.push_back(any_dead ? *mdt : 0);
            out[*ms.dmask_index].def_levels.push_back(any_dead ? 1 : 0);
            out[*ms.dmask_index].str.push_back(any_dead ? dmask : std::string());
            out[*ms.ldtx_mask_index].def_levels.push_back(any_exc ? 1 : 0);
            out[*ms.ldtx_mask_index].str.push_back(any_exc ? xmask : std::string());
            out[*ms.ldtx_vals_index].def_levels.push_back(any_exc ? 1 : 0);
            out[*ms.ldtx_vals_index].str.push_back(any_exc ? xvals : std::string());
        }

        // Row marker as a delta against the row's own timestamp: an INSERT sets
        // both from the same write, so this is almost always zero.
        auto opt_i64 = [&] (std::optional<size_t> idx, bool present, int64_t v) {
            if (!idx) { return; }
            out[*idx].def_levels.push_back(present ? 1 : 0);
            out[*idx].i64.push_back(present ? v : 0);
        };
        auto opt_i32 = [&] (std::optional<size_t> idx, bool present, int32_t v) {
            if (!idx) { return; }
            out[*idx].def_levels.push_back(present ? 1 : 0);
            out[*idx].i32.push_back(present ? v : 0);
        };
        opt_i64(ms.rm_index, r.marker.has_value(),
                r.marker ? ts_delta(r.marker->timestamp, mt.value_or(0)) : 0);
        opt_i32(ms.rm_ttl_index, bool(r.marker && r.marker->ttl),
                r.marker && r.marker->ttl ? *r.marker->ttl : 0);
        opt_i32(ms.rm_ldt_index, bool(r.marker && r.marker->expiry),
                r.marker && r.marker->expiry ? *r.marker->expiry : 0);
        opt_i64(ms.rt_ts_index, r.row_del.has_value(), r.row_del ? r.row_del->timestamp : 0);
        opt_i32(ms.rt_ldt_index, r.row_del.has_value(),
                r.row_del ? r.row_del->local_deletion_time : 0);
        opt_i64(ms.rtr_ts_index, r.row_del_regular.has_value(),
                r.row_del_regular ? r.row_del_regular->timestamp : 0);
        opt_i32(ms.rtr_ldt_index, r.row_del_regular.has_value(),
                r.row_del_regular ? r.row_del_regular->local_deletion_time : 0);
        opt_i32(ms.no_ck_index, r.no_ck, 1);
        opt_i32(ms.rtc_w_index,   r.rtc.has_value(), r.rtc ? r.rtc->weight : 0);
        opt_i32(ms.rtc_reg_index, r.rtc.has_value(), r.rtc ? r.rtc->region : 0);
        opt_i32(ms.rtc_len_index, r.rtc.has_value(), r.rtc ? r.rtc->prefix_len : 0);
        opt_i64(ms.rtc_ts_index,  bool(r.rtc && r.rtc->tomb),
                r.rtc && r.rtc->tomb ? r.rtc->tomb->timestamp : 0);
        opt_i32(ms.rtc_ldt_index, bool(r.rtc && r.rtc->tomb),
                r.rtc && r.rtc->tomb ? r.rtc->tomb->local_deletion_time : 0);
        opt_i64(ms.pt_ts_index, r.part_del.has_value(), r.part_del ? r.part_del->timestamp : 0);
        opt_i32(ms.pt_ldt_index, r.part_del.has_value(),
                r.part_del ? r.part_del->local_deletion_time : 0);
    }
    return out;
}

// Where the next row starts in a collection column's leaves. Slot counts match
// across the five leaves; value counts do not, because each leaf stores only its
// present values.
struct collection_cursor {
    size_t slot = 0;
    size_t v_key = 0, v_val = 0, v_ts = 0, v_ttl = 0, v_ldt = 0;
    // Counters only. Tracked separately because the clock leaf's values are consumed in step with
    // the value leaf, and a cursor shared between them would drift the moment one has a slot the
    // other does not.
    size_t v_clock = 0;
};

// Inverse of shred_collection(). Consumes exactly one row's slots and advances
// the cursor. Returns nullopt when the collection is absent from this row, which
// is distinct from present-and-empty.
static std::optional<collection_cell> read_collection(
        const std::vector<column_data>& cd, const mapped_schema& ms,
        size_t k, size_t vcol, size_t row_i, collection_cursor& cur) {
    // The reader never elides a leaf inside a collection group -- the group's leaves are consumed
    // together, slot by slot, by one cursor, so eliding one of them would desynchronise the rest
    // (reader.cc, elidable_leaves()). This check is therefore not reachable today; it is here so
    // that a future rule which elides a *whole* group degrades to "the collection is absent from
    // every row", which is what an all-null key leaf means, rather than indexing an empty vector.
    // The row-level tombstone leaves (__ct_ts_, __ct_ldt_) sit outside the group and are elidable;
    // they are read through a size check below, which an elided leaf fails.
    if (cd[vcol].skipped) { return std::nullopt; }
    const auto& l_key = cd[vcol];
    const auto& l_val = cd[vcol + 1];
    const auto& l_ts  = cd[vcol + 2];
    const auto& l_ttl = cd[vcol + 3];
    const auto& l_ldt = cd[vcol + 4];
    const bool counter = ms.value_is_counter[k];
    const column_data* l_clock = counter ? &cd[vcol + 5] : nullptr;

    const size_t start = cur.slot;
    if (start >= l_key.def_levels.size()) { return std::nullopt; }

    // This row's slots run to the next rep==0, which is the next row's start.
    size_t end = start + 1;
    while (end < l_key.rep_levels.size() && l_key.rep_levels[end] != 0) { ++end; }
    cur.slot = end;

    const uint64_t d0 = l_key.def_levels[start];
    if (d0 == 0) { return std::nullopt; }            // absent

    collection_cell out;
    // The tombstone leaves are row-level -- one slot per row -- so they are
    // indexed by the row, not by the slot. Read it before the empty-collection
    // return, because a deleted collection is usually deleted *and* empty.
    if (ms.ct_ts_index[k]) {
        const auto& ts_leaf = cd[*ms.ct_ts_index[k]];
        if (row_i < ts_leaf.def_levels.size() && ts_leaf.def_levels[row_i] != 0) {
            int32_t ldt = 0;
            if (ms.ct_ldt_index[k]) {
                const auto& ldt_leaf = cd[*ms.ct_ldt_index[k]];
                if (row_i < ldt_leaf.def_levels.size() && ldt_leaf.def_levels[row_i] != 0) {
                    ldt = ldt_leaf.i32[row_i];
                }
            }
            out.tomb = deletion_info{ts_leaf.i64[row_i], ldt};
        }
    }

    if (d0 == 1) { return out; }                     // present but empty

    for (size_t sl = start; sl < end; ++sl) {
        collection_element e;
        e.key = l_key.str[cur.v_key++];
        if (l_val.def_levels[sl] == 3) {
            if (counter) {
                // Repack the two typed leaves into the form the counter rebuild path expects, so
                // the change is confined to the on-disk representation: everything above this
                // still sees a packed (count, logical_clock) pair.
                const int64_t cnt = l_val.i64[cur.v_val++];
                const int64_t clk = l_clock->def_levels[sl] == 3 ? l_clock->i64[cur.v_clock++] : 0;
                e.value = pack_i64_pair(cnt, clk);
            } else {
                e.value = l_val.str[cur.v_val++];
            }
        } else if (counter && l_clock->def_levels[sl] == 3) {
            // A clock with no value cannot be reassembled and would desynchronise the cursor.
            ++cur.v_clock;
        }
        e.timestamp = l_ts.i64[cur.v_ts++];
        if (l_ttl.def_levels[sl] == 3) { e.ttl = l_ttl.i32[cur.v_ttl++]; }
        if (l_ldt.def_levels[sl] == 3) { e.local_deletion_time = l_ldt.i32[cur.v_ldt++]; }
        out.elements.push_back(std::move(e));
    }
    return out;
}

std::vector<uint8_t> projection_skip_mask(const mapped_schema& ms,
                                          const std::vector<bool>& want_regular) {
    std::vector<uint8_t> skip(ms.leaf_count(), 0);
    const size_t n = std::min(want_regular.size(), ms.value_leaf.size());

    auto mark = [&] (size_t leaf) {
        // Never a key leaf. A projection that dropped one would not return fewer columns, it would
        // return rows it could not identify.
        if (leaf < ms.n_key || leaf >= skip.size()) { return; }
        skip[leaf] = 1;
    };
    auto mark_opt = [&] (const std::vector<std::optional<size_t>>& v, size_t k) {
        if (k < v.size() && v[k]) { mark(*v[k]); }
    };

    for (size_t k = 0; k < n; ++k) {
        if (want_regular[k]) { continue; }

        // The value. A collection contributes a group of leaves that travel together -- five, or
        // six for a counter, whose extra `clock` is appended after __ldt -- so the whole group goes
        // or none of it does.
        const size_t v = ms.value_leaf[k];
        size_t span = 1;
        if (k < ms.value_is_collection.size() && ms.value_is_collection[k]) {
            span = (k < ms.value_is_counter.size() && ms.value_is_counter[k]) ? 6 : 5;
        }
        for (size_t i = 0; i < span; ++i) { mark(v + i); }

        // This column's own metadata leaves. Each is per-column, so dropping it changes only this
        // column's reconstruction -- which the caller has already said it does not want.
        mark_opt(ms.ts_exc_index, k);
        mark_opt(ms.l1_ttl_index, k);
        mark_opt(ms.l1_ldt_index, k);
        mark_opt(ms.ct_ts_index, k);
        mark_opt(ms.ct_ldt_index, k);
        // L0 gives each column four contiguous metadata leaves.
        if (k < ms.meta_base_index.size() && ms.meta_base_index[k]) {
            for (size_t i = 0; i < 4; ++i) { mark(*ms.meta_base_index[k] + i); }
        }
    }
    return skip;
}

std::vector<row> reassemble(const mapped_schema& ms,
                            const std::vector<cql_column>& cols,
                            const std::vector<column_data>& cd,
                            size_t nrows) {
    if (ms.level == folding_level::logical) {
        // Returning rows here would mean inventing write times, which is worse
        // than failing: the caller would get data that looks reconstructed.
        throw std::runtime_error("reassemble: folding level L3 is lossy by design; "
                                 "cell metadata was discarded at write time");
    }
    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }

    std::vector<row> out(nrows);
    // Per collection column, where the next row's slots and values begin. A
    // repeated column has a variable number of slots per row, so its position
    // cannot be derived from the row index.
    std::vector<collection_cursor> coll_cur(reg_idx.size());

    // A leaf the reader chose not to read. It is only ever skipped when the file's statistics
    // prove the chunk null for every row, so "skipped" and "null in this row" are the same
    // answer -- but the vectors are empty rather than full of nulls, so every access below has to
    // ask first. Key leaves and the row-timestamp leaf are never skippable (they are REQUIRED, so
    // no chunk of them is all-null); this rejects rather than trusts that, because reading past
    // the end of an empty vector would be a silent misread.
    auto absent = [&] (size_t leaf) { return cd[leaf].skipped; };
    auto require_read = [&] (size_t leaf, const char* what) {
        if (cd[leaf].skipped) {
            throw std::runtime_error(std::string("reassemble: ") + what +
                                     " leaf was not read; it is not optional");
        }
    };
    for (size_t k = 0; k < key_idx.size(); ++k) { require_read(k, "key"); }
    if (ms.ts_index) { require_read(*ms.ts_index, "row timestamp"); }

    for (size_t i = 0; i < nrows; ++i) {
        row& r = out[i];
        for (size_t k = 0; k < key_idx.size(); ++k) {
            r.key.push_back(read_value(cd[k], ms.columns[k].type, i));
        }
        const int64_t row_ts =
            ms.ts_index ? cd[*ms.ts_index].i64[i] : ms.uniform_ts.value_or(0);

        // Decode this row's sparse exceptions once, into column -> timestamp.
        std::map<size_t, int64_t> exc;
        if (ms.tsx_mask_index && !absent(*ms.tsx_mask_index) &&
            cd[*ms.tsx_mask_index].def_levels[i]) {
            // The two sparse-exception leaves are written together -- a row with an exception has
            // a value in both -- so an elided `vals` beside a live `mask` is impossible. Rejecting
            // it costs one branch per window and turns "impossible" into a loud failure rather
            // than a wrong timestamp.
            require_read(*ms.tsx_vals_index, "sparse exception values");
            const std::string& mask = cd[*ms.tsx_mask_index].str[i];
            const std::string& vals = cd[*ms.tsx_vals_index].str[i];
            size_t pos = 0;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (k / 8 < mask.size() && (uint8_t(mask[k / 8]) & (1u << (k % 8)))) {
                    exc.emplace(k, ts_undelta(row_ts, get_zigzag(vals, pos)));
                }
            }
        }

        // This row's dead cells, decoded once from the folded deletion channel into
        // column -> deletion time. Empty for a file written before the channel existed;
        // the per-column fallback below covers those.
        std::map<size_t, int32_t> dead;
        if (ms.dmask_index && !absent(*ms.dmask_index) && cd[*ms.dmask_index].def_levels[i]) {
            if (!ms.ldt_index) {
                throw std::runtime_error("reassemble: __dmask without __ldt");
            }
            require_read(*ms.ldt_index, "row deletion time");
            const int32_t row_ldt = cd[*ms.ldt_index].i32[i];
            const std::string& dm = cd[*ms.dmask_index].str[i];
            // The exception pair is written together, and only ever alongside a live
            // __dmask, so a live mask beside an elided vals is impossible. Rejecting it
            // costs one branch and turns "impossible" into a loud failure rather than a
            // wrong deletion time -- the same reasoning as the __tsx pair above.
            const std::string* xm = nullptr;
            const std::string* xv = nullptr;
            if (ms.ldtx_mask_index && !absent(*ms.ldtx_mask_index) &&
                cd[*ms.ldtx_mask_index].def_levels[i]) {
                require_read(*ms.ldtx_vals_index, "deletion exception values");
                xm = &cd[*ms.ldtx_mask_index].str[i];
                xv = &cd[*ms.ldtx_vals_index].str[i];
            }
            size_t pos = 0;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (!(k / 8 < dm.size() && (uint8_t(dm[k / 8]) & (1u << (k % 8))))) { continue; }
                int32_t ldt = row_ldt;
                if (xm && k / 8 < xm->size() && (uint8_t((*xm)[k / 8]) & (1u << (k % 8)))) {
                    ldt = int32_t(int64_t(row_ldt) + get_zigzag(*xv, pos));
                }
                dead.emplace(k, ldt);
            }
        }

        for (size_t k = 0; k < reg_idx.size(); ++k) {
            // Recorded, not computed: a collection column contributes five leaves
            // rather than one, so leaf positions no longer follow from k.
            const size_t vcol = ms.value_leaf[k];

            if (ms.value_is_collection[k]) {
                auto cc = read_collection(cd, ms, k, vcol, i, coll_cur[k]);
                if (cc) { r.collections.emplace(k, std::move(*cc)); }
                continue;
            }

            const bool present = !absent(vcol) && cd[vcol].def_levels[i] != 0;

            if (ms.level == folding_level::verbatim) {
                const size_t b = *ms.meta_base_index[k];
                require_read(b + 0, "L0 liveness");
                require_read(b + 1, "L0 cell timestamp");
                const bool live = cd[b + 0].i32[i] != 0;
                const int64_t ts = cd[b + 1].i64[i];
                // A column with no cell at all was written as absent + dead.
                if (!present && !live && ts == 0) { continue; }
                cell c;
                c.live = live;
                c.timestamp = ts;
                if (present) { c.v = read_value(cd[vcol], ms.columns[vcol].type, i); }
                if (!absent(b + 2) && cd[b + 2].def_levels[i]) { c.ttl = cd[b + 2].i32[i]; }
                if (!absent(b + 3) && cd[b + 3].def_levels[i]) {
                    c.local_deletion_time = cd[b + 3].i32[i];
                }
                r.cells.emplace(k, std::move(c));
            } else {
                // L1 and L2 have no per-column live flag -- that is L0's `__live_`
                // leaf -- so deadness has to be read off `__ldt_`:
                //
                //   value present                -> live (ldt, if any, is the expiry)
                //   no value, ldt present        -> DEAD
                //   no value, no ldt             -> absent, never written
                //
                // Collapsing the middle case into the last one loses the deletion:
                // the cell comes back as never-written and resurrects whatever it
                // shadowed. `any_deletion` is set by `!c.live`, so the leaf is always
                // there when something is dead -- the information was on disk all
                // along and simply was not being read.
                const bool has_ttl = ms.l1_ttl_index[k] && !absent(*ms.l1_ttl_index[k]) &&
                                     cd[*ms.l1_ttl_index[k]].def_levels[i] != 0;
                const bool has_percol_ldt = ms.l1_ldt_index[k] &&
                                     !absent(*ms.l1_ldt_index[k]) &&
                                     cd[*ms.l1_ldt_index[k]].def_levels[i] != 0;
                const auto dit = dead.find(k);
                const bool folded_dead = dit != dead.end();
                // A __dmask bit on a column that also has a value is contradictory: the
                // same cell cannot be both live and a tombstone. It means the mask is
                // being read against the wrong columns, so the deletion times would land
                // on the wrong cells. Fail rather than pick one.
                if (folded_dead && present) {
                    throw std::runtime_error("reassemble: __dmask marks column " +
                            std::to_string(k) + " dead but it has a value");
                }
                // Three states, and they have to stay three:
                //
                //   value present                     -> live (per-column ldt is its expiry)
                //   __dmask bit set                   -> DEAD, time from the folded channel
                //   no value but a per-column __ldt   -> DEAD, pre-fold file
                //   none of the above                 -> absent, never written
                //
                // Collapsing dead into absent is what resurrects data: the deletion stops
                // shadowing whatever it was hiding, so the old value reappears on the next
                // merge. That bug cost 540 test cases' worth of false confidence once
                // already.
                //
                // Both dead clauses are needed, and they cannot collide. A file written
                // before the folded channel has no __dmask and put every deletion time in
                // __ldt_<col>; a file written after it never puts a dead cell's time
                // there, so the legacy clause is unreachable on a new file. A live cell
                // with a TTL has present == true and is untouched by either.
                if (!present && !folded_dead && !has_percol_ldt) { continue; }
                cell c;
                c.live = present;
                if (present) { c.v = read_value(cd[vcol], ms.columns[vcol].type, i); }
                c.timestamp = row_ts;
                if (auto e = exc.find(k); e != exc.end()) {
                    c.timestamp = e->second;
                } else if (ms.exc_encoding == exception_encoding::per_column &&
                           ms.ts_exc_index[k] && !absent(*ms.ts_exc_index[k]) &&
                           cd[*ms.ts_exc_index[k]].def_levels[i]) {
                    c.timestamp = cd[*ms.ts_exc_index[k]].i64[i];
                }
                if (has_ttl) { c.ttl = cd[*ms.l1_ttl_index[k]].i32[i]; }
                if (folded_dead) {
                    c.local_deletion_time = dit->second;
                } else if (has_percol_ldt) {
                    // A live cell's expiry, or a dead cell's time in a pre-fold file.
                    c.local_deletion_time = cd[*ms.l1_ldt_index[k]].i32[i];
                }
                r.cells.emplace(k, std::move(c));
            }
        }

        // Row marker and the two tombstone groups, inverting shred().
        auto present = [&] (std::optional<size_t> idx) {
            return idx && !cd[*idx].skipped && !cd[*idx].def_levels.empty()
                   && cd[*idx].def_levels[i];
        };
        if (present(ms.rm_index)) {
            marker_info m;
            m.timestamp = ts_undelta(row_ts, cd[*ms.rm_index].i64[i]);
            if (present(ms.rm_ttl_index)) { m.ttl = cd[*ms.rm_ttl_index].i32[i]; }
            if (present(ms.rm_ldt_index)) { m.expiry = cd[*ms.rm_ldt_index].i32[i]; }
            r.marker = m;
        }
        if (present(ms.rt_ts_index)) {
            r.row_del = deletion_info{cd[*ms.rt_ts_index].i64[i],
                                      present(ms.rt_ldt_index) ? cd[*ms.rt_ldt_index].i32[i] : 0};
        }
        if (present(ms.rtr_ts_index)) {
            r.row_del_regular = deletion_info{
                    cd[*ms.rtr_ts_index].i64[i],
                    present(ms.rtr_ldt_index) ? cd[*ms.rtr_ldt_index].i32[i] : 0};
        }
        if (present(ms.no_ck_index)) { r.no_ck = true; }
        if (present(ms.rtc_w_index)) {
            rtc_info ri;
            ri.weight     = cd[*ms.rtc_w_index].i32[i];
            ri.region     = present(ms.rtc_reg_index) ? cd[*ms.rtc_reg_index].i32[i] : 0;
            ri.prefix_len = present(ms.rtc_len_index) ? cd[*ms.rtc_len_index].i32[i] : 0;
            if (present(ms.rtc_ts_index)) {
                ri.tomb = deletion_info{cd[*ms.rtc_ts_index].i64[i],
                        present(ms.rtc_ldt_index) ? cd[*ms.rtc_ldt_index].i32[i] : 0};
            }
            r.rtc = ri;
        }
        if (present(ms.pt_ts_index)) {
            r.part_del = deletion_info{cd[*ms.pt_ts_index].i64[i],
                                       present(ms.pt_ldt_index) ? cd[*ms.pt_ldt_index].i32[i] : 0};
        }
    }
    return out;
}

// Counter columns share the collection shape, so their element values are two big-endian int64s
// rather than an opaque blob, and nothing in the Parquet schema says so. Proper `value` and
// `clock` leaves would need a group inside the MAP value -- a third level of Dremel nesting -- and
// that is a schema change, not a data change (design doc 5.2). Until then the file at least
// *declares* the convention instead of requiring a reader to know it: the column names go in the
// footer's key-value metadata alongside a description of the packing.
//
// This is a lesser fix than typed leaves and is not a substitute for them. What it buys is that
// someone opening the file with parquet-tools can find out what those sixteen bytes are.
void add_counter_metadata(parquet_file_writer& w, const std::vector<cql_column>& cols) {
    std::string names;
    for (const auto& c : cols) {
        if (!c.counter) { continue; }
        if (!names.empty()) { names += ','; }
        names += c.name;
    }
    if (names.empty()) { return; }
    w.add_key_value("scylla.counter_columns", names);
    w.add_key_value("scylla.counter_encoding",
                    "map<shard_id, packed>; shard_id = 16 bytes, two big-endian int64 "
                    "(UUID msb, lsb); packed = 16 bytes, two big-endian int64 (value, "
                    "logical_clock)");
}

std::vector<uint8_t> write_rows(const std::vector<cql_column>& cols,
                                const std::vector<row>& rows,
                                folding_level level,
                                writer_options opt,
                                exception_encoding exc,
                                const std::map<std::string, format::encoding>&
                                        encoding_overrides) {
    auto ms = map_schema(cols, level, rows, exc, leaf_set::derived, encoding_overrides);
    auto data = shred(ms, cols, rows);
    // The encoding hints have to be handed over separately: the tree cannot carry
    // them. ms.columns is in leaf order -- build_tree() asserts that and writes the
    // levels back by index -- so position is the correspondence.
    std::vector<std::optional<encoding>> hints;
    hints.reserve(ms.columns.size());
    for (const auto& c : ms.columns) { hints.push_back(c.preferred); }
    parquet_file_writer w(parquet_file_writer::nested_schema{ms.tree, std::move(hints)}, opt);
    w.add_key_value("scylla.folding_level", to_string(ms.level));
    if (ms.uniform_ts) {
        w.add_key_value("scylla.uniform_timestamp", std::to_string(*ms.uniform_ts));
    }
    add_counter_metadata(w, cols);
    w.add_row_group(data);
    return w.finish();
}

} // namespace sstables::parquet
