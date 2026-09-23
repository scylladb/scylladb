/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/read_model.hh"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <numeric>
#include <ranges>
#include <set>
#include <stdexcept>

#include <fmt/ranges.h>

#include "dht/i_partitioner.hh"
#include "mutation/range_tombstone.hh"
#include "schema/schema_builder.hh"
#include "test/lib/random_utils.hh"
#include "types/types.hh"
#include "utils/overloaded_functor.hh"

namespace tests::read_model {

namespace {

partition_key make_pk(const schema& s, int32_t pk) {
    return partition_key::from_single_value(s, int32_type->decompose(pk));
}

clustering_key make_ck(const schema& s, int32_t ck) {
    return clustering_key::from_single_value(s, int32_type->decompose(ck));
}

// Whether the range between `start` and `end` contains `ck`. A bound of
// nullopt is unbounded.
bool contains(const std::optional<bound>& start, const std::optional<bound>& end, int32_t ck) {
    if (start && (start->inclusive ? ck < start->ck : ck <= start->ck)) {
        return false;
    }
    if (end && (end->inclusive ? ck > end->ck : ck >= end->ck)) {
        return false;
    }
    return true;
}

// Whether the range between `start` and `end` is empty.
bool is_empty(const std::optional<bound>& start, const std::optional<bound>& end) {
    if (!start || !end) {
        return false;
    }
    return start->ck > end->ck || (start->ck == end->ck && !(start->inclusive && end->inclusive));
}

api::timestamp_type timestamp_of(const operation& op) {
    return std::visit([] (const auto& o) { return o.ts; }, op);
}

int32_t pk_of(const operation& op) {
    return std::visit([] (const auto& o) { return o.pk; }, op);
}

std::string describe(const std::optional<int32_t>& value) {
    return value ? fmt::format("{}", *value) : "std::nullopt";
}

std::string describe(lifetime life) {
    switch (life) {
    case lifetime::permanent: return "lifetime::permanent";
    case lifetime::expired: return "lifetime::expired";
    case lifetime::expiring: return "lifetime::expiring";
    }
    std::abort();
}

std::string describe(regular_column c) {
    return c == regular_column::v1 ? "regular_column::v1" : "regular_column::v2";
}

std::string describe(const std::optional<bound>& b) {
    return b ? fmt::format("bound{{{}, {}}}", b->ck, b->inclusive) : "std::nullopt";
}

std::string_view column_name(column c) {
    switch (c) {
    case column::s: return "s";
    case column::v1: return "v1";
    case column::v2: return "v2";
    }
    std::abort();
}

std::string describe(const predicate& p) {
    static constexpr std::string_view ops[] = {"comparison::eq", "comparison::lt", "comparison::gt"};
    return fmt::format("predicate{{column::{}, {}, {}}}", column_name(p.col), ops[int(p.op)], p.value);
}

std::string_view cql_operator(comparison op) {
    switch (op) {
    case comparison::eq: return "=";
    case comparison::lt: return "<";
    case comparison::gt: return ">";
    }
    std::abort();
}

// The newest write of a cell or of a row marker.
struct newest_write {
    api::timestamp_type ts = api::missing_timestamp;
    // Whether the newest write is a value or a row marker, not a tombstone,
    // and its TTL did not expire.
    bool live = false;
    int32_t value = 0;

    void apply(api::timestamp_type write_ts, bool write_live, int32_t write_value) {
        if (write_ts > ts) {
            ts = write_ts;
            live = write_live;
            value = write_value;
        }
    }

    // Whether the write is live, and not covered by a tombstone with
    // timestamp `deletion`.
    bool is_live(api::timestamp_type deletion) const {
        return live && ts > deletion;
    }

    std::optional<int32_t> live_value(api::timestamp_type deletion) const {
        return is_live(deletion) ? std::optional(value) : std::nullopt;
    }
};

bool is_live_write(const std::optional<int32_t>& value, lifetime life) {
    return value && life != lifetime::expired;
}

struct row_state {
    newest_write marker;
    newest_write v1;
    newest_write v2;
    api::timestamp_type deletion = api::missing_timestamp;
};

struct partition_state {
    api::timestamp_type deletion = api::missing_timestamp;
    newest_write s;
    std::map<int32_t, row_state> rows;
    std::vector<range_deletion> range_deletions;

    // The timestamp of the newest tombstone which covers row `ck`.
    api::timestamp_type row_deletion_of(int32_t ck, const row_state& r) const {
        auto deletion = std::max(this->deletion, r.deletion);
        for (const auto& d : range_deletions) {
            if (contains(d.start, d.end, ck)) {
                deletion = std::max(deletion, d.ts);
            }
        }
        return deletion;
    }
};

// The newest writes and the tombstones of each partition of `h`.
std::map<int32_t, partition_state> resolve(const history& h) {
    std::map<int32_t, partition_state> partitions;
    for (const auto& op : h) {
        auto& p = partitions[pk_of(op)];
        std::visit(overloaded_functor{
            [&] (const static_cell_write& w) {
                p.s.apply(w.ts, is_live_write(w.value, w.life), w.value.value_or(0));
            },
            [&] (const regular_cell_write& w) {
                auto& r = p.rows[w.ck];
                auto& cell = w.column == regular_column::v1 ? r.v1 : r.v2;
                cell.apply(w.ts, is_live_write(w.value, w.life), w.value.value_or(0));
            },
            [&] (const row_marker_write& w) {
                p.rows[w.ck].marker.apply(w.ts, w.life != lifetime::expired, 0);
            },
            [&] (const row_deletion& d) {
                auto& r = p.rows[d.ck];
                r.deletion = std::max(r.deletion, d.ts);
            },
            [&] (const range_deletion& d) {
                p.range_deletions.push_back(d);
            },
            [&] (const partition_deletion& d) {
                p.deletion = std::max(p.deletion, d.ts);
            },
        }, op);
    }
    return partitions;
}

// The live values of a row, before the values of unselected columns are
// replaced with null.
struct live_row {
    std::optional<int32_t> ck;
    std::optional<int32_t> s;
    std::optional<int32_t> v1;
    std::optional<int32_t> v2;
};

bool satisfies(const live_row& r, const predicate& p) {
    const auto& value = p.col == column::s ? r.s : p.col == column::v1 ? r.v1 : r.v2;
    if (!value) {
        return false;
    }
    switch (p.op) {
    case comparison::eq: return *value == p.value;
    case comparison::lt: return *value < p.value;
    case comparison::gt: return *value > p.value;
    }
    std::abort();
}

answer_row project(int32_t pk, const live_row& r, const select_query& q) {
    return answer_row{
        .pk = pk,
        .ck = r.ck,
        .s = q.select_s ? r.s : std::nullopt,
        .v1 = q.select_v1 ? r.v1 : std::nullopt,
        .v2 = q.select_v2 ? r.v2 : std::nullopt,
    };
}

// The rows which partition `pk` gives, after the filter and the
// per-partition limit.
std::vector<answer_row> partition_answer(int32_t pk, const partition_state& p, const select_query& q) {
    // Only a partition deletion covers the static cell.
    const auto s = p.s.live_value(p.deletion);
    std::vector<live_row> candidates;
    for (const auto& [ck, r] : p.rows) {
        if (!contains(q.ck_start, q.ck_end, ck)) {
            continue;
        }
        const auto deletion = p.row_deletion_of(ck, r);
        auto v1 = r.v1.live_value(deletion);
        auto v2 = r.v2.live_value(deletion);
        if (r.marker.is_live(deletion) || v1 || v2) {
            candidates.push_back(live_row{ck, s, v1, v2});
        }
    }

    if (q.distinct) {
        if (candidates.empty() && !s) {
            return {};
        }
        return {project(pk, live_row{std::nullopt, s, std::nullopt, std::nullopt}, q)};
    }

    if (q.reversed) {
        std::ranges::reverse(candidates);
    }
    const bool has_ck_restriction = q.ck_start || q.ck_end;
    if (candidates.empty() && s && !has_ck_restriction) {
        candidates.push_back(live_row{std::nullopt, s, std::nullopt, std::nullopt});
    }

    const auto per_partition_limit = q.per_partition_limit.value_or(std::numeric_limits<uint64_t>::max());
    std::vector<answer_row> rows;
    for (const auto& r : candidates) {
        if (rows.size() == per_partition_limit) {
            break;
        }
        if (std::ranges::all_of(q.filter, [&r] (const predicate& p) { return satisfies(r, p); })) {
            rows.push_back(project(pk, r, q));
        }
    }
    return rows;
}

std::optional<int32_t> random_value() {
    if (tests::random::get_int(0, 4) == 0) {
        return std::nullopt;
    }
    return tests::random::get_int<int32_t>(0, 9);
}

lifetime random_lifetime() {
    switch (tests::random::get_int(0, 5)) {
    case 0: return lifetime::expired;
    case 1: return lifetime::expiring;
    default: return lifetime::permanent;
    }
}

// A random range which is not empty. A bound is either nullopt (unbounded)
// or a key from `min` to `max`.
std::pair<std::optional<bound>, std::optional<bound>> random_range(int32_t min, int32_t max) {
    auto random_bound = [&] () -> std::optional<bound> {
        if (tests::random::get_int(0, 2) == 0) {
            return std::nullopt;
        }
        return bound{tests::random::get_int(min, max), tests::random::get_bool()};
    };
    auto start = random_bound();
    auto end = random_bound();
    if (start && end) {
        if (start->ck > end->ck) {
            std::swap(start->ck, end->ck);
        }
        if (start->ck == end->ck) {
            start->inclusive = end->inclusive = true;
        }
    }
    return {start, end};
}

} // anonymous namespace

std::string create_table_statement(std::string_view ks, std::string_view cf) {
    return fmt::format("CREATE TABLE {}.{} (pk int, ck int, s int static, v1 int, v2 int, PRIMARY KEY (pk, ck))"
            " WITH tombstone_gc = {{'mode': 'disabled'}}", ks, cf);
}

schema_ptr make_schema(std::string_view ks, std::string_view cf) {
    return schema_builder(this_smp_shard_count(), ks, cf)
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("s", int32_type, column_kind::static_column)
            .with_column("v1", int32_type, column_kind::regular_column)
            .with_column("v2", int32_type, column_kind::regular_column)
            .build();
}

std::vector<int32_t> ring_order(const schema& s, std::vector<int32_t> pks) {
    std::ranges::sort(pks, [&s] (int32_t a, int32_t b) {
        return dht::decorate_key(s, make_pk(s, a)).less_compare(s, dht::decorate_key(s, make_pk(s, b)));
    });
    return pks;
}

void validate(const history& h) {
    std::set<api::timestamp_type> timestamps;
    auto check_lifetime = [] (const operation& op, const std::optional<int32_t>& value, lifetime life) {
        if (!value && life != lifetime::permanent) {
            throw std::invalid_argument(fmt::format("A tombstone cannot have a TTL: {}", describe(op)));
        }
    };
    for (const auto& op : h) {
        const auto ts = timestamp_of(op);
        if (ts == api::missing_timestamp) {
            throw std::invalid_argument(fmt::format("A write needs a timestamp: {}", describe(op)));
        }
        if (!timestamps.insert(ts).second) {
            throw std::invalid_argument(fmt::format("Two writes have timestamp {}", ts));
        }
        std::visit(overloaded_functor{
            [&] (const static_cell_write& w) { check_lifetime(op, w.value, w.life); },
            [&] (const regular_cell_write& w) { check_lifetime(op, w.value, w.life); },
            [&] (const range_deletion& d) {
                if (is_empty(d.start, d.end)) {
                    throw std::invalid_argument(fmt::format("A range deletion cannot be empty: {}", describe(op)));
                }
            },
            [] (const auto&) { },
        }, op);
    }
}

utils::chunked_vector<mutation> to_mutations(schema_ptr s, const history& h, gc_clock::time_point query_time) {
    validate(h);
    // An expired value expired an hour before `query_time`. An expiring value
    // expires a day after it.
    const auto ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::days(2));
    const auto expiry_of = [&] (lifetime life) {
        return life == lifetime::expired
                ? query_time - std::chrono::duration_cast<gc_clock::duration>(std::chrono::hours(1))
                : query_time + std::chrono::duration_cast<gc_clock::duration>(std::chrono::days(1));
    };
    const auto make_cell = [&] (const std::optional<int32_t>& value, api::timestamp_type ts, lifetime life) {
        if (!value) {
            return atomic_cell::make_dead(ts, query_time);
        }
        const auto serialized = int32_type->decompose(*value);
        if (life == lifetime::permanent) {
            return atomic_cell::make_live(*int32_type, ts, serialized);
        }
        return atomic_cell::make_live(*int32_type, ts, bytes_view(serialized), expiry_of(life), ttl);
    };
    const auto& s_def = *s->get_column_definition("s");
    const auto& v1_def = *s->get_column_definition("v1");
    const auto& v2_def = *s->get_column_definition("v2");

    std::map<int32_t, mutation> partitions;
    for (const auto& op : h) {
        const auto pk = pk_of(op);
        auto it = partitions.find(pk);
        if (it == partitions.end()) {
            it = partitions.emplace(pk, mutation(s, make_pk(*s, pk))).first;
        }
        auto& m = it->second;
        std::visit(overloaded_functor{
            [&] (const static_cell_write& w) {
                m.set_static_cell(s_def, atomic_cell_or_collection(make_cell(w.value, w.ts, w.life)));
            },
            [&] (const regular_cell_write& w) {
                const auto& def = w.column == regular_column::v1 ? v1_def : v2_def;
                m.set_clustered_cell(make_ck(*s, w.ck), def, atomic_cell_or_collection(make_cell(w.value, w.ts, w.life)));
            },
            [&] (const row_marker_write& w) {
                auto& r = m.partition().clustered_row(*s, make_ck(*s, w.ck));
                if (w.life == lifetime::permanent) {
                    r.apply(row_marker(w.ts));
                } else {
                    r.apply(row_marker(w.ts, ttl, expiry_of(w.life)));
                }
            },
            [&] (const row_deletion& d) {
                m.partition().apply_delete(*s, make_ck(*s, d.ck), tombstone(d.ts, query_time));
            },
            [&] (const range_deletion& d) {
                auto start = d.start ? make_ck(*s, d.start->ck) : clustering_key_prefix::make_empty();
                auto start_kind = !d.start || d.start->inclusive ? bound_kind::incl_start : bound_kind::excl_start;
                auto end = d.end ? make_ck(*s, d.end->ck) : clustering_key_prefix::make_empty();
                auto end_kind = !d.end || d.end->inclusive ? bound_kind::incl_end : bound_kind::excl_end;
                m.partition().apply_delete(*s, range_tombstone(std::move(start), start_kind, std::move(end), end_kind, tombstone(d.ts, query_time)));
            },
            [&] (const partition_deletion& d) {
                m.partition().apply(tombstone(d.ts, query_time));
            },
        }, op);
    }

    utils::chunked_vector<mutation> mutations;
    for (auto& [pk, m] : partitions) {
        mutations.push_back(std::move(m));
    }
    std::ranges::sort(mutations, [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return mutations;
}

std::string describe(const operation& op) {
    return std::visit(overloaded_functor{
        [] (const static_cell_write& w) {
            return fmt::format("static_cell_write{{{}, {}, {}, {}}}", w.pk, describe(w.value), w.ts, describe(w.life));
        },
        [] (const regular_cell_write& w) {
            return fmt::format("regular_cell_write{{{}, {}, {}, {}, {}, {}}}", w.pk, w.ck, describe(w.column), describe(w.value), w.ts,
                    describe(w.life));
        },
        [] (const row_marker_write& w) {
            return fmt::format("row_marker_write{{{}, {}, {}, {}}}", w.pk, w.ck, w.ts, describe(w.life));
        },
        [] (const row_deletion& d) {
            return fmt::format("row_deletion{{{}, {}, {}}}", d.pk, d.ck, d.ts);
        },
        [] (const range_deletion& d) {
            return fmt::format("range_deletion{{{}, {}, {}, {}}}", d.pk, describe(d.start), describe(d.end), d.ts);
        },
        [] (const partition_deletion& d) {
            return fmt::format("partition_deletion{{{}, {}}}", d.pk, d.ts);
        },
    }, op);
}

std::string describe(const history& h) {
    auto ops = h | std::views::transform([] (const operation& op) { return describe(op); });
    return fmt::format("history{{\n    {},\n}}", fmt::join(ops, ",\n    "));
}

// Note: keys should be kept in sync with random_query, to keep the tests useful.
history random_history(size_t max_writes) {
    std::vector<api::timestamp_type> timestamps(tests::random::get_int<size_t>(1, max_writes));
    std::iota(timestamps.begin(), timestamps.end(), 1);
    std::ranges::shuffle(timestamps, tests::random::gen());

    history h;
    for (auto ts : timestamps) {
        const int32_t pk = tests::random::get_int(1, 4);
        const int32_t ck = tests::random::get_int(1, 5);
        const auto value = random_value();
        const auto life = value ? random_lifetime() : lifetime::permanent;
        switch (tests::random::get_int(0, 9)) {
        case 0:
        case 1:
            h.push_back(static_cell_write{pk, value, ts, life});
            break;
        case 2:
        case 3:
        case 4:
            h.push_back(regular_cell_write{pk, ck, tests::random::get_bool() ? regular_column::v1 : regular_column::v2, value, ts, life});
            break;
        case 5:
            h.push_back(row_marker_write{pk, ck, ts, random_lifetime()});
            break;
        case 6:
            h.push_back(row_deletion{pk, ck, ts});
            break;
        case 7:
        case 8: {
            auto [start, end] = random_range(0, 6);
            h.push_back(range_deletion{pk, start, end, ts});
            break;
        }
        default:
            h.push_back(partition_deletion{pk, ts});
        }
    }
    return h;
}

void validate(const select_query& q) {
    if (q.partitions) {
        if (q.partitions->empty()) {
            throw std::invalid_argument("A query must read at least one partition");
        }
        auto pks = *q.partitions;
        std::ranges::sort(pks);
        if (std::ranges::adjacent_find(pks) != pks.end()) {
            throw std::invalid_argument(fmt::format("The partition keys of a query must be distinct: {}", q));
        }
    }
    if (is_empty(q.ck_start, q.ck_end)) {
        throw std::invalid_argument(fmt::format("The clustering range of a query cannot be empty: {}", q));
    }
    if (q.distinct && (q.ck_start || q.ck_end || q.reversed || q.select_v1 || q.select_v2 || !q.filter.empty() || q.per_partition_limit)) {
        throw std::invalid_argument(fmt::format("A DISTINCT query can only select the static column and have a limit: {}", q));
    }
    if (q.partition_limit && !q.filter.empty()) {
        throw std::invalid_argument(fmt::format("A query with a partition limit cannot filter: {}", q));
    }
    if ((q.limit && !*q.limit) || (q.per_partition_limit && !*q.per_partition_limit) || (q.partition_limit && !*q.partition_limit)) {
        throw std::invalid_argument(fmt::format("The limits of a query must be positive: {}", q));
    }
}

std::string to_cql(const select_query& q, std::string_view ks, std::string_view cf) {
    validate(q);
    if (q.partition_limit) {
        throw std::invalid_argument(fmt::format("CQL has no partition limit: {}", q));
    }
    if (q.reversed && !(q.partitions && q.partitions->size() == 1)) {
        throw std::invalid_argument(fmt::format("CQL orders the rows of several partitions by their clustering key: {}", q));
    }

    std::vector<std::string_view> columns{"pk"};
    if (!q.distinct) {
        columns.push_back("ck");
    }
    if (q.select_s) {
        columns.push_back("s");
    }
    if (q.select_v1) {
        columns.push_back("v1");
    }
    if (q.select_v2) {
        columns.push_back("v2");
    }

    std::vector<std::string> restrictions;
    if (q.partitions) {
        restrictions.push_back(q.partitions->size() == 1
                ? fmt::format("pk = {}", q.partitions->front())
                : fmt::format("pk IN ({})", fmt::join(*q.partitions, ", ")));
    }
    if (q.ck_start) {
        restrictions.push_back(fmt::format("ck {} {}", q.ck_start->inclusive ? ">=" : ">", q.ck_start->ck));
    }
    if (q.ck_end) {
        restrictions.push_back(fmt::format("ck {} {}", q.ck_end->inclusive ? "<=" : "<", q.ck_end->ck));
    }
    for (const auto& p : q.filter) {
        restrictions.push_back(fmt::format("{} {} {}", column_name(p.col), cql_operator(p.op), p.value));
    }

    auto cql = fmt::format("SELECT {}{} FROM {}.{}", q.distinct ? "DISTINCT " : "", fmt::join(columns, ", "), ks, cf);
    if (!restrictions.empty()) {
        cql += fmt::format(" WHERE {}", fmt::join(restrictions, " AND "));
    }
    if (q.reversed) {
        cql += " ORDER BY ck DESC";
    }
    if (q.per_partition_limit) {
        cql += fmt::format(" PER PARTITION LIMIT {}", *q.per_partition_limit);
    }
    if (q.limit) {
        cql += fmt::format(" LIMIT {}", *q.limit);
    }
    // A clustering restriction without a partition restriction requires
    // filtering too.
    if (!q.filter.empty() || (!q.partitions && (q.ck_start || q.ck_end))) {
        cql += " ALLOW FILTERING";
    }
    return cql;
}

// Note: keys should be kept in sync with random_history, to keep the tests useful.
select_query random_query() {
    select_query q;
    q.distinct = tests::random::get_int(0, 6) == 0;
    if (tests::random::get_bool()) {
        std::vector<int32_t> pks{1, 2, 3, 4, 5};
        std::ranges::shuffle(pks, tests::random::gen());
        pks.resize(tests::random::get_int(1, 3));
        q.partitions = std::move(pks);
    }
    q.select_s = tests::random::get_bool();
    if (tests::random::get_int(0, 2) == 0) {
        q.limit = tests::random::get_int<uint64_t>(1, 4);
    }
    if (q.distinct) {
        q.select_v1 = false;
        q.select_v2 = false;
        return q;
    }
    q.select_v1 = tests::random::get_bool();
    q.select_v2 = tests::random::get_bool();
    if (tests::random::get_int(0, 2) == 0) {
        std::tie(q.ck_start, q.ck_end) = random_range(0, 6);
    }
    q.reversed = q.partitions && q.partitions->size() == 1 && tests::random::get_bool();
    if (tests::random::get_int(0, 3) == 0) {
        for (int i = tests::random::get_int(1, 2); i > 0; --i) {
            const auto col = std::array{column::s, column::v1, column::v2}[tests::random::get_int(0, 2)];
            const auto op = std::array{comparison::eq, comparison::lt, comparison::gt}[tests::random::get_int(0, 2)];
            q.filter.push_back(predicate{col, op, tests::random::get_int<int32_t>(0, 9)});
        }
    }
    if (tests::random::get_int(0, 3) == 0) {
        q.per_partition_limit = tests::random::get_int<uint64_t>(1, 3);
    }
    return q;
}

std::vector<answer_row> evaluate(const schema& s, const history& h, const select_query& q) {
    validate(h);
    validate(q);
    const auto partitions = resolve(h);
    // CQL sorts the listed partition keys by value. A scan of the whole ring
    // reads partitions in ring order.
    std::vector<int32_t> pks;
    if (q.partitions) {
        pks = *q.partitions;
        std::ranges::sort(pks);
    } else {
        pks = ring_order(s, partitions | std::views::keys | std::ranges::to<std::vector>());
    }

    const auto limit = q.limit.value_or(std::numeric_limits<uint64_t>::max());
    const auto partition_limit = q.partition_limit.value_or(std::numeric_limits<uint64_t>::max());
    std::vector<answer_row> answer;
    uint64_t partition_count = 0;
    for (auto pk : pks) {
        if (answer.size() == limit || partition_count == partition_limit) {
            break;
        }
        auto it = partitions.find(pk);
        if (it == partitions.end()) {
            continue;
        }
        auto rows = partition_answer(pk, it->second, q);
        if (rows.empty()) {
            continue;
        }
        ++partition_count;
        const auto count = std::min<uint64_t>(rows.size(), limit - answer.size());
        answer.insert(answer.end(), rows.begin(), rows.begin() + count);
    }
    return answer;
}

} // namespace tests::read_model

auto fmt::formatter<tests::read_model::answer_row>::format(const tests::read_model::answer_row& r, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto value = [] (const std::optional<int32_t>& v) {
        return v ? fmt::format("{}", *v) : "null";
    };
    return fmt::format_to(ctx.out(), "{{pk: {}, ck: {}, s: {}, v1: {}, v2: {}}}", r.pk, value(r.ck), value(r.s), value(r.v1), value(r.v2));
}

auto fmt::formatter<tests::read_model::select_query>::format(const tests::read_model::select_query& q, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    using namespace tests::read_model;
    std::vector<std::string> fields;
    if (q.partitions) {
        fields.push_back(fmt::format(".partitions = std::vector<int32_t>{{{}}}", fmt::join(*q.partitions, ", ")));
    }
    if (q.ck_start) {
        fields.push_back(fmt::format(".ck_start = {}", describe(q.ck_start)));
    }
    if (q.ck_end) {
        fields.push_back(fmt::format(".ck_end = {}", describe(q.ck_end)));
    }
    if (q.reversed) {
        fields.push_back(".reversed = true");
    }
    if (q.distinct) {
        fields.push_back(".distinct = true");
    }
    if (!q.select_s) {
        fields.push_back(".select_s = false");
    }
    if (!q.select_v1) {
        fields.push_back(".select_v1 = false");
    }
    if (!q.select_v2) {
        fields.push_back(".select_v2 = false");
    }
    if (!q.filter.empty()) {
        auto predicates = q.filter | std::views::transform([] (const predicate& p) { return describe(p); });
        fields.push_back(fmt::format(".filter = {{{}}}", fmt::join(predicates, ", ")));
    }
    if (q.limit) {
        fields.push_back(fmt::format(".limit = {}", *q.limit));
    }
    if (q.per_partition_limit) {
        fields.push_back(fmt::format(".per_partition_limit = {}", *q.per_partition_limit));
    }
    if (q.partition_limit) {
        fields.push_back(fmt::format(".partition_limit = {}", *q.partition_limit));
    }
    return fmt::format_to(ctx.out(), "select_query{{{}}}", fmt::join(fields, ", "));
}
