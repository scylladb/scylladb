/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "compaction/compaction_strategy.hh"
#include "compaction/time_window_compaction_strategy.hh"
#include "db/config.hh"
#include "dht/i_partitioner.hh"
#include "locator/host_id.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"
#include "tombstone_gc.hh"
#include "tools/json_writer.hh"
#include "utils/pretty_printers.hh"
#include "tools/load_system_tables.hh"
#include "tools/scylla-sstable/scylla-sstable.hh"
#include "tools/scylla-sstable/scylla-sstable-layout.hh"
#include "tools/utils.hh"

namespace bpo = boost::program_options;

using json_writer = mutation_json::json_writer;

namespace tools {

namespace {

// The layout operation describes how the sstables of a table are organized by
// its compaction strategy: incremental and size-tiered compaction organize them
// into runs, leveled compaction into levels and time-window compaction into
// time windows. Sstables are grouped by the compaction group owning them -- a
// tablet, or a shard for vnode-based tables -- and, within it, by the above.

enum class layout_grouping { run, level, window };

layout_grouping grouping_of(compaction::compaction_strategy_type type) {
    switch (type) {
    case compaction::compaction_strategy_type::leveled:
        return layout_grouping::level;
    case compaction::compaction_strategy_type::time_window:
        return layout_grouping::window;
    default:
        return layout_grouping::run;
    }
}

std::string_view describe(layout_grouping grouping) {
    switch (grouping) {
    case layout_grouping::run:
        return "runs";
    case layout_grouping::level:
        return "levels";
    case layout_grouping::window:
        return "time windows";
    }
    std::abort();
}

// The short names --strategy accepts, in addition to the compaction strategy
// class names recognized by compaction_strategy::type().
const std::unordered_map<std::string_view, compaction::compaction_strategy_type> strategy_short_names{
    {"ics", compaction::compaction_strategy_type::incremental},
    {"stcs", compaction::compaction_strategy_type::size_tiered},
    {"lcs", compaction::compaction_strategy_type::leveled},
    {"twcs", compaction::compaction_strategy_type::time_window},
};

compaction::compaction_strategy_type parse_strategy(const sstring& name) {
    // the short names are recognized regardless of case, e.g. both lcs and LCS
    auto short_name = name;
    std::transform(short_name.begin(), short_name.end(), short_name.begin(), ::tolower);
    if (auto it = strategy_short_names.find(std::string_view(short_name)); it != strategy_short_names.end()) {
        return it->second;
    }
    try {
        return compaction::compaction_strategy::type(name);
    } catch (...) {
        throw std::invalid_argument(fmt::format("invalid value for option strategy: {}, expected one of ({}), or a compaction strategy class name",
                    name, fmt::join(strategy_short_names | std::views::keys, ", ")));
    }
}

// Everything the layout can report about a single sstable.
struct layout_sstable {
    sstring name;
    sstring generation;
    sstring version;
    sstring origin;
    sstring run;
    uint64_t size = 0;
    uint64_t total_size = 0;
    uint64_t filter_size = 0;
    uint32_t level = 0;
    int64_t window = 0;
    bool spans_windows = false;
    uint64_t partitions = 0;
    int64_t rows = 0;
    uint64_t tombstones = 0;
    std::optional<uint64_t> expired_tombstones;
    int64_t min_timestamp = 0;
    int64_t max_timestamp = 0;
    int64_t max_local_deletion_time = 0;
    int64_t first_token = 0;
    int64_t last_token = 0;
    int64_t mtime = 0;
    double compression_ratio = 0.0;
    std::optional<uint64_t> tablet;
    std::optional<unsigned> shard;
};

// A cell of the layout table. The monostate stands for an unknown value, e.g.
// the tablet of an sstable of a vnode-based table.
using layout_cell = std::variant<std::monostate, int64_t, double, sstring>;

enum class cell_format {
    plain,
    boolean,
    bytes,       // human-readable data size
    percentage,
    epoch_us,    // api::timestamp_type, microseconds since the epoch
    epoch_s,     // gc_clock/db_clock time point, seconds since the epoch
};

struct layout_column {
    const char* name;
    cell_format format;
    layout_cell (*get)(const layout_sstable&);
    const char* description;
};

int64_t percentage_of(uint64_t part, uint64_t whole) {
    return whole ? int64_t(double(part) / double(whole) * 100.0) : 0;
}

const std::vector<layout_column>& layout_columns() {
    static const std::vector<layout_column> columns{
    {"name", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.name; },
        "name of the data component"},
    {"generation", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.generation; },
        "generation of the sstable"},
    {"version", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.version; },
        "sstable format version"},
    {"origin", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.origin; },
        "what produced the sstable (memtable, compaction, repair, ...)"},
    {"run", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.run; },
        "identifier of the run the sstable belongs to"},
    {"level", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.level); },
        "level of the sstable"},
    {"window", cell_format::epoch_us, [] (const layout_sstable& s) -> layout_cell { return s.window; },
        "start of the time window the sstable belongs to"},
    {"spans-windows", cell_format::boolean, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.spans_windows); },
        "whether the data of the sstable spans more than one time window"},
    {"tablet", cell_format::plain, [] (const layout_sstable& s) -> layout_cell {
            return s.tablet ? layout_cell(int64_t(*s.tablet)) : layout_cell(); },
        "index of the tablet owning the sstable"},
    {"shard", cell_format::plain, [] (const layout_sstable& s) -> layout_cell {
            return s.shard ? layout_cell(int64_t(*s.shard)) : layout_cell(); },
        "shard owning the sstable on this node"},
    {"size", cell_format::bytes, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.size); },
        "on-disk size of the data component"},
    {"total-size", cell_format::bytes, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.total_size); },
        "on-disk size of all the components"},
    {"filter-size", cell_format::bytes, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.filter_size); },
        "on-disk size of the bloom filter component"},
    {"partitions", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.partitions); },
        "estimated number of partitions"},
    {"rows", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.rows; },
        "number of rows"},
    {"tombstones", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return int64_t(s.tombstones); },
        "estimated number of tombstones"},
    {"expired", cell_format::plain, [] (const layout_sstable& s) -> layout_cell {
            return s.expired_tombstones ? layout_cell(int64_t(*s.expired_tombstones)) : layout_cell(); },
        "estimated number of tombstones which can already be purged"},
    {"expired-pctg", cell_format::percentage, [] (const layout_sstable& s) -> layout_cell {
            return s.expired_tombstones ? layout_cell(percentage_of(*s.expired_tombstones, s.tombstones)) : layout_cell(); },
        "share of the tombstones which can already be purged"},
    {"min-timestamp", cell_format::epoch_us, [] (const layout_sstable& s) -> layout_cell { return s.min_timestamp; },
        "smallest write timestamp in the sstable"},
    {"max-timestamp", cell_format::epoch_us, [] (const layout_sstable& s) -> layout_cell { return s.max_timestamp; },
        "largest write timestamp in the sstable"},
    {"max-deletion-time", cell_format::epoch_s, [] (const layout_sstable& s) -> layout_cell { return s.max_local_deletion_time; },
        "largest local deletion time in the sstable, \"none\" if some of its data never expires"},
    {"first-token", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.first_token; },
        "token of the first partition"},
    {"last-token", cell_format::plain, [] (const layout_sstable& s) -> layout_cell { return s.last_token; },
        "token of the last partition"},
    {"mtime", cell_format::epoch_s, [] (const layout_sstable& s) -> layout_cell { return s.mtime; },
        "time the data component was last written to"},
    {"compression-ratio", cell_format::plain, [] (const layout_sstable& s) -> layout_cell {
            // uncompressed sstables have no compression ratio recorded
            return s.compression_ratio < 0.0 ? layout_cell() : layout_cell(s.compression_ratio); },
        "size of the compressed data component, relative to the uncompressed one"},
    };
    return columns;
}

const std::vector<std::string_view>& default_layout_column_names() {
    static const std::vector<std::string_view> names{
        "name", "size", "origin", "partitions", "rows", "tombstones", "expired", "min-timestamp", "max-timestamp", "mtime",
    };
    return names;
}

const layout_column& find_layout_column(std::string_view name) {
    auto it = std::ranges::find_if(layout_columns(), [name] (const layout_column& column) { return name == column.name; });
    if (it == layout_columns().end()) {
        throw std::invalid_argument(fmt::format("unknown column: {}, expected one of ({})", name,
                fmt::join(layout_columns() | std::views::transform([] (const layout_column& c) { return c.name; }), ", ")));
    }
    return *it;
}

// Splits a comma-separated option value, e.g. --columns name,size,rows
std::vector<sstring> split_option_list(const sstring& value) {
    auto items = std::views::split(std::string_view(value), std::string_view(","))
            | std::ranges::to<std::vector<sstring>>();
    std::erase(items, sstring());
    return items;
}

std::vector<const layout_column*> get_layout_columns(const bpo::variables_map& vm) {
    const auto value = vm["columns"].as<sstring>();
    if (value == "all") {
        return layout_columns() | std::views::transform([] (const layout_column& c) { return &c; })
                | std::ranges::to<std::vector<const layout_column*>>();
    }
    auto columns = split_option_list(value)
            | std::views::transform([] (const sstring& name) { return &find_layout_column(name); })
            | std::ranges::to<std::vector<const layout_column*>>();
    if (columns.empty()) {
        throw std::invalid_argument("no columns selected, --columns expects at least one column name");
    }
    return columns;
}

// A column to order the sstables by. The keys are applied in the order they
// were provided, that is in decreasing order of relevance.
struct layout_sort_key {
    const layout_column* column;
    bool descending;
};

std::vector<layout_sort_key> get_layout_sort_keys(const bpo::variables_map& vm) {
    std::vector<layout_sort_key> keys;
    for (const auto& spec : split_option_list(vm["sort"].as<sstring>())) {
        const auto separator = spec.find(':');
        bool descending = false;
        if (separator != sstring::npos) {
            const auto direction = spec.substr(separator + 1);
            if (direction == "desc") {
                descending = true;
            } else if (direction != "asc") {
                throw std::invalid_argument(fmt::format("invalid sort direction: {}, expected one of (asc, desc)", direction));
            }
        }
        keys.push_back({&find_layout_column(spec.substr(0, separator)), descending});
    }
    return keys;
}

std::strong_ordering compare_cells(const layout_cell& a, const layout_cell& b) {
    if (a.index() != b.index()) {
        return a.index() <=> b.index();
    }
    return std::visit([&b] (const auto& lhs) -> std::strong_ordering {
        using cell_type = std::decay_t<decltype(lhs)>;
        if constexpr (std::is_same_v<cell_type, std::monostate>) {
            return std::strong_ordering::equal;
        } else if constexpr (std::is_same_v<cell_type, double>) {
            return std::strong_order(lhs, std::get<double>(b));
        } else if constexpr (std::is_same_v<cell_type, sstring>) {
            return std::string_view(lhs) <=> std::string_view(std::get<sstring>(b));
        } else {
            return lhs <=> std::get<cell_type>(b);
        }
    }, a);
}

void sort_sstables(std::vector<layout_sstable>& sstables, const std::vector<layout_sort_key>& keys) {
    std::ranges::stable_sort(sstables, [&keys] (const layout_sstable& a, const layout_sstable& b) {
        for (const auto& key : keys) {
            const auto order = compare_cells(key.column->get(a), key.column->get(b));
            if (order != std::strong_ordering::equal) {
                return key.descending ? order > 0 : order < 0;
            }
        }
        return false;
    });
}

sstring format_epoch_seconds(int64_t seconds) {
    // sstables which have data that never expires have their max local deletion
    // time set to the largest representable one
    if (seconds <= 0 || seconds >= std::numeric_limits<int32_t>::max()) {
        return "none";
    }
    return fmt::format("{:%FT%TZ}", fmt::gmtime(std::time_t(seconds)));
}

sstring format_cell(const layout_cell& cell, cell_format format) {
    return std::visit([format] (const auto& value) -> sstring {
        using cell_type = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<cell_type, std::monostate>) {
            return "-";
        } else if constexpr (std::is_same_v<cell_type, sstring>) {
            return value.empty() ? sstring("-") : value;
        } else if constexpr (std::is_same_v<cell_type, double>) {
            return fmt::format("{:.3f}", value);
        } else {
            switch (format) {
            case cell_format::boolean:
                return value ? "yes" : "no";
            case cell_format::bytes:
                return fmt::format("{:i}", ::utils::pretty_printed_data_size(value));
            case cell_format::percentage:
                return fmt::format("{}%", value);
            case cell_format::epoch_us:
                return format_epoch_seconds(value / 1000000);
            case cell_format::epoch_s:
                return format_epoch_seconds(value);
            case cell_format::plain:
                return fmt::to_string(value);
            }
            std::abort();
        }
    }, cell);
}

// The aggregate of a set of sstables, printed for each group and each bucket.
struct layout_summary {
    uint64_t sstables = 0;
    uint64_t size = 0;
    uint64_t partitions = 0;
    int64_t rows = 0;
    uint64_t tombstones = 0;
    std::optional<uint64_t> expired_tombstones;
    int64_t min_timestamp = std::numeric_limits<int64_t>::max();
    int64_t max_timestamp = std::numeric_limits<int64_t>::min();
    int64_t first_token = std::numeric_limits<int64_t>::max();
    int64_t last_token = std::numeric_limits<int64_t>::min();

    void add(const layout_sstable& sst) {
        ++sstables;
        size += sst.size;
        partitions += sst.partitions;
        rows += sst.rows;
        tombstones += sst.tombstones;
        if (sst.expired_tombstones) {
            expired_tombstones = expired_tombstones.value_or(0) + *sst.expired_tombstones;
        }
        min_timestamp = std::min(min_timestamp, sst.min_timestamp);
        max_timestamp = std::max(max_timestamp, sst.max_timestamp);
        first_token = std::min(first_token, sst.first_token);
        last_token = std::max(last_token, sst.last_token);
    }
};

sstring describe(const layout_summary& summary) {
    const auto expired = summary.expired_tombstones
            ? sstring(fmt::format("{}, {}%", *summary.expired_tombstones, percentage_of(*summary.expired_tombstones, summary.tombstones)))
            : sstring("unknown");
    return fmt::format("sstables: {}, size: {:i}, partitions: {}, rows: {}, tombstones: {} (expired: {}), timestamp: [{}, {}], token: [{}, {}]",
            summary.sstables, ::utils::pretty_printed_data_size(summary.size), summary.partitions, summary.rows,
            summary.tombstones, expired,
            format_epoch_seconds(summary.min_timestamp / 1000000), format_epoch_seconds(summary.max_timestamp / 1000000),
            summary.first_token, summary.last_token);
}

// gc grace seconds only says when tombstones expire in the timeout mode
sstring describe(const tombstone_gc_options& options, std::chrono::seconds gc_grace_seconds,
        std::optional<size_t> repaired_ranges) {
    switch (options.mode()) {
    case tombstone_gc_mode::timeout:
        return fmt::format("gc grace seconds: {}, tombstones dropped more than that ago are counted as expired",
                gc_grace_seconds.count());
    case tombstone_gc_mode::immediate:
        return "every tombstone which was dropped is counted as expired";
    case tombstone_gc_mode::disabled:
        return "no tombstone can be purged";
    case tombstone_gc_mode::repair:
        if (!repaired_ranges) {
            return "expired tombstones are not reported: the repair history of the table could not be read";
        }
        return fmt::format("repaired ranges: {}, propagation delay: {}s, tombstones dropped before a range was"
                " repaired, less the propagation delay, are counted as expired in it",
                *repaired_ranges, options.propagation_delay_in_seconds().count());
    }
    std::abort();
}

// The sstables of a single run, level or time window.
struct layout_bucket {
    sstring label;
    int64_t key = 0;
    std::vector<layout_sstable> sstables;

    layout_summary summary() const {
        layout_summary summary;
        for (const auto& sst : sstables) {
            summary.add(sst);
        }
        return summary;
    }
};

// The sstables of a single compaction group: a tablet, or a shard for
// vnode-based tables.
struct layout_compaction_group {
    sstring label;
    std::vector<layout_bucket> buckets;

    layout_summary summary() const {
        layout_summary summary;
        for (const auto& bucket : buckets) {
            for (const auto& sst : bucket.sstables) {
                summary.add(sst);
            }
        }
        return summary;
    }
};

void print_layout_text(const std::vector<layout_compaction_group>& groups, const std::vector<const layout_column*>& columns) {
    // pre-render the cells, so that the columns are aligned across all the
    // groups and buckets
    auto widths = columns | std::views::transform([] (const layout_column* c) { return std::strlen(c->name); })
            | std::ranges::to<std::vector<size_t>>();
    std::unordered_map<const layout_sstable*, std::vector<sstring>> cells;
    layout_summary total;
    std::map<unsigned, layout_summary> per_shard;
    for (const auto& group : groups) {
        for (const auto& bucket : group.buckets) {
            for (const auto& sst : bucket.sstables) {
                total.add(sst);
                if (sst.shard) {
                    per_shard[*sst.shard].add(sst);
                }
                auto& row = cells[&sst];
                for (size_t i = 0; i < columns.size(); ++i) {
                    row.push_back(format_cell(columns[i]->get(sst), columns[i]->format));
                    widths[i] = std::max(widths[i], row.back().size());
                }
            }
        }
    }

    auto print_row = [&] (const std::vector<sstring>& row) {
        for (size_t i = 0; i < row.size(); ++i) {
            // the last column is not padded, to avoid trailing whitespace
            fmt::print(std::cout, "    {:<{}}", row[i], i + 1 == row.size() ? 0 : widths[i]);
        }
        fmt::print(std::cout, "\n");
    };
    const auto header = columns | std::views::transform([] (const layout_column* c) { return sstring(c->name); })
            | std::ranges::to<std::vector<sstring>>();

    for (const auto& group : groups) {
        fmt::print(std::cout, "\n=== {} ===\n{}\n", group.label, describe(group.summary()));
        for (const auto& bucket : group.buckets) {
            fmt::print(std::cout, "\n--- {}: {}\n", bucket.label, describe(bucket.summary()));
            print_row(header);
            for (const auto& sst : bucket.sstables) {
                print_row(cells.at(&sst));
            }
        }
    }
    // compaction is per compaction group, but the shard is what the groups
    // compete for, so summarize what each one holds as well
    for (const auto& [shard, summary] : per_shard) {
        fmt::print(std::cout, "\n=== SHARD #{} SUMMARY ===\n{}\n", shard, describe(summary));
    }
    fmt::print(std::cout, "\n=== TOTAL ===\n{}\n", describe(total));
}

void print_layout_json(const std::vector<layout_compaction_group>& groups, const std::vector<const layout_column*>& columns) {
    json_writer writer;
    writer.StartObject();
    writer.Key("compaction_groups");
    writer.StartArray();
    for (const auto& group : groups) {
        writer.StartObject();
        writer.Key("name");
        writer.String(group.label);
        writer.Key("buckets");
        writer.StartArray();
        for (const auto& bucket : group.buckets) {
            writer.StartObject();
            writer.Key("name");
            writer.String(bucket.label);
            writer.Key("sstables");
            writer.StartArray();
            for (const auto& sst : bucket.sstables) {
                writer.StartObject();
                for (const auto* column : columns) {
                    writer.Key(column->name);
                    std::visit(overloaded_functor{
                        [&] (const std::monostate&) { writer.Null(); },
                        [&] (int64_t value) { writer.Int64(value); },
                        [&] (double value) { writer.Double(value); },
                        [&] (const sstring& value) { writer.String(value); },
                    }, column->get(sst));
                }
                writer.EndObject();
            }
            writer.EndArray();
            writer.EndObject();
        }
        writer.EndArray();
        writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
}

// Maps the sstables of a tablet-based table onto the tablet owning them.
class tablet_owner {
    tools::tablets_t _tablets;
    std::optional<locator::host_id> _host_id;
public:
    tablet_owner(tools::tablets_t tablets, std::optional<locator::host_id> host_id)
        : _tablets(std::move(tablets)), _host_id(host_id) {
    }

    size_t size() const { return _tablets.size(); }

    // Each tablet owns the (last_token(i-1), last_token(i)] token range, and
    // remembers when it was last repaired.
    tools::repaired_ranges_t repaired_ranges() const {
        tools::repaired_ranges_t repaired_ranges;
        auto start = dht::minimum_token();
        for (const auto& [last_token, tablet] : _tablets) {
            if (tablet.repair_time) {
                repaired_ranges.emplace_back(
                        dht::token_range(dht::token_range::bound(start, false), dht::token_range::bound(last_token, true)),
                        to_gc_clock(*tablet.repair_time));
            }
            start = last_token;
        }
        return repaired_ranges;
    }

    // Each tablet owns the (last_token(i-1), last_token(i)] token range, so the
    // tablet owning an sstable is the first one whose range contains the
    // sstable's first token. The shard the sstable resides on is the shard this
    // node represents that tablet on.
    void assign(layout_sstable& sst) const {
        auto tablet = _tablets.lower_bound(dht::token::from_int64(sst.first_token));
        if (tablet == _tablets.end()) {
            sst_log.warn("{} is not owned by any tablet of the table", sst.name);
            return;
        }
        if (auto last_tablet = _tablets.lower_bound(dht::token::from_int64(sst.last_token)); tablet != last_tablet) {
            sst_log.warn("{} spans across multiple tablets, assigning it to the one owning its first token", sst.name);
        }
        sst.tablet = std::distance(_tablets.begin(), tablet);
        if (!_host_id) {
            return;
        }
        for (const auto& replica : tablet->second.replicas) {
            if (replica.host == *_host_id) {
                sst.shard = replica.shard;
            }
        }
    }
};

} // anonymous namespace

void layout_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const db::config& dbcfg, const bpo::variables_map& vm) {
    // The layout of a table is what this is for, so the sstables of the table
    // the schema describes are the ones to describe when none were named on the
    // command line -- wherever the storage options of its keyspace put them.
    const auto of_table = sstables.empty()
            ? load_sstables_of_table(schema, sst_man, dbcfg, vm, permit)
            : std::vector<sstables::shared_sstable>();
    const auto& sstables_to_describe = sstables.empty() ? of_table : sstables;
    if (sstables_to_describe.empty()) {
        throw std::invalid_argument(fmt::format("{}.{} has no sstables to describe", schema->ks_name(), schema->cf_name()));
    }

    const auto strategy = vm.count("strategy")
            ? parse_strategy(vm["strategy"].as<sstring>())
            : schema->configured_compaction_strategy();
    const auto grouping = grouping_of(strategy);
    const auto columns = get_layout_columns(vm);
    const auto sort_keys = get_layout_sort_keys(vm);
    const auto format = get_output_format_from_options(vm, output_format::text);

    auto strategy_options = schema->compaction_strategy_options();
    for (const auto& [key, value] : vm["strategy-option"].as<program_options::string_map>()) {
        strategy_options[key] = value;
    }
    const auto twcs_options = compaction::time_window_compaction_strategy_options(strategy_options);
    auto window_of = [&twcs_options] (api::timestamp_type timestamp) -> int64_t {
        if (timestamp == api::missing_timestamp) {
            return 0;
        }
        return compaction::time_window_compaction_strategy::get_window_for(twcs_options, timestamp);
    };

    const auto gc_grace_seconds = std::chrono::duration_cast<std::chrono::seconds>(schema->gc_grace_seconds());
    const auto& gc_options = schema->tombstone_gc_options();

    // The layout of a tablet-based table is described per tablet, that of a
    // vnode-based one per shard, as those are the compaction groups its sstables
    // are organized in.
    auto table = schema->id();
    try {
        // the id the table directory is named after is the authoritative one,
        // a schema which didn't come from the schema tables carries a made up id
        table = extract_from_sstable_path(vm).id;
    } catch (...) {
        sst_log.debug("failed to extract the table id from the sstable path: {:t}", std::current_exception());
    }
    const auto data_dir_path = find_data_dir(vm, dbcfg);
    std::optional<tools::local_node_info> local_node;
    if (!data_dir_path.empty()) {
        try {
            local_node = tools::load_local_node_info(dbcfg, data_dir_path, permit).get();
        } catch (...) {
            sst_log.debug("failed to read the identity of the node from {}: {:t}", data_dir_path, std::current_exception());
        }
    }
    std::optional<std::filesystem::path> system_tablets_dir;
    if (vm.count("system-tablets-dir")) {
        system_tablets_dir = std::filesystem::path(vm["system-tablets-dir"].as<sstring>());
    }
    std::optional<tablet_owner> tablets;
    // a table with no tablets in system.tablets is vnode-based, but one whose
    // system.tablets couldn't be read may be either
    auto vnode_based = data_dir_path.empty() && !system_tablets_dir;
    if (!data_dir_path.empty() || system_tablets_dir) {
        try {
            auto table_tablets = tools::load_system_tablets(dbcfg, data_dir_path, table, permit, system_tablets_dir).get();
            if (table_tablets.empty()) {
                sst_log.info("{}.{} has no tablets in system.tablets, describing it as a vnode-based table."
                        " If it is expected to be tablet-based, system.tablets has to be on disk: nodetool flush system tablets",
                        schema->ks_name(), schema->cf_name());
                vnode_based = true;
            } else {
                tablets.emplace(std::move(table_tablets), local_node ? std::optional(local_node->host_id) : std::nullopt);
            }
        } catch (...) {
            sst_log.warn("failed to read system.tablets, {}.{} is not known to be either tablet- or vnode-based: {:t}",
                    schema->ks_name(), schema->cf_name(), std::current_exception());
        }
        if (tablets && !local_node) {
            // the shard a tablet is on is the shard this node represents it on,
            // so without knowing which host this is, there is no shard to report
            sst_log.warn("the identity of the node owning {} is unknown, the sstables of {}.{} are not attributed to shards",
                    data_dir_path, schema->ks_name(), schema->cf_name());
        }
    }
    // Which tombstones are expired is left to the tombstone_gc_state a node
    // uses, so that every mode is interpreted exactly as it interprets them.
    // Under the repair mode it answers out of the repair history, which lives in
    // system.tablets for a tablet-based table and in system.repair_history for a
    // vnode-based one -- a node merges both into this same state.
    shared_tombstone_gc_state shared_gc_state;
    std::optional<size_t> repaired_ranges;
    if (gc_options.mode() == tombstone_gc_mode::repair) {
        std::optional<tools::repaired_ranges_t> ranges;
        if (tablets) {
            ranges = tablets->repaired_ranges();
        } else if (vnode_based && !data_dir_path.empty()) {
            try {
                ranges = tools::load_system_repair_history(dbcfg, data_dir_path, table, permit).get();
            } catch (...) {
                sst_log.warn("failed to read the repair history of {}.{} from {}, its expired tombstones are not"
                        " reported: {:t}", schema->ks_name(), schema->cf_name(), data_dir_path, std::current_exception());
            }
        }
        if (ranges) {
            for (const auto& [range, repair_time] : *ranges) {
                // the gc state is asked about the schema, so the history has to
                // be registered under its id, which is the id of the table only
                // when the schema came from the schema tables
                shared_gc_state.update_repair_time(schema->id(), range, repair_time);
            }
            repaired_ranges = ranges->size();
        }
    }
    // the commitlog check needs a running node, it has no bearing on an estimate
    const auto gc_state = tombstone_gc_state(shared_gc_state).with_commitlog_check_disabled();
    // nothing can be said about the expired tombstones of a table whose repair
    // history is what decides them, when that history could not be read
    const auto expiry_is_known = gc_options.mode() != tombstone_gc_mode::repair || repaired_ranges.has_value();

    // For a vnode-based table the shard owning an sstable is derived from the
    // sharding parameters of the node, which have to be provided if they
    // couldn't be read from system.topology.
    std::optional<dht::static_sharder> sharder;
    if (tablets) {
        // the compaction groups of a tablet-based table are its tablets, the
        // sharding parameters of the node have no say in them
        if (vm.count("shards") || vm.count("ignore-msb-bits")) {
            throw std::invalid_argument(fmt::format("{}.{} is a tablet-based table: --shards and --ignore-msb-bits"
                    " describe the sharding of a vnode-based one", schema->ks_name(), schema->cf_name()));
        }
    } else {
        const auto shards = vm.count("shards")
                ? std::optional(vm["shards"].as<unsigned>())
                : (vnode_based && local_node ? local_node->shard_count : std::nullopt);
        const auto ignore_msb_bits = vm.count("ignore-msb-bits")
                ? std::optional(vm["ignore-msb-bits"].as<unsigned>())
                : (vnode_based && local_node ? local_node->ignore_msb_bits : std::nullopt);
        if (shards && ignore_msb_bits) {
            sharder.emplace(*shards, *ignore_msb_bits);
        } else {
            sst_log.info("unknown sharding parameters of {}.{}, describing all its sstables as a single compaction group,"
                    " provide --shards and --ignore-msb-bits to have them described per shard",
                    schema->ks_name(), schema->cf_name());
        }
    }

    // Collect the sstables into their compaction group and, within it, into
    // their run, level or time window.
    std::map<std::pair<int64_t, int64_t>, std::map<sstring, layout_bucket>> layout;
    for (const auto& sst : sstables_to_describe) {
        const auto& stats = sst->get_stats_metadata();
        layout_sstable desc{
            .name = sst->component_basename(component_type::Data),
            .generation = fmt::to_string(sst->generation()),
            .version = fmt::to_string(sst->get_version()),
            .origin = sst->get_origin(),
            .run = fmt::to_string(sst->run_identifier()),
            .size = sst->ondisk_data_size(),
            .total_size = sst->bytes_on_disk(),
            .filter_size = sst->filter_size(),
            .level = sst->get_sstable_level(),
            .window = window_of(stats.max_timestamp),
            .spans_windows = window_of(stats.min_timestamp) != window_of(stats.max_timestamp),
            .partitions = sst->get_estimated_key_count(),
            .rows = stats.rows_count,
            .min_timestamp = stats.min_timestamp,
            .max_timestamp = stats.max_timestamp,
            .max_local_deletion_time = stats.max_local_deletion_time,
            .first_token = dht::token::to_int64(sst->get_first_decorated_key().token()),
            .last_token = dht::token::to_int64(sst->get_last_decorated_key().token()),
            .mtime = std::chrono::duration_cast<std::chrono::seconds>(sst->data_file_write_time().time_since_epoch()).count(),
            .compression_ratio = sst->get_compression_ratio(),
        };
        // A tombstone is expired if it was dropped before the point in time
        // before which the data of this sstable can be purged, which the sstable
        // itself works out of the gc state, exactly as compaction does.
        const auto gc_before = sst->get_gc_before_for_drop_estimation(gc_clock::now(), gc_state, schema);
        uint64_t expired_tombstones = 0;
        for (const auto& [deletion_time, count] : stats.estimated_tombstone_drop_time.bin) {
            desc.tombstones += count;
            if (deletion_time < gc_before.time_since_epoch().count()) {
                expired_tombstones += count;
            }
        }
        if (expiry_is_known) {
            desc.expired_tombstones = expired_tombstones;
        }
        if (tablets) {
            tablets->assign(desc);
        } else if (sharder) {
            desc.shard = sharder->shard_for_reads(sst->get_first_decorated_key().token());
        }

        sstring label;
        int64_t key = 0;
        switch (grouping) {
        case layout_grouping::run:
            label = fmt::format("RUN {}", desc.run);
            break;
        case layout_grouping::level:
            label = fmt::format("LEVEL {}", desc.level);
            key = desc.level;
            break;
        case layout_grouping::window:
            label = fmt::format("WINDOW {}", format_epoch_seconds(desc.window / 1000000));
            key = desc.window;
            break;
        }
        const auto group_key = std::pair(desc.shard ? int64_t(*desc.shard) : -1, desc.tablet ? int64_t(*desc.tablet) : -1);
        auto& bucket = layout[group_key][label];
        bucket.label = std::move(label);
        bucket.key = key;
        bucket.sstables.push_back(std::move(desc));
    }

    std::vector<layout_compaction_group> groups;
    for (auto& [group_key, buckets] : layout) {
        const auto [shard, tablet] = group_key;
        sstring label = "ALL SSTABLES";
        if (tablet >= 0 && shard >= 0) {
            label = fmt::format("TABLET #{}, SHARD #{}", tablet, shard);
        } else if (tablet >= 0) {
            label = fmt::format("TABLET #{}", tablet);
        } else if (shard >= 0) {
            label = fmt::format("SHARD #{}", shard);
        } else if (tablets) {
            label = "SSTABLES OWNED BY NO TABLET";
        }
        auto& group = groups.emplace_back(layout_compaction_group{.label = std::move(label)});
        for (auto& [_, bucket] : buckets) {
            sort_sstables(bucket.sstables, sort_keys);
            group.buckets.push_back(std::move(bucket));
        }
        // levels and time windows have a natural order, runs are ordered by
        // size, with the largest -- most expensive to compact -- one first
        if (grouping == layout_grouping::run) {
            std::ranges::sort(group.buckets, std::ranges::greater(), [] (const layout_bucket& b) { return b.summary().size; });
        } else {
            std::ranges::sort(group.buckets, std::ranges::less(), [] (const layout_bucket& b) { return b.key; });
        }
    }

    if (format == output_format::json) {
        print_layout_json(groups, columns);
        return;
    }
    fmt::print(std::cout, "table: {}.{}\ncompaction strategy: {} ({}), describing its {}\n",
            schema->ks_name(), schema->cf_name(), compaction::compaction_strategy::name(strategy),
            vm.count("strategy") ? "provided with --strategy" : "obtained from the schema", describe(grouping));
    fmt::print(std::cout, "tombstone_gc: {}, {}\n", gc_options.mode(), describe(gc_options, gc_grace_seconds, repaired_ranges));
    if (tablets) {
        fmt::print(std::cout, "tablets: {}, this node: {}\n", tablets->size(),
                local_node ? fmt::to_string(local_node->host_id) : "unknown, sstables are not attributed to shards");
    }
    print_layout_text(groups, columns);
    fmt::print(std::cout, "\nNOTE: the number of partitions, tombstones and expired tombstones are estimates,"
            " read from the metadata of the sstables.\n");
}


sstring layout_columns_help() {
    return fmt::format("{}", fmt::join(layout_columns() | std::views::transform([] (const layout_column& c) {
            return fmt::format("* {}: {}", c.name, c.description); }), "\n"));
}

sstring default_layout_columns() {
    return fmt::format("{}", fmt::join(default_layout_column_names(), ","));
}

} // namespace tools
