/*
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/algorithm_ext.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/filesystem.hpp>
#include <json/json.h>
#include "tests/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"
#include "schema_builder.hh"
#include "database.hh"
#include "release.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include <seastar/core/reactor.hh>
#include "sstables/compaction_manager.hh"
#include "transport/messages/result_message.hh"
#include "sstables/shared_index_lists.hh"

using namespace std::chrono_literals;
namespace fs=boost::filesystem;
using int_range = nonwrapping_range<int>;

reactor::io_stats s;

static bool errors_found = false;

cql_test_env* cql_env;

static void print_error(const sstring& msg) {
    std::cerr << "^^^ ERROR: " << msg << "\n";
    errors_found = true;
}

struct metrics_snapshot {
    std::chrono::high_resolution_clock::time_point hr_clock;
    steady_clock_type::duration busy_time;
    steady_clock_type::duration idle_time;
    reactor::io_stats io;
    sstables::shared_index_lists::stats index;
    cache_tracker::stats cache;

    metrics_snapshot() {
        reactor& r = *local_engine;
        io = r.get_io_stats();
        busy_time = r.total_busy_time();
        idle_time = r.total_idle_time();
        hr_clock = std::chrono::high_resolution_clock::now();
        index = sstables::shared_index_lists::shard_stats();
        cache = cql_env->local_db().row_cache_tracker().get_stats();
    }
};

struct output_item {
    sstring value;
    sstring format;
};

using output_items = std::vector<output_item>;
using sstring_vec = std::vector<sstring>;

template <typename... Args>
sstring_vec to_sstrings(Args... args)
{
    return { to_sstring(args)... };
}

struct test_group {
    using requires_cache = seastar::bool_class<class requires_cache_tag>;
    enum type {
        large_partition,
        small_partition,
    };

    std::string name;
    std::string message;
    requires_cache needs_cache;
    type partition_type;
    void (*test_fn)(column_family& cf);
};

using stats_values = std::tuple<
    double, // time
    uint64_t, // frags
    double, // frags_per_second
    uint64_t, // aio
    uint64_t, // kb
    uint64_t, // blocked
    uint64_t, // dropped
    uint64_t, // idx_hit
    uint64_t, // idx_miss
    uint64_t, // idx_blk
    uint64_t, // c_hit
    uint64_t, // c_miss
    uint64_t, // c_blk
    float // cpu
>;


struct output_writer {
    virtual void write_test_group(const test_group& group, bool running) = 0;

    virtual void write_test_names(const output_items& param_names, const output_items& stats_names) = 0;

    virtual void write_test_static_param(sstring name, sstring description) = 0;

    virtual void write_test_values(const sstring_vec& params, const stats_values& stats,
            const output_items& param_names, const output_items& stats_names) = 0;
};

std::array<sstring, std::tuple_size<stats_values>::value> stats_formats =
{
    "{:.6f}",
    "{}",
    "{:.0f}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{:.1f}%",
};

class text_output_writer final
    : public output_writer {
private:

    template <std::size_t... Is>
    inline sstring_vec stats_values_to_strings_impl(const stats_values& values, std::index_sequence<Is...> seq) {
        static_assert(stats_formats.size() == seq.size());
        sstring_vec result {format(stats_formats[Is].c_str(), std::get<Is>(values))...};
        return result;
    }

    template <typename ...Ts>
    sstring_vec stats_values_to_strings(const std::tuple<Ts...>& values) {
        return stats_values_to_strings_impl(values, std::index_sequence_for<Ts...>{});
    };
public:
    void write_test_group(const test_group& group, bool running) override {
        std::cout << std::endl << (running ? "running: " : "skipping: ") << group.name << std::endl;
        if (running) {
            std::cout << group.message << ":" << std::endl;
        }
    }

    void write_test_names(const output_items& param_names, const output_items& stats_names) override {
        for (const auto& name: param_names) {
            std::cout << format(name.format.c_str(), name.value) << " ";
        }
        for (const auto& name: stats_names) {
            std::cout << format(name.format.c_str(), name.value) << " ";
        }
       std::cout << std::endl;
    }

    void write_test_static_param(sstring name, sstring description) override {
        std::cout << description << std::endl;
    }

    void write_test_values(const sstring_vec& params, const stats_values& stats,
            const output_items& param_names, const output_items& stats_names) override {
        for (size_t i = 0; i < param_names.size(); ++i) {
            std::cout << format(param_names.at(i).format.c_str(), params.at(i)) << " ";
        }
        sstring_vec stats_strings = stats_values_to_strings(stats);
        for (size_t i = 0; i < stats_names.size(); ++i) {
            std::cout << format(stats_names.at(i).format.c_str(), stats_strings.at(i)) << " ";
        }
        std::cout << std::endl;
    }
};

static const std::string output_dir {"perf_fast_forward_output/"};

std::string get_run_date_time() {
    using namespace boost::posix_time;
    const ptime current_time = second_clock::local_time();
    auto facet = std::make_unique<time_facet>();
    facet->format("%Y-%m-%d %H:%M:%S");
    std::stringstream stream;
    stream.imbue(std::locale(std::locale::classic(), facet.release()));
    stream << current_time;
    return stream.str();
}

class json_output_writer final
    : public output_writer {
private:
    Json::Value _root;
    Json::Value _tg_properties;
    std::string _current_dir;
    stdx::optional<std::pair<sstring, sstring>> _static_param; // .first = name, .second = description
    std::unordered_map<std::string, uint32_t> _test_count;
    struct metadata {
        std::string version;
        std::string date;
        std::string commit_id;
        std::string run_date_time;
    };
    metadata _metadata;

    Json::Value get_json_metadata() {
        Json::Value versions{Json::objectValue};
        Json::Value scylla_server{Json::objectValue};
        scylla_server["version"] = _metadata.version;
        scylla_server["date"] = _metadata.date;
        scylla_server["commit_id"] = _metadata.commit_id;
        scylla_server["run_date_time"] = _metadata.run_date_time;
        versions["scylla-server"] = scylla_server;
        return versions;
    }

    std::string sanitize_filename(std::string filename) {
        boost::algorithm::replace_all(filename, "[", "begin_incl_");
        boost::algorithm::replace_all(filename, "]", "_end_incl");
        boost::algorithm::replace_all(filename, "(", "begin_excl_");
        boost::algorithm::replace_all(filename, ")", "_end_excl");
        boost::algorithm::erase_all(filename, " ");
        boost::algorithm::erase_all(filename, "{");
        boost::algorithm::erase_all(filename, "}");
        return filename;
    }

public:
    json_output_writer() {
        fs::create_directory(output_dir);

        // scylla_version() string format is "<version>-<release"
        boost::container::static_vector<std::string, 2> version_parts;
        auto version = scylla_version();
        boost::algorithm::split(version_parts, version, [](char c) { return c == '-';});

        // release format is "<scylla-build>.<date>.<git-commit-hash>"
        boost::container::static_vector<std::string, 3> release_parts;
        boost::algorithm::split(release_parts, version_parts[1], [](char c) { return c == '.';});
        _metadata.version = version_parts[0];
        _metadata.date = release_parts[1];
        _metadata.commit_id = release_parts[2];
        _metadata.run_date_time = get_run_date_time();
    }

    void write_test_group(const test_group& group, bool running) override {
        _static_param = stdx::nullopt;
        _test_count.clear();
        _root = Json::Value{Json::objectValue};
        _tg_properties = Json::Value{Json::objectValue};
        _current_dir = output_dir + group.name + "/";
        fs::create_directory(_current_dir);
        _tg_properties["name"] = group.name;
        _tg_properties["message"] = group.message;
        _tg_properties["partition_type"] = group.partition_type == test_group::large_partition ? "large" : "small";
        _tg_properties["needs_cache"] = (group.needs_cache == test_group::requires_cache::yes);
    }

    void write_test_names(const output_items& param_names, const output_items& stats_names) override {
    }

    void write_test_static_param(sstring name, sstring description) override {
        _static_param = std::pair(name, description);
    }

    // Helpers to workaround issue with ambiguous typedefs in older versions of JsonCpp - see #3208.
    template <typename T>
    void assign_to(Json::Value& value, const T& t) {
        value = t;
    }

    inline void assign_to(Json::Value& value, uint64_t u) {
        value = static_cast<Json::UInt64>(u);
    }

    template <std::size_t... Is>
    void write_test_values_impl(Json::Value& stats_value,
            const output_items& stats_names, const stats_values& values, std::index_sequence<Is...>) {
        (assign_to(stats_value[stats_names.at(Is).value], std::get<Is>(values)), ...);
    }
    template <typename... Ts>
    void write_test_values_impl(Json::Value& stats_value,
            const output_items& stats_names, const std::tuple<Ts...>& values) {
        write_test_values_impl(stats_value, stats_names, values, std::index_sequence_for<Ts...>{});
    }

    void write_test_values(const sstring_vec& params, const stats_values& values,
            const output_items& param_names, const output_items& stats_names) override {
        Json::Value root{Json::objectValue};
        root["test_group_properties"] = _tg_properties;
        Json::Value params_value{Json::objectValue};
        for (size_t i = 0; i < param_names.size(); ++i) {
            const sstring& param_name = param_names.at(i).value;
            if (!param_name.empty()) {
                params_value[param_names.at(i).value.c_str()] = params.at(i).c_str();
            }
        }
        if (_static_param) {
            params_value[_static_param->first.c_str()] = _static_param->second.c_str();
        }
        std::string all_params_names = boost::algorithm::join(
                param_names
                    | boost::adaptors::transformed([](const output_item& item) { return item.value; })
                    | boost::adaptors::filtered([](const sstring& s) { return !s.empty(); }),
                ",");
        std::string all_params_values = boost::algorithm::join(
                params
                    | boost::adaptors::indexed()
                    | boost::adaptors::filtered([&param_names](const boost::range::index_value<const sstring&>& idx) {
                        return !param_names[idx.index()].value.empty(); })
                    | boost::adaptors::transformed([](const boost::range::index_value<const sstring&>& idx) { return idx.value(); }),
                ",");
        if (_static_param) {
            all_params_names += "," + _static_param->first;
            all_params_values += "," + _static_param->first;
        }

        // Increase the test run count before we append it to all_params_values
        const auto test_run_count = ++_test_count[all_params_values];

        const std::string test_run_count_name = "test_run_count";
        params_value[test_run_count_name.c_str()] = test_run_count;
        params_value[all_params_names + "," + test_run_count_name] = all_params_values + sprint(",%d", test_run_count);

        Json::Value stats_value{Json::objectValue};
        for (size_t i = 0; i < stats_names.size(); ++i) {
            write_test_values_impl(stats_value, stats_names, values);
        }
        Json::Value result_value{Json::objectValue};
        result_value["parameters"] = params_value;
        result_value["stats"] = stats_value;
        root["results"] = result_value;

        root["versions"] = get_json_metadata();

        std::string filename = boost::algorithm::replace_all_copy(all_params_values, ",", "-") +
                "." + std::to_string(test_run_count) + ".json";

        filename = sanitize_filename(filename);
        std::ofstream result_file{(_current_dir + filename).c_str()};
        result_file << root;
    }
};

class output_manager {
private:
    std::unique_ptr<output_writer> _writer;
    output_items _param_names;
    output_items _stats_names;
public:

    output_manager(sstring format) {
        if (format == "text") {
            _writer = std::make_unique<text_output_writer>();
        } else if (format == "json") {
            _writer = std::make_unique<json_output_writer>();
        } else {
            throw std::runtime_error(sprint("Unsupported output format: %s", format));
        }
    }

    void add_test_group(const test_group& group, bool running) {
        _writer->write_test_group(group, running);
    }

    void set_test_param_names(output_items param_names, output_items stats_names) {
        _param_names = std::move(param_names);
        _stats_names = std::move(stats_names);
        _writer->write_test_names(_param_names, _stats_names);
    }

    void add_test_values(const sstring_vec& params, const stats_values& stats) {
        _writer->write_test_values(params, stats, _param_names, _stats_names);
    }

    void add_test_static_param(sstring name, sstring description) {
        _writer->write_test_static_param(name, description);
    }
};

struct test_result {
    uint64_t fragments_read;
    metrics_snapshot before;
    metrics_snapshot after;

    test_result(metrics_snapshot before, uint64_t fragments_read)
        : fragments_read(fragments_read)
        , before(before)
    { }

    double duration_in_seconds() const {
        return std::chrono::duration<double>(after.hr_clock - before.hr_clock).count();
    }

    double fragment_rate() const { return double(fragments_read) / duration_in_seconds(); }

    uint64_t aio_reads() const { return after.io.aio_reads - before.io.aio_reads; }
    uint64_t aio_read_bytes() const { return after.io.aio_read_bytes - before.io.aio_read_bytes; }
    uint64_t read_aheads_discarded() const { return after.io.fstream_read_aheads_discarded - before.io.fstream_read_aheads_discarded; }
    uint64_t reads_blocked() const { return after.io.fstream_reads_blocked - before.io.fstream_reads_blocked; }

    uint64_t index_hits() const { return after.index.hits - before.index.hits; }
    uint64_t index_misses() const { return after.index.misses - before.index.misses; }
    uint64_t index_blocks() const { return after.index.blocks - before.index.blocks; }

    uint64_t cache_hits() const { return after.cache.partition_hits - before.cache.partition_hits; }
    uint64_t cache_misses() const { return after.cache.partition_misses - before.cache.partition_misses; }
    uint64_t cache_insertions() const { return after.cache.partition_insertions - before.cache.partition_insertions; }

    float cpu_utilization() const {
        auto busy_delta = after.busy_time.count() - before.busy_time.count();
        auto idle_delta = after.idle_time.count() - before.idle_time.count();
        return float(busy_delta) / (busy_delta + idle_delta);
    }

    static output_items stats_names() {
        return {
            {"time (s)", "{:>10}"},
            {"frags",    "{:>9}"},
            {"frag/s",   "{:>10}"},
            {"aio",      "{:>6}"},
            {"(KiB)",    "{:>10}"},
            {"blocked",  "{:>7}"},
            {"dropped",  "{:>7}"},
            {"idx hit",  "{:>8}"},
            {"idx miss", "{:>8}"},
            {"idx blk",  "{:>8}"},
            {"c hit",    "{:>8}"},
            {"c miss",   "{:>8}"},
            {"c blk",    "{:>8}"},
            {"cpu",      "{:>6}"}
        };
    }

    stats_values get_stats_values() {
        return stats_values{
            duration_in_seconds(),
            fragments_read,
            fragment_rate(),
            aio_reads(),
            aio_read_bytes() / 1024,
            reads_blocked(),
            read_aheads_discarded(),
            index_hits(),
            index_misses(),
            index_blocks(),
            cache_hits(),
            cache_misses(),
            cache_insertions(),
            cpu_utilization() * 100
        };
    }
};

static void check_no_disk_reads(const test_result& r) {
    if (r.aio_reads()) {
        print_error("Expected no disk reads");
    }
}

static void check_no_index_reads(const test_result& r) {
    if (r.index_hits() || r.index_misses()) {
        print_error("Expected no index reads");
    }
}

static void check_fragment_count(const test_result& r, uint64_t expected) {
    if (r.fragments_read != expected) {
        print_error(sprint("Expected to read %d fragments", expected));
    }
}

class counting_consumer {
    uint64_t _fragments = 0;
public:
    stop_iteration consume(tombstone) { return stop_iteration::no; }
    template<typename Fragment>
    stop_iteration consume(Fragment&& f) { _fragments++; return stop_iteration::no; }
    void consume_new_partition(const dht::decorated_key&) {}
    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
    uint64_t consume_end_of_stream() { return _fragments; }
};

static
uint64_t consume_all(flat_mutation_reader& rd) {
    return rd.consume(counting_consumer(), db::no_timeout).get0();
}

static
uint64_t consume_all_with_next_partition(flat_mutation_reader& rd) {
    uint64_t fragments = 0;
    do {
        fragments += consume_all(rd);
        rd.next_partition();
        rd.fill_buffer(db::no_timeout).get();
    } while(!rd.is_end_of_stream() || !rd.is_buffer_empty());
    return fragments;
}

static void assert_partition_start(flat_mutation_reader& rd) {
    auto mfopt = rd(db::no_timeout).get0();
    assert(mfopt);
    assert(mfopt->is_partition_start());
}

// cf should belong to ks.test
static test_result scan_rows_with_stride(column_family& cf, int n_rows, int n_read = 1, int n_skip = 0) {
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        cf.schema()->full_slice(),
        default_priority_class(),
        nullptr,
        n_skip ? streamed_mutation::forwarding::yes : streamed_mutation::forwarding::no);

    metrics_snapshot before;
    assert_partition_start(rd);

    uint64_t fragments = 0;
    int ck = 0;
    while (ck < n_rows) {
        if (n_skip) {
            rd.fast_forward_to(position_range(
                position_in_partition(position_in_partition::clustering_row_tag_t(), clustering_key::from_singular(*cf.schema(), ck)),
                position_in_partition(position_in_partition::clustering_row_tag_t(), clustering_key::from_singular(*cf.schema(), ck + n_read))
            ), db::no_timeout).get();
        }
        fragments += consume_all(rd);
        ck += n_read + n_skip;
    }

    return {before, fragments};
}

static dht::decorated_key make_pkey(const schema& s, int n) {
    return dht::global_partitioner().decorate_key(s, partition_key::from_singular(s, n));
}

std::vector<dht::decorated_key> make_pkeys(schema_ptr s, int n) {
    std::vector<dht::decorated_key> keys;
    for (int i = 0; i < n; ++i) {
        keys.push_back(make_pkey(*s, i));
    }
    std::sort(keys.begin(), keys.end(), dht::decorated_key::less_comparator(s));
    return keys;
}

static test_result scan_with_stride_partitions(column_family& cf, int n, int n_read = 1, int n_skip = 0) {
    auto keys = make_pkeys(cf.schema(), n);

    int pk = 0;
    auto pr = n_skip ? dht::partition_range::make_ending_with(dht::partition_range::bound(keys[0], false)) // covering none
                     : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice());

    metrics_snapshot before;

    uint64_t fragments = 0;
    while (pk < n) {
        if (n_skip) {
            pr = dht::partition_range(
                dht::partition_range::bound(keys[pk], true),
                dht::partition_range::bound(keys[std::min(n, pk + n_read) - 1], true)
            );
            rd.fast_forward_to(pr, db::no_timeout).get();
        }
        fragments += consume_all(rd);
        pk += n_read + n_skip;
    }

    return {before, fragments};
}

static test_result slice_rows(column_family& cf, int offset = 0, int n_read = 1) {
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        cf.schema()->full_slice(),
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);

    metrics_snapshot before;
    assert_partition_start(rd);

    rd.fast_forward_to(position_range(
            position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset)),
            position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset + n_read))), db::no_timeout).get();
    uint64_t fragments = consume_all_with_next_partition(rd);

    return {before, fragments};
}

static test_result test_reading_all(flat_mutation_reader& rd) {
    metrics_snapshot before;
    return {before, consume_all(rd)};
}

static test_result slice_rows_by_ck(column_family& cf, int offset = 0, int n_read = 1) {
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(query::clustering_range::make(
            clustering_key::from_singular(*cf.schema(), offset),
            clustering_key::from_singular(*cf.schema(), offset + n_read - 1)))
        .build();
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, slice);
    return test_reading_all(rd);
}

static test_result select_spread_rows(column_family& cf, int stride = 0, int n_read = 1) {
    auto sb = partition_slice_builder(*cf.schema());
    for (int i = 0; i < n_read; ++i) {
        sb.with_range(query::clustering_range::make_singular(clustering_key::from_singular(*cf.schema(), i * stride)));
    }

    auto slice = sb.build();
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        slice);

    return test_reading_all(rd);
}

static test_result test_slicing_using_restrictions(column_family& cf, int_range row_range) {
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(std::move(row_range).transform([&] (int i) -> clustering_key {
            return clustering_key::from_singular(*cf.schema(), i);
        }))
        .build();
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, slice);
    return test_reading_all(rd);
}

static test_result slice_rows_single_key(column_family& cf, int offset = 0, int n_read = 1) {
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice(), default_priority_class(), nullptr, streamed_mutation::forwarding::yes);

    metrics_snapshot before;
    assert_partition_start(rd);
    rd.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset + n_read))), db::no_timeout).get();
    uint64_t fragments = consume_all_with_next_partition(rd);

    return {before, fragments};
}

// cf is for ks.small_part
static test_result slice_partitions(column_family& cf, int n, int offset = 0, int n_read = 1) {
    auto keys = make_pkeys(cf.schema(), n);

    auto pr = dht::partition_range(
        dht::partition_range::bound(keys[offset], true),
        dht::partition_range::bound(keys[std::min(n, offset + n_read) - 1], true)
    );

    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice());
    metrics_snapshot before;

    uint64_t fragments = consume_all_with_next_partition(rd);

    return {before, fragments};
}

static
bytes make_blob(size_t blob_size) {
    static thread_local std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
    bytes big_blob(bytes::initialized_later(), blob_size);
    for (auto&& b : big_blob) {
        b = random_bytes();
    }
    return big_blob;
}

struct table_config {
    sstring name;
    int n_rows;
    int value_size;
};

static test_result test_forwarding_with_restriction(column_family& cf, table_config& cfg, bool single_partition) {
    auto first_key = cfg.n_rows / 2;
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(query::clustering_range::make_starting_with(clustering_key::from_singular(*cf.schema(), first_key)))
        .build();

    auto pr = single_partition ? dht::partition_range::make_singular(make_pkey(*cf.schema(), 0)) : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(),
        pr,
        slice,
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);

    uint64_t fragments = 0;
    metrics_snapshot before;
    assert_partition_start(rd);

    fragments += consume_all(rd);

    rd.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), 1)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), 2))), db::no_timeout).get();

    fragments += consume_all(rd);

    rd.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), first_key - 2)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), first_key + 2))), db::no_timeout).get();

    fragments += consume_all_with_next_partition(rd);
    return {before, fragments};
}

static void drop_keyspace_if_exists(cql_test_env& env, sstring name) {
    try {
        env.local_db().find_keyspace(name);
        std::cout << "Dropping keyspace...\n";
        env.execute_cql("drop keyspace ks;").get();
    } catch (const no_such_keyspace&) {
        // expected
    }
}

static
table_config read_config(cql_test_env& env, const sstring& name) {
    auto msg = env.execute_cql(sprint("select n_rows, value_size from ks.config where name = '%s'", name)).get0();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    auto rs = rows->rs().result_set();
    if (rs.size() < 1) {
        throw std::runtime_error("config not found. Did you run --populate ?");
    }
    const std::vector<bytes_opt>& config_row = rs.rows()[0];
    if (config_row.size() != 2) {
        throw std::runtime_error("config row has invalid size");
    }
    auto n_rows = value_cast<int>(int32_type->deserialize(*config_row[0]));
    auto value_size = value_cast<int>(int32_type->deserialize(*config_row[1]));
    return {name, n_rows, value_size};
}

static
void populate(cql_test_env& env, table_config cfg) {
    drop_keyspace_if_exists(env, "ks");

    env.execute_cql("CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();

    std::cout << "Saving test config...\n";
    env.execute_cql("create table config (name text primary key, n_rows int, value_size int)").get();
    env.execute_cql(sprint("insert into ks.config (name, n_rows, value_size) values ('%s', %d, %d)", cfg.name, cfg.n_rows, cfg.value_size)).get();

    std::cout << "Creating test tables...\n";

    // Large partition with lots of rows
    env.execute_cql("create table test (pk int, ck int, value blob, primary key (pk, ck))"
        " WITH compression = { 'sstable_compression' : '' };").get();

    database& db = env.local_db();

    {
        std::cout << "Populating ks.test with " << cfg.n_rows << " rows...";

        auto insert_id = env.prepare("update test set \"value\" = ? where \"pk\" = 0 and \"ck\" = ?;").get0();

        for (int ck = 0; ck < cfg.n_rows; ++ck) {
            env.execute_prepared(insert_id, {{
                                                 cql3::raw_value::make_value(data_value(make_blob(cfg.value_size)).serialize()),
                                                 cql3::raw_value::make_value(data_value(ck).serialize())
                                             }}).get();
        }

        column_family& cf = db.find_column_family("ks", "test");

        std::cout << "flushing...\n";
        cf.flush().get();

        std::cout << "compacting...\n";
        cf.compact_all_sstables().get();
    }

    // Small partitions, but lots
    env.execute_cql("create table small_part (pk int, value blob, primary key (pk))"
        " WITH compression = { 'sstable_compression' : '' };").get();

    {
        std::cout << "Populating small_part with " << cfg.n_rows << " partitions...";

        auto insert_id = env.prepare("update small_part set \"value\" = ? where \"pk\" = ?;").get0();

        for (int pk = 0; pk < cfg.n_rows; ++pk) {
            env.execute_prepared(insert_id, {{
                                                 cql3::raw_value::make_value(data_value(make_blob(cfg.value_size)).serialize()),
                                                 cql3::raw_value::make_value(data_value(pk).serialize())
                                             }}).get();
        }

        column_family& cf = db.find_column_family("ks", "small_part");

        std::cout << "flushing...\n";
        cf.flush().get();

        std::cout << "compacting...\n";
        cf.compact_all_sstables().get();
    }
}

static unsigned cardinality(int_range r) {
    assert(r.start());
    assert(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

static unsigned cardinality(stdx::optional<int_range> ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

static stdx::optional<int_range> intersection(int_range a, int_range b) {
    auto int_tri_cmp = [] (int x, int y) {
        return x < y ? -1 : (x > y ? 1 : 0);
    };
    return a.intersection(b, int_tri_cmp);
}

// Number of fragments which is expected to be received by interleaving
// n_read reads with n_skip skips when total number of fragments is n.
static int count_for_skip_pattern(int n, int n_read, int n_skip) {
    return n / (n_read + n_skip) * n_read + std::min(n % (n_read + n_skip), n_read);
}

app_template app;
bool cancel = false;
bool cache_enabled;
bool new_test_case = false;
table_config cfg;
int_range live_range;

std::unique_ptr<output_manager> output_mgr;

void clear_cache() {
    cql_env->local_db().row_cache_tracker().clear();
}

void on_test_group() {
    if (!app.configuration().count("keep-cache-across-test-groups")
        && !app.configuration().count("keep-cache-across-test-cases")) {
        clear_cache();
    }
};

void on_test_case() {
    new_test_case = true;
    if (!app.configuration().count("keep-cache-across-test-cases")) {
        clear_cache();
    }
    if (cancel) {
        throw std::runtime_error("interrupted");
    }
};

void test_large_partition_single_key_slice(column_family& cf) {
    output_mgr->set_test_param_names({{"", "{:<2}"}, {"range", "{:<14}"}}, test_result::stats_names());
    struct first {
    };
    auto test = [&](int_range range) {
        auto r = test_slicing_using_restrictions(cf, range);
        output_mgr->add_test_values(to_sstrings(new_test_case ? "->": 0, format("{}", range)), r.get_stats_values());
        check_fragment_count(r, cardinality(intersection(range, live_range)));
        return r;
    };

    on_test_case();
    test(int_range::make({0}, {1}));
    test_result r = test(int_range::make({0}, {1}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make({0}, {cfg.n_rows / 2}));
    r = test(int_range::make({0}, {cfg.n_rows / 2}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make({0}, {cfg.n_rows}));
    r = test(int_range::make({0}, {cfg.n_rows}));
    check_no_disk_reads(r);

    assert(cfg.n_rows > 200); // assumed below

    on_test_case(); // adjacent, no overlap
    test(int_range::make({1}, {100, false}));
    test(int_range::make({100}, {109}));

    on_test_case(); // adjacent, contained
    test(int_range::make({1}, {100}));
    r = test(int_range::make_singular({100}));
    check_no_disk_reads(r);

    on_test_case(); // overlap
    test(int_range::make({1}, {100}));
    test(int_range::make({51}, {150}));

    on_test_case(); // enclosed
    test(int_range::make({1}, {100}));
    r = test(int_range::make({51}, {70}));
    check_no_disk_reads(r);

    on_test_case(); // enclosing
    test(int_range::make({51}, {70}));
    test(int_range::make({41}, {80}));
    test(int_range::make({31}, {100}));

    on_test_case(); // adjacent, singular excluded
    test(int_range::make({0}, {100, false}));
    test(int_range::make_singular({100}));

    on_test_case(); // adjacent, singular excluded
    test(int_range::make({100, false}, {200}));
    test(int_range::make_singular({100}));

    on_test_case();
    test(int_range::make_ending_with({100}));
    r = test(int_range::make({10}, {20}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({-1}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make_starting_with({100}));
    r = test(int_range::make({150}, {159}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({cfg.n_rows - 1}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({cfg.n_rows + 1}));
    check_no_disk_reads(r);

    on_test_case(); // many gaps
    test(int_range::make({10}, {20, false}));
    test(int_range::make({30}, {40, false}));
    test(int_range::make({60}, {70, false}));
    test(int_range::make({90}, {100, false}));
    test(int_range::make({0}, {100, false}));

    on_test_case(); // many gaps
    test(int_range::make({10}, {20, false}));
    test(int_range::make({30}, {40, false}));
    test(int_range::make({60}, {70, false}));
    test(int_range::make({90}, {100, false}));
    test(int_range::make({10}, {100, false}));
}

void test_large_partition_skips(column_family& cf) {
    output_mgr->set_test_param_names({{"read", "{:<7}"}, {"skip", "{:<7}"}}, test_result::stats_names());
    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_rows_with_stride(cf, cfg.n_rows, n_read, n_skip);
        output_mgr->add_test_values(to_sstrings(n_read, n_skip), r.get_stats_values());
        check_fragment_count(r, count_for_skip_pattern(cfg.n_rows, n_read, n_skip));
    };
    auto test = [&] (int n_read, int n_skip) {
        on_test_case();
        do_test(n_read, n_skip);
    };

    test(1, 0);

    test(1, 1);
    test(1, 8);
    test(1, 16);
    test(1, 32);
    test(1, 64);
    test(1, 256);
    test(1, 1024);
    test(1, 4096);

    test(64, 1);
    test(64, 8);
    test(64, 16);
    test(64, 32);
    test(64, 64);
    test(64, 256);
    test(64, 1024);
    test(64, 4096);

    if (cache_enabled) {
        output_mgr->add_test_static_param("cache_enabled", "Testing cache scan of large partition with varying row continuity.");
        for (auto n_read : {1, 64}) {
            for (auto n_skip : {1, 64}) {
                on_test_case();
                do_test(n_read, n_skip); // populate with gaps
                do_test(1, 0);
            }
        }
    }
}

void test_large_partition_slicing(column_family& cf) {
    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&] (int offset, int read) {
        on_test_case();
        auto r = slice_rows(cf, offset, read);
        output_mgr->add_test_values(to_sstrings(offset, read), r.get_stats_values());
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(cfg.n_rows / 2, 1);
    test(cfg.n_rows / 2, 32);
    test(cfg.n_rows / 2, 256);
    test(cfg.n_rows / 2, 4096);
}

void test_large_partition_slicing_clustering_keys(column_family& cf) {
    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&] (int offset, int read) {
        on_test_case();
        auto r = slice_rows_by_ck(cf, offset, read);
        output_mgr->add_test_values(to_sstrings(offset, read), r.get_stats_values());
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(cfg.n_rows / 2, 1);
    test(cfg.n_rows / 2, 32);
    test(cfg.n_rows / 2, 256);
    test(cfg.n_rows / 2, 4096);
}

void test_large_partition_slicing_single_partition_reader(column_family& cf) {
    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&](int offset, int read) {
        on_test_case();
        auto r = slice_rows_single_key(cf, offset, read);
        output_mgr->add_test_values(to_sstrings(offset, read), r.get_stats_values());
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(cfg.n_rows / 2, 1);
    test(cfg.n_rows / 2, 32);
    test(cfg.n_rows / 2, 256);
    test(cfg.n_rows / 2, 4096);
}

void test_large_partition_select_few_rows(column_family& cf) {
    output_mgr->set_test_param_names({{"stride", "{:<7}"}, {"rows", "{:<7}"}}, test_result::stats_names());
    auto test = [&](int stride, int read) {
        on_test_case();
        auto r = select_spread_rows(cf, stride, read);
        output_mgr->add_test_values(to_sstrings(stride, read), r.get_stats_values());
        check_fragment_count(r, read);
    };

    test(cfg.n_rows / 1, 1);
    test(cfg.n_rows / 2, 2);
    test(cfg.n_rows / 4, 4);
    test(cfg.n_rows / 8, 8);
    test(cfg.n_rows / 16, 16);
    test(2, cfg.n_rows / 2);
}

void test_large_partition_forwarding(column_family& cf) {
    output_mgr->set_test_param_names({{"pk-scan", "{:<7}"}}, test_result::stats_names());

    on_test_case();
    auto r = test_forwarding_with_restriction(cf, cfg, false);
    check_fragment_count(r, 2);
    output_mgr->add_test_values(to_sstrings("yes"), r.get_stats_values());

    on_test_case();
    r = test_forwarding_with_restriction(cf, cfg, true);
    check_fragment_count(r, 2);
    output_mgr->add_test_values(to_sstrings("no"), r.get_stats_values());
}

void test_small_partition_skips(column_family& cf2) {
    output_mgr->set_test_param_names({{"", "{:<2}"}, {"read", "{:<7}"}, {"skip", "{:<7}"}}, test_result::stats_names());
    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_with_stride_partitions(cf2, cfg.n_rows, n_read, n_skip);
        output_mgr->add_test_values(to_sstrings(new_test_case ? "->" : "", n_read, n_skip), r.get_stats_values());
        new_test_case = false;
        check_fragment_count(r, count_for_skip_pattern(cfg.n_rows, n_read, n_skip));
        return r;
    };
    auto test = [&] (int n_read, int n_skip) {
        on_test_case();
        return do_test(n_read, n_skip);
    };

    auto r = test(1, 0);
    check_no_index_reads(r);

    test(1, 1);
    test(1, 8);
    test(1, 16);
    test(1, 32);
    test(1, 64);
    test(1, 256);
    test(1, 1024);
    test(1, 4096);

    test(64, 1);
    test(64, 8);
    test(64, 16);
    test(64, 32);
    test(64, 64);
    test(64, 256);
    test(64, 1024);
    test(64, 4096);

    if (cache_enabled) {
        output_mgr->add_test_static_param("cache_enabled", "Testing cache scan with small partitions with varying continuity.");
        for (auto n_read : {1, 64}) {
            for (auto n_skip : {1, 64}) {
                on_test_case();
                do_test(n_read, n_skip); // populate with gaps
                do_test(1, 0);
            }
        }
    }
}

void test_small_partition_slicing(column_family& cf2) {
    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&] (int offset, int read) {
        on_test_case();
        auto r = slice_partitions(cf2, cfg.n_rows, offset, read);
        output_mgr->add_test_values(to_sstrings(offset, read), r.get_stats_values());
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(cfg.n_rows / 2, 1);
    test(cfg.n_rows / 2, 32);
    test(cfg.n_rows / 2, 256);
    test(cfg.n_rows / 2, 4096);
}

static std::initializer_list<test_group> test_groups = {
    {
        "large-partition-single-key-slice",
        "Testing effectiveness of caching of large partition, single-key slicing reads",
        test_group::requires_cache::yes,
        test_group::type::large_partition,
        test_large_partition_single_key_slice,
    },
    {
        "large-partition-skips",
        "Testing scanning large partition with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_skips,
    },
    {
        "large-partition-slicing",
        "Testing slicing of large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_slicing,
    },
    {
        "large-partition-slicing-clustering-keys",
        "Testing slicing of large partition using clustering keys",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_slicing_clustering_keys,
    },
    {
        "large-partition-slicing-single-key-reader",
        "Testing slicing of large partition, single-partition reader",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_slicing_single_partition_reader,
    },
    {
        "large-partition-select-few-rows",
        "Testing selecting few rows from a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_select_few_rows,
    },
    {
        "large-partition-forwarding",
        "Testing forwarding with clustering restriction in a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_forwarding,
    },
    {
        "small-partition-skips",
        "Testing scanning small partitions with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        test_small_partition_skips,
    },
    {
        "small-partition-slicing",
        "Testing slicing small partitions",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        test_small_partition_slicing,
    },
};

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app.add_options()
        ("run-tests", bpo::value<std::vector<std::string>>()->default_value(
                boost::copy_range<std::vector<std::string>>(
                    test_groups | boost::adaptors::transformed([] (auto&& tc) { return tc.name; }))
                ),
            "Test groups to run")
        ("list-tests", "Show available test groups")
        ("populate", "populate the table")
        ("verbose", "Enables more logging")
        ("trace", "Enables trace-level logging")
        ("enable-cache", "Enables cache")
        ("keep-cache-across-test-groups", "Clears the cache between test groups")
        ("keep-cache-across-test-cases", "Clears the cache between test cases in each test group")
        ("rows", bpo::value<int>()->default_value(1000000), "Number of CQL rows in a partition. Relevant only for population.")
        ("value-size", bpo::value<int>()->default_value(100), "Size of value stored in a cell. Relevant only for population.")
        ("name", bpo::value<std::string>()->default_value("default"), "Name of the configuration")
        ("output-format", bpo::value<sstring>()->default_value("text"), "Output file for results. 'text' (default) or 'json'")
        ;

    return app.run(argc, argv, [] {
        db::config db_cfg;

        if (app.configuration().count("list-tests")) {
            std::cout << "Test groups:\n";
            for (auto&& tc : test_groups) {
                std::cout << "\tname: " << tc.name << "\n"
                          << (tc.needs_cache ? "\trequires: --enable-cache\n" : "")
                          << (tc.partition_type == test_group::type::large_partition
                              ? "\tlarge partition test\n" : "\tsmall partition test\n")
                          << "\tdescription:\n\t\t" << boost::replace_all_copy(tc.message, "\n", "\n\t\t") << "\n\n";
            }
            return make_ready_future<int>(0);
        }

        sstring datadir = "./perf_large_partition_data";
        ::mkdir(datadir.c_str(), S_IRWXU);

        db_cfg.enable_cache(app.configuration().count("enable-cache"));
        db_cfg.enable_commitlog(false);
        db_cfg.data_file_directories({datadir}, db::config::config_source::CommandLine);

        if (!app.configuration().count("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().count("trace")) {
            logging::logger_registry().set_logger_level("sstable", seastar::log_level::trace);
        }

        std::cout << "Data directory: " << db_cfg.data_file_directories() << "\n";

        return do_with_cql_env([] (cql_test_env& env) {
            return seastar::async([&env] {
                cql_env = &env;
                sstring name = app.configuration()["name"].as<std::string>();

                if (app.configuration().count("populate")) {
                    int n_rows = app.configuration()["rows"].as<int>();
                    int value_size = app.configuration()["value-size"].as<int>();
                    table_config cfg{name, n_rows, value_size};
                    populate(env, cfg);
                } else {
                    if (smp::count != 1) {
                        throw std::runtime_error("The test must be run with one shard");
                    }

                    database& db = env.local_db();
                    column_family& cf = db.find_column_family("ks", "test");

                    cfg = read_config(env, name);
                    cache_enabled = app.configuration().count("enable-cache");
                    new_test_case = false;

                    std::cout << "Config: rows: " << cfg.n_rows << ", value size: " << cfg.value_size << "\n";

                    output_mgr = std::make_unique<output_manager>(app.configuration()["output-format"].as<sstring>());

                    sleep(1s).get(); // wait for system table flushes to quiesce

                    engine().at_exit([&] {
                        cancel = true;
                        return make_ready_future();
                    });

                    auto requested_test_groups = boost::copy_range<std::unordered_set<std::string>>(
                            app.configuration()["run-tests"].as<std::vector<std::string>>()
                    );
                    auto enabled_test_groups = test_groups | boost::adaptors::filtered([&] (auto&& tc) {
                        return requested_test_groups.count(tc.name) != 0;
                    });

                    auto run_tests = [&] (column_family& cf, test_group::type type) {
                        cf.run_with_compaction_disabled([&] {
                            return seastar::async([&] {
                                live_range = int_range({0}, {cfg.n_rows - 1});
                                boost::for_each(
                                    enabled_test_groups
                                    | boost::adaptors::filtered([type] (auto&& tc) { return tc.partition_type == type; }),
                                    [&cf] (auto&& tc) {
                                        if (tc.needs_cache && !cache_enabled) {
                                            output_mgr->add_test_group(tc, false);
                                        } else {
                                            output_mgr->add_test_group(tc, true);
                                            on_test_group();
                                            tc.test_fn(cf);
                                        }
                                    }
                                );
                            });
                        }).get();
                    };

                    run_tests(cf, test_group::type::large_partition);

                    column_family& cf2 = db.find_column_family("ks", "small_part");
                    run_tests(cf2, test_group::type::small_partition);

                }
            });
        }, db_cfg).then([] {
            return errors_found ? -1 : 0;
        });
    });
}
