/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/algorithm_ext.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <json/json.h>
#include "test/lib/cql_test_env.hh"
#include "test/lib/reader_permit.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include "schema_builder.hh"
#include "database.hh"
#include "release.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/units.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/util/closeable.hh>
#include "sstables/compaction_manager.hh"
#include "transport/messages/result_message.hh"
#include "sstables/shared_index_lists.hh"
#include "linux-perf-event.hh"
#include <fstream>

using namespace std::chrono_literals;
using namespace seastar;
namespace fs = std::filesystem;
using int_range = nonwrapping_range<int>;

reactor::io_stats s;

static bool errors_found = false;

cql_test_env* cql_env;

static void print_error(const sstring& msg) {
    std::cout << "^^^ ERROR: " << msg << "\n";
    errors_found = true;
}

class instructions_counter {
    linux_perf_event _event = linux_perf_event::user_instructions_retired();
public:
    instructions_counter() { _event.enable(); }
    uint64_t read() { return _event.read(); }
};

static thread_local instructions_counter the_instructions_counter;

struct metrics_snapshot {
    std::chrono::high_resolution_clock::time_point hr_clock;
    steady_clock_type::duration busy_time;
    steady_clock_type::duration idle_time;
    reactor::io_stats io;
    reactor::sched_stats sched;
    memory::statistics mem;
    sstables::shared_index_lists::stats index;
    cache_tracker::stats cache;
    uint64_t instructions;

    metrics_snapshot()
            : mem(memory::stats()) {
        reactor& r = *local_engine;
        io = r.get_io_stats();
        sched = r.get_sched_stats();
        busy_time = r.total_busy_time();
        idle_time = r.total_idle_time();
        hr_clock = std::chrono::high_resolution_clock::now();
        index = sstables::shared_index_lists::shard_stats();
        cache = cql_env->local_db().row_cache_tracker().get_stats();
        instructions = the_instructions_counter.read();
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

struct table_config {
    sstring name;
    int n_rows;
    int value_size;
    sstring compressor;
};

class dataset;

// Represents a function which accepts a certain set of datasets.
// For those which are accepted, can_run() will return true.
// run() will be ever invoked only on the datasets for which can_run()
// returns true.
class dataset_acceptor {
public:
    virtual ~dataset_acceptor() = default;;
    virtual bool can_run(dataset&) = 0;
    virtual void run(column_family& cf, dataset& ds) = 0;
};

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
    std::unique_ptr<dataset_acceptor> test_fn;
};

class dataset {
    std::string _name;
    std::string _message;
    std::string _table_name;
    std::string _create_table_statement;
public:
    using generator_fn = std::function<mutation_opt()>;

    virtual ~dataset() = default;

    // create_table_statement_pattern should be a format() pattern with {} in place of the table name
    dataset(const std::string& name, const std::string& message, const char* create_table_statement_pattern)
            : _name(name)
            , _message(message)
            , _table_name(boost::replace_all_copy(name, "-", "_"))
           , _create_table_statement(format(create_table_statement_pattern, _table_name))
    { }

    const std::string& name() const { return _name; }
    const std::string& table_name() const { return _table_name; }
    const std::string& description() const { return _message; }
    const std::string& create_table_statement() const { return _create_table_statement; }

    virtual generator_fn make_generator(schema_ptr, const table_config&) = 0;
};

// Adapts a function which accepts DataSet& as its argument to a dataset_acceptor
// such that can_run() selects only datasets which can be cast to DataSet&.
// This allows the function to select compatible datasets using the
// type of its argument.
template<typename DataSet>
class dataset_acceptor_impl: public dataset_acceptor {
    using test_fn = void (*)(column_family&, DataSet&);
    test_fn _fn;
private:
    static DataSet* try_cast(dataset& ds) {
        return dynamic_cast<DataSet*>(&ds);
    }
public:
    dataset_acceptor_impl(test_fn fn) : _fn(fn) {}

    bool can_run(dataset& ds) override {
        return try_cast(ds) != nullptr;
    }

    void run(column_family& cf, dataset& ds) override {
        _fn(cf, *try_cast(ds));
    }
};

template<typename DataSet>
std::unique_ptr<dataset_acceptor> make_test_fn(void (*fn)(column_family&, DataSet&)) {
    return std::make_unique<dataset_acceptor_impl<DataSet>>(fn);
}

using stats_values = std::tuple<
    double, // time
    unsigned, // iterations
    uint64_t, // frags
    double, // frags_per_second
    double, // frags_per_second mad
    double, // frags_per_second max
    double, // frags_per_second min
    double, // average aio
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
    uint64_t, // allocations
    uint64_t, // tasks
    uint64_t, // instructions
    float // cpu
>;


struct output_writer {
    virtual void write_test_group(const test_group& group, const dataset& ds, bool running) = 0;

    virtual void write_dataset_population(const dataset& ds) = 0;

    virtual void write_test_names(const output_items& param_names, const output_items& stats_names) = 0;

    virtual void write_test_static_param(sstring name, sstring description) = 0;

    virtual void write_all_test_values(const sstring_vec& params, const std::vector<stats_values>& values,
            const output_items& param_names, const output_items& stats_names) = 0;

    virtual void write_test_values(const sstring_vec& params, const stats_values& stats,
            const output_items& param_names, const output_items& stats_names) = 0;
};

std::array<sstring, std::tuple_size<stats_values>::value> stats_formats =
{
    "{:.6f}",
    "{}",
    "{}",
    "{:.0f}",
    "{:.0f}",
    "{:.0f}",
    "{:.0f}",
    "{:.1f}", // average aio
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
    void write_test_group(const test_group& group, const dataset& ds, bool running) override {
        std::cout << std::endl << (running ? "running: " : "skipping: ") << group.name << " on dataset " << ds.name() << std::endl;
        if (running) {
            std::cout << group.message << ":" << std::endl;
        }
    }

    void write_dataset_population(const dataset& ds) override {
        std::cout << std::endl << "Populating " << ds.name() << std::endl;
        std::cout << ds.description() << ":" << std::endl;
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

    void write_all_test_values(const sstring_vec& params, const std::vector<stats_values>& values,
            const output_items& param_names, const output_items& stats_names) override {
        for (auto& value : values) {
            for (size_t i = 0; i < param_names.size(); ++i) {
                std::cout << format(param_names.at(i).format.c_str(), params.at(i)) << " ";
            }
            auto stats_strings = stats_values_to_strings(value);
            for (size_t i = 0; i < stats_names.size(); ++i) {
                std::cout << format(stats_names.at(i).format.c_str(), stats_strings.at(i)) << " ";
            }
            std::cout << "\n";
        }
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

static std::string output_dir;

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
    std::optional<std::pair<sstring, sstring>> _static_param; // .first = name, .second = description
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

    void write_common_test_group(const std::string& name, const std::string& message, const dataset& ds) {
        _static_param = std::nullopt;
        _test_count.clear();
        _root = Json::Value{Json::objectValue};
        _tg_properties = Json::Value{Json::objectValue};
        _current_dir = output_dir + "/" + name + "/" + ds.name() + "/";
        fs::create_directories(_current_dir);
        _tg_properties["name"] = name;
        _tg_properties["message"] = message;
        _tg_properties["dataset"] = ds.name();
    }

    void write_test_group(const test_group& group, const dataset& ds, bool running) override {
        write_common_test_group(group.name, group.message, ds);
        _tg_properties["partition_type"] = group.partition_type == test_group::large_partition ? "large" : "small";
        _tg_properties["needs_cache"] = (group.needs_cache == test_group::requires_cache::yes);
    }

    void write_dataset_population(const dataset& ds) override {
        write_common_test_group("population", ds.description(), ds);
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

    void write_test_values_common(const sstring_vec& params, const std::vector<stats_values>& values,
            const output_items& param_names, const output_items& stats_names, bool summary_result) {
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
        const auto test_run_count = _test_count[all_params_values] + 1;
        if (summary_result) {
            ++_test_count[all_params_values];
        }

        const std::string test_run_count_name = "test_run_count";
        params_value[test_run_count_name.c_str()] = test_run_count;
        if (!all_params_names.empty()) {
            params_value[all_params_names + "," + test_run_count_name] = all_params_values + std::string(format(",{:d}", test_run_count));
        }

        Json::Value stats_value;
      if (summary_result) {
        assert(values.size() == 1);
        for (size_t i = 0; i < stats_names.size(); ++i) {
            write_test_values_impl(stats_value, stats_names, values.front());
        }
      } else {
        for (auto& vs : values) {
            Json::Value v{Json::objectValue};
            for (size_t i = 0; i < stats_names.size(); ++i) {
                write_test_values_impl(v, stats_names, vs);
            }
            stats_value.append(std::move(v));
        }
      }
        Json::Value result_value{Json::objectValue};
        result_value["parameters"] = params_value;
        result_value["stats"] = stats_value;
        root["results"] = result_value;

        root["versions"] = get_json_metadata();

        std::string filename = boost::algorithm::replace_all_copy(all_params_values, ",", "-") +
                "." + std::to_string(test_run_count) + (summary_result ? ".json" : ".all.json");

        filename = sanitize_filename(filename);
        std::ofstream result_file{(_current_dir + filename).c_str()};
        result_file << root;
    }

    void write_all_test_values(const sstring_vec& params, const std::vector<stats_values>& values,
            const output_items& param_names, const output_items& stats_names) override {
        write_test_values_common(params, values, param_names, stats_names, false);
    }

    void write_test_values(const sstring_vec& params, const stats_values& values,
            const output_items& param_names, const output_items& stats_names) override {
        write_test_values_common(params, {values}, param_names, stats_names, true);
    }
};

class output_manager {
private:
    std::vector<std::unique_ptr<output_writer>> _writers;
    output_items _param_names;
    output_items _stats_names;
public:

    output_manager(sstring oformat) {
        _writers.push_back(std::make_unique<text_output_writer>());
        if (oformat == "text") {
            // already used
        } else if (oformat == "json") {
            _writers.push_back(std::make_unique<json_output_writer>());
        } else {
            throw std::runtime_error(format("Unsupported output format: {}", oformat));
        }
    }

    void add_test_group(const test_group& group, const dataset& ds, bool running) {
        for (auto&& w : _writers) {
            w->write_test_group(group, ds, running);
        }
    }

    void add_dataset_population(const dataset& ds) {
        for (auto&& w : _writers) {
            w->write_dataset_population(ds);
        }
    }

    void set_test_param_names(output_items param_names, output_items stats_names) {
        _param_names = std::move(param_names);
        _stats_names = std::move(stats_names);
        for (auto&& w : _writers) {
            w->write_test_names(_param_names, _stats_names);
        }
    }

    void add_all_test_values(const sstring_vec& params, const std::vector<stats_values>& stats) {
        for (auto&& w : _writers) {
            w->write_all_test_values(params, stats, _param_names, _stats_names);
        }
    }

    void add_test_values(const sstring_vec& params, const stats_values& stats) {
        for (auto&& w : _writers) {
            w->write_test_values(params, stats, _param_names, _stats_names);
        }
    }

    void add_test_static_param(sstring name, sstring description) {
        for (auto&& w : _writers) {
            w->write_test_static_param(name, description);
        }
    }
};

struct test_result {
    uint64_t fragments_read;
    metrics_snapshot before;
    metrics_snapshot after;
private:
    unsigned _iterations = 1;
    double _fragment_rate_mad = 0;
    double _fragment_rate_max = 0;
    double _fragment_rate_min = 0;
    double _average_aio_operations = 0;
    sstring_vec _params;
    std::optional<sstring> _error;
public:

    test_result() = default;

    test_result(metrics_snapshot before, uint64_t fragments_read)
        : fragments_read(fragments_read)
        , before(before)
    { }

    double duration_in_seconds() const {
        return std::chrono::duration<double>(after.hr_clock - before.hr_clock).count();
    }

    unsigned iteration_count() const { return _iterations; }
    void set_iteration_count(unsigned count) { _iterations = count; }

    double fragment_rate() const { return double(fragments_read) / duration_in_seconds(); }
    double fragment_rate_mad() const { return _fragment_rate_mad; }
    double fragment_rate_max() const { return _fragment_rate_max; }
    double fragment_rate_min() const { return _fragment_rate_min; }

    void set_fragment_rate_stats(double mad, double max, double min) {
        _fragment_rate_mad = mad;
        _fragment_rate_max = max;
        _fragment_rate_min = min;
    }

    double average_aio_operations() const { return _average_aio_operations; }
    void set_average_aio_operations(double aio) { _average_aio_operations = aio; }

    uint64_t aio_reads() const { return after.io.aio_reads - before.io.aio_reads; }
    uint64_t aio_read_bytes() const { return after.io.aio_read_bytes - before.io.aio_read_bytes; }
    uint64_t aio_writes() const { return after.io.aio_writes - before.io.aio_writes; }
    uint64_t aio_written_bytes() const { return after.io.aio_write_bytes - before.io.aio_write_bytes; }
    uint64_t read_aheads_discarded() const { return after.io.fstream_read_aheads_discarded - before.io.fstream_read_aheads_discarded; }
    uint64_t reads_blocked() const { return after.io.fstream_reads_blocked - before.io.fstream_reads_blocked; }

    uint64_t index_hits() const { return after.index.hits - before.index.hits; }
    uint64_t index_misses() const { return after.index.misses - before.index.misses; }
    uint64_t index_blocks() const { return after.index.blocks - before.index.blocks; }

    uint64_t cache_hits() const { return after.cache.partition_hits - before.cache.partition_hits; }
    uint64_t cache_misses() const { return after.cache.partition_misses - before.cache.partition_misses; }
    uint64_t cache_insertions() const { return after.cache.partition_insertions - before.cache.partition_insertions; }

    uint64_t allocations() const { return after.mem.mallocs() - before.mem.mallocs(); }
    uint64_t tasks() const { return after.sched.tasks_processed - before.sched.tasks_processed; }
    uint64_t instructions() const { return after.instructions - before.instructions; }

    float cpu_utilization() const {
        auto busy_delta = after.busy_time.count() - before.busy_time.count();
        auto idle_delta = after.idle_time.count() - before.idle_time.count();
        return float(busy_delta) / (busy_delta + idle_delta);
    }

    static output_items stats_names() {
        return {
            {"time (s)", "{:>10}"},
            {"iterations", "{:>12}"},
            {"frags",    "{:>9}"},
            {"frag/s",   "{:>10}"},
            {"mad f/s",  "{:>10}"},
            {"max f/s",  "{:>10}"},
            {"min f/s",  "{:>10}"},
            {"avg aio",  "{:>10}"},
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
            {"allocs",   "{:>9}"},
            {"tasks",    "{:>7}"},
            {"insns/f",  "{:>7}"},
            {"cpu",      "{:>6}"}
        };
    }

    void set_params(sstring_vec p) { _params = std::move(p); }
    const sstring_vec& get_params() const { return _params; }

    void set_error(sstring msg) { _error = std::move(msg); }
    const std::optional<sstring>& get_error() const { return _error; }

    stats_values get_stats_values() const {
        return stats_values{
            duration_in_seconds(),
            iteration_count(),
            fragments_read,
            fragment_rate(),
            fragment_rate_mad(),
            fragment_rate_max(),
            fragment_rate_min(),
            average_aio_operations(),
            aio_reads() + aio_writes(),
            (aio_read_bytes() + aio_written_bytes()) / 1024,
            reads_blocked(),
            read_aheads_discarded(),
            index_hits(),
            index_misses(),
            index_blocks(),
            cache_hits(),
            cache_misses(),
            cache_insertions(),
            allocations(),
            tasks(),
            instructions() / fragments_read,
            cpu_utilization() * 100
        };
    }
};

static test_result check_no_disk_reads(test_result r) {
    if (r.aio_reads()) {
        r.set_error("Expected no disk reads");
    }
    return r;
}

static void check_no_index_reads(test_result& r) {
    if (r.index_hits() || r.index_misses()) {
        r.set_error("Expected no index reads");
    }
}

static void check_and_report_no_index_reads(test_result& r) {
    check_no_index_reads(r);
    if (r.get_error()) {
        print_error(*r.get_error());
    }
}

static void check_fragment_count(test_result& r, uint64_t expected) {
    if (r.fragments_read != expected) {
        r.set_error(format("Expected to read {:d} fragments", expected));
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
        rd.next_partition().get();
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
        tests::make_permit(),
        query::full_partition_range,
        cf.schema()->full_slice(),
        default_priority_class(),
        nullptr,
        n_skip ? streamed_mutation::forwarding::yes : streamed_mutation::forwarding::no);
    auto close_rd = deferred_close(rd);

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
    return dht::decorate_key(s, partition_key::from_singular(s, n));
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
    auto rd = cf.make_reader(cf.schema(), tests::make_permit(), pr, cf.schema()->full_slice());
    auto close_rd = deferred_close(rd);

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
        tests::make_permit(),
        query::full_partition_range,
        cf.schema()->full_slice(),
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);
    auto close_rd = deferred_close(rd);

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
    auto rd = cf.make_reader(cf.schema(), tests::make_permit(), pr, slice);
    auto close_rd = deferred_close(rd);

    return test_reading_all(rd);
}

static test_result select_spread_rows(column_family& cf, int stride = 0, int n_read = 1) {
    auto sb = partition_slice_builder(*cf.schema());
    for (int i = 0; i < n_read; ++i) {
        sb.with_range(query::clustering_range::make_singular(clustering_key::from_singular(*cf.schema(), i * stride)));
    }

    auto slice = sb.build();
    auto rd = cf.make_reader(cf.schema(),
        tests::make_permit(),
        query::full_partition_range,
        slice);
    auto close_rd = deferred_close(rd);

    return test_reading_all(rd);
}

static test_result test_slicing_using_restrictions(column_family& cf, int_range row_range) {
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(std::move(row_range).transform([&] (int i) -> clustering_key {
            return clustering_key::from_singular(*cf.schema(), i);
        }))
        .build();
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), tests::make_permit(), pr, slice, default_priority_class(), nullptr,
                             streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd = deferred_close(rd);

    return test_reading_all(rd);
}

static test_result slice_rows_single_key(column_family& cf, int offset = 0, int n_read = 1) {
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), tests::make_permit(), pr, cf.schema()->full_slice(), default_priority_class(), nullptr, streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
    auto close_rd = deferred_close(rd);

    metrics_snapshot before;
    assert_partition_start(rd);
    rd.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset + n_read))), db::no_timeout).get();
    uint64_t fragments = consume_all_with_next_partition(rd);

    return {before, fragments};
}

// cf is for ks.small_part
static test_result slice_partitions(column_family& cf, const std::vector<dht::decorated_key>& keys, int offset = 0, int n_read = 1) {
    auto pr = dht::partition_range(
        dht::partition_range::bound(keys[offset], true),
        dht::partition_range::bound(keys[std::min<size_t>(keys.size(), offset + n_read) - 1], true)
    );

    auto rd = cf.make_reader(cf.schema(), tests::make_permit(), pr, cf.schema()->full_slice());
    auto close_rd = deferred_close(rd);
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

// A dataset with one large partition with many clustered fragments.
// Partition key: pk int [0]
// Clusterint key: ck int [0 .. n_rows() - 1]
class clustered_ds {
public:
    virtual int n_rows(const table_config&) = 0;

    clustering_key make_ck(const schema& s, int ck) {
        return clustering_key::from_single_value(s, serialized(ck));
    }
};

// A dataset with many partitions.
// Partition key: pk int [0 .. n_partitions() - 1]
class multipart_ds {
public:
    virtual int n_partitions(const table_config&) = 0;
};

class simple_large_part_ds : public clustered_ds, public dataset {
public:
    simple_large_part_ds(std::string name, std::string desc) : dataset(name, desc,
        "create table {} (pk int, ck int, value blob, primary key (pk, ck))") {}

    int n_rows(const table_config& cfg) override {
        return cfg.n_rows;
    }
};

class large_part_ds1 : public simple_large_part_ds {
public:
    large_part_ds1() : simple_large_part_ds("large-part-ds1", "One large partition with many small rows") {}

    generator_fn make_generator(schema_ptr s, const table_config& cfg) override {
        auto value = serialized(make_blob(cfg.value_size));
        auto& value_cdef = *s->get_column_definition("value");
        auto pk = partition_key::from_single_value(*s, serialized(0));
        return [this, s, ck = 0, n_ck = n_rows(cfg), &value_cdef, value, pk] () mutable -> std::optional<mutation> {
            if (ck == n_ck) {
                return std::nullopt;
            }
            auto ts = api::new_timestamp();
            mutation m(s, pk);
            auto& row = m.partition().clustered_row(*s, make_ck(*s, ck));
            row.cells().apply(value_cdef, atomic_cell::make_live(*value_cdef.type, ts, value));
            ++ck;
            return m;
        };
    }
};

class small_part_ds1 : public multipart_ds, public dataset {
public:
    small_part_ds1() : dataset("small-part", "Many small partitions with no clustering key",
        "create table {} (pk int, value blob, primary key (pk))") {}

    generator_fn make_generator(schema_ptr s, const table_config& cfg) override {
        auto value = serialized(make_blob(cfg.value_size));
        auto& value_cdef = *s->get_column_definition("value");
        return [s, pk = 0, n_pk = n_partitions(cfg), &value_cdef, value] () mutable -> std::optional<mutation> {
            if (pk == n_pk) {
                return std::nullopt;
            }
            auto ts = api::new_timestamp();
            mutation m(s, partition_key::from_single_value(*s, serialized(pk)));
            auto& row = m.partition().clustered_row(*s, clustering_key::make_empty());
            row.cells().apply(value_cdef, atomic_cell::make_live(*value_cdef.type, ts, value));
            ++pk;
            return m;
        };
    }

    int n_partitions(const table_config& cfg) override {
        return cfg.n_rows;
    }
};

static test_result test_forwarding_with_restriction(column_family& cf, clustered_ds& ds, table_config& cfg, bool single_partition) {
    auto first_key = ds.n_rows(cfg) / 2;
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(query::clustering_range::make_starting_with(clustering_key::from_singular(*cf.schema(), first_key)))
        .build();

    auto pr = single_partition ? dht::partition_range::make_singular(make_pkey(*cf.schema(), 0)) : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(),
        tests::make_permit(),
        pr,
        slice,
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
    auto close_rd = deferred_close(rd);

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
    auto msg = env.execute_cql(format("select n_rows, value_size from ks.config where name = '{}'", name)).get0();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    const auto& rs = rows->rs().result_set();
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

static unsigned cardinality(int_range r) {
    assert(r.start());
    assert(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

static unsigned cardinality(std::optional<int_range> ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

static std::optional<int_range> intersection(int_range a, int_range b) {
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
double test_case_duration = 0.;
table_config cfg;
bool dump_all_results = false;

std::unique_ptr<output_manager> output_mgr;

void clear_cache() {
    cql_env->local_db().row_cache_tracker().clear();
}

void on_test_group() {
    if (!app.configuration().contains("keep-cache-across-test-groups")
        && !app.configuration().contains("keep-cache-across-test-cases")) {
        clear_cache();
    }
};

void on_test_case() {
    new_test_case = true;
    if (!app.configuration().contains("keep-cache-across-test-cases")) {
        clear_cache();
    }
    if (cancel) {
        throw std::runtime_error("interrupted");
    }
};

using test_result_vector = std::vector<test_result>;

void print(const test_result& tr) {
    output_mgr->add_test_values(tr.get_params(), tr.get_stats_values());
    if (tr.get_error()) {
        print_error(*tr.get_error());
    }
}

void print_all(const test_result_vector& results) {
    if (!dump_all_results || results.empty()) {
        return;
    }
    output_mgr->add_all_test_values(results.back().get_params(), boost::copy_range<std::vector<stats_values>>(
        results
        | boost::adaptors::transformed([] (const test_result& tr) {
            return tr.get_stats_values();
        })
    ));
}

class result_collector {
    std::vector<std::vector<test_result>> results;
public:
    size_t result_count() const {
        return results.size();
    }
    void add(test_result rs) {
        add(test_result_vector{std::move(rs)});
    }
    void add(test_result_vector rs) {
        if (results.empty()) {
            results.resize(rs.size());
        }
        {
            assert(rs.size() == results.size());
            for (auto j = 0u; j < rs.size(); j++) {
                results[j].emplace_back(rs[j]);
            }
        }
    }
    void done() {
        for (auto&& result : results) {
            print_all(result);

            auto average_aio = boost::accumulate(result, 0., [&] (double a, const test_result& b) {
                return a + b.aio_reads() + b.aio_writes();
            }) / (result.empty() ? 1 : result.size());

            boost::sort(result, [] (const test_result& a, const test_result& b) {
                return a.fragment_rate() < b.fragment_rate();
            });
            auto median = result[result.size() / 2];
            auto fragment_rate_min = result[0].fragment_rate();
            auto fragment_rate_max = result[result.size() - 1].fragment_rate();

            std::vector<double> deviation;
            for (auto& r : result) {
                deviation.emplace_back(fabs(median.fragment_rate() - r.fragment_rate()));
            }
            std::sort(deviation.begin(), deviation.end());
            auto fragment_rate_mad = deviation[deviation.size() / 2];
            median.set_fragment_rate_stats(fragment_rate_mad, fragment_rate_max, fragment_rate_min);
            median.set_iteration_count(result.size());
            median.set_average_aio_operations(average_aio);
            print(median);
        }
    }
};

void run_test_case(std::function<std::vector<test_result>()> fn) {
    result_collector rc;

    auto do_run = [&] {
        on_test_case();
        return fn();
    };

    auto t1 = std::chrono::steady_clock::now();
    rc.add(do_run());
    auto t2 = std::chrono::steady_clock::now();
    auto iteration_duration = (t2 - t1) / std::chrono::duration<double>(1s);

    if (test_case_duration == 0.) {
        rc.done();
        return;
    }

    if (iteration_duration == 0.0) {
        iteration_duration = 1.f;
    }
    auto iteration_count = std::max<size_t>(test_case_duration / iteration_duration, 3);

    for (auto i = 0u; i < iteration_count; i++) {
        rc.add(do_run());
    }

    rc.done();
}

void run_test_case(std::function<test_result()> fn) {
    run_test_case([&] {
        return test_result_vector { fn() };
    });
}

void test_large_partition_single_key_slice(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);
    int_range live_range = int_range({0}, {n_rows - 1});

    output_mgr->set_test_param_names({{"", "{:<2}"}, {"range", "{:<14}"}}, test_result::stats_names());
    struct first {
    };
    auto test = [&](int_range range) {
        auto r = test_slicing_using_restrictions(cf, range);
        r.set_params(to_sstrings(new_test_case ? "->": 0, format("{}", range)));
        check_fragment_count(r, cardinality(intersection(range, live_range)));
        return r;
    };

    run_test_case([&] {
        return test_result_vector {
            test(int_range::make({0}, {1})),
            check_no_disk_reads(test(int_range::make({0}, {1}))),
        };
    });

    run_test_case([&] {
        return test_result_vector {
            test(int_range::make({0}, {n_rows / 2})),
            check_no_disk_reads(test(int_range::make({0}, {n_rows / 2}))),
        };
    });

    run_test_case([&] {
      return test_result_vector {
        test(int_range::make({0}, {n_rows})),
        check_no_disk_reads(test(int_range::make({0}, {n_rows}))),
      };
    });

    assert(n_rows > 200); // assumed below

    run_test_case([&] { // adjacent, no overlap
        return test_result_vector {
            test(int_range::make({1}, {100, false})),
            test(int_range::make({100}, {109})),
        };
    });

    run_test_case([&] { // adjacent, contained
        return test_result_vector {
            test(int_range::make({1}, {100})),
            check_no_disk_reads(test(int_range::make_singular({100}))),
        };
    });

    run_test_case([&] { // overlap
        return test_result_vector {
            test(int_range::make({1}, {100})),
            test(int_range::make({51}, {150})),
        };
    });

    run_test_case([&] { // enclosed
        return test_result_vector {
            test(int_range::make({1}, {100})),
            check_no_disk_reads(test(int_range::make({51}, {70}))),
        };
    });

    run_test_case([&] { // enclosing
        return test_result_vector {
            test(int_range::make({51}, {70})),
            test(int_range::make({41}, {80})),
            test(int_range::make({31}, {100})),
        };
    });

    run_test_case([&] { // adjacent, singular excluded
        return test_result_vector {
            test(int_range::make({0}, {100, false})),
            test(int_range::make_singular({100})),
        };
    });

    run_test_case([&] { // adjacent, singular excluded
        return test_result_vector {
            test(int_range::make({100, false}, {200})),
            test(int_range::make_singular({100})),
        };
    });

    run_test_case([&] {
        return test_result_vector {
            test(int_range::make_ending_with({100})),
            check_no_disk_reads(test(int_range::make({10}, {20}))),
            check_no_disk_reads(test(int_range::make_singular({-1}))),
        };
    });

    run_test_case([&] {
        return test_result_vector {
            test(int_range::make_starting_with({100})),
            check_no_disk_reads(test(int_range::make({150}, {159}))),
            check_no_disk_reads(test(int_range::make_singular({n_rows - 1}))),
            check_no_disk_reads(test(int_range::make_singular({n_rows + 1}))),
        };
    });

    run_test_case([&] { // many gaps
        return test_result_vector {
            test(int_range::make({10}, {20, false})),
            test(int_range::make({30}, {40, false})),
            test(int_range::make({60}, {70, false})),
            test(int_range::make({90}, {100, false})),
            test(int_range::make({0}, {100, false})),
        };
    });

    run_test_case([&] { // many gaps
        return test_result_vector {
            test(int_range::make({10}, {20, false})),
            test(int_range::make({30}, {40, false})),
            test(int_range::make({60}, {70, false})),
            test(int_range::make({90}, {100, false})),
            test(int_range::make({10}, {100, false})),
        };
    });
}

void test_large_partition_skips(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);

    output_mgr->set_test_param_names({{"read", "{:<7}"}, {"skip", "{:<7}"}}, test_result::stats_names());
    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_rows_with_stride(cf, n_rows, n_read, n_skip);
        r.set_params(to_sstrings(n_read, n_skip));
        check_fragment_count(r, count_for_skip_pattern(n_rows, n_read, n_skip));
        return r;
    };
    auto test = [&] (int n_read, int n_skip) {
        run_test_case([&] {
            return do_test(n_read, n_skip);
        });
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
                run_test_case([&] {
                    return test_result_vector {
                        do_test(n_read, n_skip), // populate with gaps
                        do_test(1, 0),
                    };
                });
            }
        }
    }
}

void test_large_partition_slicing(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);

    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&] (int offset, int read) {
      run_test_case([&] {
        auto r = slice_rows(cf, offset, read);
        r.set_params(to_sstrings(offset, read));
        check_fragment_count(r, std::min(n_rows - offset, read));
        return r;
      });
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(n_rows / 2, 1);
    test(n_rows / 2, 32);
    test(n_rows / 2, 256);
    test(n_rows / 2, 4096);
}

void test_large_partition_slicing_clustering_keys(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);

    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&] (int offset, int read) {
      run_test_case([&] {
        auto r = slice_rows_by_ck(cf, offset, read);
        r.set_params(to_sstrings(offset, read));
        check_fragment_count(r, std::min(n_rows - offset, read));
        return r;
      });
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(n_rows / 2, 1);
    test(n_rows / 2, 32);
    test(n_rows / 2, 256);
    test(n_rows / 2, 4096);
}

void test_large_partition_slicing_single_partition_reader(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);

    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto test = [&](int offset, int read) {
      run_test_case([&] {
        auto r = slice_rows_single_key(cf, offset, read);
        r.set_params(to_sstrings(offset, read));
        check_fragment_count(r, std::min(n_rows - offset, read));
        return r;
      });
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(n_rows / 2, 1);
    test(n_rows / 2, 32);
    test(n_rows / 2, 256);
    test(n_rows / 2, 4096);
}

void test_large_partition_select_few_rows(column_family& cf, clustered_ds& ds) {
    auto n_rows = ds.n_rows(cfg);

    output_mgr->set_test_param_names({{"stride", "{:<7}"}, {"rows", "{:<7}"}}, test_result::stats_names());
    auto test = [&](int stride, int read) {
      run_test_case([&] {
        auto r = select_spread_rows(cf, stride, read);
        r.set_params(to_sstrings(stride, read));
        check_fragment_count(r, read);
        return r;
      });
    };

    test(n_rows / 1, 1);
    test(n_rows / 2, 2);
    test(n_rows / 4, 4);
    test(n_rows / 8, 8);
    test(n_rows / 16, 16);
    test(2, n_rows / 2);
}

void test_large_partition_forwarding(column_family& cf, clustered_ds& ds) {
    output_mgr->set_test_param_names({{"pk-scan", "{:<7}"}}, test_result::stats_names());

  run_test_case([&] {
    auto r = test_forwarding_with_restriction(cf, ds, cfg, false);
    check_fragment_count(r, 2);
    r.set_params(to_sstrings("yes"));
    return r;
  });

  run_test_case([&] {
    auto r = test_forwarding_with_restriction(cf, ds, cfg, true);
    check_fragment_count(r, 2);
    r.set_params(to_sstrings("no"));
    return r;
  });
}

void test_small_partition_skips(column_family& cf2, multipart_ds& ds) {
    auto n_parts = ds.n_partitions(cfg);

    output_mgr->set_test_param_names({{"", "{:<2}"}, {"read", "{:<7}"}, {"skip", "{:<7}"}}, test_result::stats_names());
    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_with_stride_partitions(cf2, n_parts, n_read, n_skip);
        r.set_params(to_sstrings(new_test_case ? "->" : "", n_read, n_skip));
        new_test_case = false;
        check_fragment_count(r, count_for_skip_pattern(n_parts, n_read, n_skip));
        return r;
    };
    auto test = [&] (int n_read, int n_skip) {
      test_result r;
      run_test_case([&] {
        r = do_test(n_read, n_skip);
        return r;
      });
      return r;
    };

    auto r = test(1, 0);
    check_and_report_no_index_reads(r);

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
                run_test_case([&] {
                    return test_result_vector {
                        do_test(n_read, n_skip), // populate with gaps
                        do_test(1, 0),
                    };
                });
            }
        }
    }
}

void test_small_partition_slicing(column_family& cf2, multipart_ds& ds) {
    auto n_parts = ds.n_partitions(cfg);

    output_mgr->set_test_param_names({{"offset", "{:<7}"}, {"read", "{:<7}"}}, test_result::stats_names());
    auto keys = make_pkeys(cf2.schema(), n_parts);
    auto test = [&] (int offset, int read) {
      run_test_case([&] {
        auto r = slice_partitions(cf2, keys, offset, read);
        r.set_params(to_sstrings(offset, read));
        check_fragment_count(r, std::min(n_parts - offset, read));
        return r;
      });
    };

    test(0, 1);
    test(0, 32);
    test(0, 256);
    test(0, 4096);

    test(n_parts / 2, 1);
    test(n_parts / 2, 32);
    test(n_parts / 2, 256);
    test(n_parts / 2, 4096);
}

static
auto make_datasets() {
    std::map<std::string, std::unique_ptr<dataset>> dsets;
    auto add = [&] (std::unique_ptr<dataset> ds) {
        if (dsets.contains(ds->name())) {
            throw std::runtime_error(format("Dataset with name '{}' already exists", ds->name()));
        }
        auto name = ds->name();
        dsets.emplace(std::move(name), std::move(ds));
    };
    add(std::make_unique<small_part_ds1>());
    add(std::make_unique<large_part_ds1>());
    return dsets;
}

static std::map<std::string, std::unique_ptr<dataset>> datasets = make_datasets();

static
table& find_table(database& db, dataset& ds) {
    return db.find_column_family("ks", ds.table_name());
}

static
void populate(const std::vector<dataset*>& datasets, cql_test_env& env, const table_config& cfg, size_t flush_threshold) {
    drop_keyspace_if_exists(env, "ks");

    env.execute_cql("CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();

    std::cout << "Saving test config...\n";
    env.execute_cql("create table config (name text primary key, n_rows int, value_size int)").get();
    env.execute_cql(format("insert into ks.config (name, n_rows, value_size) values ('{}', {:d}, {:d})", cfg.name, cfg.n_rows, cfg.value_size)).get();

    database& db = env.local_db();

    for (dataset* ds_ptr : datasets) {
        dataset& ds = *ds_ptr;
        output_mgr->add_dataset_population(ds);

        env.execute_cql(format("{} WITH compression = {{ 'sstable_compression': '{}' }};",
            ds.create_table_statement(), cfg.compressor)).get();

        column_family& cf = find_table(db, ds);
        auto s = cf.schema();
        size_t fragments = 0;
        result_collector rc;

        output_mgr->set_test_param_names({{"flush@ (MiB)", "{:<12}"}}, test_result::stats_names());

        cf.run_with_compaction_disabled([&] {
            return seastar::async([&] {
                auto gen = ds.make_generator(s, cfg);
                while (auto mopt = gen()) {
                    ++fragments;
                    cf.active_memtable().apply(*mopt);
                    if (cf.active_memtable().region().occupancy().used_space() > flush_threshold) {
                        metrics_snapshot before;
                        cf.flush().get();
                        auto r = test_result(std::move(before), std::exchange(fragments, 0));
                        r.set_params({format("{:d}", flush_threshold / MB)});
                        rc.add(std::move(r));
                    }
                }

                if (!rc.result_count()) {
                    print_error("Not enough data to cross the flush threshold. \n"
                                "Lower the flush threshold or increase the amount of data\n");
                }

                rc.done();

                std::cout << "flushing...\n";
                cf.flush().get();
            });
        }).get();

        std::cout << "compacting...\n";
        cf.compact_all_sstables().get();
    }
}

static std::initializer_list<test_group> test_groups = {
    {
        "large-partition-single-key-slice",
        "Testing effectiveness of caching of large partition, single-key slicing reads",
        test_group::requires_cache::yes,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_single_key_slice),
    },
    {
        "large-partition-skips",
        "Testing scanning large partition with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_skips),
    },
    {
        "large-partition-slicing",
        "Testing slicing of large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_slicing),
    },
    {
        "large-partition-slicing-clustering-keys",
        "Testing slicing of large partition using clustering keys",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_slicing_clustering_keys),
    },
    {
        "large-partition-slicing-single-key-reader",
        "Testing slicing of large partition, single-partition reader",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_slicing_single_partition_reader),
    },
    {
        "large-partition-select-few-rows",
        "Testing selecting few rows from a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_select_few_rows),
    },
    {
        "large-partition-forwarding",
        "Testing forwarding with clustering restriction in a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        make_test_fn(test_large_partition_forwarding),
    },
    {
        "small-partition-skips",
        "Testing scanning small partitions with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        make_test_fn(test_small_partition_skips),
    },
    {
        "small-partition-slicing",
        "Testing slicing small partitions",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        make_test_fn(test_small_partition_slicing),
    },
};

// Disables compaction for given tables.
// Compaction will be resumed when the returned object dies.
auto make_compaction_disabling_guard(std::vector<table*> tables) {
    shared_promise<> pr;
    for (auto&& t : tables) {
        // FIXME: discarded future.
        (void)t->run_with_compaction_disabled([f = shared_future<>(pr.get_shared_future())] {
            return f.get_future();
        });
    }
    return seastar::defer([pr = std::move(pr)] () mutable {
        pr.set_value();
    });
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("run-tests", bpo::value<std::vector<std::string>>()->default_value(
                boost::copy_range<std::vector<std::string>>(
                    test_groups | boost::adaptors::transformed([] (auto&& tc) { return tc.name; }))
                ),
            "Test groups to run")
        ("datasets", bpo::value<std::vector<std::string>>()->default_value(
                boost::copy_range<std::vector<std::string>>(datasets | boost::adaptors::map_keys)),
            "Use only the following datasets")
        ("list-tests", "Show available test groups")
        ("list-datasets", "Show available datasets")
        ("populate", "populate the table")
        ("flush-threshold", bpo::value<size_t>()->default_value(300 * MB), "Memtable size threshold for sstable flush. Used during population.")
        ("verbose", "Enables more logging")
        ("trace", "Enables trace-level logging")
        ("enable-cache", "Enables cache")
        ("keep-cache-across-test-groups", "Clears the cache between test groups")
        ("keep-cache-across-test-cases", "Clears the cache between test cases in each test group")
        ("with-compression", "Generates compressed sstables")
        ("rows", bpo::value<int>()->default_value(1000000), "Number of CQL rows in a partition. Relevant only for population.")
        ("value-size", bpo::value<int>()->default_value(100), "Size of value stored in a cell. Relevant only for population.")
        ("name", bpo::value<std::string>()->default_value("default"), "Name of the configuration")
        ("output-format", bpo::value<sstring>()->default_value("text"), "Output file for results. 'text' (default) or 'json'")
        ("test-case-duration", bpo::value<double>()->default_value(1), "Duration in seconds of a single test case (0 for a single run).")
        ("data-directory", bpo::value<sstring>()->default_value("./perf_large_partition_data"), "Data directory")
        ("output-directory", bpo::value<sstring>()->default_value("./perf_fast_forward_output"), "Results output directory (for 'json')")
        ("sstable-format", bpo::value<std::string>()->default_value("md"), "Sstable format version to use during population")
        ("use-binary-search-in-promoted-index", bpo::value<bool>()->default_value(true), "Use binary search based variant of the promoted index cursor")
        ("dump-all-results", "Write results of all iterations of all tests to text files in the output directory")
        ;

    return app.run(argc, argv, [] {
        auto db_cfg_ptr = make_shared<db::config>();
        auto& db_cfg = *db_cfg_ptr;

        if (app.configuration().contains("list-tests")) {
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

        if (app.configuration().contains("list-datasets")) {
            std::cout << "Datasets:\n";
            for (auto&& e : datasets) {
                std::cout << "\tname: " << e.first << "\n"
                          << "\tdescription:\n\t\t" << boost::replace_all_copy(e.second->description(), "\n", "\n\t\t") << "\n\n";
            }
            return make_ready_future<int>(0);
        }

        sstring datadir = app.configuration()["data-directory"].as<sstring>();
        ::mkdir(datadir.c_str(), S_IRWXU);

        output_dir = app.configuration()["output-directory"].as<sstring>();

        db_cfg.enable_cache(app.configuration().contains("enable-cache"));
        db_cfg.enable_commitlog(false);
        db_cfg.data_file_directories({datadir}, db::config::config_source::CommandLine);
        db_cfg.virtual_dirty_soft_limit(1.0); // prevent background memtable flushes.

        auto sstable_format_name = app.configuration()["sstable-format"].as<std::string>();
        if (sstable_format_name == "md") {
            db_cfg.enable_sstables_md_format(true);
        } else if (sstable_format_name == "mc") {
            db_cfg.enable_sstables_md_format(false);
        } else {
            throw std::runtime_error(format("Unsupported sstable format: {}", sstable_format_name));
        }

        sstables::use_binary_search_in_promoted_index = app.configuration()["use-binary-search-in-promoted-index"].as<bool>();

        test_case_duration = app.configuration()["test-case-duration"].as<double>();

        if (!app.configuration().contains("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().contains("trace")) {
            logging::logger_registry().set_logger_level("sstable", seastar::log_level::trace);
        }

        std::cout << "Data directory: " << db_cfg.data_file_directories() << "\n";
        std::cout << "Output directory: " << output_dir << "\n";

        auto init = [] {
            auto conf_seed = app.configuration()["random-seed"];
            auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
            std::cout << "random-seed=" << seed << '\n';
            return smp::invoke_on_all([seed] {
                seastar::testing::local_random_engine.seed(seed + this_shard_id());
            });
        };

        return init().then([db_cfg_ptr] {
          return do_with_cql_env([] (cql_test_env& env) {
            return seastar::async([&env] {
                cql_env = &env;
                sstring name = app.configuration()["name"].as<std::string>();

                dump_all_results = app.configuration().contains("dump-all-results");
                output_mgr = std::make_unique<output_manager>(app.configuration()["output-format"].as<sstring>());

                auto enabled_dataset_names = app.configuration()["datasets"].as<std::vector<std::string>>();
                auto enabled_datasets = boost::copy_range<std::vector<dataset*>>(enabled_dataset_names
                                        | boost::adaptors::transformed([&](auto&& name) {
                    if (!datasets.contains(name)) {
                        throw std::runtime_error(format("No such dataset: {}", name));
                    }
                    return datasets[name].get();
                }));

                if (app.configuration().contains("populate")) {
                    int n_rows = app.configuration()["rows"].as<int>();
                    int value_size = app.configuration()["value-size"].as<int>();
                    auto flush_threshold = app.configuration()["flush-threshold"].as<size_t>();
                    bool with_compression = app.configuration().contains("with-compression");
                    auto compressor = with_compression ? "LZ4Compressor" : "";
                    table_config cfg{name, n_rows, value_size, compressor};
                    populate(enabled_datasets, env, cfg, flush_threshold);
                } else {
                    if (smp::count != 1) {
                        throw std::runtime_error("The test must be run with one shard");
                    }

                    database& db = env.local_db();

                    cfg = read_config(env, name);
                    cache_enabled = app.configuration().contains("enable-cache");
                    new_test_case = false;

                    std::cout << "Config: rows: " << cfg.n_rows << ", value size: " << cfg.value_size << "\n";

                    sleep(1s).get(); // wait for system table flushes to quiesce

                    engine().at_exit([&] {
                        cancel = true;
                        return make_ready_future();
                    });

                    auto requested_test_groups = boost::copy_range<std::unordered_set<std::string>>(
                            app.configuration()["run-tests"].as<std::vector<std::string>>()
                    );
                    auto enabled_test_groups = test_groups | boost::adaptors::filtered([&] (auto&& tc) {
                        return requested_test_groups.contains(tc.name);
                    });

                    auto compaction_guard = make_compaction_disabling_guard(boost::copy_range<std::vector<table*>>(
                        enabled_datasets | boost::adaptors::transformed([&] (auto&& ds) {
                            return &find_table(db, *ds);
                        })));

                    auto run_tests = [&] (test_group::type type) {
                                boost::for_each(
                                    enabled_test_groups
                                    | boost::adaptors::filtered([type] (auto&& tc) { return tc.partition_type == type; }),
                                    [&] (auto&& tc) {
                                     for (auto&& ds : enabled_datasets) {
                                      if (tc.test_fn->can_run(*ds)) {
                                        if (tc.needs_cache && !cache_enabled) {
                                            output_mgr->add_test_group(tc, *ds, false);
                                        } else {
                                            output_mgr->add_test_group(tc, *ds, true);
                                            on_test_group();
                                            tc.test_fn->run(find_table(db, *ds), *ds);
                                        }
                                      }
                                     }
                                    }
                                );
                    };

                    run_tests(test_group::type::large_partition);
                    run_tests(test_group::type::small_partition);
                }
            });
        }, db_cfg_ptr).then([] {
            return errors_found ? -1 : 0;
        });
      });
    });
}
