/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <boost/algorithm/string/join.hpp>
#include <filesystem>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "compaction/compaction.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "gms/feature_service.hh"
#include "schema_builder.hh"
#include "sstables/index_reader.hh"
#include "sstables/sstables_manager.hh"
#include "types/user.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "tools/schema_loader.hh"
#include "tools/utils.hh"

using namespace seastar;

namespace bpo = boost::program_options;

namespace {

const auto app_name = "scylla-sstable";

logging::logger sst_log(app_name);

db::nop_large_data_handler large_data_handler;

struct decorated_key_hash {
    std::size_t operator()(const dht::decorated_key& dk) const {
        return dht::token::to_int64(dk.token());
    }
};

struct decorated_key_equal {
    const schema& _s;
    explicit decorated_key_equal(const schema& s) : _s(s) {
    }
    bool operator()(const dht::decorated_key& a, const dht::decorated_key& b) const {
        return a.equal(_s, b);
    }
};

using partition_set = std::unordered_set<dht::decorated_key, decorated_key_hash, decorated_key_equal>;

template <typename T>
using partition_map = std::unordered_map<dht::decorated_key, T, decorated_key_hash, decorated_key_equal>;

partition_set get_partitions(schema_ptr schema, bpo::variables_map& app_config) {
    partition_set partitions(app_config.count("partition"), {}, decorated_key_equal(*schema));
    auto pk_type = schema->partition_key_type();

    auto dk_from_hex = [&] (std::string_view hex) {
        auto pk = partition_key::from_exploded(pk_type->components(managed_bytes_view(from_hex(hex))));
        return dht::decorate_key(*schema, std::move(pk));
    };

    if (app_config.count("partition")) {
        for (const auto& pk_hex : app_config["partition"].as<std::vector<sstring>>()) {
            partitions.emplace(dk_from_hex(pk_hex));
        }
    }

    if (app_config.count("partitions-file")) {
        auto file = open_file_dma(app_config["partitions-file"].as<sstring>(), open_flags::ro).get();
        auto fstream = make_file_input_stream(file);

        temporary_buffer<char> pk_buf;
        while (auto buf = fstream.read().get()) {
            do {
                const auto it = std::find_if(buf.begin(), buf.end(), [] (char c) { return std::isspace(c); });
                const auto len = it - buf.begin();
                if (!len && !pk_buf) {
                    buf.trim_front(1); // discard extra leading whitespace
                    continue;
                }
                if (pk_buf) {
                    auto new_buf = temporary_buffer<char>(pk_buf.size() + len);
                    auto ot = new_buf.get_write();
                    ot = std::copy_n(pk_buf.begin(), pk_buf.size(), ot);
                    std::copy_n(buf.begin(), len, ot);
                    pk_buf = std::move(new_buf);
                } else {
                    pk_buf = buf.share(0, len);
                }
                buf.trim_front(len);
                if (it != buf.end()) {
                    partitions.emplace(dk_from_hex(std::string_view(pk_buf.begin(), pk_buf.size())));
                    pk_buf = {};
                    buf.trim_front(1); // remove the newline
                }
                thread::maybe_yield();
            } while (buf);
        }
        if (!pk_buf.empty()) { // last line might not have EOL
            partitions.emplace(dk_from_hex(std::string_view(pk_buf.begin(), pk_buf.size())));
        }
    }

    if (!partitions.empty()) {
        sst_log.info("filtering enabled, {} partition(s) to filter for", partitions.size());
    }

    return partitions;
}

const std::vector<sstables::shared_sstable> load_sstables(schema_ptr schema, sstables::sstables_manager& sst_man, const std::vector<sstring>& sstable_names) {
    std::vector<sstables::shared_sstable> sstables;

    parallel_for_each(sstable_names, [schema, &sst_man, &sstables] (const sstring& sst_name) -> future<> {
        const auto sst_path = std::filesystem::path(sst_name);

        if (const auto ftype_opt = co_await file_type(sst_path.c_str(), follow_symlink::yes)) {
            if (!ftype_opt) {
                throw std::invalid_argument(fmt::format("error: failed to determine type of file pointed to by provided sstable path {}", sst_path.c_str()));
            }
            if (*ftype_opt != directory_entry_type::regular) {
                throw std::invalid_argument(fmt::format("error: file pointed to by provided sstable path {} is not a regular file", sst_path.c_str()));
            }
        }

        const auto dir_path = std::filesystem::path(sst_path).remove_filename();
        const auto sst_filename = sst_path.filename();

        auto ed = sstables::entry_descriptor::make_descriptor(dir_path.c_str(), sst_filename.c_str());
        auto sst = sst_man.make_sstable(schema, dir_path.c_str(), ed.generation, ed.version, ed.format);

        co_await sst->load();

        sstables.push_back(std::move(sst));
    }).get();

    return sstables;
}

// stop_iteration::no -> continue consuming sstable content
class sstable_consumer {
public:
    virtual ~sstable_consumer() = default;
    // called at the very start
    virtual future<> on_start_of_stream() = 0;
    // stop_iteration::yes -> on_end_of_sstable() - skip sstable content
    // sstable parameter is nullptr when merging multiple sstables
    virtual future<stop_iteration> on_new_sstable(const sstables::sstable* const) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip partition content
    virtual future<stop_iteration> consume(partition_start&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(static_row&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(clustering_row&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(range_tombstone&&) = 0;
    // stop_iteration::yes -> on_end_of_sstable() - skip remaining partitions in sstable
    virtual future<stop_iteration> consume(partition_end&&) = 0;
    // stop_iteration::yes -> full stop - skip remaining sstables
    virtual future<stop_iteration> on_end_of_sstable() = 0;
    // called at the very end
    virtual future<> on_end_of_stream() = 0;
};

class consumer_wrapper {
public:
    using filter_type = std::function<bool(const dht::decorated_key&)>;
private:
    sstable_consumer& _consumer;
    filter_type _filter;
public:
    consumer_wrapper(sstable_consumer& consumer, filter_type filter)
        : _consumer(consumer), _filter(std::move(filter)) {
    }
    future<stop_iteration> operator()(mutation_fragment&& mf) {
        sst_log.trace("consume {}", mf.mutation_fragment_kind());
        if (mf.is_partition_start() && _filter && !_filter(mf.as_partition_start().key())) {
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return std::move(mf).consume(_consumer);
    }
};

using operation_specific_options = std::unordered_map<sstring, sstring>;

class dumping_consumer : public sstable_consumer {
    schema_ptr _schema;

public:
    explicit dumping_consumer(schema_ptr s, reader_permit, const operation_specific_options&) : _schema(std::move(s)) {
    }
    virtual future<> on_start_of_stream() override {
        std::cout << "{stream_start}" << std::endl;
        return make_ready_future<>();
    }
    virtual future<stop_iteration> on_new_sstable(const sstables::sstable* const sst) override {
        std::cout << "{sstable_start";
        if (sst) {
            std::cout << ": filename " << sst->get_filename();
        }
        std::cout << "}" << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_start&& ps) override {
        std::cout << ps << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(static_row&& sr) override {
        std::cout << static_row::printer(*_schema, sr) << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(clustering_row&& cr) override {
        std::cout << clustering_row::printer(*_schema, cr) << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(range_tombstone&& rt) override {
        std::cout << rt << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_end&& pe) override {
        std::cout << "{partition_end}" << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> on_end_of_sstable() override {
        std::cout << "{sstable_end}" << std::endl;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<> on_end_of_stream() override {
        std::cout << "{stream_end}" << std::endl;
        return make_ready_future<>();
    }
};

// Result can be plotted with the following example script:
//
//     import datetime
//     import json
//     import matplotlib.pyplot as plt
//
//     with open('histogram.json', 'r') as f:
//         data = json.load(f)
//
//     x = data['buckets']
//     y = data['counts']
//
//     max_y = max(y)
//
//     x = [datetime.date.fromtimestamp(i / 1000000).strftime('%Y.%m') for i in x]
//     y = [i / max_y for i in y]
//
//     fig, ax = plt.subplots()
//
//     ax.set_xlabel('Timestamp')
//     ax.set_ylabel('Normalized cell count')
//     ax.set_title('Histogram of data write-time')
//     ax.bar(x, y)
//
//     plt.show()
//
class writetime_histogram_collecting_consumer : public sstable_consumer {
private:
    enum class bucket {
        years,
        months,
        weeks,
        days,
        hours,
    };

public:
    schema_ptr _schema;
    bucket _bucket = bucket::months;
    std::map<api::timestamp_type, uint64_t> _histogram;
    uint64_t _partitions = 0;
    uint64_t _rows = 0;
    uint64_t _cells = 0;
    uint64_t _timestamps = 0;

private:
    api::timestamp_type timestamp_bucket(api::timestamp_type ts) {
        using namespace std::chrono;
        switch (_bucket) {
            case bucket::years:
                return duration_cast<microseconds>(duration_cast<years>(microseconds(ts))).count();
            case bucket::months:
                return duration_cast<microseconds>(duration_cast<months>(microseconds(ts))).count();
            case bucket::weeks:
                return duration_cast<microseconds>(duration_cast<weeks>(microseconds(ts))).count();
            case bucket::days:
                return duration_cast<microseconds>(duration_cast<days>(microseconds(ts))).count();
            case bucket::hours:
                return duration_cast<microseconds>(duration_cast<hours>(microseconds(ts))).count();
        }
    }
    void collect_timestamp(api::timestamp_type ts) {
        ts = timestamp_bucket(ts);

        ++_timestamps;
        auto it = _histogram.find(ts);
        if (it == _histogram.end()) {
            it = _histogram.emplace(ts, 0).first;
        }
        ++it->second;
    }
    void collect_column(const atomic_cell_or_collection& cell, const column_definition& cdef) {
        if (cdef.is_atomic()) {
            ++_cells;
            collect_timestamp(cell.as_atomic_cell(cdef).timestamp());
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            cell.as_collection_mutation().with_deserialized(*cdef.type, [&, this] (collection_mutation_view_description mv) {
                if (mv.tomb) {
                    collect_timestamp(mv.tomb.timestamp);
                }
                for (auto&& c : mv.cells) {
                    ++_cells;
                    collect_timestamp(c.second.timestamp());
                }
            });
        } else {
            throw std::runtime_error(fmt::format("Cannot collect timestamp of cell (column {} of uknown type {})", cdef.name_as_text(), cdef.type->name()));
        }
    }

    void collect_row(const row& r, column_kind kind) {
        ++_rows;
        r.for_each_cell([this, kind] (column_id id, const atomic_cell_or_collection& cell) {
            collect_column(cell, _schema->column_at(kind, id));
        });
    }

    void collect_static_row(const static_row& sr) {
        collect_row(sr.cells(), column_kind::static_column);
    }

    void collect_clustering_row(const clustering_row& cr) {
        if (!cr.marker().is_missing()) {
            collect_timestamp(cr.marker().timestamp());
        }
        if (cr.tomb() != row_tombstone{}) {
            collect_timestamp(cr.tomb().tomb().timestamp);
        }

        collect_row(cr.cells(), column_kind::regular_column);
    }

public:
    explicit writetime_histogram_collecting_consumer(schema_ptr s, reader_permit, const operation_specific_options& op_opts) : _schema(std::move(s)) {
        for (const auto& [key, value] : op_opts) {
            if (value == "years") {
                _bucket = bucket::years;
            } else if (value == "months") {
                _bucket = bucket::months;
            } else if (value == "weeks") {
                _bucket = bucket::weeks;
            } else if (value == "days") {
                _bucket = bucket::days;
            } else if (value == "hours") {
                _bucket = bucket::hours;
            } else {
                throw std::invalid_argument(fmt::format("error: invalid value for writetime-histogram option bucket: {}", value));
            }
        }
    }
    virtual future<> on_start_of_stream() override {
        return make_ready_future<>();
    }
    virtual future<stop_iteration> on_new_sstable(const sstables::sstable* const sst) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_start&& ps) override {
        ++_partitions;
        if (auto tomb = ps.partition_tombstone()) {
            collect_timestamp(tomb.timestamp);
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(static_row&& sr) override {
        collect_static_row(sr);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(clustering_row&& cr) override {
        collect_clustering_row(cr);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(range_tombstone&& rt) override {
        collect_timestamp(rt.tomb.timestamp);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_end&& pe) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> on_end_of_sstable() override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<> on_end_of_stream() override {
        if (_histogram.empty()) {
            sst_log.info("Histogram empty, no data to write");
            co_return;
        }
        sst_log.info("Histogram has {} entries, collected from {} partitions, {} rows, {} cells: {} timestamps total", _histogram.size(), _partitions, _rows, _cells, _timestamps);

        const auto filename = "histogram.json";

        auto file = co_await open_file_dma(filename, open_flags::wo | open_flags::create);
        auto fstream = co_await make_file_output_stream(file);

        co_await fstream.write("{");

        co_await fstream.write("\n\"buckets\": [");
        auto it = _histogram.begin();
        co_await fstream.write(format("\n  {}", it->first));
        for (++it; it != _histogram.end(); ++it) {
            co_await fstream.write(format(",\n  {}", it->first));
        }
        co_await fstream.write("\n]");

        co_await fstream.write(",\n\"counts\": [");
        it = _histogram.begin();
        co_await fstream.write(format("\n  {}", it->second));
        for (++it; it != _histogram.end(); ++it) {
            co_await fstream.write(format(",\n  {}", it->second));
        }
        co_await fstream.write("\n]");
        co_await fstream.write("\n}");

        co_await fstream.close();

        sst_log.info("Histogram written to {}", filename);

        co_return;
    }
};

// scribble here, then call with --operation=custom
class custom_consumer : public sstable_consumer {
    schema_ptr _schema;
    reader_permit _permit;
public:
    explicit custom_consumer(schema_ptr s, reader_permit p, const operation_specific_options&)
        : _schema(std::move(s)), _permit(std::move(p))
    { }
    virtual future<> on_start_of_stream() override {
        return make_ready_future<>();
    }
    virtual future<stop_iteration> on_new_sstable(const sstables::sstable* const sst) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_start&& ps) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(static_row&& sr) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(clustering_row&& cr) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(range_tombstone&& rt) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> consume(partition_end&& pe) override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<stop_iteration> on_end_of_sstable() override {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    virtual future<> on_end_of_stream() override {
        return make_ready_future<>();
    }
};

stop_iteration consume_reader(flat_mutation_reader rd, sstable_consumer& consumer, sstables::sstable* sst, const partition_set& partitions, bool no_skips) {
    auto close_rd = deferred_close(rd);
    if (consumer.on_new_sstable(sst).get() == stop_iteration::yes) {
        return consumer.on_end_of_sstable().get();
    }
    bool skip_partition = false;
    consumer_wrapper::filter_type filter;
    if (!partitions.empty()) {
        filter = [&] (const dht::decorated_key& key) {
            const auto pass = partitions.find(key) != partitions.end();
            sst_log.trace("filter({})={}", key, pass);
            skip_partition = !pass;
            return pass;
        };
    }
    while (!rd.is_end_of_stream()) {
        skip_partition = false;
        rd.consume_pausable(consumer_wrapper(consumer, filter)).get();
        sst_log.trace("consumer paused, skip_partition={}", skip_partition);
        if (!rd.is_end_of_stream() && !skip_partition) {
            if (auto* mfp = rd.peek().get(); mfp && !mfp->is_partition_start()) {
                sst_log.trace("consumer returned stop_iteration::yes for partition end, stopping");
                break;
            }
            if (consumer.consume(partition_end{}).get() == stop_iteration::yes) {
                sst_log.trace("consumer returned stop_iteration::yes for synthetic partition end, stopping");
                break;
            }
            skip_partition = true;
        }
        if (skip_partition) {
            if (no_skips) {
                mutation_fragment_opt mfopt;
                while ((mfopt = rd().get()) && !mfopt->is_end_of_partition());
            } else {
                rd.next_partition().get();
            }
        }
    }
    return consumer.on_end_of_sstable().get();
}

void consume_sstables(schema_ptr schema, reader_permit permit, std::vector<sstables::shared_sstable> sstables, bool merge, bool no_skips,
        std::function<stop_iteration(flat_mutation_reader&, sstables::sstable*)> reader_consumer) {
    sst_log.trace("consume_sstables(): {} sstables, merge={}, no_skips={}", sstables.size(), merge, no_skips);
    if (merge) {
        std::vector<flat_mutation_reader> readers;
        readers.reserve(sstables.size());
        for (const auto& sst : sstables) {
            if (no_skips) {
                readers.emplace_back(sst->make_crawling_reader_v1(schema, permit));
            } else {
                readers.emplace_back(sst->make_reader_v1(schema, permit, query::full_partition_range, schema->full_slice()));
            }
        }
        auto rd = make_combined_reader(schema, permit, std::move(readers));

        reader_consumer(rd, nullptr);
    } else {
        for (const auto& sst : sstables) {
            auto rd = no_skips
                ? sst->make_crawling_reader_v1(schema, permit)
                : sst->make_reader_v1(schema, permit, query::full_partition_range, schema->full_slice());

            if (reader_consumer(rd, sst.get()) == stop_iteration::yes) {
                break;
            }
        }
    }
}

struct options {
    bool merge = false;
    bool no_skips = false;
};

using operation_func = void(*)(schema_ptr, reader_permit, const std::vector<sstables::shared_sstable>&, const partition_set&, const options&,
        const operation_specific_options&);

class operation {
public:
    struct option {
        sstring name;
        sstring description;
    };
private:
    std::string _name;
    std::string _description;
    std::vector<option> _available_options;
    operation_func _func;

public:
    operation(std::string name, std::string description, operation_func func)
        : _name(std::move(name)), _description(std::move(description)), _func(func) {
    }
    operation(std::string name, std::string description, std::vector<option> available_options, operation_func func)
        : _name(std::move(name)), _description(std::move(description)), _available_options(std::move(available_options)), _func(func) {
    }

    const std::string& name() const { return _name; }
    const std::string& description() const { return _description; }
    const std::vector<option>& available_options() const { return _available_options; }

    void operator()(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
            const partition_set& partitions, const options& opts, const operation_specific_options& op_opts) const {
        _func(std::move(schema), std::move(permit), sstables, partitions, opts, op_opts);
    }
};

void validate_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const partition_set& partitions,
        const options& opts, const operation_specific_options& op_opts) {
    if (!partitions.empty()) {
        sst_log.warn("partition-filter is not supported for validate, ignoring");
    }
    sstables::compaction_info info;
    consume_sstables(schema, permit, sstables, opts.merge, true, [&info] (flat_mutation_reader& rd, sstables::sstable* sst) {
        if (sst) {
            sst_log.info("validating {}", sst->get_filename());
        }
        const auto valid = sstables::scrub_validate_mode_validate_reader(std::move(rd), info).get();
        sst_log.info("validated {}: {}", sst ? sst->get_filename() : "the stream", valid ? "valid" : "invalid");
        return stop_iteration::no;
    });
}

void dump_index_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const partition_set& partitions,
        const options& opts, const operation_specific_options& op_opts) {
    if (!partitions.empty()) {
        sst_log.warn("partition-filter is not supported for dump-index, ignoring");
    }
    if (opts.merge) {
        sst_log.warn("--merge not supported for dump-index, ignoring");
    }
    if (opts.no_skips) {
        sst_log.warn("--no-skips not supported for dump-index, ignoring");
    }
    std::cout << "{stream_start}" << std::endl;
    for (auto& sst : sstables) {
        sstables::index_reader idx_reader(sst, permit, default_priority_class(), {}, sstables::use_caching::yes);
        auto close_idx_reader = deferred_close(idx_reader);

        std::cout << "{sstable_index_start: " << sst->get_filename() << "}\n";
        while (!idx_reader.eof()) {
            idx_reader.read_partition_data().get();
            auto pos = idx_reader.get_data_file_position();

            auto pkey = idx_reader.get_partition_key();
            fmt::print("{}: {} ({})\n", pos, pkey.with_schema(*schema), pkey);

            idx_reader.advance_to_next_partition().get();
        }
        std::cout << "{sstable_index_end}" << std::endl;
    }
    std::cout << "{stream_end}" << std::endl;
}

template <typename SstableConsumer>
void sstable_consumer_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        const partition_set& partitions, const options& opts, const operation_specific_options& op_opts) {
    auto consumer = std::make_unique<SstableConsumer>(schema, permit, op_opts);
    consumer->on_start_of_stream().get();
    consume_sstables(schema, permit, sstables, opts.merge, opts.no_skips || partitions.empty(), [&, &consumer = *consumer] (flat_mutation_reader& rd, sstables::sstable* sst) {
        return consume_reader(std::move(rd), consumer, sst, partitions, opts.no_skips);
    });
    consumer->on_end_of_stream().get();
}

const std::vector<operation> operations{
    {"dump", "Dump content of sstable(s).", sstable_consumer_operation<dumping_consumer>},
    {"dump-index", "Dump content of sstable index(es).", dump_index_operation},
    {"writetime-histogram",
            "Generate a histogram (bucket=month) of all the timestamps (writetime), written to histogram.json.",
            {{"bucket", "the unit of time to use as bucket, one of (years, months, weeks, days, hours)"}},
            sstable_consumer_operation<writetime_histogram_collecting_consumer>},
    {"custom", "Hackable custom operation for expert users, until scripting support is implemented.", sstable_consumer_operation<custom_consumer>},
    {"validate", "Validate sstable(s) with the mutation fragment stream validator, same as scrub in validate mode.", validate_operation},
};

} // anonymous namespace

int main(int argc, char** argv) {
    app_template::seastar_options app_cfg;
    app_cfg.name = app_name;

    const auto description_template =
R"(scylla-sstable - a multifunctional command-line tool to examine the content of sstables.

Allows examining the contents of sstables with various built-in tools
(operations). The sstables to-be-examined are passed as positional
command line arguments. Sstables will be processed by the selected
operation one-by-one (can be changed with --merge). Any number of
sstables can be passed but mind the open file limits. Pass the full path
to the data component of the sstables (*-Data.db). For now it is
required that the sstable is found at a valid data path:

    /path/to/datadir/{{keyspace_name}}/{{table_name}}-{{table_id}}/

The schema to read the sstables is read from a schema.cql file. This
should contain the keyspace and table definitions, as well as any UDTs
used.
Filtering the sstable()s to process only certain partition(s) is
supported via the --partition and --partitions-file command line flags.
Partition keys are expected to be in the hexdump format used by scylla
(hex representation of the raw buffer).
Operations write their output to stdout, or file(s). The tool logs to
stderr, with a logger called {}.

The supported operations are:
{}

Examples:

# dump the content of the sstable
$ scylla-sstable --dump /path/to/md-123456-big-Data.db

# dump the content of the two sstable(s) as a unified stream
$ scylla-sstable --dump --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db

# generate a joint histogram for the specified partition
$ scylla-sstable --writetime-histogram --partition={{myhexpartitionkey}} /path/to/md-123456-big-Data.db

# validate the specified sstables
$ scylla-sstable --validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db
)";
    app_cfg.description = format(description_template, app_name, boost::algorithm::join(operations | boost::adaptors::transformed([] (const auto& op) {
        if (op.available_options().empty()) {
            return format("* {}: {}", op.name(), op.description());
        }
        const auto opt_list = boost::algorithm::join(op.available_options() | boost::adaptors::transformed(
                        [] (const auto& opt) { return fmt::format("    - {}: {}", opt.name, opt.description); }), "\n");
        return format("* {}: {}\n  Options:\n{}", op.name(), op.description(), opt_list);
    }), "\n"));

    const auto op_list = boost::algorithm::join(operations | boost::adaptors::transformed(
                    [] (const auto& op) { return sstring(op.name()); } ), ", ");

    const auto op_help = fmt::format("operation to run, one of ({})", op_list);

    tools::utils::configure_tool_mode(app_cfg, sst_log.name());

    app_template app(std::move(app_cfg));

    for (const auto& op : operations) {
        app.add_options()(op.name().c_str(), op.description().c_str());
    }

    app.add_options()
        ("schema-file", bpo::value<sstring>()->default_value("schema.cql"), "file containing the schema description")
        ("partition", bpo::value<std::vector<sstring>>(), "partition(s) to filter for, partitions are expected to be in the hex format")
        ("partitions-file", bpo::value<sstring>(), "file containing partition(s) to filter for, partitions are expected to be in the hex format")
        ("merge", "merge all sstables into a single mutation fragment stream (use a combining reader over all sstable readers)")
        ("no-skips", "don't use skips to skip to next partition when the partition filter rejects one, this is slower but works with corrupt index")
        ("operation-option", bpo::value<program_options::string_map>()->default_value({}),
                 "Map of operation-option names to values. The format is \"OPTION0=VALUE0[:OPTION1=VALUE1:...]\". "
                 "Valid options are listed in the operation descriptions."
                 "This option can be specified multiple times.")
        ;

    app.add_positional_options({
        {"sstables", bpo::value<std::vector<sstring>>(), "sstable(s) to process, can also be provided as positional arguments", -1},
    });

    //FIXME: this exposes all core options, which we are not interested in.
    //FIXME: make WARN the default log level
    return app.run(argc, argv, [&app, &op_list] {
        return async([&app, &op_list] {
            auto& app_config = app.configuration();

            const auto& operation = tools::utils::get_selected_operation(app.configuration(), operations, "operation");

            options opts;
            opts.merge = app_config.count("merge");
            opts.no_skips = app_config.count("no-skips");

            const auto operation_opts_raw = app_config["operation-option"].as<program_options::string_map>();
            const auto operation_opts = operation_specific_options(operation_opts_raw.begin(), operation_opts_raw.end());

            for (const auto& [key, _] : operation_opts) {
                auto it = std::find_if(operation.available_options().begin(), operation.available_options().end(), [&name = key] (const operation::option& opt) {
                    return opt.name == name;
                });
                if (it == operation.available_options().end()) {
                    sst_log.error("error: operation {} doesn't have option {}", operation.name(), key);
                    return 2;
                }
            }

            const auto schema = tools::load_one_schema_from_file(std::filesystem::path(app_config["schema-file"].as<sstring>())).get();

            const auto partitions = get_partitions(schema, app_config);

            db::config dbcfg;
            gms::feature_service feature_service(gms::feature_config_from_db_config(dbcfg));
            cache_tracker tracker;
            sstables::sstables_manager sst_man(large_data_handler, dbcfg, feature_service, tracker);
            auto close_sst_man = deferred_close(sst_man);

            if (!app_config.count("sstables")) {
                std::cerr << "error: no sstables specified on the command line\n";
                return 2;
            }
            const auto sstables = load_sstables(schema, sst_man, app_config["sstables"].as<std::vector<sstring>>());

            reader_concurrency_semaphore rcs_sem(reader_concurrency_semaphore::no_limits{}, app_name);
            auto stop_semaphore = deferred_stop(rcs_sem);

            const auto permit = rcs_sem.make_tracking_only_permit(schema.get(), app_name, db::no_timeout);

            operation(schema, permit, sstables, partitions, opts, operation_opts);

            return 0;
        });
    });
}
