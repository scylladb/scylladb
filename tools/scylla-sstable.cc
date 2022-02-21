/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

partition_set get_partitions(schema_ptr schema, const bpo::variables_map& app_config) {
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

        auto ed = sstables::entry_descriptor::make_descriptor(dir_path.c_str(), sst_filename.c_str(), schema->ks_name(), schema->cf_name());
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
    virtual future<stop_iteration> consume(range_tombstone_change&&) = 0;
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
    future<stop_iteration> operator()(mutation_fragment_v2&& mf) {
        sst_log.trace("consume {}", mf.mutation_fragment_kind());
        if (mf.is_partition_start() && _filter && !_filter(mf.as_partition_start().key())) {
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return std::move(mf).consume(_consumer);
    }
};

class dumping_consumer : public sstable_consumer {
    schema_ptr _schema;

public:
    explicit dumping_consumer(schema_ptr s, reader_permit, const bpo::variables_map&) : _schema(std::move(s)) {
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
    virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
        std::cout << rtc << std::endl;
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
        std::abort();
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
    explicit writetime_histogram_collecting_consumer(schema_ptr s, reader_permit, const bpo::variables_map& vm) : _schema(std::move(s)) {
        auto it = vm.find("bucket");
        if (it != vm.end()) {
            auto value = it->second.as<std::string>();
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
    virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
        collect_timestamp(rtc.tombstone().timestamp);
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
    explicit custom_consumer(schema_ptr s, reader_permit p, const bpo::variables_map&)
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
    virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
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

stop_iteration consume_reader(flat_mutation_reader_v2 rd, sstable_consumer& consumer, sstables::sstable* sst, const partition_set& partitions, bool no_skips) {
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
                mutation_fragment_v2_opt mfopt;
                while ((mfopt = rd().get()) && !mfopt->is_end_of_partition());
            } else {
                rd.next_partition().get();
            }
        }
    }
    return consumer.on_end_of_sstable().get();
}

void consume_sstables(schema_ptr schema, reader_permit permit, std::vector<sstables::shared_sstable> sstables, bool merge, bool no_skips,
        std::function<stop_iteration(flat_mutation_reader_v2&, sstables::sstable*)> reader_consumer) {
    sst_log.trace("consume_sstables(): {} sstables, merge={}, no_skips={}", sstables.size(), merge, no_skips);
    if (merge) {
        std::vector<flat_mutation_reader_v2> readers;
        readers.reserve(sstables.size());
        for (const auto& sst : sstables) {
            if (no_skips) {
                readers.emplace_back(sst->make_crawling_reader(schema, permit));
            } else {
                readers.emplace_back(sst->make_reader(schema, permit, query::full_partition_range, schema->full_slice()));
            }
        }
        auto rd = make_combined_reader(schema, permit, std::move(readers));

        reader_consumer(rd, nullptr);
    } else {
        for (const auto& sst : sstables) {
            auto rd = no_skips
                ? sst->make_crawling_reader(schema, permit)
                : sst->make_reader(schema, permit, query::full_partition_range, schema->full_slice());

            if (reader_consumer(rd, sst.get()) == stop_iteration::yes) {
                break;
            }
        }
    }
}

using operation_func = void(*)(schema_ptr, reader_permit, const std::vector<sstables::shared_sstable>&, const bpo::variables_map&);

class operation {
    std::string _name;
    std::string _summary;
    std::string _description;
    std::vector<std::string> _available_options;
    operation_func _func;

public:
    operation(std::string name, std::string summary, std::string description, operation_func func)
        : _name(std::move(name)), _summary(std::move(summary)), _description(std::move(description)), _func(func) {
    }
    operation(std::string name, std::string summary, std::string description, std::vector<std::string> available_options, operation_func func)
        : _name(std::move(name)), _summary(std::move(summary)), _description(std::move(description)), _available_options(std::move(available_options)), _func(func) {
    }

    const std::string& name() const { return _name; }
    const std::string& summary() const { return _summary; }
    const std::string& description() const { return _description; }
    const std::vector<std::string>& available_options() const { return _available_options; }

    void operator()(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map& vm) const {
        _func(std::move(schema), std::move(permit), sstables, vm);
    }
};

void validate_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map& vm) {
    const auto merge = vm.count("merge");
    sstables::compaction_data info;
    consume_sstables(schema, permit, sstables, merge, true, [&info] (flat_mutation_reader_v2& rd, sstables::sstable* sst) {
        if (sst) {
            sst_log.info("validating {}", sst->get_filename());
        }
        const auto valid = sstables::scrub_validate_mode_validate_reader(std::move(rd), info).get();
        sst_log.info("validated {}: {}", sst ? sst->get_filename() : "the stream", valid ? "valid" : "invalid");
        return stop_iteration::no;
    });
}

void dump_index_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map&) {
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

template <typename Integer>
sstring disk_string_to_string(const sstables::disk_string<Integer>& ds) {
    return sstring(ds.value.begin(), ds.value.end());
}

void dump_compression_info_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map&) {
    fmt::print("{{stream_start}}\n");

    for (auto& sst : sstables) {
        const auto& compression = sst->get_compression();

        fmt::print("{{sstable_compression_info_start: {}}}\n", sst->get_filename());
        fmt::print("{{name: {}}}\n", disk_string_to_string(compression.name));
        fmt::print("{{options:");
        if (!compression.options.elements.empty()) {
            auto it = compression.options.elements.begin();
            const auto end = compression.options.elements.end();
            fmt::print(" {}={}", disk_string_to_string(it->key), disk_string_to_string(it->value));
            for (++it; it != end; ++it) {
                fmt::print(", {}={}", disk_string_to_string(it->key), disk_string_to_string(it->value));
            }
        }
        fmt::print("}}\n");
        fmt::print("{{chunk_len: {}}}\n", compression.chunk_len);
        fmt::print("{{data_len: {}}}\n", compression.data_len);
        fmt::print("{{offsets_start}}\n");
        size_t i = 0;
        for (const auto& offset : compression.offsets) {
            fmt::print("[{}]: {}\n", i++, offset);
        }
        fmt::print("{{offsets_end}}\n");
        fmt::print("{{sstable_compression_info_end}}\n");
    }
    fmt::print("{{stream_end}}\n");
}

void dump_summary_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map&) {
    const auto composite_to_hex = [] (bytes_view bv) {
        auto cv = composite_view(bv, true);
        auto key = partition_key::from_exploded_view(cv.explode());
        return to_hex(key.representation());
    };

    fmt::print("{{stream_start}}\n");
    for (auto& sst : sstables) {
        auto& summary = sst->get_summary();

        fmt::print("{{sstable_summary_start: {}}}\n", sst->get_filename());

        fmt::print("{{header: min_index_interval: {}, size: {}, memory_size: {}, sampling_level: {}, size_at_full_sampling: {}}}\n",
                summary.header.min_index_interval,
                summary.header.size,
                summary.header.memory_size,
                summary.header.sampling_level,
                summary.header.size_at_full_sampling);

        fmt::print("{{positions:\n");
        for (size_t i = 0; i < summary.positions.size(); ++i) {
            fmt::print("[{}]: {}\n", i, summary.positions[i]);
        }
        fmt::print("}}\n");

        fmt::print("{{entries:\n");
        for (size_t i = 0; i < summary.entries.size(); ++i) {
            const auto& e = summary.entries[i];
            auto pkey = e.get_key().to_partition_key(*schema);
            fmt::print("[{}]: {{summary_entry: token: {}, key: {} ({}), position: {}}}\n",
                    i,
                    e.token,
                    pkey.with_schema(*schema),
                    pkey,
                    e.position);
        }
        fmt::print("}}\n");

        auto first_key = sstables::key_view(summary.first_key.value).to_partition_key(*schema);
        fmt::print("{{first_key: {} ({})}}\n", first_key.with_schema(*schema), first_key);

        auto last_key = sstables::key_view(summary.last_key.value).to_partition_key(*schema);
        fmt::print("{{last_key: {} ({})}}\n", last_key.with_schema(*schema), last_key);

        fmt::print("{{sstable_summary_end}}\n");
    }
    fmt::print("{{stream_end}}\n");
}

class text_dumper {
    sstables::sstable_version_types _version;
    std::function<std::string_view(const void* const)> _name_resolver;

private:
    template <typename Number>
    requires std::is_arithmetic_v<Number>
    void visit(Number& val) {
        if constexpr (std::is_same_v<std::remove_cv_t<Number>, int8_t>) {
            fmt::print(" {}", static_cast<signed int>(val));
        } else if (std::is_same_v<std::remove_cv_t<Number>, uint8_t>) {
            fmt::print(" {}", static_cast<unsigned int>(val));
        } else {
            fmt::print(" {}", val);
        }
    }

    template <typename Integer>
    void visit(const sstables::disk_string<Integer>& val) {
        fmt::print(" {}", disk_string_to_string(val));
    }

    template <typename Integer, typename T>
    void visit(const sstables::disk_array<Integer, T>& val) {
        fmt::print("\n");
        for (size_t i = 0; i != val.elements.size(); ++i) {
            fmt::print("[{}]: ", i);
            visit(val.elements[i]);
            fmt::print("\n");
        }
    }

    void visit(const sstables::disk_string_vint_size& val) {
        fmt::print(" {}", sstring(val.value.begin(), val.value.end()));
    }

    template <typename T>
    void visit(const sstables::disk_array_vint_size<T>& val) {
        fmt::print("\n");
        for (size_t i = 0; i != val.elements.size(); ++i) {
            fmt::print("[{}]: ", i);
            visit(val.elements[i]);
            fmt::print("\n");
        }
    }

    void visit(const utils::estimated_histogram& val) {
        fmt::print("\n");
        for (size_t i = 0; i < val.buckets.size(); i++) {
            auto offset = val.bucket_offsets[i == 0 ? 0 : i - 1];
            auto bucket = val.buckets[i];
            fmt::print("[{}]: offset: {}, value={}}}\n", i, offset, bucket);
        }
    }

    void visit(const utils::streaming_histogram& val) {
        fmt::print("\n");
        for (const auto& [k, v] : val.bin) {
            fmt::print("[{}]: {}\n", k, v);
        }
    }

    void visit(const db::replay_position& val) {
        fmt::print(" id: {}, pos: {}", val.id, val.pos);
    }

    void visit(const sstables::commitlog_interval& val) {
        fmt::print(" {{start: ");
        visit(val.start);
        fmt::print("}}");

        fmt::print(" {{end: ");
        visit(val.end);
        fmt::print("}}");
    }

    template <typename Integer>
    void visit(const sstables::vint<Integer>& val) {
        fmt::print("{}", val.value);
    }

    void visit(const sstables::serialization_header::column_desc& val) {
        text_dumper dumper(_version, [&val] (const void* const field) {
            if (field == &val.name) { return "name"; }
            else if (field == &val.type_name) { return "type_name"; }
            else { throw std::invalid_argument("invalid field offset"); }
        });
        const_cast<sstables::serialization_header::column_desc&>(val).describe_type(_version, std::ref(dumper));
    }

    template <typename FieldType>
    void visit_field(FieldType& field) {
        const auto name = _name_resolver(&field);
        fmt::print("{{{}:", name);
        visit(field);
        fmt::print("}}\n");
    }

    text_dumper(sstables::sstable_version_types version, std::function<std::string_view(const void* const)> name_resolver)
        : _version(version), _name_resolver(std::move(name_resolver)) {
    }

public:
    template <typename Arg1>
    void operator()(Arg1& arg1) {
        visit_field(arg1);
    }

    template <typename Arg1, typename... Arg>
    void operator()(Arg1& arg1, Arg&... arg) {
        visit_field(arg1);
        (*this)(arg...);
    }

    template <typename T>
    static void dump(sstables::sstable_version_types version, const T& obj, std::string_view name,
            std::function<std::string_view(const void* const)> name_resolver) {
        text_dumper dumper(version, std::move(name_resolver));
        fmt::print("{{{}_start}}\n", name);
        const_cast<T&>(obj).describe_type(version, std::ref(dumper));
        fmt::print("{{{}_end}}\n", name);
    }
};

void dump_validation_metadata(sstables::sstable_version_types version, const sstables::validation_metadata& metadata) {
    text_dumper::dump(version, metadata, "validation", [&metadata] (const void* const field) {
        if (field == &metadata.partitioner) { return "partitioner"; }
        else if (field == &metadata.filter_chance) { return "filter_chance"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_compaction_metadata(sstables::sstable_version_types version, const sstables::compaction_metadata& metadata) {
    text_dumper::dump(version, metadata, "compaction", [&metadata] (const void* const field) {
        if (field == &metadata.ancestors) { return "ancestors"; }
        else if (field == &metadata.cardinality) { return "cardinality"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_stats_metadata(sstables::sstable_version_types version, const sstables::stats_metadata& metadata) {
    text_dumper::dump(version, metadata, "stats", [&metadata] (const void* const field) {
        if (field == &metadata.estimated_partition_size) { return "estimated_partition_size"; }
        else if (field == &metadata.estimated_cells_count) { return "estimated_cells_count"; }
        else if (field == &metadata.position) { return "position"; }
        else if (field == &metadata.min_timestamp) { return "min_timestamp"; }
        else if (field == &metadata.max_timestamp) { return "max_timestamp"; }
        else if (field == &metadata.min_local_deletion_time) { return "min_local_deletion_time"; }
        else if (field == &metadata.max_local_deletion_time) { return "max_local_deletion_time"; }
        else if (field == &metadata.min_ttl) { return "min_ttl"; }
        else if (field == &metadata.max_ttl) { return "max_ttl"; }
        else if (field == &metadata.compression_ratio) { return "compression_ratio"; }
        else if (field == &metadata.estimated_tombstone_drop_time) { return "estimated_tombstone_drop_time"; }
        else if (field == &metadata.sstable_level) { return "sstable_level"; }
        else if (field == &metadata.repaired_at) { return "repaired_at"; }
        else if (field == &metadata.min_column_names) { return "min_column_names"; }
        else if (field == &metadata.max_column_names) { return "max_column_names"; }
        else if (field == &metadata.has_legacy_counter_shards) { return "has_legacy_counter_shards"; }
        else if (field == &metadata.columns_count) { return "columns_count"; }
        else if (field == &metadata.rows_count) { return "rows_count"; }
        else if (field == &metadata.commitlog_lower_bound) { return "commitlog_lower_bound"; }
        else if (field == &metadata.commitlog_intervals) { return "commitlog_intervals"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_serialization_header(sstables::sstable_version_types version, const sstables::serialization_header& metadata) {
    text_dumper::dump(version, metadata, "serialization_header", [&metadata] (const void* const field) {
        if (field == &metadata.min_timestamp_base) { return "min_timestamp_base"; }
        else if (field == &metadata.min_local_deletion_time_base) { return "min_local_deletion_time_base"; }
        else if (field == &metadata.min_ttl_base) { return "min_ttl_base"; }
        else if (field == &metadata.pk_type_name) { return "pk_type_name"; }
        else if (field == &metadata.clustering_key_types_names) { return "clustering_key_types_names"; }
        else if (field == &metadata.static_columns) { return "static_columns"; }
        else if (field == &metadata.regular_columns) { return "regular_columns"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_statistics_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map&) {
    auto to_string = [] (sstables::metadata_type t) {
        switch (t) {
            case sstables::metadata_type::Validation: return "validation";
            case sstables::metadata_type::Compaction: return "compaction";
            case sstables::metadata_type::Stats: return "stats";
            case sstables::metadata_type::Serialization: return "serialization";
        }
        std::abort();
    };

    fmt::print("{{stream_start}}\n");
    for (auto& sst : sstables) {
        auto& statistics = sst->get_statistics();

        fmt::print("{{sstable_statistics_start: {}}}\n", sst->get_filename());
        fmt::print("{{offsets: ");
        if (!statistics.offsets.elements.empty()) {
            auto it = statistics.offsets.elements.begin();
            const auto end = statistics.offsets.elements.end();
            fmt::print("{}={}", to_string(it->first), it->second);
            for (++it; it != end; ++it) {
                fmt::print(", {}={}", to_string(it->first), it->second);
            }
        }
        fmt::print("}}\n");
        fmt::print("{{contents_start}}\n");
        const auto version = sst->get_version();
        for (const auto& [type, _] : statistics.offsets.elements) {
            const auto& metadata_ptr = statistics.contents.at(type);
            switch (type) {
                case sstables::metadata_type::Validation:
                    dump_validation_metadata(version, *dynamic_cast<const sstables::validation_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Compaction:
                    dump_compaction_metadata(version, *dynamic_cast<const sstables::compaction_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Stats:
                    dump_stats_metadata(version, *dynamic_cast<const sstables::stats_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Serialization:
                    dump_serialization_header(version, *dynamic_cast<const sstables::serialization_header*>(metadata_ptr.get()));
                    break;
            }
        }
        fmt::print("{{contents_end}}\n");
        fmt::print("{{sstable_statistics_end}}\n");
    }
    fmt::print("{{stream_end}}\n");
}

const char* to_string(sstables::scylla_metadata_type t) {
    switch (t) {
        case sstables::scylla_metadata_type::Sharding: return "Sharding";
        case sstables::scylla_metadata_type::Features: return "Features";
        case sstables::scylla_metadata_type::ExtensionAttributes: return "ExtensionAttributes";
        case sstables::scylla_metadata_type::RunIdentifier: return "RunIdentifier";
        case sstables::scylla_metadata_type::LargeDataStats: return "LargeDataStats";
        case sstables::scylla_metadata_type::SSTableOrigin: return "SSTableOrigin";
    }
    std::abort();
}

const char* to_string(sstables::large_data_type t) {
    switch (t) {
        case sstables::large_data_type::partition_size: return "partition_size";
        case sstables::large_data_type::row_size: return "row_size";
        case sstables::large_data_type::cell_size: return "cell_size";
        case sstables::large_data_type::rows_in_partition: return "rows_in_partition";
    }
    std::abort();
}

struct scylla_metadata_visitor : public boost::static_visitor<> {
    void operator()(const sstables::sharding_metadata& val) const {
        fmt::print("\n");
        for (const auto& e : val.token_ranges.elements) {
            fmt::print("{}{}, {}{}\n",
                    e.left.exclusive ? "(" : "[",
                    disk_string_to_string(e.left.token),
                    e.right.exclusive ? ")" : "]",
                    disk_string_to_string(e.right.token));
        }
    }
    void operator()(const sstables::sstable_enabled_features& val) const {
        std::pair<sstables::sstable_feature, const char*> all_features[] = {
                {sstables::sstable_feature::NonCompoundPIEntries, "NonCompoundPIEntries"},
                {sstables::sstable_feature::NonCompoundRangeTombstones, "NonCompoundRangeTombstones"},
                {sstables::sstable_feature::ShadowableTombstones, "ShadowableTombstones"},
                {sstables::sstable_feature::CorrectStaticCompact, "CorrectStaticCompact"},
                {sstables::sstable_feature::CorrectEmptyCounters, "CorrectEmptyCounters"},
                {sstables::sstable_feature::CorrectUDTsInCollections, "CorrectUDTsInCollections"},
        };
        fmt::print(" ({}): ", val.enabled_features);
        bool first = true;
        for (const auto& [mask, name] : all_features) {
            if (mask & val.enabled_features) {
                if (first) {
                    first = false;
                } else {
                    fmt::print(" |");
                }
                fmt::print(" {}", name);
            }
        }
    }
    void operator()(const sstables::scylla_metadata::extension_attributes& val) const {
        fmt::print("\n");
        for (const auto& [k, v] : val.map) {
            fmt::print("{}: {}\n", disk_string_to_string(k), disk_string_to_string(v));
        }
    }
    void operator()(const sstables::run_identifier& val) const {
        fmt::print(" {}", val.id);
    }
    void operator()(const sstables::scylla_metadata::large_data_stats& val) const {
        fmt::print("\n");
        for (const auto& [k, v] : val.map) {
            fmt::print("{}: {{max_value: {}, threshold: {}, above_threshold: {}}}\n",
                    to_string(k),
                    v.max_value,
                    v.threshold,
                    v.above_threshold);
        }
    }
    void operator()(const sstables::scylla_metadata::sstable_origin& val) const {
        fmt::print(" {}", disk_string_to_string(val));
    }

    template <sstables::scylla_metadata_type E, typename T>
    void operator()(const sstables::disk_tagged_union_member<sstables::scylla_metadata_type, E, T>& m) const {
        fmt::print("{{{}:", to_string(E));
        (*this)(m.value);
        fmt::print("}}\n");
    }

    scylla_metadata_visitor() = default;
};

void dump_scylla_metadata_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map&) {
    fmt::print("{{stream_start}}\n");
    for (auto& sst : sstables) {
        fmt::print("{{sstable_scylla_metadata_start: {}}}\n", sst->get_filename());
        auto m = sst->get_scylla_metadata();
        if (!m) {
            fmt::print("{{sstable_scylla_metadata_end}}\n");
            continue;
        }
        for (const auto& [k, v] : m->data.data) {
            boost::apply_visitor(scylla_metadata_visitor{}, v);
        }
        fmt::print("{{sstable_scylla_metadata_end}}\n");
    }
    fmt::print("{{stream_end}}\n");
}

template <typename SstableConsumer>
void sstable_consumer_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables, const bpo::variables_map& vm) {
    const auto merge = vm.count("merge");
    const auto no_skips = vm.count("no-skips");
    const auto partitions = get_partitions(schema, vm);
    auto consumer = std::make_unique<SstableConsumer>(schema, permit, vm);
    consumer->on_start_of_stream().get();
    consume_sstables(schema, permit, sstables, merge, no_skips || partitions.empty(), [&, &consumer = *consumer] (flat_mutation_reader_v2& rd, sstables::sstable* sst) {
        return consume_reader(std::move(rd), consumer, sst, partitions, no_skips);
    });
    consumer->on_end_of_stream().get();
}

class basic_option {
public:
    const char* name;
    const char* description;

public:
    basic_option(const char* name, const char* description) : name(name), description(description) { }

    virtual void add_option(bpo::options_description& opts) const = 0;
};

template <typename T = std::monostate>
class typed_option : public basic_option {
    std::optional<T> _default_value;

    virtual void add_option(bpo::options_description& opts) const override {
        if (_default_value) {
            opts.add_options()(name, bpo::value<T>()->default_value(*_default_value), description);
        } else {
            opts.add_options()(name, bpo::value<T>(), description);
        }
    }

public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
    typed_option(const char* name, T default_value, const char* description) : basic_option(name, description), _default_value(std::move(default_value)) { }
};

template <>
class typed_option<std::monostate> : public basic_option {
    virtual void add_option(bpo::options_description& opts) const override {
        opts.add_options()(name, description);
    }
public:
    typed_option(const char* name, const char* description) : basic_option(name, description) { }
};

class option {
    shared_ptr<basic_option> _opt; // need copy to support convenient range declaration of std::vector<option>

public:
    template <typename T>
    option(typed_option<T> opt) : _opt(make_shared<typed_option<T>>(std::move(opt))) { }

    const char* name() const { return _opt->name; }
    const char* description() const { return _opt->description; }
    void add_option(bpo::options_description& opts) const { _opt->add_option(opts); }
};

const std::vector<option> all_options {
    typed_option<std::vector<sstring>>("partition", "partition(s) to filter for, partitions are expected to be in the hex format"),
    typed_option<sstring>("partitions-file", "file containing partition(s) to filter for, partitions are expected to be in the hex format"),
    typed_option<>("merge", "merge all sstables into a single mutation fragment stream (use a combining reader over all sstable readers)"),
    typed_option<>("no-skips", "don't use skips to skip to next partition when the partition filter rejects one, this is slower but works with corrupt index"),
    typed_option<std::string>("bucket", "months", "the unit of time to use as bucket, one of (years, months, weeks, days, hours)"),
};

const std::vector<operation> operations{
/* dump-data */
    {"dump-data",
            "Dump content of sstable(s)",
R"(
Dump the content of the data component. This component contains the data-proper
of the sstable. This might produce a huge amount of output. In general the
human-readable output will be larger than the binary file.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.

It is possible to filter the data to print via the --partitions or
--partitions-file options. Both expect partition key values in the hexdump
format.
)",
            {"partition", "partitions-file", "merge", "no-skips"},
            sstable_consumer_operation<dumping_consumer>},
/* dump-index */
    {"dump-index",
            "Dump content of sstable index(es)",
R"(
Dump the content of the index component. Contains the partition-index of the data
component. This is effectively a list of all the partitions in the sstable, with
their starting position in the data component and optionally a promoted index,
which contains a sampled index of the clustering rows in the partition.
Positions (both that of partition and that of rows) is valid for uncompressed
data.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.
)",
            dump_index_operation},
/* dump-compression-info */
    {"dump-compression-info",
            "Dump content of sstable compression info(s)",
R"(
Dump the content of the compression-info component. Contains compression
parameters and maps positions into the uncompressed data to that into compressed
data. Note that compression happens over chunks with configurable size, so to
get data at a position in the middle of a compressed chunk, the entire chunk has
to be decompressed.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.
)",
            dump_compression_info_operation},
/* dump-summary */
    {"dump-summary",
            "Dump content of sstable summary(es)",
R"(
Dump the content of the summary component. The summary is a sampled index of the
content of the index-component. An index of the index. Sampling rate is chosen
such that this file is small enough to be kept in memory even for very large
sstables.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.
)",
            dump_summary_operation},
/* dump-statistics */
    {"dump-statistics",
            "Dump content of sstable statistics(s)",
R"(
Dump the content of the statistics component. Contains various metadata about the
data component. In the sstable 3 format, this component is critical for parsing
the data component.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.
)",
            dump_statistics_operation},
/* dump-scylla-metadata */
    {"dump-scylla-metadata",
            "Dump content of sstable scylla metadata(s)",
R"(
Dump the content of the scylla-metadata component. Contains scylla-specific
metadata about the data component. This component won't be present in sstables
produced by Apache Cassandra.
For more information about the sstable components and the format itself, visit
https://docs.scylladb.com/architecture/sstable/.
)",
            dump_scylla_metadata_operation},
/* writetime-histogram */
    {"writetime-histogram",
            "Generate a histogram of all the timestamps (writetime)",
R"(
Crawl over all timestamps in the data component and add them to a histogram. The
bucket size by default is a month, tunable with the --bucket option.
The timestamp of all objects that have one are added to the histogram:
* cells (atomic and collection cells)
* tombstones (partition-tombstone, range-tombstone, row-tombstone,
  shadowable-tombstone, cell-tombstone, collection-tombstone, cell-tombstone)
* row-marker

This allows determining when the data was written, provided the writer of the
data didn't mangle with the timestamps.
This produces a json file `histogram.json` whose content can be plotted with the
following example python script:

     import datetime
     import json
     import matplotlib.pyplot as plt # requires the matplotlib python package

     with open('histogram.json', 'r') as f:
         data = json.load(f)

     x = data['buckets']
     y = data['counts']

     max_y = max(y)

     x = [datetime.date.fromtimestamp(i / 1000000).strftime('%Y.%m') for i in x]
     y = [i / max_y for i in y]

     fig, ax = plt.subplots()

     ax.set_xlabel('Timestamp')
     ax.set_ylabel('Normalized cell count')
     ax.set_title('Histogram of data write-time')
     ax.bar(x, y)

     plt.show()
)",
            {"bucket"},
            sstable_consumer_operation<writetime_histogram_collecting_consumer>},
/* custom */
    {"custom",
            "Hackable custom operation for expert users, until scripting support is implemented",
R"(
Poor man's scripting support. Aimed at developers as it requires editing C++
source code and re-building the binary. Will be replaced by proper scripting
support soon (don't quote me on that).
)",
            sstable_consumer_operation<custom_consumer>},
/* validate */
    {"validate",
            "Validate the sstable(s), same as scrub in validate mode",
R"(
On a conceptual level, the data in sstables is represented by objects called
mutation fragments. We have the following kinds of fragments:
* partition-start (1)
* static-row (0-1)
* clustering-row (0-N)
* range-tombstone/range-tombstone-change (0-N)
* partition-end (1)

Data from the sstable is parsed into these fragments. We use these fragments to
stream data because it allows us to represent as little as part of a partition
or as many as the entire content of an sstable.

This operation validates data on the mutation-fragment level. Any parsing errors
will also be detected, but after successful parsing the validation will happen
on the fragment level. The following things are validated:
* Partitions are ordered in strictly monotonic ascending order [1].
* Fragments are correctly ordered. Fragments must follow the order defined in the
  listing above also respecting the occurrence numbers within a partition. Note
  that clustering rows and range tombstone [change] fragments can be intermingled.
* Clustering elements are ordered according in a strictly increasing clustering
  order as defined by the schema. Range tombstones (but not range tombstone
  changes) are allowed to have weakly monotonically increasing positions.
* The stream ends with a partition-end fragment.

[1] Although partitions are said to be unordered, this is only true w.r.t. the
data type of the key components. Partitions are ordered according to their tokens
(hashes), so partitions are unordered in the sense that a hash-table is
unordered: they have a random order as perceived by they user but they have a
well defined internal order.
)",
            {"merge"},
            validate_operation},
};

} // anonymous namespace

namespace tools {

int scylla_sstable_main(int argc, char** argv) {
    const operation* found_op = nullptr;
    if (std::strcmp(argv[1], "--help") != 0 && std::strcmp(argv[1], "-h") != 0) {
        found_op = &tools::utils::get_selected_operation(argc, argv, operations, "operation");
    }

    app_template::seastar_options app_cfg;
    app_cfg.name = app_name;

    const auto description_template =
R"(scylla-sstable - a multifunctional command-line tool to examine the content of sstables.

Usage: scylla sstable {{operation}} [--option1] [--option2] ... {{sstable_path1}} [{{sstable_path2}}] ...

Allows examining the contents of sstables with various built-in tools
(operations). The sstables to-be-examined are passed as positional
command line arguments. Sstables will be processed by the selected
operation one-by-one. Any number of sstables can be passed but mind the
open file limits. Always pass the path to the data component of the
sstables (*-Data.db) even if you want to examine another component.
The schema to read the sstables is read from a schema.cql file. This
should contain the keyspace and table definitions, any UDTs used and
dropped columns in the form of relevant CQL statements. The keyspace
definition is allowed to be missing, in which case one will be
auto-generated. Dropped columns should be present in the form of insert
statements into the system_schema.dropped_columns table.
Example scylla.cql:

    CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};

    CREATE TYPE ks.type1 (f1 int, f2 text);

    CREATE TABLE ks.cf (pk int PRIMARY KEY, v frozen<type1>);

    INSERT
    INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type)
    VALUES ('ks', 'cf', 'v2', 1631011979170675, 'int');

In general you should be able to use the output of `DESCRIBE TABLE` or
the relevant parts of `DESCRIBE KEYSPACE` of `cqlsh` as well as the
`schema.cql` produced by snapshots.

Operations write their output to stdout, or file(s). The tool logs to
stderr, with a logger called {}.

The supported operations are:
{}

For more details on an operation, run: scylla sstable {{operation}} --help

Examples:

# dump the content of the sstable
$ scylla sstable dump-data /path/to/md-123456-big-Data.db

# dump the content of the two sstable(s) as a unified stream
$ scylla sstable dump-data --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db

# generate a joint histogram for the specified partition
$ scylla sstable writetime-histogram --partition={{myhexpartitionkey}} /path/to/md-123456-big-Data.db

# validate the specified sstables
$ scylla sstable validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db
)";

    if (found_op) {
        app_cfg.description = format("{}\n\n{}\n", found_op->summary(), found_op->description());
    } else  {
        app_cfg.description = format(description_template, app_name, boost::algorithm::join(operations | boost::adaptors::transformed([] (const auto& op) {
            return format("* {}: {}", op.name(), op.summary());
        }), "\n"));
    }

    tools::utils::configure_tool_mode(app_cfg, sst_log.name());

    app_template app(std::move(app_cfg));

    app.add_options()
        ("schema-file", bpo::value<sstring>()->default_value("schema.cql"), "file containing the schema description")
        ;

    app.add_positional_options({
        {"sstables", bpo::value<std::vector<sstring>>(), "sstable(s) to process, can also be provided as positional arguments", -1},
    });

    if (found_op) {
        bpo::options_description op_desc(found_op->name());
        for (const auto& opt_name : found_op->available_options()) {
            auto it = std::find_if(all_options.begin(), all_options.end(), [&] (const option& opt) { return opt.name() == opt_name; });
            assert(it != all_options.end());
            it->add_option(op_desc);
        }
        if (!found_op->available_options().empty()) {
            app.get_options_description().add(op_desc);
        }
    }

    return app.run(argc, argv, [&app, found_op] {
        return async([&app, found_op] {
            auto& app_config = app.configuration();

            const auto& operation = *found_op;

            schema_ptr schema;
            try {
                schema = tools::load_one_schema_from_file(std::filesystem::path(app_config["schema-file"].as<sstring>())).get();
            } catch (...) {
                std::cerr << "error: could not load schema file '" << app_config["schema-file"].as<sstring>() << "': " << std::current_exception() << std::endl;
                return 1;
            }

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

            operation(schema, permit, sstables, app_config);

            return 0;
        });
    });
}

} // namespace tools
