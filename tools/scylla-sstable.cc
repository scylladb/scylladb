/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <set>
#include <fmt/chrono.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/units.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/util/closeable.hh>

#include "init.hh"
#include "compaction/compaction.hh"
#include "compaction/compaction_strategy.hh"
#include "compaction/compaction_strategy_state.hh"
#include "cql3/type_json.hh"
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "db/corrupt_data_handler.hh"
#include "gms/feature_service.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/combined.hh"
#include "readers/generating.hh"
#include "schema/schema_builder.hh"
#include "sstables/index_reader.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/open_info.hh"
#include "replica/schema_describe_helper.hh"
#include "test/lib/cql_test_env.hh"
#include "tools/json_writer.hh"
#include "tools/json_mutation_stream_parser.hh"
#include "tools/load_system_tablets.hh"
#include "tools/lua_sstable_consumer.hh"
#include "tools/schema_loader.hh"
#include "tools/sstable_consumer.hh"
#include "tools/utils.hh"
#include "locator/host_id.hh"

using namespace seastar;
using namespace sstables;

using json_writer = mutation_json::json_writer;

namespace bpo = boost::program_options;

using namespace tools::utils;

using operation_func = void(*)(schema_ptr, reader_permit, const std::vector<sstables::shared_sstable>&, sstables::sstables_manager&, const bpo::variables_map&);

namespace std {
// required by boost::lexical_cast<std::string>(vector<string>), which is in turn used
// by boost::program_option for printing out the default value of an option
std::ostream& operator<<(std::ostream& os, const std::vector<sstring>& v) {
    return os << fmt::format("{}", v);
}
}

namespace {

const auto app_name = "sstable";

logging::logger sst_log(format("scylla-{}", app_name));

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

struct sstable_path_info {
    std::filesystem::path sstable_path;
    std::filesystem::path data_dir_path;
    sstring keyspace;
    sstring table;
};

sstable_path_info extract_from_sstable_path(const bpo::variables_map& app_config) {
    if (!app_config.count("sstables")) {
        throw std::invalid_argument("cannot extract information from sstable path, no sstable arguments");
    }

    auto sst_path = std::filesystem::path(app_config["sstables"].as<std::vector<sstring>>().front());
    sstring keyspace, table;
    try {
        auto [_, ks, tbl] = sstables::parse_path(sst_path);
        keyspace = std::move(ks);
        table = std::move(tbl);
    } catch (const sstables::malformed_sstable_exception&) {
        throw std::invalid_argument(fmt::format("cannot extract information from sstable path, sstable has invalid path: {}", sst_path));
    }
    const auto sst_dir_path = std::filesystem::path(sst_path).remove_filename();
    std::filesystem::path data_dir_path;
    // Detect whether sstable is in root table directory, or in a sub-directory
    // The last component is "" due to the trailing "/" left by "remove_filename()" above.
    // So we need to go back 2 more, to find the supposed keyspace component.
    if (keyspace == std::prev(sst_dir_path.end(), 3)->native()) {
        data_dir_path = sst_dir_path / ".." / "..";
    } else {
        data_dir_path = sst_dir_path / ".." / ".." / "..";
    }

    return sstable_path_info{std::move(sst_path), std::move(data_dir_path), std::move(keyspace), std::move(table)};
}

std::pair<sstring, sstring> get_keyspace_and_table_options(const bpo::variables_map& app_config) {
    sstring keyspace_name, table_name;
    auto k_it = app_config.find("keyspace");
    auto t_it = app_config.find("table");
    if (k_it != app_config.end() || t_it != app_config.end()) {
        return std::pair(k_it->second.as<sstring>(), t_it->second.as<sstring>());
    }

    try {
        auto info = extract_from_sstable_path(app_config);
        return std::pair(info.keyspace, info.table);
    } catch (...) {
        throw std::invalid_argument("don't know which schema to load: no --keyspace and --table provided, failed to extract keyspace/table from sstable paths");
    }
}

struct path_with_source {
    fs::path path;
    sstring source;
};

path_with_source obtain_data_dir(const bpo::variables_map& app_config, db::config& cfg) {
    if (app_config.contains("scylla-data-dir")) {
        return {.path = fs::path(app_config["scylla-data-dir"].as<sstring>()), .source = "--scylla-data-dir parameter"};
    } else if (app_config.contains("scylla-yaml-file")) {
        return {.path = fs::path(cfg.data_file_directories()[0]), .source = "--scylla-yaml-file parameter"};
    } else if (std::getenv("SCYLLA_CONF")) {
        return {.path = fs::path(cfg.data_file_directories()[0]), .source = "SCYLLA_CONF environment variable"};
    } else if (std::getenv("SCYLLA_HOME")) {
        return {.path = fs::path(cfg.data_file_directories()[0]), .source = "SCYLLA_HOME environment variable"};
    } else {
        const auto info = extract_from_sstable_path(app_config);
        return {.path = info.data_dir_path, .source = seastar::format("autodetected from sstable path ({})", info.sstable_path.native())};
    }
}

struct schema_with_source {
    schema_ptr schema;
    sstring source;
    std::optional<fs::path> path;
    sstring obtained_from;
};

std::optional<schema_with_source> try_load_schema_from_user_provided_source(const bpo::variables_map& app_config, db::config& cfg) {
    sstring schema_source_opt;
    try {
        if (!app_config["schema-file"].defaulted()) {
            schema_source_opt = "schema-file";
            const auto schema_file_path = std::filesystem::path(app_config["schema-file"].as<sstring>());
            return schema_with_source{.schema = tools::load_one_schema_from_file(cfg, schema_file_path).get(),
                .source = schema_source_opt,
                .path = schema_file_path,
                .obtained_from = "--schema-file parameter"};
        }
        // All the below schema sources require this.
        const auto [keyspace_name, table_name] = get_keyspace_and_table_options(app_config);
        if (app_config.contains("system-schema")) {
            schema_source_opt = "system-schema";
            return schema_with_source{.schema = tools::load_system_schema(cfg, keyspace_name, table_name),
                .source = schema_source_opt,
                .obtained_from = "--system-schema parameter"};
        }
        if (app_config.contains("schema-tables")) {
            const auto path_with_source = obtain_data_dir(app_config, cfg);
            schema_source_opt = "schema-tables";
            return schema_with_source{.schema = tools::load_schema_from_schema_tables(cfg, path_with_source.path, keyspace_name, table_name).get(),
                .source= schema_source_opt,
                .path = path_with_source.path,
                .obtained_from = format("--schema-tables parameter (data-dir path obtained via {})", path_with_source.source)};
        }
        if (app_config.contains("sstable-schema")) {
            const auto sst_path = fs::path(app_config["sstables"].as<std::vector<sstring>>().front());
            return schema_with_source{.schema = tools::load_schema_from_sstable(cfg, sst_path, keyspace_name, table_name).get(),
                .source= schema_source_opt,
                .path = sst_path,
                .obtained_from = "--sstable-schema parameter"};
        }
    } catch (...) {
        fmt::print(std::cerr, "error processing arguments: could not load schema via {}: {}\n", schema_source_opt, std::current_exception());
        return {};
    }
    // Should not happen, but if it does (we all know it will), let's at least have a message printed.
    fmt::print(std::cerr, "error processing arguments: could not load schema from known schema sources: unknown error\n");
    return {};
}

std::optional<schema_with_source> try_load_schema_autodetect(const bpo::variables_map& app_config, db::config& cfg) {
    try {
        const auto schema_file_path = std::filesystem::path(app_config["schema-file"].as<sstring>());
        return schema_with_source{.schema = tools::load_one_schema_from_file(cfg, schema_file_path).get(),
            .source = "schema-file",
            .path = schema_file_path,
            .obtained_from = "--schema-file parameters (default value)"};
    } catch (...) {
        sst_log.debug("Trying to read schema file from default location failed: {}", std::current_exception());
    }

    if (app_config.count("sstables")) {
        try {
            auto info = extract_from_sstable_path(app_config);
            return schema_with_source{.schema = tools::load_schema_from_schema_tables(cfg, info.data_dir_path, info.keyspace, info.table).get(),
                .source = "schema-tables",
                .path = info.data_dir_path,
                .obtained_from = format("sstable path ({})", info.sstable_path)};
        } catch (...) {
            sst_log.debug("Trying to find scylla data dir based on the sstable path failed: {}", std::current_exception());
        }
    } else {
        sst_log.debug("Trying to find scylla data dir based on sstable path failed: no sstable argument provided");
    }

    try {
        const auto [keyspace_name, table_name] = get_keyspace_and_table_options(app_config);
        const auto data_dir_path = std::filesystem::path(cfg.data_file_directories().at(0));
        return schema_with_source{.schema = tools::load_schema_from_schema_tables(cfg, data_dir_path, keyspace_name, table_name).get(),
            .source = "schema-tables",
            .path = data_dir_path,
            .obtained_from = "data dir"};
    } catch (...) {
        sst_log.debug("Trying to locate data dir failed: {}", std::current_exception());
    }

    try {
        sstring keyspace_name, table_name;
        try {
            std::tie(keyspace_name, table_name) = get_keyspace_and_table_options(app_config);
        } catch (std::invalid_argument&) {
            keyspace_name = "my_keyspace";
            table_name = "my_table";
        }
        if (!app_config.count("sstables")) {
            throw std::runtime_error("no sstables provided on the command-line");
        }
        const auto sst_path = app_config["sstables"].as<std::vector<sstring>>().front();
        return schema_with_source{.schema = tools::load_schema_from_sstable(cfg, fs::path(sst_path),
                keyspace_name, table_name).get(),
            .source = "sstable's serialization header",
            .obtained_from = sst_path};
    } catch (...) {
        sst_log.debug("Trying to load schema from the sstable itself failed: {}", std::current_exception());
    }

    fmt::print(std::cerr, "Failed to autodetect and load schema, try again with --logger-log-level scylla-sstable=debug to learn more or provide the schema source manually\n");
    return {};
}

const std::vector<sstables::shared_sstable> load_sstables(schema_ptr schema, sstables::sstables_manager& sst_man, const std::vector<sstring>& sstable_names) {
    std::vector<sstables::shared_sstable> sstables;
    sstables.resize(sstable_names.size());

    parallel_for_each(sstable_names, [schema, &sst_man, &sstable_names, &sstables] (const sstring& sst_name) -> future<> {
        const auto i = std::distance(sstable_names.begin(), std::find(sstable_names.begin(), sstable_names.end(), sst_name));
        auto sst_path = std::filesystem::path(sst_name);

        if (const auto ftype_opt = co_await file_type(sst_path.c_str(), follow_symlink::yes)) {
            if (!ftype_opt) {
                throw std::invalid_argument(fmt::format("failed to determine type of file pointed to by provided sstable path {}", sst_path.c_str()));
            }
            if (*ftype_opt != directory_entry_type::regular) {
                throw std::invalid_argument(fmt::format("file pointed to by provided sstable path {} is not a regular file", sst_path.c_str()));
            }
        }


        data_dictionary::storage_options options;
        auto ed = sstables::parse_path(sst_path, schema->ks_name(), schema->cf_name());

        if (s3::is_s3_fqn(sst_path)) {
            if (sst_man.config().object_storage_endpoints().empty()) {
                throw std::invalid_argument("Unable to open SSTable in S3: AWS object storage configuration missing. Please provide a --scylla-yaml-file with "
                                            "valid AWS object storage configuration.");
            }
            auto endpoint = sst_man.config().object_storage_endpoints().front().endpoint;
            options = data_dictionary::make_s3_options(endpoint, sst_path);
        } else {
            sst_path = std::filesystem::canonical(std::filesystem::path(sst_name));
            const auto dir_path = sst_path.parent_path();
            options = data_dictionary::make_local_options(dir_path);
        }
        auto sst = sst_man.make_sstable(schema, options, ed.generation, sstables::sstable_state::normal, ed.version, ed.format);

        try {
            co_await sst->load(schema->get_sharder(), sstables::sstable_open_config{.load_first_and_last_position_metadata = false});
        } catch (...) {
            // Print each individual error here since parallel_for_each
            // will propagate only one of them up the stack.
            auto msg = fmt::format("Could not load SSTable: {}", sst->get_filename());
            fmt::print(std::cerr, "{}: {}\n", msg, std::current_exception());
            throw_with_nested(std::runtime_error(msg));
        }

        sstables[i] = std::move(sst);
    }).get();

    return sstables;
}

class consumer_wrapper {
public:
    using filter_type = std::function<bool(const dht::decorated_key&)>;
private:
    mutation_fragment_v2::kind _last_kind = mutation_fragment_v2::kind::partition_end;
    sstable_consumer& _consumer;
    filter_type _filter;
public:
    consumer_wrapper(sstable_consumer& consumer, filter_type filter)
        : _consumer(consumer), _filter(std::move(filter)) {
    }
    mutation_fragment_v2::kind last_kind() const {
        return _last_kind;
    }
    future<stop_iteration> operator()(mutation_fragment_v2&& mf) {
        sst_log.trace("consume {}", mf.mutation_fragment_kind());
        if (mf.is_partition_start() && _filter && !_filter(mf.as_partition_start().key())) {
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        _last_kind = mf.mutation_fragment_kind();
        return std::move(mf).consume(_consumer);
    }
};

enum class output_format {
    text, json
};

output_format get_output_format_from_options(const bpo::variables_map& opts, output_format default_format) {
    if (auto it = opts.find("output-format"); it != opts.end()) {
        const auto& value = it->second.as<std::string>();
        if (value == "text") {
            return output_format::text;
        } else if (value == "json") {
            return output_format::json;
        } else {
            throw std::invalid_argument(fmt::format("invalid value for dump option output-format: {}", value));
        }
    }
    return default_format;
}

} // anonymous namespace

namespace tools {

void mutation_fragment_stream_json_writer::write(const clustering_row& cr) {
    writer().StartObject();
    writer().Key("type");
    writer().String("clustering-row");
    writer().Key("key");
    writer().DataKey(_writer.schema(), cr.key());
    if (cr.tomb()) {
        writer().Key("tombstone");
        _writer.write(cr.tomb().regular());
        writer().Key("shadowable_tombstone");
        _writer.write(cr.tomb().shadowable().tomb());
    }
    if (!cr.marker().is_missing()) {
        writer().Key("marker");
        _writer.write(cr.marker());
    }
    writer().Key("columns");
    _writer.write(cr.cells(), column_kind::regular_column);
    writer().EndObject();
}

void mutation_fragment_stream_json_writer::write(const range_tombstone_change& rtc) {
    writer().StartObject();
    writer().Key("type");
    writer().String("range-tombstone-change");
    const auto pos = rtc.position();
    if (pos.has_key()) {
        writer().Key("key");
        writer().DataKey(_writer.schema(), pos.key());
    }
    writer().Key("weight");
    writer().Int(static_cast<int>(pos.get_bound_weight()));
    writer().Key("tombstone");
    _writer.write(rtc.tombstone());
    writer().EndObject();
}

void mutation_fragment_stream_json_writer::start_stream() {
    writer().StartStream();
}

void mutation_fragment_stream_json_writer::start_sstable(const sstables::sstable* const sst) {
    if (sst) {
        writer().Key(fmt::to_string(sst->get_filename()));
    } else {
        writer().Key("anonymous");
    }
    writer().StartArray();
}

void mutation_fragment_stream_json_writer::start_partition(const partition_start& ps) {
    const auto& dk = ps.key();
    _clustering_array_created = false;

    writer().StartObject();

    writer().Key("key");
    writer().DataKey(_writer.schema(), dk.key(), dk.token());

    if (ps.partition_tombstone()) {
        writer().Key("tombstone");
        _writer.write(ps.partition_tombstone());
    }
}

void mutation_fragment_stream_json_writer::partition_element(const static_row& sr) {
    writer().Key("static_row");
    _writer.write(sr.cells(), column_kind::static_column);
}

void mutation_fragment_stream_json_writer::partition_element(const clustering_row& cr) {
    if (!_clustering_array_created) {
        writer().Key("clustering_elements");
        writer().StartArray();
        _clustering_array_created = true;
    }
    write(cr);
}

void mutation_fragment_stream_json_writer::partition_element(const range_tombstone_change& rtc) {
    if (!_clustering_array_created) {
        writer().Key("clustering_elements");
        writer().StartArray();
        _clustering_array_created = true;
    }
    write(rtc);
}

void mutation_fragment_stream_json_writer::end_partition() {
    if (_clustering_array_created) {
        writer().EndArray();
    }
    writer().EndObject();
}

void mutation_fragment_stream_json_writer::end_sstable() {
    writer().EndArray();
}

void mutation_fragment_stream_json_writer::end_stream() {
    writer().EndStream();
}

} // namespace tools

namespace {

class dumping_consumer : public sstable_consumer {
    class text_dumper : public sstable_consumer {
        const schema& _schema;
    public:
        explicit text_dumper(const schema& s) : _schema(s) { }
        virtual future<> consume_stream_start() override {
            fmt::print("{{stream_start}}\n");
            return make_ready_future<>();
        }
        virtual future<stop_iteration> consume_sstable_start(const sstables::sstable* const sst) override {
            fmt::print("{{sstable_start{}}}\n", sst ? fmt::format(": filename {}", sst->get_filename()) : "");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(partition_start&& ps) override {
            fmt::print("{}\n", ps);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(static_row&& sr) override {
            fmt::print("{}\n", static_row::printer(_schema, sr));
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(clustering_row&& cr) override {
            fmt::print("{}\n", clustering_row::printer(_schema, cr));
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
            fmt::print("{}\n", rtc);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(partition_end&& pe) override {
            fmt::print("{{partition_end}}\n");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume_sstable_end() override {
            fmt::print("{{sstable_end}}\n");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<> consume_stream_end() override {
            fmt::print("{{stream_end}}\n");
            return make_ready_future<>();
        }
    };
    class json_dumper : public sstable_consumer {
        tools::mutation_fragment_stream_json_writer _writer;
    public:
        explicit json_dumper(const schema& s) : _writer(s) {}
        virtual future<> consume_stream_start() override {
            _writer.start_stream();
            return make_ready_future<>();
        }
        virtual future<stop_iteration> consume_sstable_start(const sstables::sstable* const sst) override {
            _writer.start_sstable(sst);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(partition_start&& ps) override {
            _writer.start_partition(ps);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(static_row&& sr) override {
            _writer.partition_element(sr);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(clustering_row&& cr) override {
            _writer.partition_element(cr);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
            _writer.partition_element(rtc);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume(partition_end&& pe) override {
            _writer.end_partition();
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<stop_iteration> consume_sstable_end() override {
            _writer.end_sstable();
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        virtual future<> consume_stream_end() override {
            _writer.end_stream();
            return make_ready_future<>();
        }
    };

private:
    schema_ptr _schema;
    std::unique_ptr<sstable_consumer> _consumer;

public:
    explicit dumping_consumer(schema_ptr s, reader_permit, const bpo::variables_map& opts) : _schema(std::move(s)) {
        _consumer = std::make_unique<text_dumper>(*_schema);
        switch (get_output_format_from_options(opts, output_format::text)) {
            case output_format::text:
                _consumer = std::make_unique<text_dumper>(*_schema);
                break;
            case output_format::json:
                _consumer = std::make_unique<json_dumper>(*_schema);
                break;
        }
    }
    virtual future<> consume_stream_start() override { return _consumer->consume_stream_start(); }
    virtual future<stop_iteration> consume_sstable_start(const sstables::sstable* const sst) override { return _consumer->consume_sstable_start(sst); }
    virtual future<stop_iteration> consume(partition_start&& ps) override { return _consumer->consume(std::move(ps)); }
    virtual future<stop_iteration> consume(static_row&& sr) override { return _consumer->consume(std::move(sr)); }
    virtual future<stop_iteration> consume(clustering_row&& cr) override { return _consumer->consume(std::move(cr)); }
    virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override { return _consumer->consume(std::move(rtc)); }
    virtual future<stop_iteration> consume(partition_end&& pe) override { return _consumer->consume(std::move(pe)); }
    virtual future<stop_iteration> consume_sstable_end() override { return _consumer->consume_sstable_end(); }
    virtual future<> consume_stream_end() override { return _consumer->consume_stream_end(); }
};

stop_iteration consume_reader(mutation_reader rd, sstable_consumer& consumer, sstables::sstable* sst, const partition_set& partitions, bool no_skips) {
    auto close_rd = deferred_close(rd);
    if (consumer.consume_sstable_start(sst).get() == stop_iteration::yes) {
        return consumer.consume_sstable_end().get();
    }
    bool skip_partition = false;
    consumer_wrapper::filter_type filter;
    if (!partitions.empty()) {
        filter = [&] (const dht::decorated_key& key) {
            const auto pass = partitions.contains(key);
            sst_log.trace("filter({})={}", key, pass);
            skip_partition = !pass;
            return pass;
        };
    }
    auto wrapper = consumer_wrapper(consumer, filter);
    while (!rd.is_end_of_stream()) {
        skip_partition = false;
        rd.consume_pausable(std::ref(wrapper)).get();
        sst_log.trace("consumer paused, skip_partition={}", skip_partition);
        if (!rd.is_end_of_stream() && !skip_partition) {
            if (wrapper.last_kind() == mutation_fragment_v2::kind::partition_end) {
                sst_log.trace("consumer returned stop_iteration::yes for partition end, stopping");
                break;
            }
            if (wrapper(mutation_fragment_v2(*rd.schema(), rd.permit(), partition_end{})).get() == stop_iteration::yes) {
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
    return consumer.consume_sstable_end().get();
}

void consume_sstables(schema_ptr schema, reader_permit permit, std::vector<sstables::shared_sstable> sstables, bool merge, bool use_full_scan_reader,
        std::function<stop_iteration(mutation_reader&, sstables::sstable*)> reader_consumer) {
    sst_log.trace("consume_sstables(): {} sstables, merge={}, use_full_scan_reader={}", sstables.size(), merge, use_full_scan_reader);
    if (merge) {
        std::vector<mutation_reader> readers;
        readers.reserve(sstables.size());
        for (const auto& sst : sstables) {
            if (use_full_scan_reader) {
                readers.emplace_back(sst->make_full_scan_reader(schema, permit));
            } else {
                readers.emplace_back(sst->make_reader(schema, permit, query::full_partition_range, schema->full_slice()));
            }
        }
        auto rd = make_combined_reader(schema, permit, std::move(readers));

        reader_consumer(rd, nullptr);
    } else {
        for (const auto& sst : sstables) {
            auto rd = use_full_scan_reader
                ? sst->make_full_scan_reader(schema, permit)
                : sst->make_reader(schema, permit, query::full_partition_range, schema->full_slice());

            if (reader_consumer(rd, sst.get()) == stop_iteration::yes) {
                break;
            }
        }
    }
}

class scylla_sstable_compaction_group_view : public compaction::compaction_group_view {
    struct dummy_compaction_backlog_tracker : public compaction::compaction_backlog_tracker::impl {
        virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override { }
        virtual double backlog(const compaction::compaction_backlog_tracker::ongoing_writes& ow, const compaction::compaction_backlog_tracker::ongoing_compactions& oc) const override { return 0.0; }
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    sstables::sstables_manager& _sst_man;
    std::string _output_dir;
    sstables::sstable_set _main_set;
    sstables::sstable_set _maintenance_set;
    std::vector<sstables::shared_sstable> _compacted_undeleted_sstables;
    mutable compaction::compaction_strategy _compaction_strategy;
    compaction::compaction_strategy_state _compaction_strategy_state;
    tombstone_gc_state _tombstone_gc_state;
    compaction::compaction_backlog_tracker _backlog_tracker;
    std::string _group_id;
    condition_variable _staging_done_condition;
    mutable sstable_generation_generator _generation_generator;

private:
    sstables::shared_sstable do_make_sstable() const {
        const auto format = sstables::sstable_format_types::big;
        const auto version = sstables::get_highest_sstable_version();
        auto generation = _generation_generator();
        auto sst_name = sstables::sstable::filename(_output_dir, _schema->ks_name(), _schema->cf_name(), version, generation, format, component_type::Data);
        if (file_exists(sst_name).get()) {
            throw std::runtime_error(fmt::format("cannot create output sstable {}, file already exists", sst_name));
        }
        auto local = data_dictionary::make_local_options(_output_dir);
        return _sst_man.make_sstable(_schema, local, generation, sstables::sstable_state::normal, version, format);
    }
    sstables::sstable_writer_config do_configure_writer(sstring origin) const {
        return _sst_man.configure_writer(std::move(origin));
    }

public:
    scylla_sstable_compaction_group_view(schema_ptr schema, reader_permit permit, sstables::sstables_manager& sst_man, std::string output_dir)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _sst_man(sst_man)
        , _output_dir(std::move(output_dir))
        , _main_set(sstables::make_partitioned_sstable_set(_schema, token_range()))
        , _maintenance_set(sstables::make_partitioned_sstable_set(_schema, token_range()))
        , _compaction_strategy(compaction::make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
        , _compaction_strategy_state(compaction::compaction_strategy_state::make(_compaction_strategy))
        , _tombstone_gc_state(nullptr)
        , _backlog_tracker(std::make_unique<dummy_compaction_backlog_tracker>())
        , _group_id("dummy-group")
        , _generation_generator()
    { }
    virtual dht::token_range token_range() const noexcept override { return dht::token_range::make(dht::first_token(), dht::last_token()); }
    virtual const schema_ptr& schema() const noexcept override { return _schema; }
    virtual unsigned min_compaction_threshold() const noexcept override { return _schema->min_compaction_threshold(); }
    virtual bool compaction_enforce_min_threshold() const noexcept override { return false; }
    virtual future<lw_shared_ptr<const sstables::sstable_set>> main_sstable_set() const override { co_return make_lw_shared<const sstables::sstable_set>(_main_set); }
    virtual future<lw_shared_ptr<const sstables::sstable_set>> maintenance_sstable_set() const override { co_return make_lw_shared<const sstables::sstable_set>(_maintenance_set); }
    lw_shared_ptr<const sstables::sstable_set> sstable_set_for_tombstone_gc() const override { return make_lw_shared<const sstables::sstable_set>(_main_set); }
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point compaction_time) const override { return {}; }
    virtual const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override { return _compacted_undeleted_sstables; }
    virtual compaction::compaction_strategy& get_compaction_strategy() const noexcept override { return _compaction_strategy; }
    virtual compaction::compaction_strategy_state& get_compaction_strategy_state() noexcept override { return _compaction_strategy_state; }
    virtual reader_permit make_compaction_reader_permit() const override { return _permit; }
    virtual sstables::sstables_manager& get_sstables_manager() noexcept override { return _sst_man; }
    virtual sstables::shared_sstable make_sstable() const override { return do_make_sstable(); }
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const override { return do_configure_writer(std::move(origin)); }
    virtual api::timestamp_type min_memtable_timestamp() const override { return api::min_timestamp; }
    virtual api::timestamp_type min_memtable_live_timestamp() const override { return api::min_timestamp; }
    virtual api::timestamp_type min_memtable_live_row_marker_timestamp() const override { return api::min_timestamp; }
    virtual bool memtable_has_key(const dht::decorated_key& key) const override { return false; }
    virtual future<> on_compaction_completion(compaction::compaction_completion_desc desc, sstables::offstrategy offstrategy) override { return make_ready_future<>(); }
    virtual bool is_auto_compaction_disabled_by_user() const noexcept override { return false; }
    virtual bool tombstone_gc_enabled() const noexcept override { return false; }
    virtual const tombstone_gc_state& get_tombstone_gc_state() const noexcept override { return _tombstone_gc_state; }
    virtual compaction::compaction_backlog_tracker& get_backlog_tracker() override { return _backlog_tracker; }
    virtual const std::string get_group_id() const noexcept override { return _group_id; }
    virtual seastar::condition_variable& get_staging_done_condition() noexcept override { return _staging_done_condition; }
    dht::token_range get_token_range_after_split(const dht::token& t) const noexcept override { return dht::token_range(); }
    int64_t get_sstables_repaired_at() const noexcept override { return 0; }
};

void validate_output_dir(std::filesystem::path output_dir, bool accept_nonempty_output_dir) {
    auto fd = open_file_dma(output_dir.native(), open_flags::ro).get();
    unsigned entries = 0;
    fd.list_directory([&entries] (directory_entry) {
        ++entries;
        return make_ready_future<>();
    }).done().get();
    if (entries && !accept_nonempty_output_dir) {
        throw std::invalid_argument("output-directory is not empty, pass --unsafe-accept-nonempty-output-dir if you are sure you want to write into this directory");
    }
}

void validate_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map& vm) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    abort_source abort;
    // Collect JSON output and print after validation is done, to prevent
    // interleaving with error messages from validation.
    std::stringstream json_output_stream;
    json_writer writer(json_output_stream);
    writer.StartStream();
    for (const auto& sst : sstables) {
        const auto errors = sst->validate(permit, abort, [] (sstring what) { sst_log.info("{}", what); }).get();
        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();
        writer.Key("errors");
        writer.Uint64(errors);
        writer.Key("valid");
        writer.Bool(errors == 0);
        writer.EndObject();
    }
    writer.EndStream();
    fmt::print(std::cout, "{}", json_output_stream.view());
}

void scrub_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map& vm) {
    static const std::vector<std::pair<std::string, compaction::compaction_type_options::scrub::mode>> scrub_modes{
        {"abort", compaction::compaction_type_options::scrub::mode::abort},
        {"skip", compaction::compaction_type_options::scrub::mode::skip},
        {"segregate", compaction::compaction_type_options::scrub::mode::segregate},
        {"validate", compaction::compaction_type_options::scrub::mode::validate},
    };

    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }
    compaction::compaction_type_options::scrub::mode scrub_mode;
    {
        if (!vm.count("scrub-mode")) {
            throw std::invalid_argument("missing mandatory command-line argument --scrub-mode");
        }
        const auto mode_name = vm["scrub-mode"].as<std::string>();
        auto mode_it = std::ranges::find_if(scrub_modes, [&mode_name] (const std::pair<std::string, compaction::compaction_type_options::scrub::mode>& v) {
            return v.first == mode_name;
        });
        if (mode_it == scrub_modes.end()) {
            throw std::invalid_argument(fmt::format("invalid scrub-mode: {}", mode_name));
        }
        scrub_mode = mode_it->second;
    }
    auto output_dir = vm["output-dir"].as<std::string>();
    if (scrub_mode != compaction::compaction_type_options::scrub::mode::validate) {
        validate_output_dir(output_dir, vm.count("unsafe-accept-nonempty-output-dir"));
    }

    scylla_sstable_compaction_group_view compaction_group_view(schema, permit, sst_man, output_dir);

    auto compaction_descriptor = compaction::compaction_descriptor(std::move(sstables));
    compaction_descriptor.options = compaction::compaction_type_options::make_scrub(scrub_mode, compaction::compaction_type_options::scrub::quarantine_invalid_sstables::no);
    compaction_descriptor.creator = [&compaction_group_view] (shard_id) { return compaction_group_view.make_sstable(); };
    compaction_descriptor.replacer = [] (compaction::compaction_completion_desc) { };

    auto compaction_data = compaction::compaction_data{};

    compaction::compaction_progress_monitor progress_monitor;
    compaction::compact_sstables(std::move(compaction_descriptor), compaction_data, compaction_group_view, progress_monitor).get();
}

void dump_index_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    json_writer writer;
    writer.StartStream();
    for (auto& sst : sstables) {
        auto idx_reader = sst->make_index_reader(permit);
        auto close_idx_reader = deferred_close(*idx_reader);

        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartArray();

        while (!idx_reader->eof()) {
            idx_reader->read_partition_data().get();
            auto pos = idx_reader->data_file_positions().start;
            auto pkey = idx_reader->get_partition_key();

            writer.StartObject();
            if (pkey) {
                writer.Key("key");
                writer.DataKey(*schema, *pkey);
            }
            writer.Key("pos");
            writer.Uint64(pos);
            writer.EndObject();

            idx_reader->advance_to_next_partition().get();
        }
        writer.EndArray();
    }
    writer.EndStream();
}

template <typename Integer>
sstring disk_string_to_string(const sstables::disk_string<Integer>& ds) {
    return sstring(ds.value.begin(), ds.value.end());
}

void dump_compression_info_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    json_writer writer;
    writer.StartStream();

    for (auto& sst : sstables) {
        const auto& compression = sst->get_compression();

        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();
        writer.Key("name");
        writer.String(disk_string_to_string(compression.name));
        writer.Key("options");
        writer.StartObject();
        for (const auto& opt : compression.options.elements) {
            writer.Key(disk_string_to_string(opt.key));
            writer.String(disk_string_to_string(opt.value));
        }
        writer.EndObject();
        writer.Key("chunk_len");
        writer.Uint(compression.chunk_len);
        writer.Key("data_len");
        writer.Uint64(compression.data_len);
        writer.Key("offsets");
        writer.StartArray();
        for (const auto& offset : compression.offsets) {
            writer.Uint64(offset);
        }
        writer.EndArray();
        writer.EndObject();
    }
    writer.EndStream();
}

void dump_summary_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    json_writer writer;
    writer.StartStream();

    for (auto& sst : sstables) {
        auto& summary = sst->get_summary();

        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();

        writer.Key("header");
        writer.StartObject();
        writer.Key("min_index_interval");
        writer.Uint64(summary.header.min_index_interval);
        writer.Key("size");
        writer.Uint64(summary.header.size);
        writer.Key("memory_size");
        writer.Uint64(summary.header.memory_size);
        writer.Key("sampling_level");
        writer.Uint64(summary.header.sampling_level);
        writer.Key("size_at_full_sampling");
        writer.Uint64(summary.header.size_at_full_sampling);
        writer.EndObject();

        writer.Key("positions");
        writer.StartArray();
        for (const auto& pos : summary.positions) {
            writer.Uint64(pos);
        }
        writer.EndArray();

        writer.Key("entries");
        writer.StartArray();
        for (const auto& e : summary.entries) {
            writer.StartObject();

            auto pkey = e.get_key().to_partition_key(*schema);
            writer.Key("key");
            writer.DataKey(*schema, pkey, e.get_token());
            writer.Key("position");
            writer.Uint64(e.position);

            writer.EndObject();
        }
        writer.EndArray();

        auto first_key = dht::decorate_key(*schema, sstables::key_view(summary.first_key.value).to_partition_key(*schema));
        writer.Key("first_key");
        writer.DataKey(*schema, first_key);

        auto last_key = dht::decorate_key(*schema, sstables::key_view(summary.last_key.value).to_partition_key(*schema));
        writer.Key("last_key");
        writer.DataKey(*schema, last_key);

        writer.EndObject();
    }
    writer.EndStream();
}

class json_dumper {
    json_writer& _writer;
    sstables::sstable_version_types _version;
    std::function<std::string_view(const void* const)> _name_resolver;
    std::function<sstring(const void* const, bytes_view)> _disk_string_converter;

public:
    static sstring default_disk_string_converter(const void* const, bytes_view value) {
        return sstring(value.begin(), value.end());
    }

private:
    void visit(const void* const field, int8_t val) { _writer.Int(val); }
    void visit(const void* const field, uint8_t val) { _writer.Uint(val); }
    void visit(const void* const field, int val) { _writer.Int(val); }
    void visit(const void* const field, unsigned val) { _writer.Uint(val); }
    void visit(const void* const field, int64_t val) { _writer.Int64(val); }
    void visit(const void* const field, uint64_t val) { _writer.Uint64(val); }
    void visit(const void* const field, double val) {
        if (std::isnan(val)) {
            _writer.String("NaN");
        } else {
            _writer.Double(val);
        }
    }

    template <typename Integer>
    void visit(const void* const field, const sstables::disk_string<Integer>& val) {
        _writer.String(_disk_string_converter(field, val.value));
    }

    template <typename Contents>
    void visit(const void* const field, const std::optional<Contents>& val) {
        if (bool(val)) {
            visit(field, *val);
        } else {
            _writer.Null();
        }
    }

    template <typename Integer, typename T>
    void visit(const void* const field, const sstables::disk_array<Integer, T>& val) {
        _writer.StartArray();
        for (const auto& elem : val.elements) {
            visit(field, elem);
        }
        _writer.EndArray();
    }

    void visit(const void* const field, const sstables::disk_string_vint_size& val) {
        _writer.String(_disk_string_converter(field, val.value));
    }

    template <typename T>
    void visit(const void* const field, const sstables::disk_array_vint_size<T>& val) {
        _writer.StartArray();
        for (const auto& elem : val.elements) {
            visit(field, elem);
        }
        _writer.EndArray();
    }

    void visit(const void* const field, const utils::estimated_histogram& val) {
        _writer.StartArray();
        for (size_t i = 0; i < val.buckets.size(); i++) {
            _writer.StartObject();
            _writer.Key("offset");
            _writer.Int64(val.bucket_offsets[i == 0 ? 0 : i - 1]);
            _writer.Key("value");
            _writer.Int64(val.buckets[i]);
            _writer.EndObject();
        }
        _writer.EndArray();
    }

    void visit(const void* const field, const utils::streaming_histogram& val) {
        _writer.StartObject();
        for (const auto& [k, v] : val.bin) {
            _writer.Key(format("{}", k));
            _writer.Uint64(v);
        }
        _writer.EndObject();
    }

    void visit(const void* const field, const db::replay_position& val) {
        _writer.StartObject();
        _writer.Key("id");
        _writer.Uint64(val.id);
        _writer.Key("pos");
        _writer.Uint(val.pos);
        _writer.EndObject();
    }

    void visit(const void* const field, const sstables::commitlog_interval& val) {
        _writer.StartObject();
        _writer.Key("start");
        visit(field, val.start);
        _writer.Key("end");
        visit(field, val.end);
        _writer.EndObject();
    }

    void visit(const void* const field, const utils::UUID& uuid) {
        _writer.String(fmt::to_string(uuid));
    }

    template <typename Tag>
    void visit(const void* const field, const utils::tagged_uuid<Tag>& id) {
        visit(field, id.uuid());
    }

    template <typename Integer>
    void visit(const void* const field, const sstables::vint<Integer>& val) {
        visit(field, val.value);
    }

    void visit(const void* const field, const sstables::serialization_header::column_desc& val) {
        auto prev_name_resolver = std::exchange(_name_resolver, [&val] (const void* const field) {
            if (field == &val.name) { return "name"; }
            else if (field == &val.type_name) { return "type_name"; }
            else { throw std::invalid_argument("invalid field offset"); }
        });

        _writer.StartObject();
        const_cast<sstables::serialization_header::column_desc&>(val).describe_type(_version, std::ref(*this));
        _writer.EndObject();

        _name_resolver = std::move(prev_name_resolver);
    }

    json_dumper(json_writer& writer, sstables::sstable_version_types version, std::function<std::string_view(const void* const)> name_resolver,
        std::function<sstring(const void* const, bytes_view string)> disk_string_converter)
        : _writer(writer), _version(version), _name_resolver(std::move(name_resolver)), _disk_string_converter(std::move(disk_string_converter)) {
    }

public:
    template <typename Arg1>
    void operator()(Arg1& arg1) {
        _writer.Key(_name_resolver(&arg1));
        visit(&arg1, arg1);
    }

    template <typename Arg1, typename... Arg>
    void operator()(Arg1& arg1, Arg&... arg) {
        _writer.Key(_name_resolver(&arg1));
        visit(&arg1, arg1);
        (*this)(arg...);
    }

    template <typename T>
    static void dump(
            json_writer& writer,
            sstables::sstable_version_types version,
            const T& obj,
            std::string_view name,
            std::function<std::string_view(const void* const)> name_resolver,
            std::function<sstring(const void* const, bytes_view string)> disk_string_converter = &json_dumper::default_disk_string_converter) {
        json_dumper dumper(writer, version, std::move(name_resolver), std::move(disk_string_converter));
        writer.Key(name);
        writer.StartObject();
        const_cast<T&>(obj).describe_type(version, std::ref(dumper));
        writer.EndObject();
    }
};

void dump_validation_metadata(json_writer& writer, sstables::sstable_version_types version, const sstables::validation_metadata& metadata) {
    json_dumper::dump(writer, version, metadata, "validation", [&metadata] (const void* const field) {
        if (field == &metadata.partitioner) { return "partitioner"; }
        else if (field == &metadata.filter_chance) { return "filter_chance"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_compaction_metadata(json_writer& writer, sstables::sstable_version_types version, const sstables::compaction_metadata& metadata) {
    json_dumper::dump(writer, version, metadata, "compaction", [&metadata] (const void* const field) {
        if (field == &metadata.ancestors) { return "ancestors"; }
        else if (field == &metadata.cardinality) { return "cardinality"; }
        else { throw std::invalid_argument("invalid field offset"); }
    });
}

void dump_stats_metadata(json_writer& writer, sstables::sstable_version_types version, const sstables::stats_metadata& metadata) {
    json_dumper::dump(writer, version, metadata, "stats", [&metadata] (const void* const field) {
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
        else if (field == &metadata.originating_host_id) { return "originating_host_id"; }
        else { throw std::invalid_argument("invalid field offset"); }
    }, [&metadata] (const void* const field, bytes_view value) {
        if (field == &metadata.min_column_names || field == &metadata.max_column_names) {
            return to_hex(value);
        }
        return json_dumper::default_disk_string_converter(field, value);
    });
}

void dump_serialization_header(json_writer& writer, sstables::sstable_version_types version, const sstables::serialization_header& metadata) {
    json_dumper::dump(writer, version, metadata, "serialization_header", [&metadata] (const void* const field) {
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

void dump_statistics_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    auto to_string = [] (sstables::metadata_type t) {
        switch (t) {
            case sstables::metadata_type::Validation: return "validation";
            case sstables::metadata_type::Compaction: return "compaction";
            case sstables::metadata_type::Stats: return "stats";
            case sstables::metadata_type::Serialization: return "serialization";
        }
        std::abort();
    };

    json_writer writer;
    writer.StartStream();
    for (auto& sst : sstables) {
        auto& statistics = sst->get_statistics();

        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();

        writer.Key("offsets");
        writer.StartObject();
        for (const auto& [k, v] : statistics.offsets.elements) {
            writer.Key(to_string(k));
            writer.Uint(v);
        }
        writer.EndObject();

        const auto version = sst->get_version();
        for (const auto& [type, _] : statistics.offsets.elements) {
            const auto& metadata_ptr = statistics.contents.at(type);
            switch (type) {
                case sstables::metadata_type::Validation:
                    dump_validation_metadata(writer, version, *dynamic_cast<const sstables::validation_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Compaction:
                    dump_compaction_metadata(writer, version, *dynamic_cast<const sstables::compaction_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Stats:
                    dump_stats_metadata(writer, version, *dynamic_cast<const sstables::stats_metadata*>(metadata_ptr.get()));
                    break;
                case sstables::metadata_type::Serialization:
                    dump_serialization_header(writer, version, *dynamic_cast<const sstables::serialization_header*>(metadata_ptr.get()));
                    break;
            }
        }

        writer.EndObject();
    }
    writer.EndStream();
}

const char* to_string(sstables::scylla_metadata_type t) {
    switch (t) {
        case sstables::scylla_metadata_type::Sharding: return "sharding";
        case sstables::scylla_metadata_type::Features: return "features";
        case sstables::scylla_metadata_type::ExtensionAttributes: return "extension_attributes";
        case sstables::scylla_metadata_type::RunIdentifier: return "run_identifier";
        case sstables::scylla_metadata_type::LargeDataStats: return "large_data_stats";
        case sstables::scylla_metadata_type::SSTableOrigin: return "sstable_origin";
        case sstables::scylla_metadata_type::ScyllaVersion: return "scylla_version";
        case sstables::scylla_metadata_type::ScyllaBuildId: return "scylla_build_id";
        case sstables::scylla_metadata_type::ExtTimestampStats: return "ext_timestamp_stats";
        case sstables::scylla_metadata_type::SSTableIdentifier: return "sstable_identifier";
    }
    std::abort();
}

const char* to_string(sstables::large_data_type t) {
    switch (t) {
        case sstables::large_data_type::partition_size: return "partition_size";
        case sstables::large_data_type::row_size: return "row_size";
        case sstables::large_data_type::cell_size: return "cell_size";
        case sstables::large_data_type::rows_in_partition: return "rows_in_partition";
        case sstables::large_data_type::elements_in_collection: return "elements_in_collection";
    }
    std::abort();
}

const char* to_string(sstables::ext_timestamp_stats_type t) {
    switch (t) {
        case sstables::ext_timestamp_stats_type::min_live_timestamp: return "min_live_timestamp";
        case sstables::ext_timestamp_stats_type::min_live_row_marker_timestamp: return "min_live_row_marker_timestamp";
    }
    std::abort();
}

class scylla_metadata_visitor {
    json_writer& _writer;

public:
    scylla_metadata_visitor(json_writer& writer) : _writer(writer) { }

    void operator()(const sstables::sharding_metadata& val) const {
        _writer.StartArray();
        for (const auto& e : val.token_ranges.elements) {
            _writer.StartObject();

            _writer.Key("left");
            _writer.StartObject();
            _writer.Key("exclusive");
            _writer.Bool(e.left.exclusive);
            _writer.Key("token");
            _writer.String(disk_string_to_string(e.left.token));
            _writer.EndObject();

            _writer.Key("right");
            _writer.StartObject();
            _writer.Key("exclusive");
            _writer.Bool(e.right.exclusive);
            _writer.Key("token");
            _writer.String(disk_string_to_string(e.right.token));
            _writer.EndObject();

            _writer.EndObject();
        }
        _writer.EndArray();
    }
    void operator()(const sstables::sstable_enabled_features& val) const {
        std::pair<sstables::sstable_feature, const char*> all_features[] = {
                {sstables::sstable_feature::NonCompoundPIEntries, "NonCompoundPIEntries"},
                {sstables::sstable_feature::NonCompoundRangeTombstones, "NonCompoundRangeTombstones"},
                {sstables::sstable_feature::ShadowableTombstones, "ShadowableTombstones"},
                {sstables::sstable_feature::CorrectStaticCompact, "CorrectStaticCompact"},
                {sstables::sstable_feature::CorrectEmptyCounters, "CorrectEmptyCounters"},
                {sstables::sstable_feature::CorrectUDTsInCollections, "CorrectUDTsInCollections"},
                {sstables::sstable_feature::CorrectLastPiBlockWidth, "CorrectLastPiBlockWidth"},
        };
        _writer.StartObject();
        _writer.Key("mask");
        _writer.Uint64(val.enabled_features);
        _writer.Key("features");
        _writer.StartArray();
        for (const auto& [mask, name] : all_features) {
            if (mask & val.enabled_features) {
                _writer.String(name);
            }
        }
        _writer.EndArray();
        _writer.EndObject();
    }
    void operator()(const sstables::scylla_metadata::extension_attributes& val) const {
        _writer.StartObject();
        for (const auto& [k, v] : val.map) {
            _writer.Key(disk_string_to_string(k));
            _writer.String(disk_string_to_string(v));
        }
        _writer.EndObject();
    }
    void operator()(const sstables::run_identifier& val) const {
        _writer.AsString(val.id.uuid());
    }
    void operator()(const sstables::scylla_metadata::large_data_stats& val) const {
        _writer.StartObject();
        for (const auto& [k, v] : val.map) {
            _writer.Key(to_string(k));
            _writer.StartObject();
            _writer.Key("max_value");
            _writer.Uint64(v.max_value);
            _writer.Key("threshold");
            _writer.Uint64(v.threshold);
            _writer.Key("above_threshold");
            _writer.Uint(v.above_threshold);
            _writer.EndObject();
        }
        _writer.EndObject();
    }
    void operator()(const sstables::scylla_metadata::ext_timestamp_stats& val) const {
        _writer.StartObject();
        for (const auto& [k, v] : val.map) {
            _writer.Key(to_string(k));
            _writer.Int64(v);
        }
        _writer.EndObject();
    }
    template <typename Size>
    void operator()(const sstables::disk_string<Size>& val) const {
        _writer.String(disk_string_to_string(val));
    }

    template <sstables::scylla_metadata_type E, typename T>
    void operator()(const sstables::disk_tagged_union_member<sstables::scylla_metadata_type, E, T>& m) const {
        _writer.Key(to_string(E));
        (*this)(m.value);
    }

    void operator()(const sstables::scylla_metadata::sstable_identifier& sid) const {
        _writer.AsString(sid.value);
    }
};

void dump_scylla_metadata_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    json_writer writer;
    writer.StartStream();
    for (auto& sst : sstables) {
        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();
        auto m = sst->get_scylla_metadata();
        if (!m) {
            writer.EndObject();
            continue;
        }
        for (const auto& [k, v] : m->data.data) {
            std::visit(scylla_metadata_visitor(writer), v);
        }
        writer.EndObject();
    }
    writer.EndStream();
}

void validate_checksums_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map&) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    // Collect JSON output and print after validation is done, to prevent
    // interleaving with error messages from validation.
    std::stringstream json_output_stream;
    json_writer writer(json_output_stream);
    writer.StartStream();
    for (auto& sst : sstables) {
        const auto res = sstables::validate_checksums(sst, permit).get();
        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartObject();
        writer.Key("has_checksums");
        switch (res.status) {
        case validate_checksums_status::valid:
        case validate_checksums_status::invalid:
            writer.Bool(true);
            break;
        case validate_checksums_status::no_checksum:
            writer.Bool(false);
        }
        writer.Key("has_digest");
        writer.Bool(res.has_digest);
        switch (res.status) {
        case validate_checksums_status::valid:
            writer.Key("valid");
            writer.Bool(true);
            break;
        case validate_checksums_status::invalid:
            writer.Key("valid");
            writer.Bool(false);
            break;
        case validate_checksums_status::no_checksum:
        }
        writer.EndObject();
    }
    writer.EndStream();
    fmt::print(std::cout, "{}", json_output_stream.view());
}

void decompress_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map& vm) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }

    for (const auto& sst : sstables) {
        if (!sst->get_compression()) {
            sst_log.info("Sstable {} is not compressed, nothing to do", sst->get_filename());
            continue;
        }

        auto output_filename = fmt::format("{}.decompressed", sst->get_filename());

        auto ofile = open_file_dma(output_filename, open_flags::wo | open_flags::create).get();
        file_output_stream_options options;
        options.buffer_size = 4096;
        auto ostream = make_file_output_stream(std::move(ofile), options).get();
        auto close_ostream = defer([&ostream] { ostream.close().get(); });

        auto istream = sst->data_stream(0, sst->data_size(), permit, nullptr, nullptr).get();
        auto close_istream = defer([&istream] { istream.close().get(); });

        istream.consume([&] (temporary_buffer<char> buf) {
            return ostream.write(buf.get(), buf.size()).then([] {
                return consumption_result<char>(continue_consuming{});
            });
        }).get();
        ostream.flush().get();

        sst_log.info("Sstable {} decompressed into {}", sst->get_filename(), output_filename);
    }
}

void write_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& manager, const bpo::variables_map& vm) {
    static const std::vector<std::pair<std::string, mutation_fragment_stream_validation_level>> valid_validation_levels{
        {"none", mutation_fragment_stream_validation_level::none},
        {"partition_region", mutation_fragment_stream_validation_level::partition_region},
        {"token", mutation_fragment_stream_validation_level::token},
        {"partition_key", mutation_fragment_stream_validation_level::partition_key},
        {"clustering_key", mutation_fragment_stream_validation_level::clustering_key},
    };
    if (!sstables.empty()) {
        throw std::invalid_argument("write operation does not operate on input sstables");
    }
    if (!vm.count("input-file")) {
        throw std::invalid_argument("missing required option '--input-file'");
    }
    mutation_fragment_stream_validation_level validation_level;
    {
        const auto vl_name = vm["validation-level"].as<std::string>();
        auto vl_it = std::ranges::find_if(valid_validation_levels, [&vl_name] (const std::pair<std::string, mutation_fragment_stream_validation_level>& v) {
            return v.first == vl_name;
        });
        if (vl_it == valid_validation_levels.end()) {
            throw std::invalid_argument(fmt::format("invalid validation-level {}", vl_name));
        }
        validation_level = vl_it->second;
    }
    auto input_file = vm["input-file"].as<std::string>();
    auto output_dir = vm["output-dir"].as<std::string>();
    auto generation = sstables::generation_type(utils::UUID_gen::get_time_UUID());
    auto format = sstables::sstable_format_types::big;
    auto version = sstables::get_highest_sstable_version();

    {
        auto sst_name = sstables::sstable::filename(output_dir, schema->ks_name(), schema->cf_name(), version, generation, format, component_type::Data);
        if (file_exists(sst_name).get()) {
            throw std::invalid_argument(fmt::format("cannot create output sstable {}, file already exists", sst_name));
        }
    }

    auto ifile = open_file_dma(input_file, open_flags::ro).get();
    auto istream = make_file_input_stream(std::move(ifile));
    auto parser = tools::json_mutation_stream_parser{schema, permit, std::move(istream), sst_log};
    auto reader = make_generating_reader(schema, permit, std::move(parser));
    auto writer_cfg = manager.configure_writer("scylla-sstable");
    writer_cfg.validation_level = validation_level;
    auto local = data_dictionary::make_local_options(output_dir);
    auto sst = manager.make_sstable(schema, local, generation, sstables::sstable_state::normal, version, format);

    sst->write_components(std::move(reader), 1, schema, writer_cfg, encoding_stats{}).get();

    fmt::print(std::cout, "{}", generation);
}

void script_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& manager, const bpo::variables_map& vm) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }
    const auto merge = vm.count("merge");
    const auto partitions = partition_set(0, {}, decorated_key_equal(*schema));
    if (!vm.count("script-file")) {
        throw std::invalid_argument("missing required option '--script-file'");
    }
    const auto script_file = vm["script-file"].as<std::string>();
    auto script_params = vm["script-arg"].as<program_options::string_map>();
    auto consumer = make_lua_sstable_consumer(schema, permit, script_file, std::move(script_params)).get();
    consumer->consume_stream_start().get();
    consume_sstables(schema, permit, sstables, merge, false, [&, &consumer = *consumer] (mutation_reader& rd, sstables::sstable* sst) {
        return consume_reader(std::move(rd), consumer, sst, partitions, false);
    });
    consumer->consume_stream_end().get();
}

template <typename SstableConsumer>
void sstable_consumer_operation(schema_ptr schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sst_man, const bpo::variables_map& vm) {
    if (sstables.empty()) {
        throw std::invalid_argument("no sstables specified on the command line");
    }
    const auto merge = vm.count("merge");
    const auto no_skips = vm.count("no-skips");
    const auto partitions = get_partitions(schema, vm);
    const auto use_full_scan_reader = no_skips || partitions.empty();
    auto consumer = std::make_unique<SstableConsumer>(schema, permit, vm);
    consumer->consume_stream_start().get();
    consume_sstables(schema, permit, sstables, merge, use_full_scan_reader, [&, &consumer = *consumer] (mutation_reader& rd, sstables::sstable* sst) {
        return consume_reader(std::move(rd), consumer, sst, partitions, no_skips);
    });
    consumer->consume_stream_end().get();
}

void shard_of_with_vnodes(const std::vector<sstables::shared_sstable>& sstables,
                          sstables::sstables_manager& sstable_manager,
                          const bpo::variables_map& vm) {
    if (!vm.count("shards")) {
        throw std::invalid_argument("missing required option '--shards'");
    }
    unsigned shard_count = vm["shards"].as<unsigned>();
    unsigned ignore_msb_bits = vm["ignore-msb-bits"].as<unsigned>();

    json_writer writer;
    writer.StartStream();
    for (auto& sst : sstables) {
        // sst was loaded with the smp::count as its shard_count but that's not
        // necessarily identical to the "shards" specified in the command line.
        // reload the sst with the specified shard_count and ignore_msb_bits
        auto schema = schema_builder(sst->get_schema()).with_sharder(
            shard_count, ignore_msb_bits).build();
        auto new_sst = sstable_manager.make_sstable(
            schema,
            data_dictionary::make_local_options(fs::path(sst->get_storage().prefix())),
            sst->generation(),
            sstable_state::normal,
            sst->get_version());
        new_sst->load_owner_shards(schema->get_sharder()).get();

        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartArray();
        for (unsigned shard_id : new_sst->get_shards_for_this_sstable()) {
            writer.Uint(shard_id);
        }
        writer.EndArray();
    }
    writer.EndStream();
}

void shard_of_with_tablets(schema_ptr schema,
                           const std::vector<sstables::shared_sstable>& sstables,
                           std::filesystem::path data_dir_path,
                           sstables::sstables_manager& sstable_manager,
                           reader_permit permit) {
    auto& dbcfg = sstable_manager.config();
    auto tablets = tools::load_system_tablets(dbcfg, data_dir_path,
                                              schema->ks_name(), schema->cf_name(),
                                              permit).get();
    json_writer writer;
    writer.StartStream();
    for (auto& sst : sstables) {
        writer.Key(fmt::to_string(sst->get_filename()));
        writer.StartArray();

        // token ranges are distributed across tablets, so we just check for
        // the token range of each sstable
        auto first_token = sst->get_first_decorated_key().token();
        auto last_token = sst->get_last_decorated_key().token();
        // each tablet holds a range of (last_token(i-1), last_token(i)], where
        // "last_token" is the value of the column with the same name in
        // the "system.tablets" table, and "i" is the index of current tablet.
        auto tablet = tablets.upper_bound(first_token);
        if (auto last_tablet = tablets.lower_bound(last_token); tablet != last_tablet) {
            fmt::print(std::cerr, "sstable spans across multiple tablets\n");
        }
        if (tablet == tablets.end()) {
            fmt::print(std::cerr, "unable to find replica set for sstable: {}\n", sst->get_filename());
            continue;
        }
        auto& [token, replica_set] = *tablet;
        for (auto& replica : replica_set) {
            writer.StartObject();
            writer.Key("host");
            writer.String(fmt::to_string(replica.host));
            writer.Key("shard");
            writer.Uint(replica.shard);
            writer.EndObject();
        }

        writer.EndArray();
    }
    writer.EndStream();
}

void shard_of_operation(schema_ptr schema, reader_permit permit,
                        const std::vector<sstables::shared_sstable>& sstables,
                        sstables::sstables_manager& sstable_manager,
                        const bpo::variables_map& vm) {
    // uses "tablets" by default. as new scylla installations enable "tablet" by
    // default
    if (vm.count("tablets") && vm.count("vnodes")) {
        throw std::invalid_argument("--tablets and --vnodes are mutually exclusive");
    }
    if (!vm.count("tablets") && !vm.count("vnodes")) {
        throw std::invalid_argument("Please specify '--tablets' or '--vnodes'");
    }
    if (vm.count("vnodes")) {
        shard_of_with_vnodes(sstables, sstable_manager, vm);
    } else {
        auto info = extract_from_sstable_path(vm);
        shard_of_with_tablets(schema, sstables, info.data_dir_path, sstable_manager, permit);
    }
}

void print_query_results_text(const cql3::result& result) {
    const auto& metadata = result.get_metadata();
    const auto& column_metadata = metadata.get_names();

    struct column_values {
        size_t max_size{0};
        sstring header_format;
        sstring row_format;
        std::vector<sstring> values;

        void add(sstring value) {
            max_size = std::max(max_size, value.size());
            values.push_back(std::move(value));
        }
    };

    std::vector<column_values> columns;
    columns.resize(column_metadata.size());

    for (size_t i = 0; i < column_metadata.size(); ++i) {
        columns[i].add(column_metadata[i]->name->text());
    }

    for (const auto& row : result.result_set().rows()) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i]) {
                columns[i].add(column_metadata[i]->type->to_string(linearized(managed_bytes_view(*row[i]))));
            } else {
                columns[i].add("");
            }
        }
    }

    std::vector<sstring> separators(columns.size(), sstring());
    for (size_t i = 0; i < columns.size(); ++i) {
        auto& col_values = columns[i];
        col_values.header_format = seastar::format(" {{:<{}}} ", col_values.max_size);
        col_values.row_format = seastar::format(" {{:>{}}} ", col_values.max_size);
        for (size_t c = 0; c < col_values.max_size; ++c) {
            separators[i] += "-";
        }
    }

    for (size_t r = 0; r < result.result_set().rows().size() + 1; ++r) {
        std::vector<sstring> row;
        row.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& format = r == 0 ? columns[i].header_format : columns[i].row_format;
            row.push_back(fmt::format(fmt::runtime(std::string_view(format)), columns[i].values[r]));
        }
        fmt::print("{}\n", fmt::join(row, "|"));
        if (!r) {
            fmt::print("-{}-\n", fmt::join(separators, "-+-"));
        }
    }
}

void print_query_results_json(const cql3::result& result) {
    const auto& metadata = result.get_metadata();
    const auto& column_metadata = metadata.get_names();

    rjson::streaming_writer writer(std::cout);

    writer.StartArray();
    for (const auto& row : result.result_set().rows()) {
        writer.StartObject();
        for (size_t i = 0; i < row.size(); ++i) {
            writer.Key(column_metadata[i]->name->text());
            if (!row[i]) {
                writer.Null();
                continue;
            }
            const auto value = to_json_string(*column_metadata[i]->type, *row[i]);
            const auto type = to_json_type(*column_metadata[i]->type, *row[i]);
            writer.RawValue(value, type);
        }
        writer.EndObject();
    }
    writer.EndArray();
}

class query_operation_result_visitor : public cql_transport::messages::result_message::visitor {
    output_format _output_format;
private:
    [[noreturn]] void throw_on_unexpected_message(const char* message_kind) {
        throw std::runtime_error(std::format("unexpected result message, expected rows, got {}", message_kind));
    }
public:
    query_operation_result_visitor(output_format of) : _output_format(of) { }
    virtual void visit(const cql_transport::messages::result_message::void_message&) override { throw_on_unexpected_message("void_message"); }
    virtual void visit(const cql_transport::messages::result_message::set_keyspace&) override { throw_on_unexpected_message("set_keyspace"); }
    virtual void visit(const cql_transport::messages::result_message::prepared::cql&) override { throw_on_unexpected_message("prepared::cql"); }
    virtual void visit(const cql_transport::messages::result_message::schema_change&) override { throw_on_unexpected_message("schema_change"); }
    virtual void visit(const cql_transport::messages::result_message::bounce_to_shard&) override { throw_on_unexpected_message("bounce_to_shard"); }
    virtual void visit(const cql_transport::messages::result_message::exception&) override { throw_on_unexpected_message("exception"); }

    virtual void visit(const cql_transport::messages::result_message::rows& rows) override {
        const auto& result = rows.rs();
        switch (_output_format) {
            case output_format::text:
                print_query_results_text(result);
                break;
            case output_format::json:
                print_query_results_json(result);
                break;
        }
    }
};

void query_operation_validate_query(const sstring& query, std::string_view table_name, data_dictionary::database db) {
    std::vector<std::unique_ptr<cql3::statements::raw::parsed_statement>> raw_statements;
    try {
        raw_statements = cql3::query_processor::parse_statements(query, cql3::dialect{});
    } catch (...) {
        throw std::invalid_argument(seastar::format("failed to parse query: {}", std::current_exception()));
    }
    if (raw_statements.size() != 1) {
        throw std::invalid_argument(seastar::format("expected exactly 1 query, got {}", raw_statements.size()));
    }

    const auto raw_statement = raw_statements.front().get();

    if (auto cf_statement = dynamic_cast<cql3::statements::raw::cf_statement*>(raw_statement)) {
        if (!cf_statement->has_keyspace()) {
            throw std::invalid_argument("query must have keyspace and the keyspace has to be scylla_sstable");
        }
        if (cf_statement->keyspace() != "scylla_sstable") {
            throw std::invalid_argument(seastar::format("query must select from scylla_sstable keyspace, got {} instead", std::string_view(cf_statement->keyspace())));
        }
        if (cf_statement->column_family() != table_name) {
            throw std::invalid_argument(seastar::format("query must select from {} table, got {} instead", table_name, std::string_view(cf_statement->column_family())));
        }
    } else {
        throw std::invalid_argument("query must be a select query");
    }

    seastar::shared_ptr<cql3::cql_statement> statement;
    cql3::cql_stats cql_stats;

    try {
        auto prepared_statement = raw_statement->prepare(db, cql_stats);
        statement = prepared_statement->statement;
    } catch (...) {
        throw std::invalid_argument(seastar::format("failed to prepare query: {}", std::current_exception()));
    }

    if (dynamic_cast<cql3::statements::select_statement*>(statement.get()) == nullptr) {
        throw std::invalid_argument("query must be a select query");
    }
}

void query_operation(schema_ptr sstable_schema, reader_permit permit, const std::vector<sstables::shared_sstable>& sstables,
        sstables::sstables_manager& sstable_manager, const bpo::variables_map& vm) {
    if (vm.contains("query") && vm.contains("query-file")) {
        throw std::invalid_argument("cannot provide both -q|--query and --query-file");
    }

    if (smp::count > 1) {
        // Assuming smp==1 allows simplifying the code below.
        throw std::runtime_error("query operation cannot run with --smp > 1");
    }

    const auto format = get_output_format_from_options(vm, output_format::text);

    do_with_cql_env_noreentrant_in_thread([&] (cql_test_env& env) mutable -> future<> {
        auto& db = env.local_db();

        const auto keyspace_name = "scylla_sstable";
        co_await env.execute_cql(seastar::format("CREATE KEYSPACE {} WITH replication = {{'class': 'LocalStrategy'}}", keyspace_name));
        auto& keyspace = db.find_keyspace(keyspace_name);

        // Clone and modify the schema:
        // * Change keyspace name to scylla_sstable
        // * Generate a new ID
        // * Drop all properties
        //
        // This will help avoid conflicts when querying sstables of system-tables
        // and allows cql_test_env to work with a simple config (no EAR setup).
        auto builder = schema_builder(keyspace_name, sstable_schema->cf_name());
        for (const auto& col_kind : {column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column}) {
            for (const auto& col : sstable_schema->columns(col_kind)) {
                builder.with_column(col.name(), col.type, col_kind, col.view_virtual());

                // Register any user types, so they are known by the time we create the table.
                if (col.type->is_user_type()) {
                    keyspace.add_user_type(dynamic_pointer_cast<const user_type_impl>(col.type));
                }
            }
        }
        auto schema = builder.build();

        const auto table_name = schema->cf_name();

        schema_describe_helper describe_helper = replica::make_schema_describe_helper(schema, db.as_data_dictionary());

        const auto original_schema_description = sstable_schema->describe(describe_helper, cql3::describe_option::STMTS_AND_INTERNALS);
        const auto schema_description = schema->describe(describe_helper, cql3::describe_option::STMTS_AND_INTERNALS);

        const sstring original_create_statement = original_schema_description.create_statement.value().linearize();
        const sstring schema_create_statement = schema_description.create_statement.value().linearize();

        sst_log.debug("\noriginal schema:\n{}\nreplacement schema:\n{}\n\nNote: original keyspace name of {} was replaced with {}, original id of {} was replaced with {} and all properties were dropped!\n",
                original_create_statement,
                schema_create_statement,
                sstable_schema->ks_name(),
                keyspace_name,
                sstable_schema->id(),
                schema->id());

        co_await env.execute_cql(schema_create_statement);

        auto& table = db.find_column_family(keyspace_name, table_name);

        // We don't want to register the sstables with the table object,
        // to avoid any attempt to compact/split/merge/rewrite them.
        // Also, they were created with a foreign stable-manager (not part of
        // cql_test_env).
        // Use the virtual reader facility to isolate the sstables from
        // cql-test-env.
        table.set_virtual_reader(mutation_source([&] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr tr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) -> mutation_reader {
            return make_combined_reader(
                    schema,
                    permit,
                    sstables |
                            std::views::transform([&] (const sstables::shared_sstable& sst) {
                                    return sst->make_reader(schema, permit, range, slice, tr, fwd_sm, fwd_mr);
                            }) |
                            std::ranges::to<std::vector<mutation_reader>>(),
                    fwd_sm,
                    fwd_mr);
        }));

        sstring query;
        if (vm.contains("query")) {
            query = sstring(vm["query"].as<std::string>());
        } else if (vm.contains("query-file")) {
            auto file = co_await open_file_dma(vm["query-file"].as<std::string>(), open_flags::ro);
            auto fstream = make_file_input_stream(file);
            query = co_await util::read_entire_stream_contiguous(fstream);
        } else {
            query = seastar::format("SELECT * FROM {}.{} ", keyspace_name, table_name);
        }

        query_operation_validate_query(query, table_name, db.as_data_dictionary());

        sst_log.debug("query_operation(): running query {}", query);

        const auto result = co_await env.execute_cql(query);
        result->throw_if_exception();

        query_operation_result_visitor visitor{format};
        result->accept(visitor);
    }, {});
}

const std::vector<operation_option> global_options {
    typed_option<sstring>("schema-file", "schema.cql", "use the file containing the schema description as the schema source"),
    typed_option<sstring>("keyspace", "keyspace name"),
    typed_option<sstring>("table", "table name"),
    typed_option<>("system-schema", "the table designated by --keyspace and --table is a system table, use the hard-coded in-memory hard-coded schema as the schema source"),
    typed_option<>("schema-tables", "use the schema-tables as the schema source (see --scylla-yaml-file and --scylla-data-dir)"
            ", the name of the table can be provided with --keyspace and --table or left to auto-detect if possible"),
    typed_option<>("sstable-schema", "use the schema stored in the sstable itself as the schema source"),
    typed_option<sstring>("scylla-yaml-file", "path to the scylla.yaml config file, to obtain the data directory path from,"
            " this can be also provided directly with --scylla-data-dir"),
    typed_option<sstring>("scylla-data-dir", "path to the scylla data dir (usually /var/lib/scylla/data), to read the schema tables from"),
};

const std::vector<operation_option> global_positional_options{
    typed_option<std::vector<sstring>>("sstables", "sstable(s) to process for operations that have sstable inputs, can also be provided as positional arguments", -1),
};

const std::map<operation, operation_func> operations_with_func{
/* dump-data */
    {{"dump-data",
            "Dump content of sstable(s)",
R"(
Dump the content of the data component. This component contains the data-proper
of the sstable. This might produce a huge amount of output. In general the
human-readable output will be larger than the binary file.

It is possible to filter the data to print via the --partitions or
--partitions-file options. Both expect partition key values in the hexdump
format.

Supports both a text and JSON output. The text output uses the built-in scylla
printers, which are also used when logging mutation-related data structures.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-data
for more information on this operation, including the schema of the JSON output.
)",
            {
                    typed_option<std::vector<sstring>>("partition", "partition(s) to filter for, partitions are expected to be in the hex format"),
                    typed_option<sstring>("partitions-file", "file containing partition(s) to filter for, partitions are expected to be in the hex format"),
                    typed_option<>("merge", "merge all sstables into a single mutation fragment stream (use a combining reader over all sstable readers)"),
                    typed_option<>("no-skips", "don't use skips to skip to next partition when the partition filter rejects one, this is slower but works with corrupt index"),
                    typed_option<std::string>("output-format", "json", "the output-format, one of (text, json)"),
            }},
            sstable_consumer_operation<dumping_consumer>},
/* dump-index */
    {{"dump-index",
            "Dump content of sstable index(es)",
R"(
Dump the content of the index component. Contains the partition-index of the data
component. This is effectively a list of all the partitions in the sstable, with
their starting position in the data component and optionally a promoted index,
which contains a sampled index of the clustering rows in the partition.
Positions (both that of partition and that of rows) is valid for uncompressed
data.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-index
for more information on this operation, including the schema of the JSON output.
)"},
            dump_index_operation},
/* dump-compression-info */
    {{"dump-compression-info",
            "Dump content of sstable compression info(s)",
R"(
Dump the content of the compression-info component. Contains compression
parameters and maps positions into the uncompressed data to that into compressed
data. Note that compression happens over chunks with configurable size, so to
get data at a position in the middle of a compressed chunk, the entire chunk has
to be decompressed.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-compression-info
for more information on this operation, including the schema of the JSON output.
)"},
            dump_compression_info_operation},
/* dump-summary */
    {{"dump-summary",
            "Dump content of sstable summary(es)",
R"(
Dump the content of the summary component. The summary is a sampled index of the
content of the index-component. An index of the index. Sampling rate is chosen
such that this file is small enough to be kept in memory even for very large
sstables.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-summary
for more information on this operation, including the schema of the JSON output.

)"},
            dump_summary_operation},
/* dump-statistics */
    {{"dump-statistics",
            "Dump content of sstable statistics(s)",
R"(
Dump the content of the statistics component. Contains various metadata about the
data component. In the sstable 3 format, this component is critical for parsing
the data component.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-statistics
for more information on this operation, including the schema of the JSON output.
)"},
            dump_statistics_operation},
/* dump-scylla-metadata */
    {{"dump-scylla-metadata",
            "Dump content of sstable scylla metadata(s)",
R"(
Dump the content of the scylla-metadata component. Contains scylla-specific
metadata about the data component. This component won't be present in sstables
produced by Apache Cassandra.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#dump-scylla-metadata
for more information on this operation, including the schema of the JSON output.
)"},
            dump_scylla_metadata_operation},
/* validate */
    {{"validate",
            "Validate the sstable(s), same as scrub in validate mode",
R"(
Validates the content of the sstable on the mutation-fragment level, see
https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#sstable-content
for more details.
Any parsing errors will also be detected, but after successful parsing the
validation will happen on the fragment level.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#validate
for more information on this operation.
)"},
            validate_operation},
/* scrub */
    {{"scrub",
            "Scrub the sstable(s), in the specified mode",
R"(
Read and re-write the sstable, getting rid of or fixing broken parts, depending
on the selected mode.
Output sstables are written to the directory specified via `--output-directory`.
They will be written with the BIG format and the highest supported sstable
format, with generations chosen by scylla-sstable. Generations are chosen such
that they are unique between the sstables written by the current scrub.
The output directory is expected to be empty, if it isn't scylla-sstable will
abort the scrub. This can be overridden by the
`--unsafe-accept-nonempty-output-dir` command line flag, but note that scrub will
be aborted if an sstable cannot be written because its generation clashes with
pre-existing sstables in the directory.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#scrub
for more information on this operation, including what the different modes do.
)",
            {
                    typed_option<std::string>("scrub-mode", "scrub mode to use, one of (abort, skip, segregate, validate)"),
                    typed_option<std::string>("output-dir", ".", "directory to place the scrubbed sstables to"),
                    typed_option<>("unsafe-accept-nonempty-output-dir", "allow the operation to write into a non-empty output directory, acknowledging the risk that this may result in sstable clash"),
            }},
            scrub_operation},
/* validate-checksums */
    {{"validate-checksums",
            "Validate the checksums of the sstable(s)",
R"(
Validate both the whole-file and the per-chunk checksums of the data component.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#validate-checksums
for more information on this operation.
)"},
            validate_checksums_operation},
/* decompress */
    {{"decompress",
            "Decompress sstable(s)",
R"(
Decompress Data.db if compressed. Noop if not compressed. The decompressed data
is written to Data.db.decompressed. E.g. for an sstable:

    md-12311-big-Data.db

the output will be:

    md-12311-big-Data.db.decompressed
)"},
            decompress_operation},
/* write */
    {{"write",
            "Write an sstable",
R"(
Write an sstable based on a JSON representation of the content. The JSON
representation has to have the same schema as that of a single sstable
from the output of the dump-data operation (corresponding to the $SSTABLE
symbol).

Note that "write" doesn't yet support all the features of the scylladb
storage engine. The following is not supported:
* Counters.
* Non-strictly atomic cells, this includes frozen multi-cell types like
  collections, tuples and UDTs.

Parsing uses a streaming json parser, it is safe to pass in input-files
of any size.

The output sstable will use the BIG format, the highest supported sstable
format and random UUID generation (printed to stdout). By default it is
placed in the local directory, can be changed with --output-dir. If the
output sstable clashes with an existing sstable, the write will fail.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#write
for more information on this operation, including the schema of the JSON input.
)",
            {
                    typed_option<std::string>("input-file", "the file containing the input"),
                    typed_option<std::string>("output-dir", ".", "directory to place the output sstable(s) to"),
                    typed_option<std::string>("validation-level", "clustering_key", "degree of validation on the output, one of (partition_region, token, partition_key, clustering_key)"),
            }},
            write_operation},
/* script */
    {{"script",
            "Run a script on content of an sstable",
R"(
Read the sstable(s) and pass the resulting fragment stream to the script
specified by `--script-file`. Currently only Lua scripts are supported.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#script
for more information on this operation, including the API documentation.
)",
            {
                typed_option<>("merge", "merge all sstables into a single mutation fragment stream (use a combining reader over all sstable readers)"),
                typed_option<std::string>("script-file", "script file to load and execute"),
                typed_option<program_options::string_map>("script-arg", {}, "parameter(s) for the script"),
            }},
            script_operation},
/* shard-of */
    {{"shard-of",
            "Print out the shard which 'owns' the sstable",
            "Print out the intersection(s) of the shard-ranges and the partition ranges",
            {
                typed_option<unsigned>("shards", "the number of shards the source scylla instance has"),
                typed_option<unsigned>("ignore-msb-bits", 12u, "'murmur3_partitioner_ignore_msb_bits' set by scylla.yaml"),
                typed_option<>("tablets", "assume that tokens are distributed with tablets"),
                typed_option<>("vnodes", "assume that tokens are distributed with vnodes"),
            }},
            shard_of_operation},
/* query */
    {{"query",
            "Run a query on the content of the sstable(s)",
R"(
The query is run on the combined content of all input sstables.

By default, the following query is run: SELECT * FROM $table.
Custom queries can be provided either via the --query (on the command-line) or
via --query-file (in a file).
When writing queries by hand, there are some things to keep in in mind:
* The keyspace of the table is changed to scylla_sstable. This is to avoid any
  collisions in case the sstables belong to a system keyspace.
* If the schema is read from the sstable itself, partition key columns will be
  $pk0..$pkN and clustering key columns will be $ck0..$ckN. This is because the
  in-sstable schema doesn't contain key column names.
  If the table-name wasn't provided with --table, the table name will be
  my_table.

Chose the output format with --output-format. Text is similar to CQLSH text
output, while json is similar to SELECT JSON output.
Default output format is text.

This operation needs a temporary directory to write files to -- as it sets up a
cql_test_env. This temporary directory will have a size of a couple of megabytes.
By default it will create this in /tmp, this can be changed with the `TEMPDIR`
environment variable. This temporary directory is removed on exit.

See https://docs.scylladb.com/operating-scylla/admin-tools/scylla-sstable#query
for more information on this operation, including usage examples.
)",
            {
                typed_option<std::string>("query,q", "execute the query provided on the command-line"),
                typed_option<std::string>("query-file", "execute the query from the file, the file is expected to contain a single query"),
                typed_option<std::string>("output-format", "text", "the output-format, one of (text, json)"),
            }},
            query_operation},
};

} // anonymous namespace

namespace tools {

int scylla_sstable_main(int argc, char** argv) {
    constexpr auto description_template =
R"(scylla-{} - a multifunctional command-line tool to examine the content of sstables.

Usage: scylla sstable {{operation}} [--option1] [--option2] ... [{{sstable_path1}}] [{{sstable_path2}}] ...

Contains various tools (operations) to examine or produce sstables.

# Operations

The operation to execute is the mandatory, first positional argument.
Operations write their output to stdout, or file(s). Logs are written to
stderr, with a logger called {}.

The supported operations are:
{}

For more details on an operation, run: scylla sstable {{operation}} --help

# Sstables

Operations that read sstables, take the sstables to-be-examined
as positional command line arguments. Sstables will be processed by the
selected operation one-by-one. Any number of sstables can be passed but
mind the open file limits and the memory consumption. Always pass the
path to the data component of the sstables (*-Data.db) even if you want
to examine another component.
NOTE: currently you have to prefix dir local paths with `./`.

# Schema

To be able to interpret the sstables, their schema is required. There
are multiple ways to obtain the schema:
* system schema
* schema file
* schema tables
* schema from the sstable itself

## System schema

If the examined sstables belong to a system table, whose schema is
hardcoded in ScyllaDB (and thus known), it is enough to provide just
the name of said table via the --keyspace and --table command line
parameters. Alternatively, the keyspace and tablename can be deduced from
the path of the sstable, if the sstable is in its natural directory, in
ScyllaDB's data dir.
The table has to be from one of the following system keyspaces:
* system
* system_schema
* system_distributed
* system_distributed_everywhere

## Schema file

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

## schema tables

The schema can be read from the schema tables located on disk.
The path to the data directory can be provided via multiple ways:
* autodetected from the sstable's path -- if the sstable(s) are located in their
  native table directory.
* --scylla-data-dir
* --scylla-yaml-file -- the scylla.yaml contains the data directory path(s).
* The SCYLLA_HOME and/or SCYLLA_CONF environment variables. These allow locating
  the scylla.yaml file.

This is the most complete method, which should always result in a complete schema.

## schema from the sstable itself

The sstable stores a basic schema in the statistics component, which can be used
for most operations on the sstable. This schema should be enough for all dump-
operations.

# Examples

Dump the content of the sstable:
$ scylla sstable dump-data /path/to/md-123456-big-Data.db

Dump the content of the two sstable(s) as a unified stream:
$ scylla sstable dump-data --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db

Validate the specified sstables:
$ scylla sstable validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db


)";

    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{
            .name = app_name,
            .description = seastar::format(description_template, app_name, sst_log.name(), fmt::join(operations | std::views::transform([] (const auto& op) {
                return seastar::format("* {}: {}", op.name(), op.summary());
            }), "\n")),
            .logger_name = sst_log.name(),
            .lsa_segment_pool_backend_size_mb = 100,
            .operations = std::move(operations),
            .global_options = &global_options,
            .global_positional_options = &global_positional_options,
            .db_cfg_ext = db_config_and_extensions()
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [&app] (const operation& operation, const bpo::variables_map& app_config) {
        schema_ptr schema;
        std::optional<schema_with_source> schema_with_source;

        auto& dbcfg = *app.cfg().db_cfg_ext->db_cfg;

        sstring scylla_yaml_path;
        sstring scylla_yaml_path_source;

        if (app_config.count("scylla-yaml-file")) {
            scylla_yaml_path = app_config["scylla-yaml-file"].as<sstring>();
            scylla_yaml_path_source = "user provided";
        } else if (std::getenv("SCYLLA_CONF") || std::getenv("SCYLLA_HOME")) {
            scylla_yaml_path = db::config::get_conf_sub("scylla.yaml").string();
            scylla_yaml_path_source = "environment provided";
        } else {
            scylla_yaml_path = db::config::get_conf_sub("scylla.yaml").string();
            scylla_yaml_path_source = "dev default";

            // On production machines, the default of ./conf/scylla.yaml will not
            // work, try /etc/scylla/scylla.yaml instead.
            if (!file_exists(scylla_yaml_path).get()) {
                scylla_yaml_path = "/etc/scylla/scylla.yaml";
                scylla_yaml_path_source = "prod default";
            }
        }

        if (file_exists(scylla_yaml_path).get()) {
            dbcfg.read_from_file(scylla_yaml_path, [] (const sstring& opt, const sstring& msg, std::optional<::utils::config_file::value_status> status) {
                sst_log.debug("error processing configuration item: {} : {}", msg, opt);
            }).get();
            dbcfg.setup_directories();
            sst_log.debug("Successfully read scylla.yaml from {} location of {}", scylla_yaml_path_source, scylla_yaml_path);
        } else {
            dbcfg.experimental_features.set(db::experimental_features_t::all());
            sst_log.debug("Failed to read scylla.yaml from {} location of {}, some functionality may be unavailable", scylla_yaml_path_source, scylla_yaml_path);
        }

        dbcfg.enable_cache(false);
        dbcfg.volatile_system_keyspace_for_testing(true);

        // Override whatever value the option has. Setting this to `true` is correct because
        // schema loader doesn't attempt to create any keyspace and doesn't go through any
        // validation code; there is no topology either. Thanks to that, we won't run into
        // any problems due to enforcing RF-rack-valid keyspaces.
        //
        // On the other hand, we gain access to the code hidden behind the option
        dbcfg.rf_rack_valid_keyspaces(true, ::utils::config_file::config_source::CommandLine);

        {
            unsigned schema_sources = 0;
            schema_sources += !app_config["schema-file"].defaulted();
            schema_sources += app_config.contains("system-schema");
            schema_sources += app_config.contains("schema-tables");
            schema_sources += app_config.contains("sstable-schema");

            if (!schema_sources) {
                sst_log.debug("No user-provided schema source, attempting to auto-detect it");
                schema_with_source = try_load_schema_autodetect(app_config, dbcfg);
            } else if (schema_sources == 1) {
                sst_log.debug("Single schema source provided");
                schema_with_source = try_load_schema_from_user_provided_source(app_config, dbcfg);
            } else {
                fmt::print(std::cerr, "Multiple schema sources provided, please provide exactly one of: --schema-file, --system-schema, --schema-tables or --sstable-schema (with the accompanying --keyspace and --table if necessary)\n");
            }
        }
        if (schema_with_source) {
            schema = std::move(schema_with_source->schema);
            sst_log.debug("Succesfully loaded schema from {}{}, obtained from {}",
                    schema_with_source->source,
                    schema_with_source->path ? seastar::format(" ({})", schema_with_source->path->native()) : "",
                    schema_with_source->obtained_from);
            sst_log.trace("Loaded schema: {}", schema);
        } else {
            return 1;
        }

        gms::feature_service feature_service({get_disabled_features_from_db_config(dbcfg)});
        auto scf = make_sstable_compressor_factory_for_tests_in_thread();
        cache_tracker tracker;
        sstables::directory_semaphore dir_sem(1);
        abort_source abort;

        sstables::storage_manager::config stm_cfg;
        stm_cfg.s3_clients_memory = 100_MiB;
        stm_cfg.skip_metrics_registration = true;
        sharded<sstables::storage_manager> sstm;
        sstm.start(std::ref(dbcfg), stm_cfg).get();
        auto stop_sstm = defer([&sstm] { sstm.stop().get(); });

        db::nop_large_data_handler large_data_handler;
        db::nop_corrupt_data_handler corrupt_data_handler(db::corrupt_data_handler::register_metrics::no);

        sstables::sstables_manager sst_man(
            "scylla_sstable",
            large_data_handler,
            corrupt_data_handler,
            dbcfg,
            feature_service,
            tracker,
            1_GiB,
            dir_sem,
            [host_id = locator::host_id::create_random_id()] { return host_id; },
            *scf,
            abort,
            current_scheduling_group(),
            &sstm.local());
        auto close_sst_man = deferred_close(sst_man);

        std::vector<sstables::shared_sstable> sstables;
        if (app_config.count("sstables")) {
            const auto sstable_names = app_config["sstables"].as<std::vector<sstring>>();
            if (std::set(sstable_names.begin(), sstable_names.end()).size() != sstable_names.size()) {
                fmt::print(std::cerr, "error processing arguments: duplicate sstable arguments found\n");
                return 1;
            }
            try {
                sstables = load_sstables(schema, sst_man, sstable_names);
            } catch (...) {
                fmt::print(std::cerr, "error loading sstables: {}\n", std::current_exception());
                return 1;
            }
        }

        reader_concurrency_semaphore rcs_sem(reader_concurrency_semaphore::no_limits{}, app_name, reader_concurrency_semaphore::register_metrics::no);
        auto stop_semaphore = deferred_stop(rcs_sem);

        const auto permit = rcs_sem.make_tracking_only_permit(schema, app_name, db::no_timeout, {});

        try {
            operations_with_func.at(operation)(schema, permit, sstables, sst_man, app_config);
        } catch (std::invalid_argument& e) {
            fmt::print(std::cerr, "error processing arguments: {}\n", e.what());
            return 1;
        } catch (...) {
            fmt::print(std::cerr, "error running operation: {}\n", std::current_exception());
            return 2;
        }

        return 0;
    });
}

} // namespace tools
