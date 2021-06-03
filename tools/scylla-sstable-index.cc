/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <filesystem>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>
#include <seastar/util/closeable.hh>

#include "schema_builder.hh"
#include "db/marshal/type_parser.hh"
#include "db/large_data_handler.hh"
#include "db/config.hh"
#include "gms/feature_service.hh"
#include "sstables/index_reader.hh"
#include "sstables/open_info.hh"
#include "sstables/sstables_manager.hh"

using namespace seastar;

namespace {

const auto app_name = "scylla-sstable-index";

logging::logger silog(app_name);

db::nop_large_data_handler large_data_handler;
reader_concurrency_semaphore rcs_sem(reader_concurrency_semaphore::no_limits{}, app_name);

schema_ptr make_primary_key_schema_from_types(std::vector<data_type> types) {
    schema_builder builder("ks", "cf");
    unsigned i = 0;
    for (const auto& type : types) {
        builder.with_column(to_bytes(format("pk{}", i++)), type, column_kind::partition_key);
    }
    builder.with_column("dummy", utf8_type, column_kind::regular_column);
    return builder.build();
}

void list_partitions(const schema& s, sstables::index_reader& idx_reader) {
    while (!idx_reader.eof()) {
        idx_reader.read_partition_data().get();
        const auto& idx_entry = idx_reader.current_partition_entry();

        auto pkey = idx_entry.get_key().to_partition_key(s);
        fmt::print("{}: {} ({})\n", idx_entry.position(), pkey.with_schema(s), pkey);

        idx_reader.advance_to_next_partition().get();
    }
    silog.debug("done with index");
}

}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app_template::config app_cfg;
    app_cfg.name = app_name;
    app_cfg.description =
R"(scylla-sstable-index - a command-line tool to examine sstable indexes.

Lists all partitions contained in the sstable index. As all partitions
in an sstable have a corresponding entry in the sstable index, this in
effect lists all partitions contained in the sstable.

The printout has the following format:
$pos: $human_readable_value (pk{$raw_hex_value})

Where:
* $pos: the position of the partition in the (decompressed) data file
* $human_readable_value: the human readable partition key
* $raw_hex_value: the raw hexadecimal value of the binary representation
  of the partition key

For now the tool requires the types making up the partition key to be
specified on the command line, using the `--type|-t` command line
argument, using the Cassandra type class name notation for types.
E.g. if you have the following schema:

    CREATE TABLE my_keyspace.my_table (
        id1 int,
        id2 text,
        value text,
        PRIMARY KEY ((id1, id2))
    );

The tool has to be invoked with:

    ./scylla-sstable-index --type=Int32Type --type=UTF8Type $sstable_path

Note that for types, the Cassandra type class name has to be used. The
type class name can be used in the short form, i.e. without the
`org.apache.cassandra.db.marshal.` prefix.
See https://github.com/scylladb/scylla/blob/master/docs/design-notes/cql3-type-mapping.md
for a mapping of cql3 types to Cassandra type class names.

Types that takes other types as parameters, like collections can be written as:

    frozen<map<int, text>> -> FrozenType(MapType(Int32Type,UTF8Type))

Don't forget to quote such expressions when passing on the command line.

Note: UDT is not supported for now.
)";

    app_template app(std::move(app_cfg));

    app.add_options()
        ("type,t", bpo::value<std::vector<sstring>>(), "the types making up the partition key, if the partition key is compound, list all types"
                " in it in order; types have to be specified in Cassandra class notation, see description for more details")
        ;

    app.add_positional_options({
        {"sstable", bpo::value<std::string>(), "sstable index file to examine, can be given as a positional argument", 1}
    });

    //FIXME: this exposes all core options, which we are not interested in.
    return app.run(argc, argv, [&app] {
        return async([&app] {
            if (!app.configuration().contains("sstable")) {
                throw std::invalid_argument("error: missing mandatory positional argument 'sstable'");
            }
            const auto sst_path = std::filesystem::path(app.configuration()["sstable"].as<std::string>());

            if (const auto ftype_opt = file_type(sst_path.c_str(), follow_symlink::yes).get()) {
                if (!ftype_opt) {
                    throw std::invalid_argument(fmt::format("error: failed to determine type of file pointed to by provided sstable path {}", sst_path.c_str()));
                }
                if (*ftype_opt != directory_entry_type::regular) {
                    throw std::invalid_argument(fmt::format("error: file pointed to by provided sstable path {} is not a regular file", sst_path.c_str()));
                }
            }

            const auto dir_path = std::filesystem::path(sst_path).remove_filename();
            const auto sst_filename = sst_path.filename();

            const auto primary_key_schema = [&] {
                if (app.configuration().contains("type")) {
                    auto types = boost::copy_range<std::vector<data_type>>(app.configuration()["type"].as<std::vector<sstring>>()
                            | boost::adaptors::transformed([] (const sstring_view type_name) { return db::marshal::type_parser::parse(type_name); }));
                    return make_primary_key_schema_from_types(std::move(types));
                } else { // infer data-dir from sstable path
                    throw std::invalid_argument("error: missing mandatory argument 'type'");
                }
            }();

            silog.debug("dir={}, filename={}", dir_path.c_str(), sst_filename);

            db::config dbcfg;
            gms::feature_service feature_service(gms::feature_config_from_db_config(dbcfg));
            sstables::sstables_manager sst_man(large_data_handler, dbcfg, feature_service);
            auto close_sst_man = deferred_close(sst_man);

            auto ed = sstables::entry_descriptor::make_descriptor(dir_path.c_str(), sst_filename.c_str());
            auto sst = sst_man.make_sstable(primary_key_schema, dir_path.c_str(), ed.generation, ed.version, ed.format);

            sst->load().get();

            {
                sstables::index_reader idx_reader(sst, rcs_sem.make_permit(primary_key_schema.get(), "idx"), default_priority_class(), {});

                list_partitions(*primary_key_schema, idx_reader);

                idx_reader.close().get();
            }

            sst = {};
        });
    });
}
