/*
 * Copyright (C) 2018 ScyllaDB
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

#include <variant>
#include <stdexcept>

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "db/extensions.hh"
#include "db/config.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "sstables/sstables.hh"

SEASTAR_TEST_CASE(simple_schema_extension) {
    class dummy_ext : public schema_extension {
    public:
        dummy_ext(bytes b) : _bytes(b) {}
        dummy_ext(const std::map<sstring, sstring>& map) : _bytes(ser::serialize_to_buffer<bytes>(map)) {}
        dummy_ext(const sstring&) {
            throw std::runtime_error("should not reach");
        }
        bytes serialize() const override {
            return _bytes;
        }
    private:
        bytes _bytes;
    };

    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension("los_lobos", [](db::extensions::schema_ext_config cfg) {
        return std::visit([](auto v) { return ::make_shared<dummy_ext>(v); }, cfg);
    });

    /**
     * Ensure we can create a table with the extension, and that it (and its data) is preserved
     * by the schema creation (which serializes and deserializes mutations)
     */
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (id int primary key, value int) with los_lobos = { 'king of' : 'swing', 'ninja' : 'mission' };").discard_result().then([&e] {
            auto& db = e.local_db();
            auto& cf = db.find_column_family("ks", "cf");
            auto s = cf.schema();
            auto& ext = s->extensions().at("los_lobos");

            BOOST_REQUIRE(!ext->is_placeholder());
            BOOST_CHECK_EQUAL(ext->serialize(), dummy_ext(std::map<sstring, sstring>{{"king of", "swing"},{"ninja", "mission"}}).serialize());
        });
    }, db::config(ext));
}

using namespace sstables;

SEASTAR_TEST_CASE(simple_sstable_extension) {
    static size_t counter = 0;

    class dummy_ext : public file_io_extension {
    public:
        future<file> wrap_file(sstable& t, component_type type, file, open_flags flags) override {
            if (type == component_type::Data && t.get_schema()->cf_name() == "cf") {
                ++counter;
            }
            return make_ready_future<file>();
        }
    };

    auto ext = std::make_shared<db::extensions>();
    ext->add_sstable_file_io_extension("los_lobos", std::make_unique<dummy_ext>());

    counter = 0;
    /**
     * Ensure the extension is invoked on read/write of sstables, esp. data.
     */
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (id int primary key, value int);").discard_result().then([&e] {
            BOOST_REQUIRE(counter == 0);
            // minimal data
            return e.execute_cql("insert into ks.cf (id, value) values (1, 100);").discard_result().then([&e] {
                // flush all shards
                return e.db().invoke_on_all([](database& db) {
                    auto& cf = db.find_column_family("ks", "cf");
                    return cf.flush();
                }).then([] {
                    BOOST_REQUIRE(counter > 1);
                });
            });
        });
    }, db::config(ext));
}

