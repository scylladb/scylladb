/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "db/extensions.hh"
#include "db/config.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "sstables/sstables.hh"
#include "cdc/cdc_extension.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "transport/messages/result_message.hh"
#include "utils/overloaded_functor.hh"

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

SEASTAR_TEST_CASE(simple_schema_extension) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<dummy_ext>("los_lobos");

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
    }, ::make_shared<db::config>(ext));
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
    }, ::make_shared<db::config>(ext));
}

SEASTAR_TEST_CASE(cdc_schema_extension) {
    auto ext = std::make_shared<db::extensions>();
    // Extensions have to be registered here - config needs to have them before construction of test env.
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    auto cfg = ::make_shared<db::config>(ext);

    return do_with_cql_env([] (cql_test_env& e) {
        auto assert_ext_correctness = [] (cql_test_env& e, cdc::cdc_extension expected_ext) {
            auto& actual_ext = e.local_db().find_column_family("ks", "cf").schema()->extensions().at("cdc");
            BOOST_REQUIRE(!actual_ext->is_placeholder());
            BOOST_CHECK_EQUAL(actual_ext->serialize(), expected_ext.serialize());
        };

        return e.execute_cql("CREATE TABLE cf (id int PRIMARY KEY, value int) WITH cdc = {'enabled':'true','postimage':'true','preimage':'true','ttl':'6789'};")
        .discard_result().then([&] {
            assert_ext_correctness(e, cdc::cdc_extension{{{"enabled","true"},{"postimage","true"},{"preimage","true"},{"ttl","6789"}}});
        }).then([&] {
            return e.execute_cql("ALTER TABLE cf WITH cdc = {'enabled':'true','postimage':'false','preimage':'false','ttl':'1234'};")
            .discard_result().then([&] {
                assert_ext_correctness(e, cdc::cdc_extension{{{"enabled","true"},{"postimage","false"},{"preimage","false"},{"ttl","1234"}}});
            });
        });
    }, cfg);
}

SEASTAR_TEST_CASE(paxos_grace_seconds_extension) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<db::paxos_grace_seconds_extension>(db::paxos_grace_seconds_extension::NAME);
    auto cfg = ::make_shared<db::config>(ext);

    return do_with_cql_env([] (cql_test_env& e) {
        // Verify that paxos_grace_seconds extensions gets recognized properly
        auto f = e.execute_cql("CREATE TABLE cf (pk int PRIMARY KEY) WITH paxos_grace_seconds=1000")
            .discard_result().then([&e] {
                auto& actual_ext = e.local_db().find_column_family("ks", "cf").schema()->extensions().at("paxos_grace_seconds");
                BOOST_REQUIRE(!actual_ext->is_placeholder());
                BOOST_CHECK_EQUAL(actual_ext->serialize(), db::paxos_grace_seconds_extension(1000).serialize());
            });
        // Check that paxos_grace_seconds option correctly sets TTL on system.paxos entries for the test table
        f = f.then([&e] {
            return e.prepare("INSERT INTO cf (pk) VALUES (?) IF NOT EXISTS");
        }).then([&e] (const cql3::prepared_cache_key_type& prep_id) {
            return e.execute_prepared(prep_id, {cql3::raw_value::make_value(int32_type->decompose(1))});
        }).discard_result().then([&e] {
            return e.execute_cql("SELECT row_key, TTL(promise) FROM system.paxos")
                .then([&e] (::shared_ptr<cql_transport::messages::result_message> msg) {
                    assert_that(msg)
                        .is_rows()
                        .with_size(1);
                    auto res = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                    auto rows = res->rs().result_set().rows();
                    const auto& row = rows.at(0);
                    BOOST_REQUIRE(int32_type->equal(*row[0], int32_type->decompose(1)));
                    // Check the TTL value using "less-than-equal" predicate to account for accidental stalls during testing 
                    BOOST_REQUIRE(int32_type->compare(*row[1], int32_type->decompose(1000)) <= 0);
                });
        });

        return f;
    }, cfg);
}

SEASTAR_TEST_CASE(test_extension_remove) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension("knas", [](db::extensions::schema_ext_config args) {
        return std::visit(overloaded_functor {
            [](const std::map<sstring, sstring>& map) -> shared_ptr<schema_extension> {
                if (map.at("krakel") == "none") {
                    return {};
                }
                return ::make_shared<dummy_ext>(map);
            },
            [](const sstring&) -> shared_ptr<schema_extension> {
                throw std::runtime_error("should not reach");
            },
            [](const bytes& b) -> shared_ptr<schema_extension> {
                return ::make_shared<dummy_ext>(b);
            }
        }, args);
    });

    /**
     * Ensure we can create a table with the extension, and that it (and its data) is preserved
     * by the schema creation (which serializes and deserializes mutations)
     */
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create table cf (id int primary key, value int) with knas = { 'krakel' : 'spektakel', 'ninja' : 'mission' };").discard_result().then([&e] {
            auto& db = e.local_db();
            auto& cf = db.find_column_family("ks", "cf");
            auto s = cf.schema();
            auto& ext = s->extensions().at("knas");

            BOOST_REQUIRE(!ext->is_placeholder());
            BOOST_CHECK_EQUAL(ext->serialize(), dummy_ext(std::map<sstring, sstring>{{"krakel", "spektakel"},{"ninja", "mission"}}).serialize());

            // now remove it
            return e.execute_cql("alter table cf with knas = { 'krakel' : 'none' };").discard_result().then([&e] {
                auto& db = e.local_db();
                auto& cf = db.find_column_family("ks", "cf");
                auto s = cf.schema();
                auto i = s->extensions().find("knas");
                // should be gone.
                BOOST_REQUIRE(i == s->extensions().end());
            });
        });
    }, ::make_shared<db::config>(ext));
}
