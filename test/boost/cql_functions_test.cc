/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <seastar/net/inet_address.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "types/vector.hh"
#include "utils/assert.hh"
#include "utils/big_decimal.hh"
#include "utils/unique_view.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "schema/schema_builder.hh"

BOOST_AUTO_TEST_SUITE(cql_functions_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_functions) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.create_table([](std::string_view ks_name) {
            // CQL: create table cf (p1 varchar primary key, u uuid, tu timeuuid);
            return *schema_builder(ks_name, "cf")
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("c1", int32_type, column_kind::clustering_key)
                    .with_column("tu", timeuuid_type)
                    .build();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 1, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 2, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, c1, tu) values ('key1', 3, now());").discard_result();
        }).then([&e] {
            return e.execute_cql("select tu from cf where p1 in ('key1');");
        }).then([] (shared_ptr<cql_transport::messages::result_message> msg) {
            using namespace cql_transport::messages;
            struct validator : result_message::visitor {
                std::vector<bytes_opt> res;
                virtual void visit(const result_message::void_message&) override { throw "bad"; }
                virtual void visit(const result_message::set_keyspace&) override { throw "bad"; }
                virtual void visit(const result_message::prepared::cql&) override { throw "bad"; }
                virtual void visit(const result_message::schema_change&) override { throw "bad"; }
                virtual void visit(const result_message::rows& rows) override {
                    const auto& rs = rows.rs().result_set();
                    BOOST_REQUIRE_EQUAL(rs.rows().size(), 3);
                    for (auto&& rw : rs.rows()) {
                        BOOST_REQUIRE_EQUAL(rw.size(), 1);
                        res.push_back(to_bytes_opt(rw[0]));
                    }
                }
                virtual void visit(const result_message::bounce_to_shard& rows) override { throw "bad"; }
                virtual void visit(const result_message::exception&) override { throw "bad"; }
            };
            validator v;
            msg->accept(v);
            // No boost::adaptors::sorted
            std::ranges::sort(v.res);
            BOOST_REQUIRE_EQUAL(std::ranges::distance(v.res | utils::views::unique), 3);
        }).then([&] {
            return e.execute_cql("select sum(c1), count(c1) from cf where p1 = 'key1';");
        }).then([] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(6)},
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(*) from cf where p1 = 'key1';");
        }).then([] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {long_type->decompose(3L)},
                 });
        }).then([&] {
            return e.execute_cql("select count(1) from cf where p1 = 'key1';");
        }).then([] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {long_type->decompose(3L)},
                 });
        }).then([&e] {
            // Inane, casting to own type, but couldn't find more interesting example
            return e.execute_cql("insert into cf (p1, c1) values ((text)'key2', 7);").discard_result();
        }).then([&e] {
            return e.execute_cql("select c1 from cf where p1 = 'key2';");
        }).then([] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(7)},
                 });
        });
    });
}

struct aggregate_function_test {
    private:
    cql_test_env& _e;
    shared_ptr<const abstract_type> _column_type;
    std::vector<data_value> _sorted_values;

    sstring table_name() {
        sstring tbl_name = "cf_" + _column_type->cql3_type_name();
        // Substitute troublesome characters from `cql3_type_name()':
        std::for_each(tbl_name.begin(), tbl_name.end(), [] (char& c) {
            if (c == '<' || c == '>' || c == ',' || c == ' ') { c = '_'; }
        });
        return tbl_name;
    }
    void call_function_and_expect(const char* fname, data_type type, data_value expected) {
        auto msg = _e.execute_cql(format("select {}(value) from {}", fname, table_name())).get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({type})
            .with_row({
                expected.serialize()
            });
    }
public:
    template<typename... T>
    explicit aggregate_function_test(cql_test_env& e, shared_ptr<const abstract_type> column_type, T... sorted_values)
        : _e(e), _column_type(column_type), _sorted_values{data_value(sorted_values)...}
    {
        const auto cf_name = table_name();
        _e.create_table([column_type, cf_name] (std::string_view ks_name) {
            return *schema_builder(ks_name, cf_name)
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck", int32_type, column_kind::clustering_key)
                    .with_column("value", column_type)
                    .build();
        }).get();

        auto prepared = _e.prepare(format("insert into {} (pk, ck, value) values ('key1', ?, ?)", cf_name)).get();
        for (int i = 0; i < (int)_sorted_values.size(); i++) {
            const auto& value = _sorted_values[i];
            BOOST_ASSERT(&*value.type() == &*_column_type);
            std::vector<cql3::raw_value> raw_values {
                cql3::raw_value::make_value(int32_type->decompose(int32_t(i))),
                cql3::raw_value::make_value(value.serialize())
            };
            _e.execute_prepared(prepared, std::move(raw_values)).get();
        }
    }
    aggregate_function_test& test_min() {
        call_function_and_expect("min", _column_type, _sorted_values.front());
        return *this;
    }
    aggregate_function_test& test_max() {
        call_function_and_expect("max", _column_type, _sorted_values.back());
        return *this;
    }
    aggregate_function_test& test_count() {
        call_function_and_expect("count", long_type, int64_t(_sorted_values.size()));
        return *this;
    }
    aggregate_function_test& test_sum(data_value expected_result) {
        call_function_and_expect("sum", _column_type, expected_result);
        return *this;
    }
    aggregate_function_test& test_avg(data_value expected_result) {
        call_function_and_expect("avg", _column_type, expected_result);
        return *this;
    }
    aggregate_function_test& test_min_max_count() {
        test_min();
        test_max();
        test_count();
        return *this;
    }
};

SEASTAR_TEST_CASE(test_aggregate_functions_byte_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, byte_type, int8_t(1), int8_t(2), int8_t(3))
            .test_min_max_count()
            .test_sum(int8_t(6))
            .test_avg(int8_t(2));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_short_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, short_type, int16_t(1), int16_t(2), int16_t(3))
            .test_min_max_count()
            .test_sum(int16_t(6))
            .test_avg(int16_t(2));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_int32_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, int32_type, int32_t(1), int32_t(2), int32_t(3))
            .test_min_max_count()
            .test_sum(int32_t(6))
            .test_avg(int32_t(2));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_long_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, long_type, int64_t(1), int64_t(2), int64_t(3))
            .test_min_max_count()
            .test_sum(int64_t(6))
            .test_avg(int64_t(2));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_varint_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, varint_type,
            utils::multiprecision_int(1),
            utils::multiprecision_int(2),
            utils::multiprecision_int(3)
        ).test_min_max_count()
            .test_sum(utils::multiprecision_int(6))
            .test_avg(utils::multiprecision_int(2));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_decimal_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, decimal_type,
            big_decimal("1.0"),
            big_decimal("2.0"),
            big_decimal("3.0")
        ).test_min_max_count()
            .test_sum(big_decimal("6.0"))
            .test_avg(big_decimal("2.0"));
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_float_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, float_type, 1.0f, 2.0f, 3.0f)
            .test_min_max_count()
            .test_sum(6.0f)
            .test_avg(2.0f);
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_double_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, double_type, 1.0, 2.0, 3.0)
            .test_min_max_count()
            .test_sum(6.0)
            .test_avg(2.0);
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_ordered_utf8_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, utf8_type, sstring("abcd"), sstring("efgh"), sstring("ijkl"))
            .test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_ordered_bytes_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, bytes_type, bytes("abcd"), bytes("efgh"), bytes("ijkl"))
            .test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_ordered_ascii_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, ascii_type,
            ascii_native_type{"abcd"},
            ascii_native_type{"efgh"},
            ascii_native_type{"ijkl"}
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_ordered_simple_data_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, simple_date_type,
            simple_date_native_type{1},
            simple_date_native_type{2},
            simple_date_native_type{3}
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_timestamp_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const db_clock::time_point now = db_clock::now();
        aggregate_function_test(e, timestamp_type,
            now,
            now + std::chrono::seconds(1),
            now + std::chrono::seconds(2)
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_timeuuid_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, timeuuid_type,
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000000")},
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000001")},
            timeuuid_native_type{utils::UUID("00000000-0000-1000-0000-000000000002")}
        ).test_count(); // min and max will fail, because we SCYLLA_ASSERT using UUID order, not timestamp order.
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_time_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const db_clock::time_point now = db_clock::now();
        aggregate_function_test(e, time_type,
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch() - std::chrono::seconds(1)).count()},
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch()).count()},
            time_native_type{std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch() + std::chrono::seconds(1)).count()}
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_uuid_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, uuid_type,
            utils::UUID("00000000-0000-1000-0000-000000000000"),
            utils::UUID("00000000-0000-1000-0000-000000000001"),
            utils::UUID("00000000-0000-1000-0000-000000000002")
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_boolean_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, boolean_type, false, true).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_inet_addr_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        aggregate_function_test(e, inet_addr_type,
            net::inet_address("0.0.0.0"),
            net::inet_address("::"),
            net::inet_address("::1"),
            net::inet_address("0.0.0.1"),
            net::inet_address("1::1"),
            net::inet_address("1.0.0.1")
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_list_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto list_type_int = list_type_impl::get_instance(int32_type, false);
        aggregate_function_test(e, list_type_int,
            make_list_value(list_type_int, {1, 2, 3}),
            make_list_value(list_type_int, {1, 2, 4}),
            make_list_value(list_type_int, {2, 2, 3})
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_set_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto set_type_int = set_type_impl::get_instance(int32_type, false);
        aggregate_function_test(e, set_type_int,
            make_set_value(set_type_int, {1, 2, 3}),
            make_set_value(set_type_int, {1, 2, 4}),
            make_set_value(set_type_int, {2, 3, 4})
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_tuple_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto tuple_type_int_text = tuple_type_impl::get_instance({int32_type, utf8_type});
        aggregate_function_test(e, tuple_type_int_text,
            make_tuple_value(tuple_type_int_text, {1, "aaa"}),
            make_tuple_value(tuple_type_int_text, {1, "bbb"}),
            make_tuple_value(tuple_type_int_text, {2, "aaa"})
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_map_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto map_type_int_text = map_type_impl::get_instance(int32_type, utf8_type, false);
        aggregate_function_test(e, map_type_int_text,
            make_map_value(map_type_int_text, {std::make_pair(data_value(1), data_value("asdf"))}),
            make_map_value(map_type_int_text, {std::make_pair(data_value(2), data_value("asdf"))}),
            make_map_value(map_type_int_text, {std::make_pair(data_value(2), data_value("bsdf"))})
        ).test_min_max_count();
    });
}

SEASTAR_TEST_CASE(test_aggregate_functions_vector_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto vector_type_int = vector_type_impl::get_instance(int32_type, 3);
        aggregate_function_test(e, vector_type_int,
            make_list_value(vector_type_int, {1, 2, 3}),
            make_list_value(vector_type_int, {1, 2, 4}),
            make_list_value(vector_type_int, {2, 2, 3})
        ).test_min_max_count();
    });
}

BOOST_AUTO_TEST_SUITE_END()
