/*
 * Copyright (C) 2020 ScyllaDB
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
#include "cql3/query_options.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"
#include "db/config.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"

#include <seastar/testing/thread_test_case.hh>

static std::vector<std::vector<bytes_opt>> to_bytes(const cql_transport::messages::result_message::rows& rows) {
    auto rs = rows.rs().result_set().rows();
    std::vector<std::vector<bytes_opt>> results;
    for (auto it = rs.begin(); it != rs.end(); ++it) {
        results.push_back(*it);
    }
    return results;
}

static cql_test_config mk_lwt_cdc_test_config() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    auto cfg = ::make_shared<db::config>(std::move(ext));

    auto features = cfg->experimental_features();
    features.emplace_back(db::experimental_features_t::CDC);
    features.emplace_back(db::experimental_features_t::LWT);
    cfg->experimental_features(features);
    return cql_test_config(std::move(cfg));
};

SEASTAR_THREAD_TEST_CASE(test_lwt_with_cdc) {
    BOOST_REQUIRE_MESSAGE(smp::count == 1, "This test has to be run with -c1");

    // Creates temporary `query_options` with `serial_consistency == SERIAL`:
    auto qo_sc = [] () {
        using cqo = cql3::query_options;
        using cqoso = cqo::specific_options;
        return std::make_unique<cqo>(
                cqo::DEFAULT.get_consistency(),
                cqo::DEFAULT.get_timeout_config(),
                std::vector<cql3::raw_value>{},
                cqoso{cqoso::DEFAULT.page_size, cqoso::DEFAULT.state, db::consistency_level::SERIAL, cqoso::DEFAULT.timestamp});
    };

    do_with_cql_env_thread([qo_sc] (cql_test_env& e) {
        const auto base_tbl_name = "tbl_lwt";
        const int pk = 1, ck = 11;

        cquery_nofail(e, format("CREATE TABLE ks.{} (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {{'enabled':'true'}}", base_tbl_name));
        // (0) successful insert:
        cquery_nofail(e, format("INSERT INTO ks.{} (pk, ck, val) VALUES ({}, {}, 111) IF NOT EXISTS", base_tbl_name, pk, ck), qo_sc());
        // (1) failed insert:
        cquery_nofail(e, format("INSERT INTO ks.{} (pk, ck, val) VALUES ({}, {}, 222) IF NOT EXISTS", base_tbl_name, pk, ck), qo_sc());
        // (2) successful update:
        cquery_nofail(e, format("UPDATE ks.{} set val=333 WHERE pk = {} and ck = {} IF EXISTS", base_tbl_name, pk, ck), qo_sc());
        // (3) failed update:
        cquery_nofail(e, format("UPDATE ks.{} set val=444 WHERE pk = 888 and ck = 777 IF EXISTS", base_tbl_name, pk, ck), qo_sc());
        // (4) successful row delete:
        cquery_nofail(e, format("DELETE FROM ks.{} WHERE pk = {} AND ck = {} IF EXISTS", base_tbl_name, pk, ck), qo_sc());
        // (5) failed row delete:
        cquery_nofail(e, format("DELETE FROM ks.{} WHERE pk = {} AND ck = {} IF EXISTS", base_tbl_name, pk, ck), qo_sc());

        const sstring query = format("SELECT \"{}\" FROM ks.{}", cdc::log_meta_column_name("operation"), cdc::log_name(base_tbl_name));
        auto msg = e.execute_cql(query).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        auto results = to_bytes(*rows);
        BOOST_REQUIRE_EQUAL(results.size(), 3);  // 1 successful insert + 1 successful update + 1 successful row delete == 3

        BOOST_REQUIRE_EQUAL(results[0].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[0].front(), data_value(static_cast<int8_t>(cdc::operation::insert)).serialize_nonnull()); // log entry from (0)

        BOOST_REQUIRE_EQUAL(results[1].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[1].front(), data_value(static_cast<int8_t>(cdc::operation::update)).serialize_nonnull()); // log entry from (2)

        BOOST_REQUIRE_EQUAL(results[2].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[2].front(), data_value(static_cast<int8_t>(cdc::operation::row_delete)).serialize_nonnull()); // log entry from (4)
    }, mk_lwt_cdc_test_config()).get();
}
