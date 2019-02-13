/*
 * Copyright (C) 2019 ScyllaDB
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

#include "tests/test_services.hh"

static db::nop_large_data_handler nop_lp_handler;

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

column_family::config column_family_test_config() {
    column_family::config cfg;
    cfg.large_data_handler = &nop_lp_handler;
    return cfg;
}

column_family_for_tests::column_family_for_tests()
    : column_family_for_tests(
        schema_builder(some_keyspace, some_column_family)
            .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
            .build()
    )
{ }

column_family_for_tests::column_family_for_tests(schema_ptr s)
    : _data(make_lw_shared<data>())
{
    _data->s = s;
    _data->cfg.enable_disk_writes = false;
    _data->cfg.enable_commitlog = false;
    _data->cfg.large_data_handler = &nop_lp_handler;
    _data->cf = make_lw_shared<column_family>(_data->s, _data->cfg, column_family::no_commitlog(), _data->cm, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
}
