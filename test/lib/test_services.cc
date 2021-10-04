/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "test/lib/test_services.hh"
#include "db/config.hh"
#include "dht/i_partitioner.hh"
#include "gms/feature_service.hh"
#include "repair/row_level.hh"

dht::token create_token_from_key(const dht::i_partitioner& partitioner, sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = partitioner.get_token(key_view);
    assert(token == partitioner.get_token(key_view));
    return token;
}

range<dht::token> create_token_range_from_keys(const dht::sharder& sinfo, const dht::i_partitioner& partitioner, sstring start_key, sstring end_key) {
    dht::token start = create_token_from_key(partitioner, start_key);
    assert(this_shard_id() == sinfo.shard_of(start));
    dht::token end = create_token_from_key(partitioner, end_key);
    assert(this_shard_id() == sinfo.shard_of(end));
    assert(end >= start);
    return range<dht::token>::make(start, end);
}

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

db::nop_large_data_handler nop_lp_handler;
db::config test_db_config;
gms::feature_service test_feature_service(gms::feature_config_from_db_config(test_db_config));

column_family::config column_family_test_config(sstables::sstables_manager& sstables_manager, reader_concurrency_semaphore& compaction_semaphore) {
    column_family::config cfg;
    cfg.sstables_manager = &sstables_manager;
    cfg.compaction_concurrency_semaphore = &compaction_semaphore;
    return cfg;
}

column_family_for_tests::data::data()
    : semaphore(reader_concurrency_semaphore::no_limits{}, "column_family_for_tests")
{ }


column_family_for_tests::column_family_for_tests(sstables::sstables_manager& sstables_manager, schema_registry& registry)
    : column_family_for_tests(
            sstables_manager,
            schema_builder(registry, some_keyspace, some_column_family)
                .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
                .build())
{ }

column_family_for_tests::column_family_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s)
    : _data(make_lw_shared<data>())
{
    _data->s = s;
    _data->cfg = column_family_test_config(sstables_manager, _data->semaphore);
    _data->cfg.enable_disk_writes = false;
    _data->cfg.enable_commitlog = false;
    _data->cf = make_lw_shared<column_family>(_data->s, _data->cfg, column_family::no_commitlog(), _data->cm, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
}
