/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/statements/cf_prop_defs.hh"

namespace cql3 {

namespace statements {

const sstring cf_prop_defs::KW_COMMENT = "comment";
const sstring cf_prop_defs::KW_READREPAIRCHANCE = "read_repair_chance";
const sstring cf_prop_defs::KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
const sstring cf_prop_defs::KW_GCGRACESECONDS = "gc_grace_seconds";
const sstring cf_prop_defs::KW_MINCOMPACTIONTHRESHOLD = "min_threshold";
const sstring cf_prop_defs::KW_MAXCOMPACTIONTHRESHOLD = "max_threshold";
const sstring cf_prop_defs::KW_CACHING = "caching";
const sstring cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
const sstring cf_prop_defs::KW_MIN_INDEX_INTERVAL = "min_index_interval";
const sstring cf_prop_defs::KW_MAX_INDEX_INTERVAL = "max_index_interval";
const sstring cf_prop_defs::KW_SPECULATIVE_RETRY = "speculative_retry";
const sstring cf_prop_defs::KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
const sstring cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";

const sstring cf_prop_defs::KW_COMPACTION = "compaction";
const sstring cf_prop_defs::KW_COMPRESSION = "compression";

const sstring cf_prop_defs::COMPACTION_STRATEGY_CLASS_KEY = "class";

}

}
