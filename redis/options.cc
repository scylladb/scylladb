/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "redis/options.hh"
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"
#include <seastar/core/format.hh>
#include "redis/keyspace_utils.hh"

using namespace seastar;

namespace redis {

schema_ptr get_schema(service::storage_proxy& proxy, const sstring& ks_name, const sstring& cf_name) {
    auto db = proxy.data_dictionary();
    auto schema = db.find_schema(ks_name, cf_name);
    return schema;
}

}
