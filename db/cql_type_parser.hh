/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>

#include "types/types.hh"

#include "seastarx.hh"

class types_metadata;

namespace data_dictionary {
class keyspace_metadata;
class user_types_storage;
class user_types_metadata;
}

namespace db {
namespace cql_type_parser {

data_type parse(const sstring& keyspace, const sstring& type, const data_dictionary::user_types_metadata& utm);
data_type parse(const sstring& keyspace, const sstring& type, const data_dictionary::user_types_storage& uts);

class raw_builder {
public:
    raw_builder(data_dictionary::keyspace_metadata &ks);
    ~raw_builder();

    void add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types);
    future<std::vector<user_type>> build();
private:
    class impl;
    std::unique_ptr<impl>
        _impl;
};

}
}
