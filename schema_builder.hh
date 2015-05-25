/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "schema.hh"

struct schema_builder {
    sstring _ks_name;
    sstring _cf_name;
    std::vector<schema::column> _partition_key;
    std::vector<schema::column> _clustering_key;
    std::vector<schema::column> _static_columns;
    std::vector<schema::column> _regular_columns;
public:
    schema_builder(const sstring& ks_name, const sstring& cf_name)
        : _ks_name(ks_name)
        , _cf_name(cf_name)
    { }

    schema_builder& with_column(bytes name, data_type type, column_kind kind = column_kind::regular_column) {
        switch (kind) {
            case column_kind::partition_key:
                _partition_key.emplace_back(schema::column{name, type});
                break;
            case column_kind::clustering_key:
                _clustering_key.emplace_back(schema::column{name, type});
                break;
            case column_kind::static_column:
                _static_columns.emplace_back(schema::column{name, type});
                break;
            case column_kind::regular_column:
                _regular_columns.emplace_back(schema::column{name, type});
                break;
        };
        return *this;
    }

    schema_ptr build() {
        return make_lw_shared<schema>(schema({},
            _ks_name,
            _cf_name,
            _partition_key,
            _clustering_key,
            _regular_columns,
            _static_columns,
            utf8_type));
    }
};
