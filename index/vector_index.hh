/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/secondary_index_manager.hh"

#include <vector>

namespace secondary_index {

class vector_index: public custom_index {

public:
    vector_index() = default;
    ~vector_index() override = default;
    void validate(const schema &schema, cql3::statements::index_prop_defs &properties, const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs) override;
};

std::unique_ptr<secondary_index::custom_index> vector_index_factory();

}