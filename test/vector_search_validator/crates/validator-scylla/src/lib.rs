/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod cql;

use vector_search_validator_tests::TestCase;

pub async fn test_cases() -> impl Iterator<Item = (String, TestCase)> {
    vec![("cql", cql::new().await)]
        .into_iter()
        .map(|(name, test_case)| (name.to_string(), test_case))
}
