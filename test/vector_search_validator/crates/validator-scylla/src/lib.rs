/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod common;
mod cql;
mod high_availability;

use async_backtrace::framed;
use vector_search_validator_tests::TestCase;

#[framed]
pub async fn test_cases() -> impl Iterator<Item = (String, TestCase)> {
    vec![
        ("scylla_cql", cql::new().await),
        ("scylla_high_availability", high_availability::new().await),
    ]
    .into_iter()
    .map(|(name, test_case)| (name.to_string(), test_case))
}
