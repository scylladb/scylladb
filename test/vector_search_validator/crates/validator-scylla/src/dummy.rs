/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::common;
use vector_search_validator_tests::*;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test("dummy", timeout, dummy)
}

async fn dummy(_actors: TestActors) {
    info!("running dummy test");
    assert!(false);
}
