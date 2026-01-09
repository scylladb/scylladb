/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common;
use async_backtrace::framed;
use vector_search_validator_tests::TestCase;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = common::DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
}
