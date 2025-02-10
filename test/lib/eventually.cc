/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/eventually.hh"

sleep_fn seastar_sleep_fn = [] (std::chrono::milliseconds ms) -> future<> {
    return seastar::sleep(ms);
};

sleep_fn manual_clock_sleep_fn = [] (std::chrono::milliseconds ms) -> future<> {
    auto end = manual_clock::now() + ms;
    while (manual_clock::now() < end) {
        manual_clock::advance(std::chrono::milliseconds(1));
        co_await yield();
    }
};
