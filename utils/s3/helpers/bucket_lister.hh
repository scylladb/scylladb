/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/s3/client.hh"

namespace s3 {

class client::bucket_lister final : public abstract_lister::impl {
    shared_ptr<client> _client;
    sstring _bucket;
    sstring _prefix;
    sstring _max_keys;
    std::optional<future<>> _opt_done_fut;
    lister::filter_type _filter;
    seastar::queue<std::optional<directory_entry>> _queue;

    future<> start_listing();

public:

    bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix = "", size_t objects_per_page = 64, size_t entries_batch = 512 / sizeof(std::optional<directory_entry>));
    bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, lister::filter_type filter, size_t objects_per_page = 64, size_t entries_batch = 512 / sizeof(std::optional<directory_entry>));

    future<std::optional<directory_entry>> get() override;
    future<> close() noexcept override;
};

} // s3
