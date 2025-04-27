/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "multipart_upload.hh"
#include "utils/s3/client.hh"
#include <seastar/core/units.hh>

namespace s3 {

class client::copy_s3_object final : multipart_upload {
public:
    copy_s3_object(shared_ptr<client> cln, sstring source_object, sstring target_object, size_t part_size, std::optional<tag> tag, abort_source* as);

    copy_s3_object(shared_ptr<client> cln, sstring source_object, sstring target_object, std::optional<tag> tag, abort_source* as);

    future<> copy();

private:
    future<> copy_put();

    future<> copy_multipart(size_t source_size);

    future<> copy_part(size_t offset, size_t part_size);

    static constexpr size_t _default_copy_part_size = 5_GiB;
    const size_t _max_copy_part_size;
    sstring _source_object;
};

} // namespace s3
