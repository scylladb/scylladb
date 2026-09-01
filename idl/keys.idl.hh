/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

class clustering_key_prefix {
    utils::range_of<managed_bytes_view> auto components() [[reconstruct_as=components_reconstruction_type]];
};

class partition_key {
    utils::range_of<managed_bytes_view> auto components() [[reconstruct_as=components_reconstruction_type]];
};
