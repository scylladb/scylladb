/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// The serializers for partition_key and clustering_key_prefix are hand-written
// rather than generated: the generated writer exploded each key into a
// std::vector<bytes> twice per write (once for sizing, once for the write).
// This file only pulls the replacement in so that keys.dist.hh keeps providing
// ser::serializer<> for both keys, as every other IDL module expects.
#include "keys/keys_serializer.hh"
