/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"

namespace replica::logstor {

record_value encode_record_value(const mutation& m) {
    canonical_mutation cm(m);
    return record_value(std::move(cm.representation()));
}

mutation decode_record_value(const record_value& v, schema_ptr s, const record_header&) {
    return canonical_mutation(v.representation()).to_mutation(std::move(s));
}

mutation to_mutation(const log_record& r, schema_ptr s) {
    return decode_record_value(r.value, std::move(s), r.header);
}

} // namespace replica::logstor
