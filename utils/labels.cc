// Copyright (C) 2025-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#include "utils/labels.hh"

seastar::metrics::label level_label("__level");
seastar::metrics::label_instance basic_level("__level", "basic");
seastar::metrics::label_instance cdc_label("__cdc", "1");
seastar::metrics::label_instance cas_label("__cas", "1");
seastar::metrics::label_instance alternator_label("__alternator", "1");
