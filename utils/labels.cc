/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/labels.hh"

seastar::metrics::label level_label("__level");
seastar::metrics::label_instance basic_level("__level", 1);
seastar::metrics::label_instance advanced_level("__level", 2);
seastar::metrics::label_instance specific_level("__level", 3);