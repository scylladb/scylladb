/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

namespace locator {

class node;

using node_ptr = lw_shared_ptr<const node>;

} // namespace locator
