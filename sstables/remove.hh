/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

namespace sstables {

future<> remove_by_toc_name(sstring sstable_toc_name);

}


