/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <db/config.hh>

future<> init_ms_fd_gossiper(sstring listen_address, db::seed_provider_type seed_provider);
