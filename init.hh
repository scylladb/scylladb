/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include "db/config.hh"
#include "database.hh"

future<> init_storage_service(distributed<database>& db);
future<> init_ms_fd_gossiper(sstring listen_address, db::seed_provider_type seed_provider, sstring cluster_name = "Test Cluster");
