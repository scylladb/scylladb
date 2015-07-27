/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <seastar/core/sharded.hh>

class database;

namespace debug {

extern seastar::sharded<database>* db;


}

