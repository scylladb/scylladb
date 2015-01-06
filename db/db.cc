/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

// Used to ensure that all .hh files build, as well as a place to put
// out-of-line implementations.

#include "composites/composite.hh"
#include "composites/c_builder.hh"
#include "composites/cell_name.hh"
#include "on_disk_atom.hh"
#include "deletion_info.hh"
#include "range_tombstone.hh"
#include "composites/c_type.hh"
#include "composites/cell_name_type.hh"
#include "cell.hh"
#include "counter_update_cell.hh"
#include "deleted_cell.hh"
#include "expiring_cell.hh"

const db::composites::composite::EOC db::composites::composite::EOC::START{-1};
const db::composites::composite::EOC db::composites::composite::EOC::NONE{-1};
const db::composites::composite::EOC db::composites::composite::EOC::END{1};
