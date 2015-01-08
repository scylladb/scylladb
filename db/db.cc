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
#include "counter_cell.hh"
#include "i_mutation.hh"
#include "row_position.hh"
#include "marshal/reversed_type.hh"
#include "marshal/collection_type.hh"

const db::composites::composite::EOC db::composites::composite::EOC::START{-1};
const db::composites::composite::EOC db::composites::composite::EOC::NONE{-1};
const db::composites::composite::EOC db::composites::composite::EOC::END{1};

const db::row_position::kind db::row_position::kind::ROW_KEY{0};
const db::row_position::kind db::row_position::kind::MIN_BOUND{1};
const db::row_position::kind db::row_position::kind::MAX_BOUND{2};
std::unique_ptr<db::row_position::row_position_serializer> db::row_position::serializer(new db::row_position::row_position_serializer);
