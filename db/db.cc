/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

// Used to ensure that all .hh files build, as well as a place to put
// out-of-line implementations.

#include "core/shared_ptr.hh"
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
#include "marshal/list_type.hh"
#include "composites/abstract_c_type.hh"

const db::composites::composite::EOC db::composites::composite::EOC::START{-1};
const db::composites::composite::EOC db::composites::composite::EOC::NONE{-1};
const db::composites::composite::EOC db::composites::composite::EOC::END{1};

const db::row_position::kind db::row_position::kind::ROW_KEY{0};
const db::row_position::kind db::row_position::kind::MIN_BOUND{1};
const db::row_position::kind db::row_position::kind::MAX_BOUND{2};
std::unique_ptr<db::row_position::row_position_serializer> db::row_position::serializer(new db::row_position::row_position_serializer);

namespace db {
namespace composites {

thread_local comparator<cell> abstract_c_type::right_native_cell {
    [] (cell& o1, cell& o2) {
        auto& x = dynamic_cast<native_cell&>(o2);
        return -(x.compare_to(o1.name()));
    }
};

thread_local comparator<cell> abstract_c_type::neither_native_cell {
    [] (cell& o1, cell& o2) {
        auto x = dynamic_pointer_cast<composite>(o1.name());
        auto y = dynamic_pointer_cast<composite>(o2.name());
        return abstract_c_type::compare_unsigned(*x, *y);
    }
};

thread_local comparator<shared_ptr<cell>> abstract_c_type::asymmetric_right_native_cell {
    [] (shared_ptr<cell>& o1, shared_ptr<cell>& o2) {
        auto x = dynamic_pointer_cast<native_cell>(o2);
        auto y = dynamic_pointer_cast<composite>(o1);
        return -(x->compare_to(y));
    }
};

thread_local comparator<shared_ptr<cell>> abstract_c_type::asymmetric_neither_native_cell {
    [] (shared_ptr<cell>& o1, shared_ptr<cell>& o2) {
        auto x = dynamic_pointer_cast<composite>(o1);
        auto y = dynamic_pointer_cast<composite>(o2->name());
        return abstract_c_type::compare_unsigned(*x, *y);
    }
};

} // composites
} // db
