#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <utility>
#include "cql3/expr/expression.hh"
#include "utils/overloaded_functor.hh"
#include <cassert>

using namespace cql3::expr;

bind_variable new_bind_variable(int bind_index) {
    return bind_variable {
        .shape = bind_variable::shape_type::scalar,
        .bind_index = bind_index,
        .receiver = nullptr
    };
}

BOOST_AUTO_TEST_CASE(expr_visit_get_int) {
    expression e = new_bind_variable(1245);

    int read_value = visit(overloaded_functor {
        [](const bind_variable& bv) -> int { return bv.bind_index; },
        [](const auto&) -> int { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(read_value, 1245);
}

BOOST_AUTO_TEST_CASE(expr_visit_void_return) {
    expression e = new_bind_variable(1245);

    visit(overloaded_functor {
        [](const bind_variable& bv) { BOOST_REQUIRE_EQUAL(bv.bind_index, 1245); },
        [](const auto&) { throw std::runtime_error("Unreachable"); }
    }, e);
}

BOOST_AUTO_TEST_CASE(expr_visit_const_ref) {
    const expression e = new_bind_variable(123);

    const bind_variable& ref = visit(overloaded_functor {
        [](const bind_variable& bv) -> const bind_variable& { return bv; },
        [](const auto&) -> const bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref.bind_index, 123);
}

BOOST_AUTO_TEST_CASE(expr_visit_ref) {
    expression e = new_bind_variable(456);

    bind_variable& ref = visit(overloaded_functor {
        [](bind_variable& bv) -> bind_variable& { return bv; },
        [](auto&) -> bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref.bind_index, 456);

    ref.bind_index = 135;

    bind_variable& ref2 = visit(overloaded_functor {
        [](bind_variable& bv) -> bind_variable& { return bv; },
        [](auto&) -> bind_variable& { throw std::runtime_error("Unreachable"); }
    }, e);

    BOOST_REQUIRE_EQUAL(ref2.bind_index, 135);
}
