/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// TODO: test final types more

class simple_compound {
    uint32_t foo;
    uint32_t bar;
};

class writable_final_simple_compound final stub [[writable]] {
    uint32_t foo;
    uint32_t bar;
};

class writable_simple_compound stub [[writable]] {
    uint32_t foo;
    uint32_t bar;
};

struct wrapped_vector {
    std::vector<simple_compound> vector;
};

struct vectors_of_compounds {
    std::vector<simple_compound> first;
    wrapped_vector second;
};

struct writable_wrapped_vector stub [[writable]] {
    std::vector<simple_compound> vector;
};

struct writable_vectors_of_compounds stub [[writable]] {
    std::vector<writable_simple_compound> first;
    writable_wrapped_vector second;
};

struct writable_vector stub [[writable]] {
    std::vector<simple_compound> vector;
};


struct writable_variants stub [[writable]] {
    int id;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> first;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> second;
    boost::variant<writable_vector, simple_compound, writable_final_simple_compound> third;
};

struct compound_with_optional {
    std::experimental::optional<simple_compound> first;
    simple_compound second;
};

class non_final_composite_test_object {
    simple_compound x();
};

class final_composite_test_object final {
    simple_compound x();
};

struct empty_struct { };

struct empty_final_struct final { };

struct just_a_variant stub [[writable]] {
    boost::variant<writable_simple_compound, simple_compound> variant;
};