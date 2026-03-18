/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once


#include "types/types.hh"
#include "exceptions/exceptions.hh"
#include "vint-serialization.hh"

class vector_type_impl : public concrete_type<std::vector<data_value>> {
    using intern = type_interning_helper<vector_type_impl, data_type, vector_dimension_t>;
protected:
    data_type _elements_type;
    vector_dimension_t _dimension;
public:
    vector_type_impl(data_type elements_type, vector_dimension_t dimension);
    static shared_ptr<const vector_type_impl> get_instance(data_type type, vector_dimension_t dimension);
    data_type get_elements_type() const {
        return _elements_type;
    }
    vector_dimension_t get_dimension() const {
        return _dimension;
    }
    static std::strong_ordering compare_vectors(data_type elements_comparator, vector_dimension_t dimension,
                        managed_bytes_view o1, managed_bytes_view o2);
                        
    std::vector<managed_bytes> split_fragmented(FragmentedView auto v) const {
        std::vector<managed_bytes> elements;
        elements.reserve(_dimension);
        auto fixed_len = _elements_type->value_length_if_fixed();
        if (fixed_len) {
            for (size_t i = 0; i < _dimension; ++i) {
                elements.push_back(managed_bytes(read_vector_element_fixed(v, *fixed_len)));
            }
        } else {
            for (size_t i = 0; i < _dimension; ++i) {
                elements.push_back(managed_bytes(read_vector_element_variable(v)));
            }
        }
        return elements;
    }

    template <typename Range> // range of managed_bytes or managed_bytes_view
    requires requires (Range it) { {*std::begin(it)} -> std::convertible_to<managed_bytes_view>; }
    static managed_bytes build_value_fragmented(Range&& range, std::optional<size_t> value_length_if_fixed) {
        bool is_fixed_length = value_length_if_fixed.has_value();
        size_t size = 0;

        for (auto&& v : range) {
            size += v.size();
            if (!is_fixed_length) {
                size += (size_t)unsigned_vint::serialized_size(v.size());
            }
        }

        auto ret = bytes(bytes::initialized_later(), size);
        auto out = ret.begin();

        for (auto&& v : range) {
            if (!is_fixed_length) {
                out += unsigned_vint::serialize(v.size(), out);
            }
            auto read_bytes = [v] (bytes_view element_bytes) {return read_simple_bytes(element_bytes, v.size());};
            auto element = with_linearized(managed_bytes_view(v), read_bytes);
            out = std::copy_n(element.begin(), element.size(), out);
        }

        return managed_bytes(ret);
    }

    template <typename RangeOf_bytes>  // also accepts bytes_view
    requires requires (RangeOf_bytes it) { {*std::begin(it)} -> std::convertible_to<bytes_view>; }
    static bytes build_value(RangeOf_bytes&& range, std::optional<size_t> value_length_if_fixed) {
        bool is_fixed_length = value_length_if_fixed.has_value();
        size_t size = 0;

        for (auto&& v : range) {
            size += v.size();
            if (!is_fixed_length) {
                size += (size_t)unsigned_vint::serialized_size(v.size());
            }
        }

        auto ret = bytes(bytes::initialized_later(), size);

        auto out = ret.begin();

        for (auto&& v : range) {
            if (!is_fixed_length) {
                out += unsigned_vint::serialize(v.size(), out);
            }
            out = std::copy_n(v.begin(), v.size(), out);
        }

        return ret;
    }
private:
    static sstring make_name(data_type type, vector_dimension_t dimension);

};

// Read a vector element with known fixed size.
template <FragmentedView View>
View read_vector_element_fixed(View& v, size_t element_size) {
    if (element_size == 0) {
        throw exceptions::invalid_request_exception("null/unset is not supported inside vectors");
    }

    if (element_size > v.size_bytes()) {
        throw exceptions::invalid_request_exception("Not enough bytes to read a vector element");
    }

    return read_simple_bytes(v, element_size);
}

// Read a vector element with variable-length encoding (vint-prefixed size).
template <FragmentedView View>
View read_vector_element_variable(View& v) {
    auto element_size = with_linearized(v, unsigned_vint::deserialize);
    v.remove_prefix(unsigned_vint::serialized_size(element_size));

    if (element_size == 0) {
        throw exceptions::invalid_request_exception("null/unset is not supported inside vectors");
    }

    if ((size_t)element_size > v.size_bytes()) {
        throw exceptions::invalid_request_exception("Not enough bytes to read a vector element");
    }

    return read_simple_bytes(v, element_size);
}

template <FragmentedView View>
View read_vector_element(View& v, std::optional<size_t> value_length_if_fixed) {
    if (value_length_if_fixed) {
        return read_vector_element_fixed(v, *value_length_if_fixed);
    } else {
        return read_vector_element_variable(v);
    }
}

data_value make_vector_value(data_type type, vector_type_impl::native_type value);
