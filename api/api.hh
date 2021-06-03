/*
 * Copyright 2015-present ScyllaDB
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

#pragma once

#include <seastar/json/json_elements.hh>
#include <type_traits>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/units/detail/utility.hpp>
#include "api/api-doc/utils.json.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include <seastar/http/exception.hh>
#include "api_init.hh"
#include "seastarx.hh"

namespace api {

template<class T>
std::vector<sstring> container_to_vec(const T& container) {
    std::vector<sstring> res;
    for (auto i : container) {
        res.push_back(boost::lexical_cast<std::string>(i));
    }
    return res;
}

template<class T>
std::vector<T> map_to_key_value(const std::map<sstring, sstring>& map) {
    std::vector<T> res;
    for (auto i : map) {
        res.push_back(T());
        res.back().key = i.first;
        res.back().value = i.second;
    }
    return res;
}

template<class T, class MAP>
std::vector<T>& map_to_key_value(const MAP& map, std::vector<T>& res) {
    for (auto i : map) {
        T val;
        val.key = boost::lexical_cast<std::string>(i.first);
        val.value = boost::lexical_cast<std::string>(i.second);
        res.push_back(val);
    }
    return res;
}
template <typename T, typename S = T>
T map_sum(T&& dest, const S& src) {
    for (auto i : src) {
        dest[i.first] += i.second;
    }
    return std::move(dest);
}

template <typename MAP>
std::vector<sstring> map_keys(const MAP& map) {
    std::vector<sstring> res;
    for (const auto& i : map) {
        res.push_back(boost::lexical_cast<std::string>(i.first));
    }
    return res;
}

/**
 * General sstring splitting function
 */
inline std::vector<sstring> split(const sstring& text, const char* separator) {
    if (text == "") {
        return std::vector<sstring>();
    }
    std::vector<sstring> tokens;
    return boost::split(tokens, text, boost::is_any_of(separator));
}

/**
 * Split a column family parameter
 */
inline std::vector<sstring> split_cf(const sstring& cf) {
    return split(cf, ",");
}

/**
 * A helper function to sum values on an a distributed object that
 * has a get_stats method.
 *
 */
template<class T, class F, class V>
future<json::json_return_type>  sum_stats(distributed<T>& d, V F::*f) {
    return d.map_reduce0([f](const T& p) {return p.get_stats().*f;}, 0,
            std::plus<V>()).then([](V val) {
        return make_ready_future<json::json_return_type>(val);
    });
}



inline
httpd::utils_json::histogram to_json(const utils::ihistogram& val) {
    httpd::utils_json::histogram h;
    h = val;
    h.sum = val.estimated_sum();
    return h;
}

inline
httpd::utils_json::rate_moving_average meter_to_json(const utils::rate_moving_average& val) {
    httpd::utils_json::rate_moving_average m;
    m = val;
    return m;
}

inline
httpd::utils_json::rate_moving_average_and_histogram timer_to_json(const utils::rate_moving_average_and_histogram& val) {
    httpd::utils_json::rate_moving_average_and_histogram h;
    h.hist = to_json(val.hist);
    h.meter = meter_to_json(val.rate);
    return h;
}

template<class T, class F>
future<json::json_return_type>  sum_histogram_stats(distributed<T>& d, utils::timed_rate_moving_average_and_histogram F::*f) {

    return d.map_reduce0([f](const T& p) {return (p.get_stats().*f).hist;}, utils::ihistogram(),
            std::plus<utils::ihistogram>()).then([](const utils::ihistogram& val) {
        return make_ready_future<json::json_return_type>(to_json(val));
    });
}

template<class T, class F>
future<json::json_return_type>  sum_timer_stats(distributed<T>& d, utils::timed_rate_moving_average_and_histogram F::*f) {

    return d.map_reduce0([f](const T& p) {return (p.get_stats().*f).rate();}, utils::rate_moving_average_and_histogram(),
            std::plus<utils::rate_moving_average_and_histogram>()).then([](const utils::rate_moving_average_and_histogram& val) {
        return make_ready_future<json::json_return_type>(timer_to_json(val));
    });
}

inline int64_t min_int64(int64_t a, int64_t b) {
    return std::min(a,b);
}

inline int64_t max_int64(int64_t a, int64_t b) {
    return std::max(a,b);
}

/**
 * A helper struct for ratio calculation
 * It combine total and the sub set for the ratio and its
 * to_json method return the ration sub/total
 */
template<typename T>
struct basic_ratio_holder : public json::jsonable {
    T total = 0;
    T sub = 0;
    virtual std::string to_json() const {
        if (total == 0) {
            return "0";
        }
        return std::to_string(sub/total);
    }
    basic_ratio_holder() = default;
    basic_ratio_holder& add(T _total, T _sub) {
        total += _total;
        sub += _sub;
        return *this;
    }
    basic_ratio_holder(T _total, T _sub) {
        total = _total;
        sub = _sub;
    }
    basic_ratio_holder<T>& operator+=(const basic_ratio_holder<T>& a) {
        return add(a.total, a.sub);
    }
    friend basic_ratio_holder<T> operator+(basic_ratio_holder a, const basic_ratio_holder<T>& b) {
        return a += b;
    }
};

typedef basic_ratio_holder<double>  ratio_holder;
typedef basic_ratio_holder<int64_t> integral_ratio_holder;

class unimplemented_exception : public base_exception {
public:
    unimplemented_exception()
            : base_exception("API call is not supported yet", reply::status_type::internal_server_error) {
    }
};

inline void unimplemented() {
    throw unimplemented_exception();
}

template <class T>
std::vector<T> concat(std::vector<T> a, std::vector<T>&& b) {
    a.reserve( a.size() + b.size());
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

template <class T, class Base = T>
class req_param {
public:
    sstring name;
    sstring param;
    T value;

    req_param(const request& req, sstring name, T default_val) : name(name) {
        param = req.get_query_param(name);
        if (param.empty()) {
            value = default_val;
            return;
        }
        try {
            // boost::lexical_cast does not use boolalpha. Converting a
            // true/false throws exceptions. We don't want that.
            if constexpr (std::is_same_v<Base, bool>) {
                // Cannot use boolalpha because we (probably) want to
                // accept 1 and 0 as well as true and false. And True. And fAlse.
                std::transform(param.begin(), param.end(), param.begin(), ::tolower);
                if (param == "true" || param == "1") {
                    value = T(true);
                } else if (param == "false" || param == "0") {
                    value = T(false);
                } else {
                    throw boost::bad_lexical_cast{};
                }
            } else {
                value = T{boost::lexical_cast<Base>(param)};
            }
        } catch (boost::bad_lexical_cast&) {
            throw bad_param_exception(format("{} ({}): type error - should be {}", name, param, boost::units::detail::demangle(typeid(Base).name())));
        }
    }

    operator T() const { return value; }
};

utils_json::estimated_histogram time_to_json_histogram(const utils::time_estimated_histogram& val);

}
