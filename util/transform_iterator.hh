/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef UTIL_TRANSFORM_ITERATOR_HH_
#define UTIL_TRANSFORM_ITERATOR_HH_

template <typename Iterator, typename Func>
class transform_iterator {
    Iterator _i;
    Func _f;
public:
    transform_iterator(Iterator i, Func f) : _i(i), _f(f) {}
    auto operator*() { return _f(*_i); }
    transform_iterator& operator++() {
        ++_i;
        return *this;
    }
    transform_iterator operator++(int) {
        transform_iterator ret(*this);
        _i++;
        return ret;
    }
    bool operator==(const transform_iterator& x) const {
        return _i == x._i;
    }
    bool operator!=(const transform_iterator& x) const {
        return !operator==(x);
    }
};

template <typename Iterator, typename Func>
inline
transform_iterator<Iterator, Func>
make_transform_iterator(Iterator i, Func f) {
    return transform_iterator<Iterator, Func>(i, f);
}

#endif /* UTIL_TRANSFORM_ITERATOR_HH_ */
