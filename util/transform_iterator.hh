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
