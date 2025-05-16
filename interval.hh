/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include <algorithm>
#include <list>
#include <vector>
#include <optional>
#include <iosfwd>
#include <compare>
#include <ranges>
#include <fmt/format.h>

template <typename Comparator, typename T>
concept IntervalComparatorFor = requires (T a, T b, Comparator& cmp) {
    { cmp(a, b) } -> std::same_as<std::strong_ordering>;
};

template <typename LessComparator, typename T>
concept IntervalLessComparatorFor = requires (T a, T b, LessComparator& cmp) {
    { cmp(a, b) } -> std::same_as<bool>;
};

inline
bool
require_ordering_and_on_equal_return(
        std::strong_ordering input,
        std::strong_ordering match_to_return_true,
        bool return_if_equal) {
    if (input == match_to_return_true) {
        return true;
    } else if (input == std::strong_ordering::equal) {
        return return_if_equal;
    } else {
        return false;
    }
}

template<typename T>
class interval_bound {
    T _value;
    bool _inclusive;
public:
    interval_bound(T value, bool inclusive = true)
              : _value(std::move(value))
              , _inclusive(inclusive)
    { }
    const T& value() const & { return _value; }
    T&& value() && { return std::move(_value); }
    bool is_inclusive() const { return _inclusive; }
    bool operator==(const interval_bound& other) const {
        return (_value == other._value) && (_inclusive == other._inclusive);
    }
    bool equal(const interval_bound& other, IntervalComparatorFor<T> auto&& cmp) const {
        return _inclusive == other._inclusive && cmp(_value, other._value) == 0;
    }
};

template<typename T>
class interval;

// An interval which can have inclusive, exclusive or open-ended bounds on each end.
// The end bound can be smaller than the start bound.
template<typename T>
class wrapping_interval {
    template <typename U>
    using optional = std::optional<U>;
public:
    using bound = interval_bound<T>;

    template <typename Transformer>
    using transformed_type = typename std::remove_cv_t<std::remove_reference_t<std::invoke_result_t<Transformer, T>>>;
private:
    bool _has_start = false;
    bool _has_end = false;
    bool _start_inclusive = false;
    bool _end_inclusive = false;
    bool _singular = false;
    union {
        T _start_value;
    };
    union {
        T _end_value;
    };

    void destroy() {
        if (_has_start) {
            _start_value.~T();
        }
        if (_has_end && (!_singular || !_has_start)) {
            _end_value.~T();
        }
    }
    void copy_from(const wrapping_interval& o) {
        _has_start = o._has_start;
        _has_end = o._has_end;
        _start_inclusive = o._start_inclusive;
        _end_inclusive = o._end_inclusive;
        _singular = o._singular;
        if (_has_start) {
            new (&_start_value) T(o._start_value);
        }
        if (_has_end && (!_singular || !_has_start)) {
            new (&_end_value) T(o._end_value);
        }
    }
    void move_from(wrapping_interval&& o) {
        _has_start = o._has_start;
        _has_end = o._has_end;
        _start_inclusive = o._start_inclusive;
        _end_inclusive = o._end_inclusive;
        _singular = o._singular;
        if (_has_start) {
            new (&_start_value) T(std::move(o._start_value));
        }
        if (_has_end && (!_singular || !_has_start)) {
            new (&_end_value) T(std::move(o._end_value));
        }
    }
public:
    wrapping_interval(optional<bound> start, optional<bound> end, bool singular = false)
        : _has_start(false), _has_end(false), _start_inclusive(false), _end_inclusive(false), _singular(singular)
    {
        if (start) {
            new (&_start_value) T(start->value());
            _has_start = true;
            _start_inclusive = start->is_inclusive();
        }
        if (!_singular && end) {
            new (&_end_value) T(end->value());
            _has_end = true;
            _end_inclusive = end->is_inclusive();
        } else if (_singular && start) {
            // For singular, only _start_value is used
            _has_end = false;
            _end_inclusive = false;
        } else if (end) {
            new (&_end_value) T(end->value());
            _has_end = true;
            _end_inclusive = end->is_inclusive();
        }
    }
    wrapping_interval(T value)
        : _has_start(true), _has_end(false), _start_inclusive(true), _end_inclusive(false), _singular(true)
    {
        new (&_start_value) T(std::move(value));
    }
    constexpr wrapping_interval() : _has_start(false), _has_end(false), _start_inclusive(false), _end_inclusive(false), _singular(false) {}
    wrapping_interval(const wrapping_interval& o) {
        copy_from(o);
    }
    wrapping_interval(wrapping_interval&& o) noexcept {
        move_from(std::move(o));
    }
    wrapping_interval& operator=(const wrapping_interval& o) {
        if (this != &o) {
            destroy();
            copy_from(o);
        }
        return *this;
    }
    wrapping_interval& operator=(wrapping_interval&& o) noexcept {
        if (this != &o) {
            destroy();
            move_from(std::move(o));
        }
        return *this;
    }
    ~wrapping_interval() {
        destroy();
    }
private:
    struct bound_ref {
        const T* value;
        bool inclusive;
        bool present;
        bound_ref() : value(nullptr), inclusive(false), present(false) {}
        bound_ref(const T* v, bool i) : value(v), inclusive(i), present(true) {}
    };
    struct start_bound_ref : bound_ref {
        start_bound_ref(const T* v, bool i, bool p) : bound_ref(v, i) { this->present = p; }
    };
    struct end_bound_ref : bound_ref {
        end_bound_ref(const T* v, bool i, bool p) : bound_ref(v, i) { this->present = p; }
    };
    start_bound_ref start_bound() const {
        if (_has_start) {
            return start_bound_ref(&_start_value, _start_inclusive, true);
        } else {
            return start_bound_ref(nullptr, false, false);
        }
    }
    end_bound_ref end_bound() const {
        if (_singular) {
            if (_has_start) {
                return end_bound_ref(&_start_value, _start_inclusive, true);
            } else {
                return end_bound_ref(nullptr, false, false);
            }
        } else if (_has_end) {
            return end_bound_ref(&_end_value, _end_inclusive, true);
        } else {
            return end_bound_ref(nullptr, false, false);
        }
    }
    static bool greater_than_or_equal(const end_bound_ref& end, const start_bound_ref& start, IntervalComparatorFor<T> auto&& cmp) {
        if (!end.present || !start.present) {
            return true;
        } else {
            return require_ordering_and_on_equal_return(
                cmp(*end.value, *start.value),
                std::strong_ordering::greater,
                end.inclusive && start.inclusive);
        }
    }
    static bool less_than(const end_bound_ref& end, const start_bound_ref& start, IntervalComparatorFor<T> auto&& cmp) {
        return !greater_than_or_equal(end, start, cmp);
    }
    static bool less_than_or_equal(const start_bound_ref& first, const start_bound_ref& second, IntervalComparatorFor<T> auto&& cmp) {
        if (!first.present) {
            return true;
        } else if (second.present) {
            return require_ordering_and_on_equal_return(
                cmp(*first.value, *second.value),
                std::strong_ordering::less,
                first.inclusive || !second.inclusive);
        } else {
            return false;
        }
    }
    static bool less_than(const start_bound_ref& first, const start_bound_ref& second, IntervalComparatorFor<T> auto&& cmp) {
        if (second.present) {
            if (!first.present) {
                return true;
            } else {
                return require_ordering_and_on_equal_return(
                    cmp(*first.value, *second.value),
                    std::strong_ordering::less,
                    first.inclusive && !second.inclusive);
            }
        } else {
            return false;
        }
    }
    static bool greater_than_or_equal(const end_bound_ref& first, const end_bound_ref& second, IntervalComparatorFor<T> auto&& cmp) {
        if (!first.present) {
            return true;
        } else if (second.present) {
            return require_ordering_and_on_equal_return(
                cmp(*first.value, *second.value),
                std::strong_ordering::greater,
                first.inclusive || !second.inclusive);
        } else {
            return false;
        }
    }
public:
    bool before(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
        auto s = start_bound();
        if (!s.present) {
            return false;
        }
        auto r = cmp(point, *s.value);
        if (r < 0) {
            return true;
        }
        if (!s.inclusive && r == 0) {
            return true;
        }
        return false;
    }
    bool other_is_before(const wrapping_interval<T>& o, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
        SCYLLA_ASSERT(!o.is_wrap_around(cmp));
        auto s = start_bound();
        auto oe = o.end_bound();
        if (!s.present || !oe.present) {
            return false;
        }
        auto r = cmp(*oe.value, *s.value);
        if (r < 0) {
            return true;
        }
        if (r > 0) {
            return false;
        }
        if (!oe.inclusive && !s.inclusive) {
            return true;
        }
        if (oe.inclusive != s.inclusive) {
            return true;
        }
        return false;
    }
    bool after(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
        auto e = end_bound();
        if (!e.present) {
            return false;
        }
        auto r = cmp(*e.value, point);
        if (r < 0) {
            return true;
        }
        if (!e.inclusive && r == 0) {
            return true;
        }
        return false;
    }
    bool overlaps(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);
        if (this_wraps && other_wraps) {
            return true;
        } else if (this_wraps) {
            auto unwrapped = unwrap();
            return other.overlaps(unwrapped.first, cmp) || other.overlaps(unwrapped.second, cmp);
        } else if (other_wraps) {
            auto unwrapped = other.unwrap();
            return overlaps(unwrapped.first, cmp) || overlaps(unwrapped.second, cmp);
        }
        auto s = start_bound();
        auto e = end_bound();
        auto os = other.start_bound();
        auto oe = other.end_bound();
        if (!s.present && !os.present) {
            return true;
        }
        return greater_than_or_equal(e, os, cmp) && greater_than_or_equal(oe, s, cmp);
    }
    static wrapping_interval make(bound start, bound end) {
        return wrapping_interval({std::move(start)}, {std::move(end)});
    }
    static constexpr wrapping_interval make_open_ended_both_sides() {
        return {};
    }
    static wrapping_interval make_singular(T value) {
        return {std::move(value)};
    }
    static wrapping_interval make_starting_with(bound b) {
        return wrapping_interval({std::move(b)}, {});
    }
    static wrapping_interval make_ending_with(bound b) {
        return wrapping_interval({}, {std::move(b)});
    }
    bool is_singular() const {
        return _singular;
    }
    bool is_full() const {
        return !_has_start && !_has_end;
    }
    void reverse() {
        if (!_singular) {
            if (_has_start && _has_end) {
                std::swap(_start_value, _end_value);
                std::swap(_has_start, _has_end);
                std::swap(_start_inclusive, _end_inclusive);
            } else {
                std::swap(_has_start, _has_end);
                std::swap(_start_inclusive, _end_inclusive);
                if (_has_start && !_has_end) {
                    new (&_start_value) T(std::move(_end_value));
                } else if (_has_end && !_has_start) {
                    new (&_end_value) T(std::move(_start_value));
                }
            }
        }
    }
    const optional<bound> start() const {
        if (_has_start) {
            return bound(_start_value, _start_inclusive);
        } else {
            return std::nullopt;
        }
    }
    const optional<bound> end() const {
        if (_singular) {
            if (_has_start) {
                return bound(_start_value, _start_inclusive);
            } else {
                return std::nullopt;
            }
        } else if (_has_end) {
            return bound(_end_value, _end_inclusive);
        } else {
            return std::nullopt;
        }
    }
    bool is_wrap_around(IntervalComparatorFor<T> auto&& cmp) const {
        auto s = start_bound();
        auto e = end_bound();
        if (e.present && s.present) {
            auto r = cmp(*e.value, *s.value);
            return r < 0 || (r == 0 && (!s.inclusive || !e.inclusive));
        } else {
            return false;
        }
    }
    std::pair<wrapping_interval, wrapping_interval> unwrap() const {
        return {
            { {}, end() },
            { start(), {} }
        };
    }
    bool contains(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        if (is_wrap_around(cmp)) {
            auto unwrapped = unwrap();
            return unwrapped.first.contains(point, cmp)
                   || unwrapped.second.contains(point, cmp);
        } else {
            return !before(point, cmp) && !after(point, cmp);
        }
    }
    bool contains(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);
        auto s = start_bound();
        auto e = end_bound();
        auto os = other.start_bound();
        auto oe = other.end_bound();
        if (this_wraps && other_wraps) {
            return require_ordering_and_on_equal_return(
                            cmp(*s.value, *os.value),
                            std::strong_ordering::less,
                            s.inclusive || !os.inclusive)
                && require_ordering_and_on_equal_return(
                            cmp(*e.value, *oe.value),
                            std::strong_ordering::greater,
                            e.inclusive || !oe.inclusive);
        }
        if (!this_wraps && !other_wraps) {
            return less_than_or_equal(s, os, cmp)
                    && greater_than_or_equal(e, oe, cmp);
        }
        if (other_wraps) {
            return !_has_start && !_has_end;
        }
        return (os.present && require_ordering_and_on_equal_return(
                                    cmp(*s.value, *os.value),
                                    std::strong_ordering::less,
                                    s.inclusive || !os.inclusive))
                || (oe.present && (require_ordering_and_on_equal_return(
                                        cmp(*e.value, *oe.value),
                                        std::strong_ordering::greater,
                                        e.inclusive || !oe.inclusive)));
    }
    std::vector<wrapping_interval> subtract(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        std::vector<wrapping_interval> result;
        std::list<wrapping_interval> left;
        std::list<wrapping_interval> right;
        if (is_wrap_around(cmp)) {
            auto u = unwrap();
            left.emplace_back(std::move(u.first));
            left.emplace_back(std::move(u.second));
        } else {
            left.push_back(*this);
        }
        if (other.is_wrap_around(cmp)) {
            auto u = other.unwrap();
            right.emplace_back(std::move(u.first));
            right.emplace_back(std::move(u.second));
        } else {
            right.push_back(other);
        }
        while (!left.empty() && !right.empty()) {
            auto& r1 = left.front();
            auto& r2 = right.front();
            if (less_than(r2.end_bound(), r1.start_bound(), cmp)) {
                right.pop_front();
            } else if (less_than(r1.end_bound(), r2.start_bound(), cmp)) {
                result.emplace_back(std::move(r1));
                left.pop_front();
            } else {
                auto tmp = std::move(r1);
                left.pop_front();
                auto r2e = r2.end_bound();
                auto tmpe = tmp.end_bound();
                if (!greater_than_or_equal(r2e, tmpe, cmp)) {
                    left.push_front({bound(*r2e.value, !r2e.inclusive), tmp.end()});
                }
                auto r2s = r2.start_bound();
                auto tmps = tmp.start_bound();
                if (!less_than_or_equal(r2s, tmps, cmp)) {
                    left.push_front({tmp.start(), bound(*r2s.value, !r2s.inclusive)});
                }
            }
        }
        std::ranges::copy(left, std::back_inserter(result));
        return result;
    }
    std::pair<wrapping_interval<T>, wrapping_interval<T>> split(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(contains(split_point, std::forward<decltype(cmp)>(cmp)));
        wrapping_interval left(start(), bound(split_point));
        wrapping_interval right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    std::optional<wrapping_interval<T>> split_after(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        auto e = end_bound();
        if (contains(split_point, std::forward<decltype(cmp)>(cmp))
                && (!e.present || cmp(split_point, *e.value) != 0)) {
            return wrapping_interval(bound(split_point, false), end());
        } else if (e.present && cmp(split_point, *e.value) >= 0) {
            return std::nullopt;
        } else {
            return *this;
        }
    }
    template<typename Bound, typename Transformer, typename U = transformed_type<Transformer>>
    static std::optional<typename wrapping_interval<U>::bound> transform_bound(Bound&& b, Transformer&& transformer) {
        if (b) {
            return { { transformer(std::forward<Bound>(b).value().value()), b->is_inclusive() } };
        }
        return {};
    }
    template<typename Transformer, typename U = transformed_type<Transformer>>
    wrapping_interval<U> transform(Transformer&& transformer) && {
        return wrapping_interval<U>(transform_bound(start(), transformer), transform_bound(end(), transformer), _singular);
    }
    template<typename Transformer, typename U = transformed_type<Transformer>>
    wrapping_interval<U> transform(Transformer&& transformer) const & {
        return wrapping_interval<U>(transform_bound(start(), transformer), transform_bound(end(), transformer), _singular);
    }
    bool equal(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        return start() == other.start()
               && end() == other.end()
               && _singular == other._singular;
    }
    bool operator==(const wrapping_interval& other) const {
        return start() == other.start() && end() == other.end() && _singular == other._singular;
    }
private:
    friend class interval<T>;
};

template<typename U>
struct fmt::formatter<wrapping_interval<U>> : fmt::formatter<string_view> {
    auto format(const wrapping_interval<U>& r, fmt::format_context& ctx) const {
        auto out = ctx.out();
        if (r.is_singular()) {
            return fmt::format_to(out, "{{{}}}", r._start_value);
        }

        if (!r._has_start) {
            out = fmt::format_to(out, "(-inf, ");
        } else {
            if (r._start_inclusive) {
                out = fmt::format_to(out, "[");
            } else {
                out = fmt::format_to(out, "(");
            }
            out = fmt::format_to(out, "{},", r._start_value);
        }

        if (!r._has_end) {
            out = fmt::format_to(out, "+inf)");
        } else {
            out = fmt::format_to(out, "{}", r._end_value);
            if (r._end_inclusive) {
                out = fmt::format_to(out, "]");
            } else {
                out = fmt::format_to(out, ")");
            }
        }

        return out;
    }
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const wrapping_interval<U>& r) {
    fmt::print(out, "{}", r);
    return out;
}

// An interval which can have inclusive, exclusive or open-ended bounds on each end.
// The end bound can never be smaller than the start bound.
template<typename T>
class interval {
    template <typename U>
    using optional = std::optional<U>;
public:
    using bound = interval_bound<T>;

    template <typename Transformer>
    using transformed_type = typename wrapping_interval<T>::template transformed_type<Transformer>;
private:
    wrapping_interval<T> _interval;
public:
    interval(T value)
        : _interval(std::move(value))
    { }
    constexpr interval() : interval({}, {}) { }
    // Can only be called if start <= end. IDL ctor.
    interval(optional<bound> start, optional<bound> end, bool singular = false)
        : _interval(std::move(start), std::move(end), singular)
    { }
    // Can only be called if !r.is_wrap_around().
    explicit interval(wrapping_interval<T>&& r)
        : _interval(std::move(r))
    { }
    // Can only be called if !r.is_wrap_around().
    explicit interval(const wrapping_interval<T>& r)
        : _interval(r)
    { }
    operator wrapping_interval<T>() const & {
        return _interval;
    }
    operator wrapping_interval<T>() && {
        return std::move(_interval);
    }

    // the point is before the interval.
    // Comparator must define a total ordering on T.
    bool before(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        return _interval.before(point, std::forward<decltype(cmp)>(cmp));
    }
    // the other interval is before this interval.
    // Comparator must define a total ordering on T.
    bool other_is_before(const interval<T>& o, IntervalComparatorFor<T> auto&& cmp) const {
        return _interval.other_is_before(o, std::forward<decltype(cmp)>(cmp));
    }
    // the point is after the interval.
    // Comparator must define a total ordering on T.
    bool after(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        return _interval.after(point, std::forward<decltype(cmp)>(cmp));
    }
    // check if two intervals overlap.
    // Comparator must define a total ordering on T.
    bool overlaps(const interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        // if both this and other have an open start, the two intervals will overlap.
        if (!start() && !other.start()) {
            return true;
        }

        return wrapping_interval<T>::greater_than_or_equal(_interval.end_bound(), other._interval.start_bound(), cmp)
            && wrapping_interval<T>::greater_than_or_equal(other._interval.end_bound(), _interval.start_bound(), cmp);
    }
    static interval make(bound start, bound end) {
        return interval({std::move(start)}, {std::move(end)});
    }
    static constexpr interval make_open_ended_both_sides() {
        return {{}, {}};
    }
    static interval make_singular(T value) {
        return {std::move(value)};
    }
    static interval make_starting_with(bound b) {
        return {{std::move(b)}, {}};
    }
    static interval make_ending_with(bound b) {
        return {{}, {std::move(b)}};
    }
    bool is_singular() const {
        return _interval.is_singular();
    }
    bool is_full() const {
        return _interval.is_full();
    }
    const optional<bound>& start() const {
        return _interval.start();
    }
    const optional<bound>& end() const {
        return _interval.end();
    }
    // the point is inside the interval
    // Comparator must define a total ordering on T.
    bool contains(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        return !before(point, cmp) && !after(point, cmp);
    }
    // Returns true iff all values contained by other are also contained by this.
    // Comparator must define a total ordering on T.
    bool contains(const interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        return wrapping_interval<T>::less_than_or_equal(_interval.start_bound(), other._interval.start_bound(), cmp)
                && wrapping_interval<T>::greater_than_or_equal(_interval.end_bound(), other._interval.end_bound(), cmp);
    }
    // Returns intervals which cover all values covered by this interval but not covered by the other interval.
    // Ranges are not overlapping and ordered.
    // Comparator must define a total ordering on T.
    std::vector<interval> subtract(const interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        auto subtracted = _interval.subtract(other._interval, std::forward<decltype(cmp)>(cmp));
        return subtracted | std::views::transform([](auto&& r) {
            return interval(std::move(r));
        }) | std::ranges::to<std::vector>();
    }
    // split interval in two around a split_point. split_point has to be inside the interval
    // split_point will belong to first interval
    // Comparator must define a total ordering on T.
    std::pair<interval<T>, interval<T>> split(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(contains(split_point, std::forward<decltype(cmp)>(cmp)));
        interval left(start(), bound(split_point));
        interval right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    // Create a sub-interval including values greater than the split_point. If split_point is after
    // the end, returns std::nullopt.
    std::optional<interval> split_after(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        if (end() && cmp(split_point, end()->value()) >= 0) {
            return std::nullopt;
        } else if (start() && cmp(split_point, start()->value()) < 0) {
            return *this;
        } else {
            return interval(interval_bound<T>(split_point, false), end());
        }
    }
    // Creates a new sub-interval which is the intersection of this interval and an interval starting with "start".
    // If there is no overlap, returns std::nullopt.
    std::optional<interval> trim_front(std::optional<bound>&& start, IntervalComparatorFor<T> auto&& cmp) const {
        return intersection(interval(std::move(start), {}), cmp);
    }
    // Transforms this interval into a new interval of a different value type
    // Supplied transformer should transform value of type T (the old type) into value of type U (the new type).
    template<typename Transformer, typename U = transformed_type<Transformer>>
    interval<U> transform(Transformer&& transformer) && {
        return interval<U>(std::move(_interval).transform(std::forward<Transformer>(transformer)));
    }
    template<typename Transformer, typename U = transformed_type<Transformer>>
    interval<U> transform(Transformer&& transformer) const & {
        return interval<U>(_interval.transform(std::forward<Transformer>(transformer)));
    }
    bool equal(const interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        return _interval.equal(other._interval, std::forward<decltype(cmp)>(cmp));
    }
    bool operator==(const interval& other) const {
        return _interval == other._interval;
    }
    // Takes a vector of possibly overlapping intervals and returns a vector containing
    // a set of non-overlapping intervals covering the same values.
    template<IntervalComparatorFor<T> Comparator, typename IntervalVec>
    requires requires (IntervalVec vec) {
        { vec.begin() } -> std::random_access_iterator;
        { vec.end() } -> std::random_access_iterator;
        { vec.reserve(1) };
        { vec.front() } -> std::same_as<interval&>;
    }
    static IntervalVec deoverlap(IntervalVec intervals, Comparator&& cmp) {
        auto size = intervals.size();
        if (size <= 1) {
            return intervals;
        }

        std::sort(intervals.begin(), intervals.end(), [&](auto&& r1, auto&& r2) {
            return wrapping_interval<T>::less_than(r1._interval.start_bound(), r2._interval.start_bound(), cmp);
        });

        IntervalVec deoverlapped_intervals;
        deoverlapped_intervals.reserve(size);

        auto&& current = intervals[0];
        for (auto&& r : intervals | std::views::drop(1)) {
            bool includes_end = wrapping_interval<T>::greater_than_or_equal(r._interval.end_bound(), current._interval.start_bound(), cmp)
                                && wrapping_interval<T>::greater_than_or_equal(current._interval.end_bound(), r._interval.end_bound(), cmp);
            if (includes_end) {
                continue; // last.start <= r.start <= r.end <= last.end
            }
            bool includes_start = wrapping_interval<T>::greater_than_or_equal(current._interval.end_bound(), r._interval.start_bound(), cmp);
            if (includes_start) {
                current = interval(std::move(current.start()), std::move(r.end()));
            } else {
                deoverlapped_intervals.emplace_back(std::move(current));
                current = std::move(r);
            }
        }

        deoverlapped_intervals.emplace_back(std::move(current));
        return deoverlapped_intervals;
    }

private:
    // These private functions optimize the case where a sequence supports the
    // lower and upper bound operations more efficiently, as is the case with
    // some boost containers.
    struct std_ {};
    struct built_in_ : std_ {};

    template<typename Range, IntervalLessComparatorFor<T> LessComparator,
             typename = decltype(std::declval<Range>().lower_bound(std::declval<T>(), std::declval<LessComparator>()))>
    typename std::ranges::const_iterator_t<Range> do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.lower_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator,
             typename = decltype(std::declval<Range>().upper_bound(std::declval<T>(), std::declval<LessComparator>()))>
    typename std::ranges::const_iterator_t<Range> do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.upper_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::ranges::const_iterator_t<Range> do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::lower_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::ranges::const_iterator_t<Range> do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::upper_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }
public:
    // Return the lower bound of the specified sequence according to these bounds.
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::ranges::const_iterator_t<Range> lower_bound(Range&& r, LessComparator&& cmp) const {
        return _has_start
            ? (_start_inclusive
                ? do_lower_bound(_start_value, std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : do_upper_bound(_start_value, std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_()))
            : std::ranges::begin(r);
    }
    // Return the upper bound of the specified sequence according to these bounds.
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::ranges::const_iterator_t<Range> upper_bound(Range&& r, LessComparator&& cmp) const {
        return _has_end
             ? (_end_inclusive
                ? do_upper_bound(_end_value, std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : do_lower_bound(_end_value, std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_()))
             : (_singular
                ? do_upper_bound(_start_value, std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : std::ranges::end(r));
    }
    // Returns a subset of the range that is within these bounds.
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    std::ranges::subrange<std::ranges::const_iterator_t<Range>>
    slice(Range&& range, LessComparator&& cmp) const {
        return std::ranges::subrange(lower_bound(range, cmp), upper_bound(range, cmp));
    }

    // Returns the intersection between this interval and other.
    std::optional<interval> intersection(const interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        auto p = std::minmax(_interval, other._interval, [&cmp] (auto&& a, auto&& b) {
            return wrapping_interval<T>::less_than(a.start_bound(), b.start_bound(), cmp);
        });
        if (wrapping_interval<T>::greater_than_or_equal(p.first.end_bound(), p.second.start_bound(), cmp)) {
            auto end = std::min(p.first.end_bound(), p.second.end_bound(), [&cmp] (auto&& a, auto&& b) {
                return !wrapping_interval<T>::greater_than_or_equal(a, b, cmp);
            });
            return interval(p.second.start(), end.b);
        }
        return {};
    }

    friend class fmt::formatter<interval<T>>;
};

template<typename U>
struct fmt::formatter<interval<U>> : fmt::formatter<string_view> {
    auto format(const interval<U>& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", r._interval);
    }
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const interval<U>& r) {
    fmt::print(out, "{}", r);
    return out;
}

template<template<typename> typename T, typename U>
concept Interval = std::is_same<T<U>, wrapping_interval<U>>::value || std::is_same<T<U>, interval<U>>::value;

// Allow using interval<T> in a hash table. The hash function 31 * left +
// right is the same one used by Cassandra's AbstractBounds.hashCode().
namespace std {

template<typename T>
struct hash<wrapping_interval<T>> {
    using argument_type = wrapping_interval<T>;
    using result_type = decltype(std::hash<T>()(std::declval<T>()));
    result_type operator()(argument_type const& s) const {
        auto hash = std::hash<T>();
        auto left = s._has_start ? hash(s._start_value) : 0;
        auto right = s._has_end ? hash(s._end_value) : 0;
        return 31 * left + right;
    }
};

template<typename T>
struct hash<interval<T>> {
    using argument_type = interval<T>;
    using result_type = decltype(std::hash<T>()(std::declval<T>()));
    result_type operator()(argument_type const& s) const {
        return hash<wrapping_interval<T>>()(s);
    }
};

}
