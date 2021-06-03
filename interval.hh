/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <list>
#include <vector>
#include <optional>
#include <iosfwd>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/sliced.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <compare>

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
    template<typename Comparator>
    bool equal(const interval_bound& other, Comparator&& cmp) const {
        return _inclusive == other._inclusive && cmp(_value, other._value) == 0;
    }
};

template<typename T>
class nonwrapping_interval;

template <typename T>
using interval = nonwrapping_interval<T>;

// An interval which can have inclusive, exclusive or open-ended bounds on each end.
// The end bound can be smaller than the start bound.
template<typename T>
class wrapping_interval {
    template <typename U>
    using optional = std::optional<U>;
public:
    using bound = interval_bound<T>;

    template <typename Transformer>
    using transformed_type = typename std::remove_cv_t<std::remove_reference_t<std::result_of_t<Transformer(T)>>>;
private:
    optional<bound> _start;
    optional<bound> _end;
    bool _singular;
public:
    wrapping_interval(optional<bound> start, optional<bound> end, bool singular = false)
        : _start(std::move(start))
        , _singular(singular) {
        if (!_singular) {
            _end = std::move(end);
        }
    }
    wrapping_interval(T value)
        : _start(bound(std::move(value), true))
        , _end()
        , _singular(true)
    { }
    wrapping_interval() : wrapping_interval({}, {}) { }
private:
    // Bound wrappers for compile-time dispatch and safety.
    struct start_bound_ref { const optional<bound>& b; };
    struct end_bound_ref { const optional<bound>& b; };

    start_bound_ref start_bound() const { return { start() }; }
    end_bound_ref end_bound() const { return { end() }; }

    template<typename Comparator>
    static bool greater_than_or_equal(end_bound_ref end, start_bound_ref start, Comparator&& cmp) {
        return !end.b || !start.b || require_ordering_and_on_equal_return(
                cmp(end.b->value(), start.b->value()) <=> 0,
                std::strong_ordering::greater,
                end.b->is_inclusive() && start.b->is_inclusive());
    }

    template<typename Comparator>
    static bool less_than(end_bound_ref end, start_bound_ref start, Comparator&& cmp) {
        return !greater_than_or_equal(end, start, cmp);
    }

    template<typename Comparator>
    static bool less_than_or_equal(start_bound_ref first, start_bound_ref second, Comparator&& cmp) {
        return !first.b || (second.b && require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()) <=> 0,
                std::strong_ordering::less,
                first.b->is_inclusive() || !second.b->is_inclusive()));
    }

    template<typename Comparator>
    static bool less_than(start_bound_ref first, start_bound_ref second, Comparator&& cmp) {
        return second.b && (!first.b || require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()) <=> 0,
                std::strong_ordering::less,
                first.b->is_inclusive() && !second.b->is_inclusive()));
    }

    template<typename Comparator>
    static bool greater_than_or_equal(end_bound_ref first, end_bound_ref second, Comparator&& cmp) {
        return !first.b || (second.b && require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()) <=> 0,
                std::strong_ordering::greater,
                first.b->is_inclusive() || !second.b->is_inclusive()));
    }
public:
    // the point is before the interval (works only for non wrapped intervals)
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool before(const T& point, Comparator&& cmp) const {
        assert(!is_wrap_around(cmp));
        if (!start()) {
            return false; //open start, no points before
        }
        auto r = cmp(point, start()->value());
        if (r < 0) {
            return true;
        }
        if (!start()->is_inclusive() && r == 0) {
            return true;
        }
        return false;
    }
    // the point is after the interval (works only for non wrapped intervals)
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool after(const T& point, Comparator&& cmp) const {
        assert(!is_wrap_around(cmp));
        if (!end()) {
            return false; //open end, no points after
        }
        auto r = cmp(end()->value(), point);
        if (r < 0) {
            return true;
        }
        if (!end()->is_inclusive() && r == 0) {
            return true;
        }
        return false;
    }
    // check if two intervals overlap.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool overlaps(const wrapping_interval& other, Comparator&& cmp) const {
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

        // No interval should reach this point as wrap around.
        assert(!this_wraps);
        assert(!other_wraps);

        // if both this and other have an open start, the two intervals will overlap.
        if (!start() && !other.start()) {
            return true;
        }

        return greater_than_or_equal(end_bound(), other.start_bound(), cmp)
            && greater_than_or_equal(other.end_bound(), start_bound(), cmp);
    }
    static wrapping_interval make(bound start, bound end) {
        return wrapping_interval({std::move(start)}, {std::move(end)});
    }
    static wrapping_interval make_open_ended_both_sides() {
        return {{}, {}};
    }
    static wrapping_interval make_singular(T value) {
        return {std::move(value)};
    }
    static wrapping_interval make_starting_with(bound b) {
        return {{std::move(b)}, {}};
    }
    static wrapping_interval make_ending_with(bound b) {
        return {{}, {std::move(b)}};
    }
    bool is_singular() const {
        return _singular;
    }
    bool is_full() const {
        return !_start && !_end;
    }
    void reverse() {
        if (!_singular) {
            std::swap(_start, _end);
        }
    }
    const optional<bound>& start() const {
        return _start;
    }
    const optional<bound>& end() const {
        return _singular ? _start : _end;
    }
    // Range is a wrap around if end value is smaller than the start value
    // or they're equal and at least one bound is not inclusive.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool is_wrap_around(Comparator&& cmp) const {
        if (_end && _start) {
            auto r = cmp(end()->value(), start()->value());
            return r < 0
                   || (r == 0 && (!start()->is_inclusive() || !end()->is_inclusive()));
        } else {
            return false; // open ended interval or singular interval don't wrap around
        }
    }
    // Converts a wrap-around interval to two non-wrap-around intervals.
    // The returned intervals are not overlapping and ordered.
    // Call only when is_wrap_around().
    std::pair<wrapping_interval, wrapping_interval> unwrap() const {
        return {
            { {}, end() },
            { start(), {} }
        };
    }
    // the point is inside the interval
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool contains(const T& point, Comparator&& cmp) const {
        if (is_wrap_around(cmp)) {
            auto unwrapped = unwrap();
            return unwrapped.first.contains(point, cmp)
                   || unwrapped.second.contains(point, cmp);
        } else {
            return !before(point, cmp) && !after(point, cmp);
        }
    }
    // Returns true iff all values contained by other are also contained by this.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool contains(const wrapping_interval& other, Comparator&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);

        if (this_wraps && other_wraps) {
            return require_ordering_and_on_equal_return(
                            cmp(start()->value(), other.start()->value()) <=> 0,
                            std::strong_ordering::less,
                            start()->is_inclusive() || !other.start()->is_inclusive())
                && require_ordering_and_on_equal_return(
                            cmp(end()->value(), other.end()->value()) <=> 0,
                            std::strong_ordering::greater,
                            end()->is_inclusive() || !other.end()->is_inclusive());
        }

        if (!this_wraps && !other_wraps) {
            return less_than_or_equal(start_bound(), other.start_bound(), cmp)
                    && greater_than_or_equal(end_bound(), other.end_bound(), cmp);
        }

        if (other_wraps) { // && !this_wraps
            return !start() && !end();
        }

        // !other_wraps && this_wraps
        return (other.start() && require_ordering_and_on_equal_return(
                                    cmp(start()->value(), other.start()->value()) <=> 0,
                                    std::strong_ordering::less,
                                    start()->is_inclusive() || !other.start()->is_inclusive()))
                || (other.end() && (require_ordering_and_on_equal_return(
                                        cmp(end()->value(), other.end()->value()) <=> 0,
                                        std::strong_ordering::greater,
                                        end()->is_inclusive() || !other.end()->is_inclusive())));
    }
    // Returns intervals which cover all values covered by this interval but not covered by the other interval.
    // Ranges are not overlapping and ordered.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::vector<wrapping_interval> subtract(const wrapping_interval& other, Comparator&& cmp) const {
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

        // left and right contain now non-overlapping, ordered intervals

        while (!left.empty() && !right.empty()) {
            auto& r1 = left.front();
            auto& r2 = right.front();
            if (less_than(r2.end_bound(), r1.start_bound(), cmp)) {
                right.pop_front();
            } else if (less_than(r1.end_bound(), r2.start_bound(), cmp)) {
                result.emplace_back(std::move(r1));
                left.pop_front();
            } else { // Overlap
                auto tmp = std::move(r1);
                left.pop_front();
                if (!greater_than_or_equal(r2.end_bound(), tmp.end_bound(), cmp)) {
                    left.push_front({bound(r2.end()->value(), !r2.end()->is_inclusive()), tmp.end()});
                }
                if (!less_than_or_equal(r2.start_bound(), tmp.start_bound(), cmp)) {
                    left.push_front({tmp.start(), bound(r2.start()->value(), !r2.start()->is_inclusive())});
                }
            }
        }

        boost::copy(left, std::back_inserter(result));

        // TODO: Merge adjacent intervals (optimization)
        return result;
    }
    // split interval in two around a split_point. split_point has to be inside the interval
    // split_point will belong to first interval
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::pair<wrapping_interval<T>, wrapping_interval<T>> split(const T& split_point, Comparator&& cmp) const {
        assert(contains(split_point, std::forward<Comparator>(cmp)));
        wrapping_interval left(start(), bound(split_point));
        wrapping_interval right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    // Create a sub-interval including values greater than the split_point. Returns std::nullopt if
    // split_point is after the end (but not included in the interval, in case of wraparound intervals)
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::optional<wrapping_interval<T>> split_after(const T& split_point, Comparator&& cmp) const {
        if (contains(split_point, std::forward<Comparator>(cmp))
                && (!end() || cmp(split_point, end()->value()) != 0)) {
            return wrapping_interval(bound(split_point, false), end());
        } else if (end() && cmp(split_point, end()->value()) >= 0) {
            // whether to return std::nullopt or the full interval is not
            // well-defined for wraparound intervals; we return nullopt
            // if split_point is after the end.
            return std::nullopt;
        } else {
            return *this;
        }
    }
    template<typename Bound, typename Transformer, typename U = transformed_type<Transformer>>
    static std::optional<typename wrapping_interval<U>::bound> transform_bound(Bound&& b, Transformer&& transformer) {
        if (b) {
            return { { transformer(std::forward<Bound>(b).value().value()), b->is_inclusive() } };
        };
        return {};
    }
    // Transforms this interval into a new interval of a different value type
    // Supplied transformer should transform value of type T (the old type) into value of type U (the new type).
    template<typename Transformer, typename U = transformed_type<Transformer>>
    wrapping_interval<U> transform(Transformer&& transformer) && {
        return wrapping_interval<U>(transform_bound(std::move(_start), transformer), transform_bound(std::move(_end), transformer), _singular);
    }
    template<typename Transformer, typename U = transformed_type<Transformer>>
    wrapping_interval<U> transform(Transformer&& transformer) const & {
        return wrapping_interval<U>(transform_bound(_start, transformer), transform_bound(_end, transformer), _singular);
    }
    template<typename Comparator>
    bool equal(const wrapping_interval& other, Comparator&& cmp) const {
        return bool(_start) == bool(other._start)
               && bool(_end) == bool(other._end)
               && (!_start || _start->equal(*other._start, cmp))
               && (!_end || _end->equal(*other._end, cmp))
               && _singular == other._singular;
    }
    bool operator==(const wrapping_interval& other) const {
        return (_start == other._start) && (_end == other._end) && (_singular == other._singular);
    }

    template<typename U>
    friend std::ostream& operator<<(std::ostream& out, const wrapping_interval<U>& r);
private:
    friend class nonwrapping_interval<T>;
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const wrapping_interval<U>& r) {
    if (r.is_singular()) {
        return out << "{" << r.start()->value() << "}";
    }

    if (!r.start()) {
        out << "(-inf, ";
    } else {
        if (r.start()->is_inclusive()) {
            out << "[";
        } else {
            out << "(";
        }
        out << r.start()->value() << ", ";
    }

    if (!r.end()) {
        out << "+inf)";
    } else {
        out << r.end()->value();
        if (r.end()->is_inclusive()) {
            out << "]";
        } else {
            out << ")";
        }
    }

    return out;
}

// An interval which can have inclusive, exclusive or open-ended bounds on each end.
// The end bound can never be smaller than the start bound.
template<typename T>
class nonwrapping_interval {
    template <typename U>
    using optional = std::optional<U>;
public:
    using bound = interval_bound<T>;

    template <typename Transformer>
    using transformed_type = typename wrapping_interval<T>::template transformed_type<Transformer>;
private:
    wrapping_interval<T> _interval;
public:
    nonwrapping_interval(T value)
        : _interval(std::move(value))
    { }
    nonwrapping_interval() : nonwrapping_interval({}, {}) { }
    // Can only be called if start <= end. IDL ctor.
    nonwrapping_interval(optional<bound> start, optional<bound> end, bool singular = false)
        : _interval(std::move(start), std::move(end), singular)
    { }
    // Can only be called if !r.is_wrap_around().
    explicit nonwrapping_interval(wrapping_interval<T>&& r)
        : _interval(std::move(r))
    { }
    // Can only be called if !r.is_wrap_around().
    explicit nonwrapping_interval(const wrapping_interval<T>& r)
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
    template<typename Comparator>
    bool before(const T& point, Comparator&& cmp) const {
        return _interval.before(point, std::forward<Comparator>(cmp));
    }
    // the point is after the interval.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool after(const T& point, Comparator&& cmp) const {
        return _interval.after(point, std::forward<Comparator>(cmp));
    }
    // check if two intervals overlap.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool overlaps(const nonwrapping_interval& other, Comparator&& cmp) const {
        // if both this and other have an open start, the two intervals will overlap.
        if (!start() && !other.start()) {
            return true;
        }

        return wrapping_interval<T>::greater_than_or_equal(_interval.end_bound(), other._interval.start_bound(), cmp)
            && wrapping_interval<T>::greater_than_or_equal(other._interval.end_bound(), _interval.start_bound(), cmp);
    }
    static nonwrapping_interval make(bound start, bound end) {
        return nonwrapping_interval({std::move(start)}, {std::move(end)});
    }
    static nonwrapping_interval make_open_ended_both_sides() {
        return {{}, {}};
    }
    static nonwrapping_interval make_singular(T value) {
        return {std::move(value)};
    }
    static nonwrapping_interval make_starting_with(bound b) {
        return {{std::move(b)}, {}};
    }
    static nonwrapping_interval make_ending_with(bound b) {
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
    template<typename Comparator>
    bool contains(const T& point, Comparator&& cmp) const {
        return !before(point, cmp) && !after(point, cmp);
    }
    // Returns true iff all values contained by other are also contained by this.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    bool contains(const nonwrapping_interval& other, Comparator&& cmp) const {
        return wrapping_interval<T>::less_than_or_equal(_interval.start_bound(), other._interval.start_bound(), cmp)
                && wrapping_interval<T>::greater_than_or_equal(_interval.end_bound(), other._interval.end_bound(), cmp);
    }
    // Returns intervals which cover all values covered by this interval but not covered by the other interval.
    // Ranges are not overlapping and ordered.
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::vector<nonwrapping_interval> subtract(const nonwrapping_interval& other, Comparator&& cmp) const {
        auto subtracted = _interval.subtract(other._interval, std::forward<Comparator>(cmp));
        return boost::copy_range<std::vector<nonwrapping_interval>>(subtracted | boost::adaptors::transformed([](auto&& r) {
            return nonwrapping_interval(std::move(r));
        }));
    }
    // split interval in two around a split_point. split_point has to be inside the interval
    // split_point will belong to first interval
    // Comparator must define a total ordering on T.
    template<typename Comparator>
    std::pair<nonwrapping_interval<T>, nonwrapping_interval<T>> split(const T& split_point, Comparator&& cmp) const {
        assert(contains(split_point, std::forward<Comparator>(cmp)));
        nonwrapping_interval left(start(), bound(split_point));
        nonwrapping_interval right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    // Create a sub-interval including values greater than the split_point. If split_point is after
    // the end, returns std::nullopt.
    template<typename Comparator>
    std::optional<nonwrapping_interval> split_after(const T& split_point, Comparator&& cmp) const {
        if (end() && cmp(split_point, end()->value()) >= 0) {
            return std::nullopt;
        } else if (start() && cmp(split_point, start()->value()) < 0) {
            return *this;
        } else {
            return nonwrapping_interval(interval_bound<T>(split_point, false), end());
        }
    }
    // Creates a new sub-interval which is the intersection of this interval and an interval starting with "start".
    // If there is no overlap, returns std::nullopt.
    template<typename Comparator>
    std::optional<nonwrapping_interval> trim_front(std::optional<bound>&& start, Comparator&& cmp) const {
        return intersection(nonwrapping_interval(std::move(start), {}), cmp);
    }
    // Transforms this interval into a new interval of a different value type
    // Supplied transformer should transform value of type T (the old type) into value of type U (the new type).
    template<typename Transformer, typename U = transformed_type<Transformer>>
    nonwrapping_interval<U> transform(Transformer&& transformer) && {
        return nonwrapping_interval<U>(std::move(_interval).transform(std::forward<Transformer>(transformer)));
    }
    template<typename Transformer, typename U = transformed_type<Transformer>>
    nonwrapping_interval<U> transform(Transformer&& transformer) const & {
        return nonwrapping_interval<U>(_interval.transform(std::forward<Transformer>(transformer)));
    }
    template<typename Comparator>
    bool equal(const nonwrapping_interval& other, Comparator&& cmp) const {
        return _interval.equal(other._interval, std::forward<Comparator>(cmp));
    }
    bool operator==(const nonwrapping_interval& other) const {
        return _interval == other._interval;
    }
    // Takes a vector of possibly overlapping intervals and returns a vector containing
    // a set of non-overlapping intervals covering the same values.
    template<typename Comparator>
    static std::vector<nonwrapping_interval> deoverlap(std::vector<nonwrapping_interval> intervals, Comparator&& cmp) {
        auto size = intervals.size();
        if (size <= 1) {
            return intervals;
        }

        std::sort(intervals.begin(), intervals.end(), [&](auto&& r1, auto&& r2) {
            return wrapping_interval<T>::less_than(r1._interval.start_bound(), r2._interval.start_bound(), cmp);
        });

        std::vector<nonwrapping_interval> deoverlapped_intervals;
        deoverlapped_intervals.reserve(size);

        auto&& current = intervals[0];
        for (auto&& r : intervals | boost::adaptors::sliced(1, intervals.size())) {
            bool includes_end = wrapping_interval<T>::greater_than_or_equal(r._interval.end_bound(), current._interval.start_bound(), cmp)
                                && wrapping_interval<T>::greater_than_or_equal(current._interval.end_bound(), r._interval.end_bound(), cmp);
            if (includes_end) {
                continue; // last.start <= r.start <= r.end <= last.end
            }
            bool includes_start = wrapping_interval<T>::greater_than_or_equal(current._interval.end_bound(), r._interval.start_bound(), cmp);
            if (includes_start) {
                current = nonwrapping_interval(std::move(current.start()), std::move(r.end()));
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

    template<typename Range, typename LessComparator,
             typename = decltype(std::declval<Range>().lower_bound(std::declval<T>(), std::declval<LessComparator>()))>
    typename std::remove_reference<Range>::type::const_iterator do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.lower_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, typename LessComparator,
             typename = decltype(std::declval<Range>().upper_bound(std::declval<T>(), std::declval<LessComparator>()))>
    typename std::remove_reference<Range>::type::const_iterator do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.upper_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, typename LessComparator>
    typename std::remove_reference<Range>::type::const_iterator do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::lower_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, typename LessComparator>
    typename std::remove_reference<Range>::type::const_iterator do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::upper_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }
public:
    // Return the lower bound of the specified sequence according to these bounds.
    template<typename Range, typename LessComparator>
    typename std::remove_reference<Range>::type::const_iterator lower_bound(Range&& r, LessComparator&& cmp) const {
        return start()
            ? (start()->is_inclusive()
                ? do_lower_bound(start()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : do_upper_bound(start()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_()))
            : std::cbegin(r);
    }
    // Return the upper bound of the specified sequence according to these bounds.
    template<typename Range, typename LessComparator>
    typename std::remove_reference<Range>::type::const_iterator upper_bound(Range&& r, LessComparator&& cmp) const {
        return end()
             ? (end()->is_inclusive()
                ? do_upper_bound(end()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : do_lower_bound(end()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_()))
             : (is_singular()
                ? do_upper_bound(start()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : std::cend(r));
    }
    // Returns a subset of the range that is within these bounds.
    template<typename Range, typename LessComparator>
    boost::iterator_range<typename std::remove_reference<Range>::type::const_iterator>
    slice(Range&& range, LessComparator&& cmp) const {
        return boost::make_iterator_range(lower_bound(range, cmp), upper_bound(range, cmp));
    }

    // Returns the intersection between this interval and other.
    template<typename Comparator>
    std::optional<nonwrapping_interval> intersection(const nonwrapping_interval& other, Comparator&& cmp) const {
        auto p = std::minmax(_interval, other._interval, [&cmp] (auto&& a, auto&& b) {
            return wrapping_interval<T>::less_than(a.start_bound(), b.start_bound(), cmp);
        });
        if (wrapping_interval<T>::greater_than_or_equal(p.first.end_bound(), p.second.start_bound(), cmp)) {
            auto end = std::min(p.first.end_bound(), p.second.end_bound(), [&cmp] (auto&& a, auto&& b) {
                return !wrapping_interval<T>::greater_than_or_equal(a, b, cmp);
            });
            return nonwrapping_interval(p.second.start(), end.b);
        }
        return {};
    }

    template<typename U>
    friend std::ostream& operator<<(std::ostream& out, const nonwrapping_interval<U>& r);
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const nonwrapping_interval<U>& r) {
    return out << r._interval;
}

template<template<typename> typename T, typename U>
concept Interval = std::is_same<T<U>, wrapping_interval<U>>::value || std::is_same<T<U>, nonwrapping_interval<U>>::value;

// Allow using interval<T> in a hash table. The hash function 31 * left +
// right is the same one used by Cassandra's AbstractBounds.hashCode().
namespace std {

template<typename T>
struct hash<wrapping_interval<T>> {
    using argument_type = wrapping_interval<T>;
    using result_type = decltype(std::hash<T>()(std::declval<T>()));
    result_type operator()(argument_type const& s) const {
        auto hash = std::hash<T>();
        auto left = s.start() ? hash(s.start()->value()) : 0;
        auto right = s.end() ? hash(s.end()->value()) : 0;
        return 31 * left + right;
    }
};

template<typename T>
struct hash<nonwrapping_interval<T>> {
    using argument_type = nonwrapping_interval<T>;
    using result_type = decltype(std::hash<T>()(std::declval<T>()));
    result_type operator()(argument_type const& s) const {
        return hash<wrapping_interval<T>>()(s);
    }
};

}
