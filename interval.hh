/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <list>
#include <vector>
#include <optional>
#include <iosfwd>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/sliced.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <compare>
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
    constexpr wrapping_interval() : wrapping_interval({}, {}) { }
private:
    // Bound wrappers for compile-time dispatch and safety.
    struct start_bound_ref { const optional<bound>& b; };
    struct end_bound_ref { const optional<bound>& b; };

    start_bound_ref start_bound() const { return { start() }; }
    end_bound_ref end_bound() const { return { end() }; }

    static bool greater_than_or_equal(end_bound_ref end, start_bound_ref start, IntervalComparatorFor<T> auto&& cmp) {
        return !end.b || !start.b || require_ordering_and_on_equal_return(
                cmp(end.b->value(), start.b->value()),
                std::strong_ordering::greater,
                end.b->is_inclusive() && start.b->is_inclusive());
    }

    static bool less_than(end_bound_ref end, start_bound_ref start, IntervalComparatorFor<T> auto&& cmp) {
        return !greater_than_or_equal(end, start, cmp);
    }

    static bool less_than_or_equal(start_bound_ref first, start_bound_ref second, IntervalComparatorFor<T> auto&& cmp) {
        return !first.b || (second.b && require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()),
                std::strong_ordering::less,
                first.b->is_inclusive() || !second.b->is_inclusive()));
    }

    static bool less_than(start_bound_ref first, start_bound_ref second, IntervalComparatorFor<T> auto&& cmp) {
        return second.b && (!first.b || require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()),
                std::strong_ordering::less,
                first.b->is_inclusive() && !second.b->is_inclusive()));
    }

    static bool greater_than_or_equal(end_bound_ref first, end_bound_ref second, IntervalComparatorFor<T> auto&& cmp) {
        return !first.b || (second.b && require_ordering_and_on_equal_return(
                cmp(first.b->value(), second.b->value()),
                std::strong_ordering::greater,
                first.b->is_inclusive() || !second.b->is_inclusive()));
    }
public:
    // the point is before the interval (works only for non wrapped intervals)
    // Comparator must define a total ordering on T.
    bool before(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
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
    // the other interval is before this interval (works only for non wrapped intervals)
    // Comparator must define a total ordering on T.
    bool other_is_before(const wrapping_interval<T>& o, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
        SCYLLA_ASSERT(!o.is_wrap_around(cmp));
        if (!start() || !o.end()) {
            return false;
        }
        auto r = cmp(o.end()->value(), start()->value());
        if (r < 0) {
            return true;
        }
        if (r > 0) {
            return false;
        }
        // o.end()->value() == start()->value(), we decide based on inclusiveness
        const auto ei = o.end()->is_inclusive();
        const auto si = start()->is_inclusive();
        if (!ei && !si) {
            return true;
        }
        // At least one is inclusive, check that the other isn't
        if (ei != si) {
            return true;
        }
        return false;
    }
    // the point is after the interval (works only for non wrapped intervals)
    // Comparator must define a total ordering on T.
    bool after(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(!is_wrap_around(cmp));
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

        // No interval should reach this point as wrap around.
        SCYLLA_ASSERT(!this_wraps);
        SCYLLA_ASSERT(!other_wraps);

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
    static constexpr wrapping_interval make_open_ended_both_sides() {
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
    bool is_wrap_around(IntervalComparatorFor<T> auto&& cmp) const {
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
    bool contains(const T& point, IntervalComparatorFor<T> auto&& cmp) const {
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
    bool contains(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        bool this_wraps = is_wrap_around(cmp);
        bool other_wraps = other.is_wrap_around(cmp);

        if (this_wraps && other_wraps) {
            return require_ordering_and_on_equal_return(
                            cmp(start()->value(), other.start()->value()),
                            std::strong_ordering::less,
                            start()->is_inclusive() || !other.start()->is_inclusive())
                && require_ordering_and_on_equal_return(
                            cmp(end()->value(), other.end()->value()),
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
                                    cmp(start()->value(), other.start()->value()),
                                    std::strong_ordering::less,
                                    start()->is_inclusive() || !other.start()->is_inclusive()))
                || (other.end() && (require_ordering_and_on_equal_return(
                                        cmp(end()->value(), other.end()->value()),
                                        std::strong_ordering::greater,
                                        end()->is_inclusive() || !other.end()->is_inclusive())));
    }
    // Returns intervals which cover all values covered by this interval but not covered by the other interval.
    // Ranges are not overlapping and ordered.
    // Comparator must define a total ordering on T.
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
    std::pair<wrapping_interval<T>, wrapping_interval<T>> split(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        SCYLLA_ASSERT(contains(split_point, std::forward<decltype(cmp)>(cmp)));
        wrapping_interval left(start(), bound(split_point));
        wrapping_interval right(bound(split_point, false), end());
        return std::make_pair(std::move(left), std::move(right));
    }
    // Create a sub-interval including values greater than the split_point. Returns std::nullopt if
    // split_point is after the end (but not included in the interval, in case of wraparound intervals)
    // Comparator must define a total ordering on T.
    std::optional<wrapping_interval<T>> split_after(const T& split_point, IntervalComparatorFor<T> auto&& cmp) const {
        if (contains(split_point, std::forward<decltype(cmp)>(cmp))
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
    bool equal(const wrapping_interval& other, IntervalComparatorFor<T> auto&& cmp) const {
        return bool(_start) == bool(other._start)
               && bool(_end) == bool(other._end)
               && (!_start || _start->equal(*other._start, cmp))
               && (!_end || _end->equal(*other._end, cmp))
               && _singular == other._singular;
    }
    bool operator==(const wrapping_interval& other) const {
        return (_start == other._start) && (_end == other._end) && (_singular == other._singular);
    }

private:
    friend class interval<T>;
};

template<typename U>
struct fmt::formatter<wrapping_interval<U>> : fmt::formatter<string_view> {
    auto format(const wrapping_interval<U>& r, fmt::format_context& ctx) const {
        auto out = ctx.out();
        if (r.is_singular()) {
            return fmt::format_to(out, "{{{}}}", r.start()->value());
        }

        if (!r.start()) {
            out = fmt::format_to(out, "(-inf, ");
        } else {
            if (r.start()->is_inclusive()) {
                out = fmt::format_to(out, "[");
            } else {
                out = fmt::format_to(out, "(");
            }
            out = fmt::format_to(out, "{},", r.start()->value());
        }

        if (!r.end()) {
            out = fmt::format_to(out, "+inf)");
        } else {
            out = fmt::format_to(out, "{}", r.end()->value());
            if (r.end()->is_inclusive()) {
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
        return boost::copy_range<std::vector<interval>>(subtracted | boost::adaptors::transformed([](auto&& r) {
            return interval(std::move(r));
        }));
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
        for (auto&& r : intervals | boost::adaptors::sliced(1, intervals.size())) {
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
    typename std::remove_reference<Range>::type::const_iterator do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.lower_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator,
             typename = decltype(std::declval<Range>().upper_bound(std::declval<T>(), std::declval<LessComparator>()))>
    typename std::remove_reference<Range>::type::const_iterator do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, built_in_) const {
        return r.upper_bound(value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::remove_reference<Range>::type::const_iterator do_lower_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::lower_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }

    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::remove_reference<Range>::type::const_iterator do_upper_bound(const T& value, Range&& r, LessComparator&& cmp, std_) const {
        return std::upper_bound(r.begin(), r.end(), value, std::forward<LessComparator>(cmp));
    }
public:
    // Return the lower bound of the specified sequence according to these bounds.
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    typename std::remove_reference<Range>::type::const_iterator lower_bound(Range&& r, LessComparator&& cmp) const {
        return start()
            ? (start()->is_inclusive()
                ? do_lower_bound(start()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_())
                : do_upper_bound(start()->value(), std::forward<Range>(r), std::forward<LessComparator>(cmp), built_in_()))
            : std::cbegin(r);
    }
    // Return the upper bound of the specified sequence according to these bounds.
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
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
    template<typename Range, IntervalLessComparatorFor<T> LessComparator>
    boost::iterator_range<typename std::remove_reference<Range>::type::const_iterator>
    slice(Range&& range, LessComparator&& cmp) const {
        return boost::make_iterator_range(lower_bound(range, cmp), upper_bound(range, cmp));
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
        auto left = s.start() ? hash(s.start()->value()) : 0;
        auto right = s.end() ? hash(s.end()->value()) : 0;
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
