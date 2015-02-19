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

#ifndef PREFETCH_HH_
#define PREFETCH_HH_

#include <atomic>
#include <boost/mpl/range_c.hpp>
#include <boost/mpl/for_each.hpp>
#include "align.hh"

static constexpr size_t  cacheline_size = 64;
template <size_t N, int RW, int LOC>
struct prefetcher;

template<int RW, int LOC>
struct prefetcher<0, RW, LOC> {
    prefetcher(uintptr_t ptr) {}
};

template <size_t N, int RW, int LOC>
struct prefetcher {
    prefetcher(uintptr_t ptr) {
        __builtin_prefetch(reinterpret_cast<void*>(ptr), RW, LOC);
        std::atomic_signal_fence(std::memory_order_seq_cst);
        prefetcher<N-64, RW, LOC>(ptr + 64);
    }
};

// LOC is a locality from __buitin_prefetch() gcc documentation:
// "The value locality must be a compile-time constant integer between zero and three. A value of
//  zero means that the data has no temporal locality, so it need not be left in the cache after
//  the access. A value of three means that the data has a high degree of temporal locality and
//  should be left in all levels of cache possible. Values of one and two mean, respectively, a
//  low or moderate degree of temporal locality. The default is three."
template<typename T, int LOC = 3>
void prefetch(T* ptr) {
    prefetcher<align_up(sizeof(T), cacheline_size), 0, LOC>(reinterpret_cast<uintptr_t>(ptr));
}

template<typename Iterator, int LOC = 3>
void prefetch(Iterator begin, Iterator end) {
    std::for_each(begin, end, [] (auto v) { prefetch<decltype(*v), LOC>(v); });
}

template<size_t C, typename T, int LOC = 3>
void prefetch_n(T** pptr) {
    boost::mpl::for_each< boost::mpl::range_c<size_t,0,C> >( [pptr] (size_t x) { prefetch<T, LOC>(*(pptr + x)); } );
}

template<size_t L, int LOC = 3>
void prefetch(void* ptr) {
    prefetcher<L*cacheline_size, 0, LOC>(reinterpret_cast<uintptr_t>(ptr));
}

template<size_t L, typename Iterator, int LOC = 3>
void prefetch_n(Iterator begin, Iterator end) {
    std::for_each(begin, end, [] (auto v) { prefetch<L, LOC>(v); });
}

template<size_t L, size_t C, typename T, int LOC = 3>
void prefetch_n(T** pptr) {
    boost::mpl::for_each< boost::mpl::range_c<size_t,0,C> >( [pptr] (size_t x) { prefetch<L, LOC>(*(pptr + x)); } );
}

template<typename T, int LOC = 3>
void prefetchw(T* ptr) {
    prefetcher<align_up(sizeof(T), cacheline_size), 1, LOC>(reinterpret_cast<uintptr_t>(ptr));
}

template<typename Iterator, int LOC = 3>
void prefetchw_n(Iterator begin, Iterator end) {
    std::for_each(begin, end, [] (auto v) { prefetchw<decltype(*v), LOC>(v); });
}

template<size_t C, typename T, int LOC = 3>
void prefetchw_n(T** pptr) {
    boost::mpl::for_each< boost::mpl::range_c<size_t,0,C> >( [pptr] (size_t x) { prefetchw<T, LOC>(*(pptr + x)); } );
}

template<size_t L, int LOC = 3>
void prefetchw(void* ptr) {
    prefetcher<L*cacheline_size, 1, LOC>(reinterpret_cast<uintptr_t>(ptr));
}

template<size_t L, typename Iterator, int LOC = 3>
void prefetchw_n(Iterator begin, Iterator end) {
   std::for_each(begin, end, [] (auto v) { prefetchw<L, LOC>(v); });
}

template<size_t L, size_t C, typename T, int LOC = 3>
void prefetchw_n(T** pptr) {
    boost::mpl::for_each< boost::mpl::range_c<size_t,0,C> >( [pptr] (size_t x) { prefetchw<L, LOC>(*(pptr + x)); } );
}

#endif
