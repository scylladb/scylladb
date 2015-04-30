/*
 * Copyright (C) 2013 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

/** @file nway_merger.hh
 *  Implementation of a osv::nway_merger class.
 *  nway_merger::merge() function merges N sorted containers with the complexity
 *  of O(M*log(N)), where M is a total number of elements in all merged
 *  containers.
 */

#ifndef _NWAY_MERGE_HH
#define _NWAY_MERGE_HH

#include <queue>
#include <vector>
#include <list>
#include <functional>

namespace osv {

/** @class Ptr
 * Compares two containers by the elemnt at the head
 */
template <class Ptr>
class std_ptr_front_comparator {
public:
    /**
     * We want out heap to sort the elements in an increasing order.
     * @param a
     * @param b
     *
     * @return
     */
    bool operator()(const Ptr a, const Ptr b)
    {
        return a->front() > b->front();
    }
};

/** @class nway_merger "nway_merger.hh"
 * Merge N containers S sorted in an increasing order into an iterator as a
 * sorted sequence in an increasing order. The containers collection is passed
 * to the method merge() in a container C.
 *
 * @note
 * In order to invert the ordering of the elements in the output stream one may
 * invert the semantics in the operator>(const S::value_type) to return "<="
 * result and sort the input streams in a descreasing order. The resulting
 * stream will have a decreasing order then.
 *
 * #### Algorithm:
 * merge() method implements the "ideal merge" algorithm as described at
 * http://xlinux.nist.gov/dads//HTML/idealmerge.html
 *
 * @note
 * stl::priority_queue(heap) as we defined it will hold the "smallest" element
 * at the "top".
 *
 * 1. Input containers should be sorted in an increasing order.
 * 2. Push all the containers into the heap sorting by their front() elements.
 * 3. Remove the container from the top of the heap. It'll have the smallest
 *    element among all containers.
 * 4. Remove the front() element from this container and push it into the output
 *    iterator.
 * 5. If this container still has elements, push it back to the heap.
 * 6. Repeat steps (3)-(5) until there are containers in the heap.
 *
 * #### Complexity:
 * O(M*log(N)), where M is a total number of elements in all merged containers
 * (provided the complexity of a comparison between two S values is constant).
 *
 * @note S::value_type must implement operator>().
 */
template <class C,
          class Comp = std_ptr_front_comparator<typename C::value_type> >
class nway_merger {
public:
    /**
     * Merges the containers and outputs the resulting stream into the output
     * iterator res (see class description for more details).
     *
     * The input (sorted) container should implement:
     *  - front() - return the element from the HEAD element.
     *  - empty() - returns true if there are no more elements left
     *  - begin() - return the iterator pointing to the HEAD element:
     *  - erase(it) - deleting the element pointed by the iterator it.
     *
     * Output iterator should implement:
     *  - Required operators to implement the *it = xx in order to consume the
     *    xx value.
     *  - operator++() - to move to the next output position.
     *
     * @param sorted_lists collection of the pointers to the sorted collections
     *                     to merge
     * @param res Output stream for the results of an nway_merge
     */
    template <class OutputIt>
    void merge(const C& sorted_lists, OutputIt res)
    {
        create_heap(sorted_lists);

        while (!_heads_heap.empty()) {
            SPtr t = _heads_heap.top();
            _heads_heap.pop();
            auto t_it = t->begin();

            /* Get the element from the "HEAD" of the container */
            *res = *t_it;
            ++res;

            /* Erase the "HEAD" */
            t->erase(t_it);

            if (!t->empty()) {
                _heads_heap.push(t);
            }
        }
    }

    /**
     * Pops the "smallest" element from the merged stream and pushes it into the
     * output stream.
     *
     * The input ordering requirements is the same as described in
     * merge() above.
     * This functions performs a single step of the nway_merge algorithm:
     * 1) Sorts the front elements.
     * 2) Pushes the least among them into the output iterator.
     *
     * This function is convenient when you want to merge the input streams that
     * are sometimes empty in a step-by-step manner.
     *
     * @param sorted_lists
     * @param res
     *
     * @return true if the element has been popped and false if there was nothing
     *         to pop (all input sequences were empty).
     */
    template <class OutputIt>
    bool pop(OutputIt res)
    {
        refill_heap();

        if (!_heads_heap.empty()) {
            SPtr t = _heads_heap.top();
            _heads_heap.pop();
            auto t_it = t->begin();

            /* Get the element from the "HEAD" of the container */
            *res = *t_it;
            ++res;

            /* Erase the "HEAD" */
            t->erase(t_it);

            if (!t->empty()) {
                _heads_heap.push(t);
            } else {
                _empty_lists.emplace_back(t);
            }

            return true;
        } else {
            return false;
        }
    }

    void clear() { _heads_heap = heap_type(); }

    /**
     * Create a new heap from the sorted sequences.
     * @param sorted_lists
     */
    void create_heap(const C& sorted_lists) {

        clear();

        /* Create a heap */
        for (SPtr c : sorted_lists) {
            if (!c->empty()) {
                _heads_heap.emplace(c);
            } else {
                _empty_lists.emplace_back(c);
            }
        }
    }

    /**
     * Push back all sequences from the _empty_list back to the heap.
     *
     * TODO:
     * Come up with something better that walking on the whole list and check
     * each list. One option is to use bitfield array and then use
     * count_leading_zeros() based function to efficiently get the next set bit
     * which may represent the non-empty list.
     *
     * This inefficiency may count in case of VMs with a large number of vCPUs
     * when most of the queues would be empty.
     */
    void refill_heap() {
        /* TODO: Improve this by iterating only on those that are not empty */
        auto it = _empty_lists.begin();
        while (it != _empty_lists.end()) {
            if (!(*it)->empty()) {
                _heads_heap.emplace(*it);

                auto tmp_it = it;
                ++it;

                _empty_lists.erase(tmp_it);
            } else {
                ++it;
            }
        }
    }

    template <class EmptyChecker>
    bool empty(EmptyChecker checker) const {
        return checker();
    }

    // A stupid implementation of an empty_checker()
    bool silly_empty_checker() const {
        if (!_heads_heap.empty()) {
            return false;
        }

        for (SPtr c : _empty_lists) {
            if (!c->empty()) {
                return false;
            }
        }

        return true;
    }

private:
    typedef typename C::value_type                             SPtr;
    typedef std::priority_queue<SPtr, std::vector<SPtr>, Comp> heap_type;

    heap_type _heads_heap;
    C* _sorted_lists;
    std::list<SPtr> _empty_lists;
};
} /* namespace osv */


#endif /* _NWAY_MERGE_HH */
