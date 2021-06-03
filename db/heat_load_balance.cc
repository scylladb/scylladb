/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <vector>
#include <list>
#include <random>
#include <fmt/ranges.h>
#include "heat_load_balance.hh"

logging::logger hr_logger("heat_load_balance");

// Return a uniformly-distributed random number in [0,1)
// We use per-thread state for thread safety.  We seed the random number generator
// once with a real random value, if available,
static thread_local std::default_random_engine random_engine{std::random_device{}()};
float
rand_float() {
    static thread_local std::uniform_real_distribution<float> u(0, 1);
    float ret = u(random_engine);
    // Gcc 5 has a bug (fixed in Gcc 6) where the above random number
    // generator could return 1.0, contradicting the documentation. Let's
    // replace 1.0 by the largest number below it. It's not really important
    // what we replace it with... Could have also chosen any arbitrary
    // constant in [0,1), or to run the random number generator again (this
    // is what the fix in Gcc 6 does).
    if (ret == 1.0f) {
        ret = std::nextafter(ret, 0.0f);
    }
    return ret;
}

// randone() takes a vector of N probability, and randomly returns one of
// the indexes in this vector, with the probability to choose each index
// given by the probability in the vector.
//
// The given probabilities must sum up to 1.0. This assumption is not
// verified by randone().
//
// TODO:
// This implementation has complexity O(N). If we plan to call randone()
// many times on the same probability vector, and if N can grow large,
// we should consider a different implementation, known as "The Alias Method",
// which has O(N) preperation stage but then only O(1) for each call.
// The alias method was first suggested by A.J. Walker in 1977 and later
// refined by Knuth and others. Here is a short overview of this method:
// The O(N) implementation of randone() divides the interval [0,1) into
// consecutive intervals of length p[i] (which sum to 1), then picks a random
// point in [0,1) and checks which of these intervals it covers. The
// observation behind the Alias Method is that the same technique will
// continue to work if we take these intervals and rearrange and/or cut them
// up, as long as we keep their total lengths. The goal would be to cut them
// up in such a way that it makes it easy (O(1)) to find which interval is
// underneath each point we pick on [0,1).
// To do that, we begin by dividing [0,1) to N intervals of equal length 1/N,
// and then packing in each of those at most two intervals belonging to
// different i’s. Now, to find to which i a point belongs to, all we need
// to do is to find in which of the equal-length interval it is (a trivial
// division and truncation), and then finding out which one of the two
// possibilities that are left holds (one array indexing and comparison).
// How do we pack the equal-length 1/N intervals correctly? We begin by
// putting in the first one a p[i] such that p[i] <= 1/N (always possible,
// of course). If the inequality was strict, so p[i] did not completely fill
// the first 1/N-length interval, we pick another p[j] where p[j] >= 1/N
// (again, possible), take away from it what is needed to fill up the
// 1/N-length interval, reducing p[j] for the rest of the algorithm. Now,
// we continue the same algorithm with one interval less and one less value,
// so it will end in O(N) time.
// For really large N (which we'll never need here...) there are even papers
// on how to ensure that the initialization stage is really O(N) and not
// O(NlogN) - see https://web.archive.org/web/20131029203736/http://web.eecs.utk.edu/~vose/Publications/random.pdf

static unsigned
randone(const std::vector<float>& p, float rnd = rand_float()) {
    unsigned last = p.size() - 1;
    for (unsigned i = 0; i < last; i++) {
        rnd -=  p[i];
        if (rnd < 0) {
            return i;
        }
    }
    // Note: if we're here and rnd isn't 0 (or very close to 0) then the
    // p[i]s do not sum to 1... But we don't check this assumption here.
    return last;
}

// ssample() produces a random combination (i.e., unordered subset) of
// length K out of N items 0..N-1, where the different items should be
// included with different probabilities, given by a vector p of N
// probabilities, whose sum should be 1.0.
// It returns a vector<int> with size K whose items are different integers
// between 0 and N-1.
//
// The meaning of a probability p[i] is that if we count the individual
// items appearing in returned combinations, the count of item i will be a
// fraction p[i] of the overall count. Note that p[i] must not be higher
// than 1/K: even if we return item i in *every* K-combination, item i will
// still be only 1/K of the produced items. To reach p[i] > 1/K will mean
// some combinations will need to contain more than one copy of i - which
// contradicts the defintion of a "combination".
//
// Though ssample() is required to fulfill the first-order inclusion
// probabilities p (the probability of each item appearing in the returned
// combination), it is NOT required to make any guarantees on the high-order
// inclusion probabilities, i.e., the probablities for pairs of items to
// be returned together in the same combination. This greatly simplifies
// the implementation, and means we can use the "Systematic Sampling"
// technique (explained below) which only makes guarantees on the first-order
// inclusion probablities. In our use case, fulfilling *only* the 1st order
// inclusion probabilities is indeed enough: We want that each node gets a
// given amount of work, but don't care if the different K nodes we choose
// in one request are correlated.
//
// Not making any guarantees on high-order inclusion probablities basically
// means that the items are not independent. To understand what this means,
// consider a simple example: say we have N=4 items with equal probability
// and want to draw random pairs (K=2). Our implementation will return {0,1}
// half of the time, and {2,3} the other half of the time. That distribution
// achieves and achieve the desired probabilities (each item will be given 1/4
// of the work), but the pair {1,2}, for example, will never appear in any
// individual draw.
//
// "Systematic Sampling" is a very simple method of reproducing a set of
// desired 1st-order inclusion probabilities. A good overview can found in
// http://stats.stackexchange.com/questions/139279/systematic-sampling-with-unequal-probabilities
// Basically, Systematic Sampling is a simple extension of the randone()
// algorithm above. Both start by putting the given probabilities one after
// another on the segment [0,1). randone() then drew one random number in
// [0,1) and looked on which of the segments this point falls. Here, we draw
// a random number x in [0, 1/K), look under it, but then look under x+1/K,
// x+2/K, ..., x + (K-1)/K,  and these produce K different items with
// appropriate probabilities:
// 1. The items are necessarily different because of our assumption that
//    none of the p[i] are larger than 1/K),
// 2. The probability to choose each item is exactly p_i*K.
//
// ssample() only calls for one random number generation (this is important
// for performance) but calls randone() on the same probablity vector K times,
// which makes it even more interesting to implement the Alias Method
// described above. However, for very small N like 3, the difference is not
// likely to be noticable.
//
// TODO: For the special case of K == N-1, we can have a slightly more
// efficient implementation, which calculates the probability for each of
// the N combinations (the combination lacking item i can be proven to have
// probablity 1 - K*p[i]) and then uses one randone() call with these
// modified probablities.
// TODO: Consider making this a template of K, N and have specialized
// implementations for low N (e.g., 3), K=N-1, etc.
// TODO: write to a pre-allocated return vector to avoid extra allocation.

std::vector<int>
ssample(unsigned k, const std::vector<float>& p) {
    const float interval = 1.0 / k;
    const float rnd = rand_float() * interval; // random number in [0, 1/k)
    std::vector<int> ret;
    ret.reserve(k);
    float offset = 0;
    for (unsigned i = 0; i < k; i++) {
        ret.emplace_back(randone(p, rnd + offset));
        offset += interval;
    }
    hr_logger.trace("ssample returning {}", ret);
    return ret;
}

// Given the cache hit rate (cache hits / request) of N different nodes,
// calculate the fraction of requests that we'd like to send of each of
// these nodes to achieve the same number of misses per second on all nodes
std::vector<float>
miss_equalizing_probablities(const std::vector<float>& hit_rates) {
    std::vector<float> ret;
    ret.reserve(hit_rates.size());
    // R[i] is the reciprocal miss rate 1/(1-H[i]).
    float r_sum = 0;
    for (float h : hit_rates) {
        float r = 1 / (1 - h);
        ret.emplace_back(r);
        r_sum += r;
    }
    for (float& r : ret) {
        r /= r_sum;
    }
    return ret;
}

// Given a set of desired probablities with sum 1, clip the probablities 
// to be not higher than the given limit. The rest of the probabilities are
// increased, in an attempt to preserve the ratios between probabilities,
// if possible - but keep all the probabilities below the limit.
void
clip_probabilities(std::vector<float>& p, float limit) {
    // TODO: We have iterations here because it's possible that increasing
    // one proability will bring it also over the limit. Can we find a
    // single-step algorithm to do this?
    float ratio = 1.0;
    for (;;) {
        float clipped = 0;
        float sum_unclipped = 0;
        for (float& x : p) {
            if (x >= limit) {
                clipped += x - limit;
                x = limit;
            } else {
                x *= ratio;
                sum_unclipped += x;
            }
        }
        // "ratio" is how much we need to increase the unclipped
        // probabilities
        if (clipped == 0) {
            return; // done
        }
        ratio = (sum_unclipped + clipped) / sum_unclipped;
    }
}

// Run the "probability redistribution" algorithm, which aims for the
// desired probability distribution of the nodes, but does as much work
// as we can (i.e., 1/k) locally and redistributing the rest.
// Returns the vector of proabilities that node "me" should use to send
// requests.
std::vector<float>
redistribute(const std::vector<float>& p, unsigned me, unsigned k) {
    unsigned rf = p.size();
    std::vector<float> pp(rf);

    // "Keep for node i"
    // A surplus node keeps its entire desired amount of request, N*p,
    // for itself. A mixed node is cut off by 1/C.
    pp[me] = std::min(rf * p[me], 1.0f / k);
    hr_logger.trace("pp[me({})]  = {}", me, pp[me]);

    std::vector<float> deficit(rf);
    float total_deficit = 0;
    int mixed_count = 0;
    for (unsigned j = 0; j < rf; j++) {
        float NPj = rf * p[j];
        float deficit_j = NPj - 1.0f / k;
        if (deficit_j >= 0) {
            // mixed node
            mixed_count++;
            deficit[j] = deficit_j;
            total_deficit += deficit_j;
        }
    }
    // Each of the mixed nodes have the same same surplus:
    float mixed_surplus = 1 - 1.0f / k;

    hr_logger.trace("starting distribution of mixed-node surplus to other mixed nodes:"
                    " mixed_count={}, deficit={}, mixed_surplus={}", mixed_count, deficit, mixed_surplus);

    float my_surplus;
    if (deficit[me] == 0) {
        // surplus node
        my_surplus = 1 - rf * p[me];
    } else {
        // mixed node, which will be converted below to either a deficit
        // node or a surplus node. We can easily calculate now how much
        // surplus will be left. It will be useful to know below if "me"
        // will be a surplus node, because we only need to know how much
        // work "me" *sends*, so if me is not a surplus node, we won't need
        // to do the second step (of distributing surplus to the deficit
        // nodes), and won't even need to update deficit[].
        if (deficit[me] <= mixed_surplus) {
            // Node will be converted to a surplus node
            my_surplus = mixed_surplus - deficit[me];
        } else {
            // Node will be converted to a deficit node, and will not be
            // left with any surplus
            my_surplus = 0;
        }
    }
    hr_logger.trace("my_surplus={}", my_surplus);

    // Mixed node redistribution algorithm, to "convert" mixed nodes into
    // pure surplus or pure deficit nodes, while flowing probability between
    // the mixed nodes (we only need to track this flow here if "me" is the
    // node doing the sending - in pp[]).
    if (deficit[me]) {
        // "me" is a mixed node. 
        hr_logger.trace("CASE1");
        // We need a list of the mixed nodes sorted in increasing deficit order.
        // Actually, we only need to sort those nodes with deficit <=
        // min(deficit[me], mixed_surplus).
        // TODO: use NlgN sort instead of this ridiculous N^2 implementation.
        // TODO: can we do this without a NlgN (although very small N, not even
        // the full rf)? Note also the distribution code below is N^2 anway
        // (two nested for loops).
        std::list<std::pair<unsigned, float>> sorted_deficits;
        for (unsigned i = 0; i < rf; i++) {
            if (deficit[i] && deficit[i] <= deficit[me] &&
                    deficit[i] < mixed_surplus) {
                auto it = sorted_deficits.begin();
                while (it != sorted_deficits.end() && it->second < deficit[i])
                    ++it;
                sorted_deficits.insert(it, std::make_pair(i, deficit[i]));
            }
        }
        hr_logger.trace("sorted_deficits={}{}", sorted_deficits | boost::adaptors::map_keys, sorted_deficits | boost::adaptors::map_values);
        float s = 0;
        int count = mixed_count;
        for (auto& d : sorted_deficits) {
            hr_logger.trace("next sorted deficit={{{}, {}}}", d.first, d.second);
            // What "diff" to distribute
            auto diff = d.second - s;
            s = d.second;
            hr_logger.trace("diff={}, pp before={}, count={}", diff, pp, count);
            --count;
            // Distribute diff among all the mixed nodes with higher deficit.
            // There should be exactly "count" of those excluding me.
            if (!count) {
                break;
            }
            for (unsigned i = 0; i < rf; i++) {
                hr_logger.trace("{} {} {} {}", i, d.first, deficit[i], d.second);
                // The ">=" here is ok: If several deficits are tied, the first one
                // contributes the diff to all those nodes (all are equal, so >=),
                // while when we get to the following nodes, they have diff==0
                // (because of the tied deficit) so we don't care that this loop
                // doesn't quite match count nodes.
                if (i != me && deficit[i] >= d.second) {
                    pp[i] += diff / count;
                    hr_logger.trace("pp[{}]={} (case a)", i, pp[i]);
                }
            }

            hr_logger.trace("     pp after1=", pp);
            if (d.first == me) {
                // We only care what "me" sends, and only the elements in
                // the sorted list earlier than me could have forced it to
                // send, so the rest of the algorithm isn't interesting.
                break;
            }
        }
        // additionally, if me is converted to a deficit node, we need to
        // take the remaining surplus (mixed_surplus minus the last deficit
        // in sorted_deficits) and distribute it to the other count-1
        // converted-to-surplus nodes. Of course we can only do this if
        // count > 1 - if count==1, we remain with just one mixed node
        // and cannot eliminate its surplus without "fixing" some of the
        // decisions made earlier
        if (deficit[me] > mixed_surplus) {
            auto last_deficit = sorted_deficits.back().second;
            auto diff = mixed_surplus - last_deficit;
            if (count > 1) {
                hr_logger.trace("CASE4. surplus {} count {}", diff, count);
                for (unsigned i = 0; i < rf; i++) {
                    if (i != me && deficit[i] > last_deficit) {
                        hr_logger.trace("adding {}  to pp[{}]={}", (diff / (count-1)), i,  pp[i]);
                        pp[i] += diff / (count - 1);
                    }
                }
                // TODO: confirm that this loop worked exactly count - 1 times.
            } else {
                hr_logger.trace("CASE3a. surplus={}", diff);
                // CASE3: count == 1 is possible. example for p = 0.2, 0.3, 0.5:
                //    surplus  0.5  0.5  0.5
                //    deficit  0.1  0.4  1.0
                // after first step redistributing 0.1 to 3 nodes:
                //    surplus  0.4  0.4  0.4
                //    deficit  0.0  0.3  0.9
                // after first step redistributing 0.3 to 2 nodes:
                //    surplus  0.4  0.1  0.1
                //    deficit  0.0  0.0  0.6
                // So we're left with 1 mixed node (count=1), and can't
                // redistribute its surplus to itself!
                // This happens because the original distribution step was
                // already a mistake: In this case the *only* solution is for node
                // 0 and 1 is to send all their surplus (total of 1.0) to fill
                // node 2's entire deficit (1.0). Node 0 can't afford to send
                // any of its surplus to node 1 - and if it does (like we did in
                // the first step redistributing 0.1), we end up with
                // deficit remaining on node 2!
                //
                // Special case of one remaining mixed node. Tell the other
                // nodes not to give each other as much (we don't have to
                // do this here, as we only care about "me") and instead
                // "me" will give them their surplus
                for (unsigned i = 0; i < rf; i++) {
                    if (i != me) {
                        pp[i] += diff / (mixed_count - 1);
                        hr_logger.trace("pp[{}]={} (case b)", i, pp[i]);
                    }
                }
            }
            hr_logger.trace("      pp after2={}", pp);
        } else {
            // Additionally, if the algorithm ends with a single mixed node
            // we need to apply a fix. Above we already handled the case that
            // this single mixed node is "me", so it needs to send more to the
            // other nodes. Here we need to handle the opposite side - me is
            // one of the nodes which sent too much to other nodes and needs
            // to send to the mixed node instead.
            // TODO: find a more efficient way to check if the alorithm will
            // end with just one mixed node and its surplus :-(
            unsigned n_converted_to_deficit = 0;
            unsigned mix_i = 0; // only used if n_converted_to_deficit==1
            float last_deficit = 0;
            for (unsigned i = 0; i < rf; i++) {
                if (deficit[i] > mixed_surplus) {
                    n_converted_to_deficit++;
                    mix_i = i;
                } else {
                    last_deficit = std::max(last_deficit, deficit[i]);
                }
            }
            if (n_converted_to_deficit == 1) {
                auto diff = mixed_surplus - last_deficit;
                hr_logger.trace("CASE3b. surplus={}", diff);
                pp[mix_i] += diff / (mixed_count - 1);
                hr_logger.trace("pp[{}]={} (case c)", mix_i, pp[mix_i]);
                for (unsigned i = 0; i < rf; i++) {
                    if (deficit[i] > 0) { // mixed node
                        if (i != mix_i && i != me) {
                            pp[i] -= diff / (mixed_count - 1) / (mixed_count - 2);
                            hr_logger.trace("pp[{}]={} (case d)", i, pp[i]);
                        }
                    }
                }
            }
        }
    }

    if (my_surplus) {
        // "me" is a surplus node, or became one during the mixed node
        // redistribution algorithm.  We need to know the new deficit nodes
        // produced by that algorithm. i.e., we need to update deficit[].
        float new_total_deficit = 0;
        for (unsigned i = 0; i < rf; i++) {
            if (deficit[i] > 0) {
                // Mixed node.
                if (deficit[i] > mixed_surplus) {
                    // The mixed-node redistribution algorithm converted it
                    // to a deficit node, with this deficit:
                    deficit[i] -= mixed_surplus;
                    new_total_deficit += deficit[i];
                } else {
                    // The mixed-node redistribution algorithm converted it
                    // to a surplus node, with no deficit:
                    deficit[i] = 0;
                }
            }
        }
        // Split "me"'s surplus to the other nodes' remaining deficit,
        // according to their share in the total remaining deficit.
        for (unsigned j = 0; j < rf ; j++) {
            if (deficit[j] > 0) {
                // note j!= me because surplus node has deficit==0.
                // Note pp[j] +=, not =, because this node might have
                // already flowed some work to other nodes in the
                // mixed node redistribution algorithm above.
                pp[j] += deficit[j] / new_total_deficit * my_surplus; 
                hr_logger.trace("pp[{}]={} (case e)", j, pp[j]);
            }
        }
    }
    return pp;
}
