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

/*
 * Given a vector of cache hit ratio (hits per request) for each of N nodes,
 * and knowing which of them is the current node, our goal is to return a
 * random vector of K different nodes (i.e., a combination of K out of N),
 * where the goals of the random distribution are:
 *
 * 1. If we send each request to the K returned nodes, the *misses per
 *    second* of all nodes will be the same. In other words, nodes with
 *    low hit ratios will be sent less work.
 *
 * 2. We know that this node is one of the N nodes. As much as possible,
 *    without breaking goal 1, we should return this node as one of the
 *    results.
 *
 * 3. We assume that *this node* got chosen uniformly randomly among the
 *    N nodes (in other words, the client chose a coordinator node, us,
 *    uniformly, and we need to choose K nodes and forward the request
 *    to them).
 */
#include <vector>
#include <cassert>
#include <boost/range/adaptor/map.hpp>
#include "log.hh"

extern logging::logger hr_logger;

class rand_exception {};

float rand_float();
std::vector<int> ssample(unsigned k, const std::vector<float>& p);
std::vector<float> miss_equalizing_probablities(const std::vector<float>& hit_rates);
void clip_probabilities(std::vector<float>& p, float limit);
std::vector<float> redistribute(const std::vector<float>& p, unsigned me, unsigned k);
 
 
template<typename Node>
class combination_generator {
private:
    std::vector<float> _pp;
    std::vector<Node> _nodes;
    unsigned _k;
    // If "extra" is true, in addition to the regular k nodes returned by
    // get(), it returns one extra node which the caller should use if one
    // of the nodes returned does not answer.
    // The "extra" is guaranteed to be different from any of the regular nodes,
    // but does not participate in the probability calculation and we do
    // not make a guarantee how it will be distributed (it will in fact
    // be uniformly distributed over the remaining nodes).
    // In particular, the caller should only use the extra node in
    // exceptional situations. If the caller always plans to send a request
    // to one additional node up-front, it should use a combination_generator
    // of k+1 - and extra=false.
    bool _extra;
public:
    combination_generator(std::vector<float>&& pp, std::vector<Node>&& nodes, unsigned k, bool extra)
        : _pp(std::move(pp)), _nodes(std::move(nodes)), _k(k), _extra(extra) {
        // TODO: throw if _pp.size() != _nodes.size() or not 1 <= k < _pp.size()
    }
    std::vector<Node> get() {
        auto n = _pp.size();
        auto ke = _k + (_extra ? 1 : 0);
        assert(ke <= n);
        std::vector<Node> ret;
        ret.reserve(ke);
        std::vector<int> r = ssample(_k, _pp);
        for (int i : r) {
            ret.push_back(_nodes[i]);
        }
        if (_extra) {
            // Choose one of the remaining n-k nodes as the extra (k+1)th
            // returned node. Currently, we choose the nodes with equal
            // probablities. We could have also used _pp or the original p
            // for this - I don't know which is better, if it even matters.
            std::vector<bool> used(n);
            for (int i : r) {
                used[i] = true;
            }
            int m = ::rand_float() * (n - _k);
            for (unsigned i = 0; i < n; i++) {
                if (!used[i]) {
                    if (!m) {
                        ret.push_back(_nodes[i]);
                        break;
                    }
                    --m;
                }
            }
        }
        assert(ret.size() == ke);
        return ret;
    }
};


template<typename Node>
std::vector<Node>
miss_equalizing_combination(
    const std::vector<std::pair<Node,float>>& node_hit_rate, unsigned me, int bf, bool extra=false)
{
    auto rf = node_hit_rate.size();

    // FIXME: don't take std::pair<node,float> but separate vectors
    std::vector<float> hit_rates;
    hit_rates.reserve(rf);
    for (auto& nh : node_hit_rate) {
        hit_rates.emplace_back(nh.second);
    }
    auto p = miss_equalizing_probablities(hit_rates);
    // When we'll ask for combinations of "bf" different nodes, probabilities
    // higher than 1/bf cannot be achieved (1/bf itsef can be achieved by
    // returning this node in every returned combination). So no matter what
    // we do, we can't actually achieve the desired probabilities. Let's
    // try for the best we can
    clip_probabilities(p, 1.0f / bf);


    hr_logger.trace("desired probabilities: {}, {}", node_hit_rate | boost::adaptors::map_keys, p);

    // If me >= rf, this node is NOT one of the replicas, and we just need
    // to use the probabilties for these replicas, without doing the
    // redistribution to prefer the local replica.
    if (me < rf) {
        p = redistribute(p, me, bf);
    }

    hr_logger.trace("returned _pp={}", p);
    std::vector<Node> nodes(rf);
    for (unsigned i = 0; i < rf; i++) {
        nodes[i] = node_hit_rate[i].first;
    }
    return combination_generator<Node>(std::move(p), std::move(nodes), bf, extra).get();
}

