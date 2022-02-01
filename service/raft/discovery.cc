/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/discovery.hh"

namespace service {

void check_peer(const raft::server_address& peer) {
    if (!peer.info.size()) {
        throw std::logic_error("Discovery requires peer internet address to be set");
    }
}

discovery::discovery(raft::server_address self, const peer_list& seeds)
    : _self(std::move(self)) {

    // self must have a non-empty Internet address
    check_peer(_self);
    for (const auto& addr : seeds) {
        check_peer(addr);
    }
    _peer_list.push_back(_self);

    step(seeds);
}

void discovery::step(const peer_list& peers) {

    if (_is_leader) {
        return;
    }

    peer_set new_peers;
    // Set to true if we learned about a new peer or
    // received Raft server ID for one of the seeds.
    bool refresh_peer_list = false;

    for (const auto& addr : peers) {
        // peer must have a non-empty Internet address
        if (addr.info == _self.info) {
            // do not include _self into _peers
            continue;
        }
        auto it = _peers.find(addr);
        // Update peer information if it's a new peer or provides
        // a Raft ID for an existing peer.
        if (it == _peers.end() || it->id == raft::server_id{}) {
            refresh_peer_list = true;
            if (it == _peers.end()) {
                _peers.emplace(addr);
                new_peers.emplace(addr);
            } else {
                // Update Raft ID
                _peers.erase(it);
                _peers.emplace(addr);
            }
        } else {
            // If we have this peer, its ID must be the
            // same as we know (with the exceptions of seeds,
            // for which servers might not know ids at first).
            assert(it == _peers.end() || it->id == addr.id || addr.id == raft::server_id{});
        }
    }
    if (refresh_peer_list) {
        _peer_list = {_peers.begin(), _peers.end()};
        _peer_list.push_back(_self);
    }
    maybe_become_leader();
    if (_is_leader) {
        return;
    }
    for (const auto& peer : new_peers) {
        _requests.push_back(std::make_pair(peer, _peer_list));
    }
}

void discovery::maybe_become_leader() {
    /*
     * _responded is a subset of _peers.
     * When all contacted peers have responded, we're ready
     * to choose a node with the smallest id for the leader.
     */
    if (_responded.size() < _peers.size()) {
        return;
    }
    auto min_id = std::min_element(_peer_list.begin(), _peer_list.end());
    if (min_id != _peer_list.end() && min_id->id == _self.id) {
        _is_leader = true;
    }
}

std::optional<discovery::peer_list> discovery::request(const peer_list& peers) {
    step(peers);
    if (_is_leader) {
        return std::nullopt;
    }
    return _peer_list;
}

void discovery::response(raft::server_address from, const peer_list& peers) {
    assert(_peers.contains(from));
    _responded.emplace(from);
    step(peers);
}

discovery::output discovery::get_output() {
    if (_is_leader) {
        return i_am_leader{};
    } else if (!_requests.empty()) {
        return std::move(_requests);
    } else {
        if (_responded.size() == _peers.size()) {
            // All have responded, but we're not a leader.
            // Try to find out who it is. Don't waste traffic on
            // the peer list.
            for (const auto& peer : _peers) {
                _requests.push_back(std::make_pair(peer, peer_list{}));
            }
        } else {
            // Contact new peers
            for (const auto& peer : _peers) {
                if (_responded.contains(peer)) {
                    continue;
                }
                _requests.push_back(std::make_pair(peer, _peer_list));
            }
        }
        return pause{};
    }
}

} // end of namespace raft
