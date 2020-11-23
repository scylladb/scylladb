/*
 * Copyright (C) 2020 ScyllaDB
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
#include "fsm.hh"
#include <random>
#include <seastar/core/coroutine.hh>

namespace raft {

fsm::fsm(server_id id, term_t current_term, server_id voted_for, log log,
        failure_detector& failure_detector, fsm_config config) :
        _my_id(id), _current_term(current_term), _voted_for(voted_for),
        _log(std::move(log)), _failure_detector(failure_detector), _config(config) {

    _observed.advance(*this);
    set_configuration(_log.get_snapshot().config);
    logger.trace("{}: starting log length {}", _my_id, _log.last_idx());

    assert(!bool(_current_leader));
}

future<> fsm::wait() {
    check_is_leader();

   return _log_limiter_semaphore->sem.wait();
}

template<typename T>
const log_entry& fsm::add_entry(T command) {
    // It's only possible to add entries on a leader.
    check_is_leader();

    _log.emplace_back(log_entry{_current_term, _log.next_idx(), std::move(command)});
    _sm_events.signal();

    return *_log[_log.last_idx()];
}

template const log_entry& fsm::add_entry(command command);
template const log_entry& fsm::add_entry(log_entry::dummy dummy);

void fsm::advance_commit_idx(index_t leader_commit_idx) {

    auto new_commit_idx = std::min(leader_commit_idx, _log.stable_idx());

    logger.trace("advance_commit_idx[{}]: leader_commit_idx={}, new_commit_idx={}",
        _my_id, leader_commit_idx, new_commit_idx);

    if (new_commit_idx > _commit_idx) {
        _commit_idx = new_commit_idx;
        _sm_events.signal();
        logger.trace("advance_commit_idx[{}]: signal apply_entries: committed: {}",
            _my_id, _commit_idx);
    }
}


void fsm::update_current_term(term_t current_term)
{
    assert(_current_term < current_term);
    _current_term = current_term;
    _voted_for = server_id{};

    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<> dist(1, ELECTION_TIMEOUT.count());
    // Reset the randomized election timeout on each term
    // change, even if we do not plan to campaign during this
    // term: the main purpose of the timeout is to avoid
    // starting our campaign simultaneously with other followers.
    _randomized_election_timeout = ELECTION_TIMEOUT + logical_clock::duration{dist(re)};
}

void fsm::become_leader() {
    assert(!std::holds_alternative<leader>(_state));
    assert(!_tracker);
    _state = leader{};
    _current_leader = _my_id;
    _votes = std::nullopt;
    _tracker.emplace(_my_id);
    _log_limiter_semaphore.emplace(this);
    _log_limiter_semaphore->sem.consume(_log.non_snapshoted_length());
    _tracker->set_configuration(_current_config.servers, _log.next_idx());
    _last_election_time = _clock.now();
    replicate();
}

void fsm::become_follower(server_id leader) {
    _current_leader = leader;
    _state = follower{};
    _tracker = std::nullopt;
    _log_limiter_semaphore = std::nullopt;
    _votes = std::nullopt;
    if (_current_leader) {
        _last_election_time = _clock.now();
    }
}

void fsm::become_candidate() {
    _state = candidate{};
    _tracker = std::nullopt;
    _log_limiter_semaphore = std::nullopt;
    update_current_term(term_t{_current_term + 1});
    // 3.4 Leader election
    // A possible outcome is that a candidate neither wins nor
    // loses the election: if many followers become candidates at
    // the same time, votes could be split so that no candidate
    // obtains a majority. When this happens, each candidate will
    // time out and start a new election by incrementing its term
    // and initiating another round of RequestVote RPCs.
    _last_election_time = _clock.now();
    _votes.emplace();
    _votes->set_configuration(_current_config.servers);
    _voted_for = _my_id;

    if (_votes->tally_votes() == vote_result::WON) {
        // A single node cluster.
        become_leader();
        return;
    }

    for (const auto& server : _current_config.servers) {
        if (server.id == _my_id) {
            continue;
        }
        logger.trace("{} [term: {}, index: {}, last log term: {}] sent vote request to {}",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), server.id);

        send_to(server.id, vote_request{_current_term, _log.last_idx(), _log.last_term()});
    }
}

future<fsm_output> fsm::poll_output() {
    logger.trace("fsm::poll_output() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());

    while (true) {
        auto diff = _log.last_idx() - _log.stable_idx();

        if (diff > 0 || !_messages.empty() || !_observed.is_equal(*this)) {
            break;
        }
        co_await _sm_events.wait();
    }
    co_return get_output();
}

fsm_output fsm::get_output() {
    fsm_output output;

    auto diff = _log.last_idx() - _log.stable_idx();

    if (diff > 0) {
        output.log_entries.reserve(diff);

        for (auto i = _log.stable_idx() + 1; i <= _log.last_idx(); i++) {
            // Copy before saving to storage to prevent races with log updates,
            // e.g. truncation of the log.
            // TODO: avoid copies by making sure log truncate is
            // copy-on-write.
            output.log_entries.emplace_back(_log[i]);
        }
    }

    if (_observed._current_term != _current_term || _observed._voted_for != _voted_for) {
        output.term = _current_term;
        output.vote = _voted_for;
    }

    // see if there was a new snapshot that has to be handled
    if (_observed._snapshot.id != _log.get_snapshot().id) {
        output.snp = _log.get_snapshot();
    }

    // Return committed entries.
    // Observer commit index may be smaller than snapshot index
    // in which case we should not attemp commiting entries belonging
    // to a snapshot.
    auto observed_ci =  std::max(_observed._commit_idx, _log.get_snapshot().idx);
    if (observed_ci < _commit_idx) {
        output.committed.reserve(_commit_idx - observed_ci);

        for (auto idx = observed_ci + 1; idx <= _commit_idx; ++idx) {
            const auto& entry = _log[idx];
            if (!std::holds_alternative<configuration>(entry->data)) {
                output.committed.push_back(entry);
            }
        }
    }

    // Get a snapshot of all unsent messages.
    // Do it after populting log_entries and committed arrays
    // to not lose messages in case arrays population throws
    std::swap(output.messages, _messages);

    // Advance the observed state.
    _observed.advance(*this);

    // Be careful to do that only after any use of stable_idx() in this
    // function and after any code that may throw
    if (output.log_entries.size()) {
        // We advance stable index before the entries are
        // actually persisted, because if writing to stable storage
        // will fail the FSM will be stopped and get_output() will
        // never be called again, so any new sate that assumes that
        // the entries are stable will not be observed.
        advance_stable_idx(output.log_entries.back()->idx);
    }

    return output;
}

void fsm::advance_stable_idx(index_t idx) {
    _log.stable_to(idx);
    if (is_leader()) {
        auto& progress = _tracker->find(_my_id);
        progress.match_idx = idx;
        progress.next_idx = index_t{idx + 1};
        replicate();
        check_committed();
    }
}

void fsm::check_committed() {

    index_t new_commit_idx = _tracker->committed(_commit_idx);

    if (new_commit_idx <= _commit_idx) {
        return;
    }

    if (_log[new_commit_idx]->term != _current_term) {

        // 3.6.2 Committing entries from previous terms
        // Raft never commits log entries from previous terms by
        // counting replicas. Only log entries from the leader’s
        // current term are committed by counting replicas; once
        // an entry from the current term has been committed in
        // this way, then all prior entries are committed
        // indirectly because of the Log Matching Property.
        logger.trace("check_committed[{}]: cannot commit because of term {} != {}",
            _my_id, _log[new_commit_idx]->term, _current_term);
        return;
    }
    logger.trace("check_committed[{}]: commit {}", _my_id, new_commit_idx);
    _commit_idx = new_commit_idx;
    // We have a quorum of servers with match_idx greater than the
    // current commit index. Commit && apply more entries.
    _sm_events.signal();
}

void fsm::tick_leader() {
    if (_clock.now() - _last_election_time >= ELECTION_TIMEOUT) {
        // 6.2 Routing requests to the leader
        // A leader in Raft steps down if an election timeout
        // elapses without a successful round of heartbeats to a majority
        // of its cluster; this allows clients to retry their requests
        // with another server.
        return become_follower(server_id{});
    }

    size_t active = 1; // +1 for self
    for (auto& [id, progress] : *_tracker) {
        if (progress.id != _my_id) {
            if (_failure_detector.is_alive(progress.id)) {
                active++;
            }
            if (progress.state == follower_progress::state::PROBE) {
                // allow one probe to be resent per follower per time tick
                progress.probe_sent = false;
            } else if (progress.state == follower_progress::state::PIPELINE &&
                progress.in_flight == follower_progress::max_in_flight) {
                progress.in_flight--; // allow one more packet to be sent
            }
            if (progress.match_idx < _log.stable_idx() || progress.commit_idx < _commit_idx) {
                logger.trace("tick[{}]: replicate to {} because match={} < stable={} || "
                    "follower commit_idx={} < commit_idx={}",
                    _my_id, progress.id, progress.match_idx, _log.stable_idx(),
                    progress.commit_idx, _commit_idx);

                replicate_to(progress, true);
            }
        }
    }
    if (active >= _tracker->size()/2 + 1) {
        // Advance last election time if we heard from
        // the quorum during this tick.
        _last_election_time = _clock.now();
    }
}

void fsm::tick() {
    _clock.advance();

    if (is_leader()) {
        tick_leader();
    } else if (_current_leader && _failure_detector.is_alive(_current_leader)) {
        // Ensure the follower doesn't disrupt a valid leader
        // simple because there were no AppendEntries RPCs recently.
        _last_election_time = _clock.now();
    } else if (is_past_election_timeout()) {
        logger.trace("tick[{}]: becoming a candidate, last election: {}, now: {}", _my_id,
            _last_election_time, _clock.now());
        become_candidate();
    }
}

void fsm::append_entries(server_id from, append_request_recv&& request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={} num entries={}",
            _my_id, request.current_term, request.prev_log_idx, request.prev_log_term,
            request.leader_commit_idx, request.entries.size() ? request.entries[0].idx : index_t(0), request.entries.size());

    assert(is_follower());
    // 3.4. Leader election
    // A server remains in follower state as long as it receives
    // valid RPCs from a leader.
    _last_election_time = _clock.now();

    // Ensure log matching property, even if we append no entries.
    // 3.5
    // Until the leader has discovered where it and the
    // follower’s logs match, the leader can send
    // AppendEntries with no entries (like heartbeats) to save
    // bandwidth.
    auto [match, term] = _log.match_term(request.prev_log_idx, request.prev_log_term);
    if (!match) {
        logger.trace("append_entries[{}]: no matching term at position {}: expected {}, found {}",
                _my_id, request.prev_log_idx, request.prev_log_term, term);
        // Reply false if log doesn't contain an entry at
        // prevLogIndex whose term matches prevLogTerm (§5.3).
        send_to(from, append_reply{_current_term, _commit_idx, append_reply::rejected{request.prev_log_idx, _log.last_idx()}});
        return;
    }

    // If there are no entries it means that the leader wants
    // to ensure forward progress. Reply with the last index
    // that matches.
    index_t last_new_idx = request.prev_log_idx;

    if (!request.entries.empty()) {
        last_new_idx = _log.maybe_append(std::move(request.entries));
    }

    advance_commit_idx(request.leader_commit_idx);

    send_to(from, append_reply{_current_term, _commit_idx, append_reply::accepted{last_new_idx}});
}

void fsm::append_entries_reply(server_id from, append_reply&& reply) {
    assert(is_leader());

    follower_progress& progress = _tracker->find(from);

    if (progress.state == follower_progress::state::PIPELINE) {
        if (progress.in_flight) {
            // in_flight is not precise, so do not let it underflow
            progress.in_flight--;
        }
    }

    progress.commit_idx = reply.commit_idx;

    if (std::holds_alternative<append_reply::accepted>(reply.result)) {
        // accepted
        index_t last_idx = std::get<append_reply::accepted>(reply.result).last_new_idx;

        logger.trace("append_entries_reply[{}->{}]: accepted match={} last index={}",
            _my_id, from, progress.match_idx, last_idx);

        progress.match_idx = std::max(progress.match_idx, last_idx);
        // out next_idx may be large because of optimistic increase in pipeline mode
        progress.next_idx = std::max(progress.next_idx, index_t(last_idx + 1));

        progress.become_pipeline();

        // check if any new entry can be committed
        check_committed();
    } else {
        // rejected
        append_reply::rejected rejected = std::get<append_reply::rejected>(reply.result);

        logger.trace("append_entries_reply[{}->{}]: rejected match={} index={}",
            _my_id, from, progress.match_idx, rejected.non_matching_idx);

        // check reply validity
        if (progress.is_stray_reject(rejected)) {
            logger.trace("append_entries_reply[{}->{}]: drop stray append reject", _my_id, from);
            return;
        }

        // Start re-sending from the non matching index, or from
        // the last index in the follower's log.
        // FIXME: make it more efficient
        progress.next_idx = std::min(rejected.non_matching_idx, index_t(rejected.last_idx + 1));

        progress.become_probe();

        // We should not fail to apply an entry following the matched one.
        assert(progress.next_idx != progress.match_idx);
    }

    logger.trace("append_entries_reply[{}->{}]: next_idx={}, match_idx={}",
        _my_id, from, progress.next_idx, progress.match_idx);

    replicate_to(progress, false);
}

void fsm::request_vote(server_id from, vote_request&& request) {

    // We can cast a vote in any state. If the candidate's term is
    // lower than ours, we ignore the request. Otherwise we first
    // update our current term and convert to a follower.
    assert(_current_term == request.current_term);

    bool can_vote =
	    // We can vote if this is a repeat of a vote we've already cast...
        _voted_for == from ||
        // ...we haven't voted and we don't think there's a leader yet in this term...
        (_voted_for == server_id{} && _current_leader == server_id{});

    // ...and we believe the candidate is up to date.
    if (can_vote && _log.is_up_to_date(request.last_log_idx, request.last_log_term)) {

        logger.trace("{} [term: {}, index: {}, log_term: {}, voted_for: {}] "
            "voted for {} [log_term: {}, log_index: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.last_log_term, request.last_log_idx);
        // If a server grants a vote, it must reset its election
        // timer. See Raft Summary.
        _last_election_time = _clock.now();
        _voted_for = from;

        send_to(from, vote_reply{_current_term, true});
    } else {
        // If a vote is not granted, this server is a potential
        // viable candidate, so it should not reset its election
        // timer, to avoid election disruption by non-viable
        // candidates.
        logger.trace("{} [term: {}, index: {}, log_term: {}, voted_for: {}] "
            "rejected vote for {} [log_term: {}, log_index: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.last_log_term, request.last_log_idx);

        send_to(from, vote_reply{_current_term, false});
    }
}

void fsm::request_vote_reply(server_id from, vote_reply&& reply) {
    assert(is_candidate());

    logger.trace("{} received a {} vote from {}", _my_id, reply.vote_granted ? "yes" : "no", from);

    _votes->register_vote(from, reply.vote_granted);

    switch (_votes->tally_votes()) {
    case vote_result::UNKNOWN:
        break;
    case vote_result::WON:
        become_leader();
        break;
    case vote_result::LOST:
        become_follower(server_id{});
        break;
    }
}

static size_t entry_size(const log_entry& e) {
    struct overloaded {
        size_t operator()(const command& c) {
            return c.size();
        }
        size_t operator()(const configuration& c) {
            size_t size = 0;
            for (auto& s : c.servers) {
                size += sizeof(s.id);
                size += s.info.size();
            }
            return size;
        }
        size_t operator()(const log_entry::dummy& d) {
            return 0;
        }
    };
    return std::visit(overloaded{}, e.data) + sizeof(e);
}

void fsm::replicate_to(follower_progress& progress, bool allow_empty) {

    logger.trace("replicate_to[{}->{}]: called next={} match={}",
        _my_id, progress.id, progress.next_idx, progress.match_idx);

    while (progress.can_send_to()) {
        index_t next_idx = progress.next_idx;
        if (progress.next_idx > _log.stable_idx()) {
            next_idx = index_t(0);
            logger.trace("replicate_to[{}->{}]: next past stable next={} stable={}, empty={}",
                    _my_id, progress.id, progress.next_idx, _log.stable_idx(), allow_empty);
            if (!allow_empty) {
                // Send out only persisted entries.
                return;
            }
        }

        allow_empty = false; // allow only one empty message

        if (progress.next_idx < _log.start_idx()) {
            // The next index to be sent points to a snapshot so
            // we need to transfer the snasphot before we can
            // continue syncing the log.
            progress.become_snapshot();
            send_to(progress.id, install_snapshot{_current_term, _log.get_snapshot()});
            logger.trace("replicate_to[{}->{}]: send snapshot next={} snapshot={}",
                    _my_id, progress.id, progress.next_idx,  _log.get_snapshot().idx);
            return;
        }

        index_t prev_idx = index_t(0);
        term_t prev_term = _current_term;
        if (progress.next_idx != 1) {
            auto& s =  _log.get_snapshot();
            prev_idx = index_t(progress.next_idx - 1);
            assert (prev_idx >= s.idx);
            prev_term = s.idx == prev_idx ? s.term : _log[prev_idx]->term;
        }

        append_request_send req = {{
                .current_term = _current_term,
                .leader_id = _my_id,
                .prev_log_idx = prev_idx,
                .prev_log_term = prev_term,
                .leader_commit_idx = _commit_idx
            },
            std::vector<log_entry_ptr>()
        };

        if (next_idx) {
            size_t size = 0;
            while (next_idx <= _log.stable_idx() && size < _config.append_request_threshold) {
                const auto& entry = _log[next_idx];
                req.entries.push_back(entry);
                logger.trace("replicate_to[{}->{}]: send entry idx={}, term={}",
                             _my_id, progress.id, entry->idx, entry->term);
                size += entry_size(*entry);
                next_idx++;
                if (progress.state == follower_progress::state::PROBE) {
                    break; // in PROBE mode send only one entry
                }
            }

            if (progress.state == follower_progress::state::PIPELINE) {
                progress.in_flight++;
                // Optimistically update next send index. In case
                // a message is lost there will be negative reply that
                // will re-send idx.
                progress.next_idx = next_idx;
            }
        } else {
            logger.trace("replicate_to[{}->{}]: send empty", _my_id, progress.id);
        }

        send_to(progress.id, std::move(req));

        if (progress.state == follower_progress::state::PROBE) {
             progress.probe_sent = true;
        }
    }
}

void fsm::replicate() {
    assert(is_leader());
    for (auto& [id, progress] : *_tracker) {
        if (progress.id != _my_id) {
            replicate_to(progress, false);
        }
    }
}

bool fsm::can_read() {
    check_is_leader();

    if (_log[_log.last_idx()]->term != _current_term) {
        return false;
    }

    // TODO: for now always return false to let the caller know that
    // applying dummy entry is needed before reading (to confirm the leadership),
    // but in the future we may return true here if we can guaranty leadership
    // by means of a "stable leader" optimization. "Stable leader" ensures that
    // a follower does not vote for other leader if it recently (during a couple
    // of last ticks) heard from existing one, so if the leader is already committed 
    // entries during this tick it guaranties that it communicated with
    // majority of nodes and no other leader could have been elected.

    return false;
}

void fsm::snapshot_status(server_id id, bool success) {
    auto& progress = _tracker->find(id);

    if (progress.state != follower_progress::state::SNAPSHOT) {
        logger.trace("snasphot_status[{}]: called not in snapshot state", _my_id);
        return;
    }

    // No matter if snapshot transfer failed or not move back to probe state
    progress.become_probe();

    if (success) {
        progress.next_idx = _log.get_snapshot().idx + index_t(1);
        // If snapshot was successfully transfered start replication immediately
        replicate_to(progress, false);
    }
    // Otherwise wait for a heartbeat. Next attempt will move us to snapshotting state
    // again and snapshot transfer will be attempted one more time.
}

void fsm::apply_snapshot(snapshot snp, size_t trailing) {
    size_t units = _log.apply_snapshot(std::move(snp), trailing);
    if (is_leader()) {
        logger.trace("apply_snapshot[{}]: signal {} available units", _my_id, units);
        _log_limiter_semaphore->sem.signal(units);
    }
}

void fsm::stop() {
    _sm_events.broken();
}

std::ostream& operator<<(std::ostream& os, const fsm& f) {
    os << "current term: " << f._current_term << ", ";
    os << "current leader: " << f._current_leader << ", ";
    os << "len messages: " << f._messages.size() << ", ";
    os << "voted for: " << f._voted_for << ", ";
    os << "commit idx:" << f._commit_idx << ", ";
    // os << "last applied: " << f._last_applied << ", ";
    os << "log (" << f._log << "), ";
    os << "observed (current term: " << f._observed._current_term << ", ";
    os << "voted for: " << f._observed._voted_for << ", ";
    os << "commit index: " << f._observed._commit_idx << "), ";
    os << "current time: " << f._clock.now() << ", ";
    os << "last election time: " << f._last_election_time << ", ";
    if (f._votes) {
        os << "votes (" << *f._votes << "), ";
    }
    os << "messages: " << f._messages.size() << ", ";
    os << "current_config (";
    for (auto& server: f._current_config.servers) {
        os << server.id << ", ";
    }
    os << "), ";

    if (std::holds_alternative<leader>(f._state)) {
        os << "leader, ";
    } else if (std::holds_alternative<candidate>(f._state)) {
        os << "candidate";
    } else if (std::holds_alternative<follower>(f._state)) {
        os << "follower";
    }
    if (f._tracker) {
        os << "followers (";
        for (const auto& [server_id, follower_progress]: *f._tracker) {
            os << server_id << ", ";
            os << follower_progress.next_idx << ", ";
            os << follower_progress.match_idx << ", ";
            if (follower_progress.state == follower_progress::state::PROBE) {
                os << "PROBE, ";
            } else if (follower_progress.state == follower_progress::state::PIPELINE) {
                os << "PIPELINE, ";
            }
            os << follower_progress.in_flight;
            os << "; ";
        }
        os << ")";

    }
    return os;
}

} // end of namespace raft
