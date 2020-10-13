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
#include "log.hh"

namespace raft {

log_entry_ptr& log::get_entry(index_t i) {
    return _log[i - start_idx()];
}

log_entry_ptr& log::operator[](size_t i) {
    assert(index_t(i) >= start_idx());
    return get_entry(index_t(i));
}

void log::emplace_back(log_entry&& e) {
    _log.emplace_back(seastar::make_lw_shared(std::move(e)));
}

bool log::empty() const {
    return _log.empty();
}

bool log::is_up_to_date(index_t idx, term_t term) const {
    // 3.6.1 Election restriction
    // Raft determines which of two logs is more up-to-date by comparing the
    // index and term of the last entries in the logs. If the logs have last
    // entries with different terms, then the log with the later term is more
    // up-to-date. If the logs end with the same term, then whichever log is
    // longer is more up-to-date.
    return term > last_term() || (term == last_term() && idx >= last_idx());
}

index_t log::last_idx() const {
    return index_t(_log.size()) + start_idx() - index_t(1);
}

index_t log::next_idx() const {
    return last_idx() + index_t(1);
}

void log::truncate_head(index_t idx) {
    assert(idx >= start_idx());
    auto it = _log.begin() + (idx - start_idx());
    _log.erase(it, _log.end());
    stable_to(std::min(_stable_idx, last_idx()));
}

void log::truncate_tail(index_t idx) {
    assert(start_idx() <= idx);

    if (idx >= last_idx()) {
        _log.clear();
    } else if (idx > start_idx()) {
        _log.erase(_log.begin(), _log.begin() + idx - start_idx() + 1);
    }

    _stable_idx = std::max(idx, _stable_idx);
}

index_t log::start_idx() const {
    // log my contain entries included in the snapshot, so start idx
    // may be smaller that snapshot index
    return (_log.empty() ? _snapshot.idx  + index_t(1): _log[0]->idx);
}

term_t log::last_term() const {
    if (_log.empty()) {
        return term_t(0);
    }
    return _log.back()->term;
}

void log::stable_to(index_t idx) {
    assert(idx <= last_idx());
    _stable_idx = idx;
}

std::pair<bool, term_t> log::match_term(index_t idx, term_t term) const {
    if (idx == 0) {
        // Special case of empty log on leader,
        // TLA+ line 324.
        return std::make_pair(true, term_t(0));
    }

    // idx cannot point into the snapshot
    assert(idx >= start_idx() || idx == _snapshot.idx);

    term_t my_term;

    if (idx == _snapshot.idx) {
        my_term = _snapshot.term;
    } else {
        auto i = idx - start_idx();

        if (i >= _log.size()) {
            // We have a gap between the follower and the leader.
            return std::make_pair(false, term_t(0));
        }

        my_term =  _log[i]->term;
    }

    return my_term == term ? std::make_pair(true, term_t(0)) : std::make_pair(false, my_term);
}

index_t log::maybe_append(std::vector<log_entry>&& entries) {
    assert(!entries.empty());

    index_t last_new_idx = entries.back().idx;

    // We must scan through all entries if the log already
    // contains them to ensure the terms match.
    for (auto& e : entries) {
        if (e.idx <= last_idx()) {
            if (e.idx < start_idx()) {
                logger.trace("append_entries: skipping entry with idx {} less than log start {}",
                    e.idx, start_idx());
                continue;
            }
            if (e.term == get_entry(e.idx)->term) {
                logger.trace("append_entries: entries with index {} has matching terms {}", e.idx, e.term);
                continue;
            }
            logger.trace("append_entries: entries with index {} has non matching terms e.term={}, _log[i].term = {}",
                e.idx, e.term, get_entry(e.idx)->term);
            // If an existing entry conflicts with a new one (same
            // index but different terms), delete the existing
            // entry and all that follow it (§5.3).
            truncate_head(e.idx);
        }
        // Assert log monotonicity
        assert(e.idx == next_idx());
        _log.emplace_back(seastar::make_lw_shared(std::move(e)));
    }

    return last_new_idx;
}

size_t log::apply_snapshot(snapshot&& snp, size_t trailing) {
    size_t ret = 0;
    if (snp.idx - start_idx() > index_t(trailing)) {
        ret = _log.size();
       // call truncate first since it uses old snapshot
       truncate_tail(index_t(snp.idx - trailing));
       ret -= _log.size();
    }

    _snapshot = std::move(snp);
    return ret;
}

std::ostream& operator<<(std::ostream& os, const log& l) {
    os << "next idx: " << l.next_idx() << ", ";
    os << "last idx: " << l.last_idx() << ", ";
    os << "stable idx: " << l.stable_idx() << ", ";
    os << "start idx: " << l.start_idx() << ", ";
    os << "last term: " << l.last_term();
    return os;
}

} // end of namespace raft
