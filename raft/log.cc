/*
 * Copyright (C) 2020-present ScyllaDB
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
    return _log[i - _first_idx];
}

log_entry_ptr& log::operator[](size_t i) {
    assert(!_log.empty() && index_t(i) >= _first_idx);
    return get_entry(index_t(i));
}

void log::emplace_back(log_entry_ptr&& e) {
    _log.emplace_back(std::move(e));
    if (std::holds_alternative<configuration>(_log.back()->data)) {
        _prev_conf_idx = _last_conf_idx;
        _last_conf_idx = last_idx();
    }
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
    return index_t(_log.size()) + _first_idx - index_t(1);
}

index_t log::next_idx() const {
    return last_idx() + index_t(1);
}

void log::truncate_uncommitted(index_t idx) {
    assert(idx >= _first_idx);
    auto it = _log.begin() + (idx - _first_idx);
    _log.erase(it, _log.end());
    stable_to(std::min(_stable_idx, last_idx()));
    if (_last_conf_idx > last_idx()) {
        // If _prev_conf_idx is 0, this log does not contain any
        // other configuration changes, since no two uncommitted
        // configuration changes can be in progress.
        assert(_prev_conf_idx < _last_conf_idx);
        _last_conf_idx = _prev_conf_idx;
        _prev_conf_idx = index_t{0};
    }
}

void log::init_last_conf_idx() {
    for (auto it = _log.rbegin(); it != _log.rend(); ++it) {
        if (std::holds_alternative<configuration>((**it).data)) {
            if (_last_conf_idx == index_t{0}) {
                _last_conf_idx = (**it).idx;
            } else {
                _prev_conf_idx = (**it).idx;
                break;
            }
        }
   }
}

term_t log::last_term() const {
    if (_log.empty()) {
        return _snapshot.term;
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

    // We got some very old AppendEntries we can safely ignore.
    if (idx < _snapshot.idx) {
        return std::make_pair(false, last_term());
    }

    term_t my_term;

    if (idx == _snapshot.idx) {
        my_term = _snapshot.term;
    } else {
        auto i = idx - _first_idx;

        if (i >= _log.size()) {
            // We have a gap between the follower and the leader.
            return std::make_pair(false, term_t(0));
        }

        my_term =  _log[i]->term;
    }

    return my_term == term ? std::make_pair(true, term_t(0)) : std::make_pair(false, my_term);
}

std::optional<term_t> log::term_for(index_t idx) const {
    if (!_log.empty() && idx >= _first_idx) {
        return _log[idx - _first_idx]->term;
    }
    if (idx == _snapshot.idx) {
        return _snapshot.term;
    }
    return {};
}

const configuration& log::get_configuration() const {
    return _last_conf_idx ? std::get<configuration>(_log[_last_conf_idx - _first_idx]->data) : _snapshot.config;
}

index_t log::maybe_append(std::vector<log_entry_ptr>&& entries) {
    assert(!entries.empty());

    index_t last_new_idx = entries.back()->idx;

    // We must scan through all entries if the log already
    // contains them to ensure the terms match.
    for (auto& e : entries) {
        if (e->idx <= last_idx()) {
            if (e->idx < _first_idx) {
                logger.trace("append_entries: skipping entry with idx {} less than log start {}",
                    e->idx, _first_idx);
                continue;
            }
            if (e->term == get_entry(e->idx)->term) {
                logger.trace("append_entries: entries with index {} has matching terms {}", e->idx, e->term);
                continue;
            }
            logger.trace("append_entries: entries with index {} has non matching terms e.term={}, _log[i].term = {}",
                e->idx, e->term, get_entry(e->idx)->term);
            // If an existing entry conflicts with a new one (same
            // index but different terms), delete the existing
            // entry and all that follow it (§5.3).
            truncate_uncommitted(e->idx);
        }
        // Assert log monotonicity
        assert(e->idx == next_idx());
        emplace_back(std::move(e));
    }

    return last_new_idx;
}

size_t log::apply_snapshot(snapshot&& snp, size_t trailing) {
    assert (snp.idx > _snapshot.idx);

    size_t removed;
    auto idx = snp.idx;

    if (idx > last_idx()) {
        // Remove all entries ignoring the 'trailing' argument,
        // since otherwise there would be a gap between old
        // entries and the next entry index.
        removed = _log.size();
        _log.clear();
        _first_idx = idx + index_t{1};
    } else {
        removed = _log.size() - (last_idx() - idx);
        removed -= std::min(trailing, removed);
        _log.erase(_log.begin(), _log.begin() + removed);
        _first_idx = _first_idx + index_t{removed};
    }

    _stable_idx = std::max(idx, _stable_idx);

    if (_first_idx > _prev_conf_idx) {
        _prev_conf_idx = index_t{0};
        if (_first_idx > _last_conf_idx) {
            _last_conf_idx = index_t{0};
        }
    }

    _snapshot = std::move(snp);

    return removed;
}

std::ostream& operator<<(std::ostream& os, const log& l) {
    os << "first idx: " << l._first_idx << ", ";
    os << "last idx: " << l.last_idx() << ", ";
    os << "next idx: " << l.next_idx() << ", ";
    os << "stable idx: " << l.stable_idx() << ", ";
    os << "last term: " << l.last_term();
    return os;
}

} // end of namespace raft
