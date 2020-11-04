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

#include <boost/icl/interval_map.hpp>
#include <boost/range/adaptor/map.hpp>

#include "compatible_ring_position.hh"
#include "compaction_strategy_impl.hh"
#include "leveled_compaction_strategy.hh"
#include "time_window_compaction_strategy.hh"

#include "sstable_set_impl.hh"

namespace sstables {

void sstable_run::insert(shared_sstable sst) {
    _all.insert(std::move(sst));
}

void sstable_run::erase(shared_sstable sst) {
    _all.erase(sst);
}

uint64_t sstable_run::data_size() const {
    return boost::accumulate(_all | boost::adaptors::transformed(std::mem_fn(&sstable::data_size)), uint64_t(0));
}

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run) {
    os << "Run = {\n";
    if (run.all().empty()) {
        os << "  Identifier: not found\n";
    } else {
        os << format("  Identifier: {}\n", (*run.all().begin())->run_identifier());
    }

    auto frags = boost::copy_range<std::vector<shared_sstable>>(run.all());
    boost::sort(frags, [] (const shared_sstable& x, const shared_sstable& y) {
        return x->get_first_decorated_key().token() < y->get_first_decorated_key().token();
    });
    os << "  Fragments = {\n";
    for (auto& frag : frags) {
        os << format("    {}={}:{}\n", frag->generation(), frag->get_first_decorated_key().token(), frag->get_last_decorated_key().token());
    }
    os << "  }\n}\n";
    return os;
}

sstable_set_data::sstable_set_data(std::unique_ptr<sstable_set_impl> impl)
    : impl(std::move(impl)) {
}

void sstable_set_data::insert(shared_sstable sst) {
    auto it = sstables_and_times_added.find(sst);
    if (it != sstables_and_times_added.end()) {
        // the sstable has already been added in some version
        it->second++;
        return;
    }
    try {
        impl->insert(sst);
        all_runs[sst->run_identifier()].insert(sst);
        sstables_and_times_added.emplace(sst, 1);
    } catch (...) {
        impl->erase(sst);
        auto runs_it = all_runs.find(sst->run_identifier());
        if (runs_it != all_runs.end()) {
            runs_it->second.erase(sst);
            if (runs_it->second.empty()) {
                all_runs.erase(runs_it);
            }
        }
        throw;
    }
}

std::vector<shared_sstable> sstable_set_data::select(const dht::partition_range& range) const {
    return impl->select(range);
}

std::unordered_set<shared_sstable> sstable_set_data::select_by_run_id(utils::UUID run_id) const {
    return all_runs.at(run_id);
}

// Called when a version that was adding an sstable was removed or when the sstable was later erased in that version.
void sstable_set_data::remove(shared_sstable sst) {
    if (--sstables_and_times_added.at(sst) == 0) {
        impl->erase(sst);
        all_runs[sst->run_identifier()].erase(sst);
        sstables_and_times_added.erase(sst);
    }
}

sstable_set_version_reference::~sstable_set_version_reference() {
    if (_p) {
        _p->remove_reference();
        if (_p->can_merge_with_next()) {
            // merging will destroy the last reference to the version and the version will be deleted as a result
            _p->merge_with_next();
        } else if (_p->can_delete()) {
            delete _p;
            _p = nullptr;
        }
    }
}

sstable_set_version_reference::sstable_set_version_reference(sstable_set_version* p) : _p(p) {
    if (_p) {
        _p->add_reference();
    }
}

sstable_set_version_reference::sstable_set_version_reference(const sstable_set_version_reference& ref)  : _p(ref._p) {
    if (_p) {
        _p->add_reference();
    }
}

sstable_set_version_reference::sstable_set_version_reference(sstable_set_version_reference&& ref) noexcept : _p(ref._p) {
    ref._p = nullptr;
}

sstable_set_version_reference& sstable_set_version_reference::operator=(const sstable_set_version_reference& ref) {
    *this = sstable_set_version_reference(ref);
    return *this;
}

sstable_set_version_reference& sstable_set_version_reference::operator=(sstable_set_version_reference&& ref) noexcept {
    if (this != &ref) {
        // Destroying this reference may invalide other references, so we're taking over the pointer managed by
        // the moved reference, and reassigning it after calling the destructor
        auto ptr = ref._p;
        ref._p = nullptr;
        this->~sstable_set_version_reference();
        _p = ptr;
    }
    return *this;
}

static sstable_set_version_reference make_sstable_set_version(std::unique_ptr<sstable_set_impl> impl, schema_ptr s) {
    sstable_set_version* new_version = new sstable_set_version(std::move(impl), std::move(s));
    return new_version->get_reference_to_this();
}

sstable_list::sstable_list(std::unique_ptr<sstable_set_impl> impl, schema_ptr s)
    : _version(make_sstable_set_version(std::move(impl), std::move(s))) {
}

sstable_list::sstable_list(const sstable_list& sstl)
    : _version(sstl._version->get_reference_to_new_copy()) {
    // copying an sstable_list creates a new sstable_set_version
}

sstable_list::sstable_list(sstable_list&& sstl) noexcept = default;

sstable_list& sstable_list::operator=(const sstable_list& sstl) {
    if (this != &sstl) {
        *this = sstable_list(sstl);
    }
    return *this;
}

sstable_list& sstable_list::operator=(sstable_list&& sstl) noexcept {
    if (this != &sstl) {
        this->~sstable_list();
        _version = std::move(sstl._version);
    }
    return *this;
}

// Moves the iterator to the next sstable which is contained by the associated sstable_set, or to the end
// If the iterator already references a satisfying sstable, no changes are made.
void sstable_list::const_iterator::advance() {
    while (_it != (*_ver)->all().end() && !(*_ver)->contains(_it->first)) {
        _it++;
    }
}

sstable_list::const_iterator::const_iterator(std::map<shared_sstable, unsigned>::const_iterator it, const sstable_set_version_reference* ver)
    : _it(std::move(it))
    , _ver(ver) {
        advance();
}

sstable_list::const_iterator& sstable_list::const_iterator::operator++() {
    assert(_it != (*_ver)->all().end());
    _it++;
    advance();
    return *this;
}

sstable_list::const_iterator sstable_list::const_iterator::operator++(int) {
    const_iterator it = *this;
    operator++();
    return it;
}

const shared_sstable& sstable_list::const_iterator::operator*() const {
    return _it->first;
}

bool sstable_list::const_iterator::operator==(const const_iterator& it) const {
    assert(_ver == it._ver);
    return _it == it._it;
}

sstable_list::const_iterator sstable_list::begin() const {
    return const_iterator(_version->all().begin(), &_version);
}

sstable_list::const_iterator sstable_list::end() const {
    return const_iterator(_version->all().end(), &_version);
}

size_t sstable_list::size() const {
    return _version->size();
}

void sstable_list::insert(shared_sstable sst) {
    _version = _version->insert(sst);
}

void sstable_list::erase(shared_sstable sst) {
    _version = _version->erase(sst);
}

bool sstable_list::contains(shared_sstable sst) const {
    return _version->contains(sst);
}

bool sstable_list::empty() const {
    return _version->size() == 0;
}

const sstable_set_version& sstable_list::version() const {
    return *_version;
}

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s) {
    if (!impl->empty()) {
        throw std::logic_error("Can't create an sstable_set using a non-empty sstable_set_impl");
    }
    _all = make_lw_shared<sstable_list>(std::move(impl), std::move(s));
}

sstable_set::sstable_set(const sstable_set& x)
    : _all(make_lw_shared<sstable_list>(*x._all)) {
}

sstable_set::sstable_set(sstable_set&& x) noexcept = default;

sstable_set& sstable_set::operator=(const sstable_set& ssts) {
    *this = sstable_set(ssts);
    return *this;
}

sstable_set& sstable_set::operator=(sstable_set&& ssts) noexcept = default;


std::vector<shared_sstable> sstable_set::select(const dht::partition_range& range) const {
    return _all->version().select(range);
}

// Return all runs which contain any of the input sstables.
std::vector<sstable_run> sstable_set::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    return _all->version().select_sstable_runs(sstables);
}

lw_shared_ptr<const sstable_list> sstable_set::all() const {
    return _all;
}

void sstable_set::insert(shared_sstable sst) {
    _all->insert(sst);
}

void sstable_set::erase(shared_sstable sst) {
    _all->erase(sst);
}

sstable_set::incremental_selector::~incremental_selector() = default;

sstable_set::incremental_selector::incremental_selector(sstable_set::incremental_selector&&) noexcept = default;

sstable_set::incremental_selector::incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s, lw_shared_ptr<sstable_list> sstl)
    : _impl(std::move(impl))
    , _cmp(s)
    , _sstl(std::move(sstl)) {
}

sstable_set::incremental_selector::selection sstable_set::incremental_selector::select(const dht::ring_position_view& pos) const {
    if (!_current_range_view || !_current_range_view->contains(pos, _cmp)) {
        std::vector<shared_sstable> current_versioned_sstables;
        std::tie(_current_range, current_versioned_sstables, _current_next_position) = _impl->select(pos);
        _current_sstables = boost::copy_range<std::vector<shared_sstable>>(current_versioned_sstables
                             | boost::adaptors::filtered([this] (shared_sstable sst) { return _sstl->contains(sst); })
                             | boost::adaptors::transformed([] (shared_sstable sst) { return sst; }));
        _current_range_view = _current_range->transform([] (const dht::ring_position& rp) { return dht::ring_position_view(rp); });
    }
    return {_current_sstables, _current_next_position};
}

sstables::sstable_set::incremental_selector sstable_set::make_incremental_selector() const {
    return incremental_selector(_all->version().make_incremental_selector(), *_all->version().get_schema(), _all);
}

sstable_set_version::~sstable_set_version() {
    for (auto& added_sst : _added) {
        _base_set->remove(added_sst);
    }
    if (_prev) {
        _prev->_next.erase(this);
    }
}

// the sstable_set_impl must be empty
sstable_set_version::sstable_set_version(std::unique_ptr<sstable_set_impl> impl, schema_ptr schema)
    : _base_set(make_lw_shared<sstable_set_data>(std::move(impl)))
    , _schema(std::move(schema)) {
}

// Creates a new version based on ver
sstable_set_version::sstable_set_version(sstable_set_version* ver)
    : _base_set(ver->_base_set)
    , _schema(ver->_schema)
    , _prev(ver) {
        _prev->_next.insert(this);
}

// Merges changes made in this version into the next version (can be called only when there is a single next version,
// and no further changes can be made to this one, i.e. no sstable_list references this version).
void sstable_set_version::merge_with_next() noexcept {
    auto next_version = *_next.begin();
    next_version->_added = std::move(_added);
    next_version->_erased = std::move(_erased);
    auto next_nh = _next.extract(*_next.begin());
    if (_prev) {
        _prev->_next.erase(this);
        _prev->_next.insert(std::move(next_nh));
    }
    next_version->_prev = std::move(_prev); // destroys this by overwriting the last reference
}

void sstable_set_version::propagate_inserted_sstable(const shared_sstable& sst) noexcept {
    if (_reference_count > _next.size()) {
        // If there exists a reference outside child versions (from sstable_list), this version can still be read from, so we can't modify it
        return;
    }
    for (auto& ver_chck : _next) {
        if (!ver_chck->_added.contains(sst)) {
            return;
        }
    }
    // Remove the sstable from child versions and get a node handle to insert in this version
    auto sst_nh = (*_next.begin())->_added.extract(sst);
    for (auto& ver_chck : _next) {
        auto nh = ver_chck->_added.extract(sst_nh.value());
        if (!nh.empty()) {
            _base_set->remove(nh.value());
        }
    }
    auto it = _erased.find(sst_nh.value());
    if (it != _erased.end()) {
        // If the sstable was erased in this version and added in all its children, its as if it weren't added or inserted in any of them
        // because we won't read from this version anymore.
        _erased.erase(it);
        _base_set->remove(sst_nh.value());
    } else {
        auto added_it = _added.insert(std::move(sst_nh)).position;
        if (_prev) {
            _prev->propagate_inserted_sstable(*added_it);
        }
    }
}

void sstable_set_version::propagate_erased_sstable(const shared_sstable& sst) noexcept {
    if (_reference_count > _next.size()) {
        // If there exists a reference outside child versions (from sstable_list), this version can still be read from, so we can't modify it
        return;
    }
    for (auto& ver_chck : _next) {
        if (!ver_chck->_erased.contains(sst)) {
            return;
        }
    }
    // Remove the sstable from child versions and get a node handle to insert in this version
    auto sst_nh = (*_next.begin())->_erased.extract(sst);
    for (auto& ver_chck : _next) {
        ver_chck->_erased.extract(sst_nh.value());
    }
    auto it = _added.find(sst_nh.value());
    if (it != _added.end()) {
        // If the sstable was added in this version and erased in all its children, its as if it weren't added or inserted in any of them
        // because we won't read from this version anymore.
        _added.erase(it);
        _base_set->remove(sst_nh.value());
    } else {
        auto erased_it = _erased.insert(std::move(sst_nh)).position;
        if (_prev) {
            _prev->propagate_erased_sstable(*erased_it);
        }
    }
}

// Called when a reference to the version gets removed - if the reference was from an sstable_list, it's the first time we can propagate any
// changes, and if the reference was from another sstable_set_version, we want to check if there were any changes that were present in all
// versions based on this one, but absent in the version that was just removed.
void sstable_set_version::propagate_changes_from_next_versions() noexcept {
    if (_reference_count > _next.size() || _next.empty()) {
        // If there exists a reference outside child versions (from sstable_list), this version can still be read from, so we can't modify it
        // Or there are no child versions so there is nothing to propagate
        return;
    }
    sstable_set_version* next_ver = *_next.begin();
    // Propagate additions
    for (auto ver : _next) {
        if (ver->_added.size() < next_ver->_added.size()) {
            next_ver = ver;
        }
    }
    for (auto it = next_ver->_added.begin(); it != next_ver->_added.end();) {
        auto& sst = *it;
        it++;
        propagate_inserted_sstable(sst);
    }

    next_ver = *_next.begin();
    // Propagate erasures
    for (auto ver : _next) {
        if (ver->_erased.size() < next_ver->_erased.size()) {
            next_ver = ver;
        }
    }
    for (auto it = next_ver->_erased.begin(); it != next_ver->_erased.end();) {
        auto& sst = *it;
        it++;
        propagate_erased_sstable(sst);
    }
}

const sstable_set_version* sstable_set_version::get_previous_version() const {
    return _prev.get();
}

bool sstable_set_version::can_merge_with_next() const noexcept {
    return _reference_count == 1 && _next.size() == 1;
}

bool sstable_set_version::can_delete() const noexcept {
    return _reference_count == 0;
}

void sstable_set_version::add_reference() noexcept {
    _reference_count++;
}

void sstable_set_version::remove_reference() noexcept {
    _reference_count--;
    propagate_changes_from_next_versions();
}

schema_ptr sstable_set_version::get_schema() const {
    return _schema;
}

std::vector<shared_sstable> sstable_set_version::select(const dht::partition_range& range) const {
    return boost::copy_range<std::vector<shared_sstable>>(_base_set->select(range)
         | boost::adaptors::filtered([this] (shared_sstable sst) { return this->contains(sst); }));
}

// Return all runs which contain any of the input sstables.
std::vector<sstable_run> sstable_set_version::select_sstable_runs(const std::vector<shared_sstable>& sstables) const {
    auto run_ids = boost::copy_range<std::unordered_set<utils::UUID>>(sstables | boost::adaptors::transformed(std::mem_fn(&sstable::run_identifier)));
    return boost::copy_range<std::vector<sstable_run>>(run_ids | boost::adaptors::transformed([this] (utils::UUID run_id) {
        return sstable_run(boost::copy_range<std::unordered_set<shared_sstable>>(_base_set->select_by_run_id(run_id)
                         | boost::adaptors::filtered([this] (shared_sstable sst) { return this->contains(sst); })));
    }));
}

const std::map<shared_sstable, unsigned>& sstable_set_version::all() const {
    return _base_set->sstables_and_times_added;
}

// Provides strong exception guarantee
sstable_set_version_reference sstable_set_version::insert(shared_sstable sst) {
    if (this->contains(sst)) {
        return get_reference_to_this();
    }
    if (_next.size()) {
        auto sstvr = get_reference_to_new_copy();
        // The new version has no copies based on it, so inserting into it doesn't create another version
        return sstvr->insert(sst);
    }
    auto it = _erased.find(sst);
    if (it != _erased.end()) {
        _erased.erase(it);
    } else {
        _base_set->insert(sst);
        try {
            _added.insert(sst);
            if (_prev) {
                _prev->propagate_inserted_sstable(sst);
            }
        } catch (...) {
            _base_set->remove(sst);
            throw;
        }
    }
    return get_reference_to_this();
}

// Provides strong exception guarantee
sstable_set_version_reference sstable_set_version::erase(shared_sstable sst) {
    if (!this->contains(sst)) {
        return get_reference_to_this();
    }
    if (_next.size()) {
        auto sstvr = get_reference_to_new_copy();
        // The new version has no copies based on it, so erasing from it doesn't create another version
        return sstvr->erase(sst);
    }
    auto it = _added.find(sst);
    if (it != _added.end()) {
        _added.erase(it);
        _base_set->remove(sst);
    } else {
        _erased.insert(sst);
        if (_prev) {
            _prev->propagate_erased_sstable(sst);
        }
    }
    return get_reference_to_this();
}

bool sstable_set_version::contains(shared_sstable sst) const {
    return _added.contains(sst) || (!_erased.contains(sst) && _prev && _prev->contains(sst));
}

size_t sstable_set_version::size() const {
    return _added.size() - _erased.size() + (_prev ? _prev->size() : 0);
}

std::unique_ptr<incremental_selector_impl> sstable_set_version::make_incremental_selector() const {
    return _base_set->impl->make_incremental_selector();
}

flat_mutation_reader
sstable_set_version::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_list& sstl) const {
    return _base_set->impl->create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pr, slice, pc, std::move(trace_state), fwd, fwd_mr, sstl);
}

sstable_set_version_reference sstable_set_version::get_reference_to_this() {
    return sstable_set_version_reference(this);
}

sstable_set_version_reference sstable_set_version::get_reference_to_new_copy() {
    return sstable_set_version_reference(new sstable_set_version(this));
}

std::unique_ptr<sstable_set_impl> bag_sstable_set::clone() const {
    return std::make_unique<bag_sstable_set>(*this);
}

std::vector<shared_sstable> bag_sstable_set::select(const dht::partition_range& range) const {
    return _sstables;
}

void bag_sstable_set::insert(shared_sstable sst) {
    _sstables.push_back(std::move(sst));
}

void bag_sstable_set::erase(shared_sstable sst) {
    auto it = boost::range::find(_sstables, sst);
    if (it != _sstables.end()){
        _sstables.erase(it);
    }
}

bool bag_sstable_set::empty() const {
    return _sstables.empty();
}

class bag_sstable_set::incremental_selector : public incremental_selector_impl {
    const std::vector<shared_sstable>& _sstables;
public:
    incremental_selector(const std::vector<shared_sstable>& sstables)
        : _sstables(sstables) {
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view&) override {
        return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), _sstables, dht::ring_position_view::max());
    }
};

std::unique_ptr<incremental_selector_impl> bag_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_sstables);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema& s, const dht::partition_range& range) {
    return interval_type::closed(
            compatible_ring_position_or_view(s, dht::ring_position_view(range.start()->value())),
            compatible_ring_position_or_view(s, dht::ring_position_view(range.end()->value())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const dht::partition_range& range) const {
    return make_interval(*_schema, range);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const schema_ptr& s, const sstable& sst) {
    return interval_type::closed(
            compatible_ring_position_or_view(s, dht::ring_position(sst.get_first_decorated_key())),
            compatible_ring_position_or_view(s, dht::ring_position(sst.get_last_decorated_key())));
}

partitioned_sstable_set::interval_type partitioned_sstable_set::make_interval(const sstable& sst) {
    return make_interval(_schema, sst);
}

partitioned_sstable_set::interval_type partitioned_sstable_set::singular(const dht::ring_position& rp) const {
    // We should use the view here, since this is used for queries.
    auto rpv = dht::ring_position_view(rp);
    auto crp = compatible_ring_position_or_view(*_schema, std::move(rpv));
    return interval_type::closed(crp, crp);
}

std::pair<partitioned_sstable_set::map_iterator, partitioned_sstable_set::map_iterator>
partitioned_sstable_set::query(const dht::partition_range& range) const {
    if (range.start() && range.end()) {
        return _leveled_sstables.equal_range(make_interval(range));
    }
    else if (range.start() && !range.end()) {
        auto start = singular(range.start()->value());
        return { _leveled_sstables.lower_bound(start), _leveled_sstables.end() };
    } else if (!range.start() && range.end()) {
        auto end = singular(range.end()->value());
        return { _leveled_sstables.begin(), _leveled_sstables.upper_bound(end) };
    } else {
        return { _leveled_sstables.begin(), _leveled_sstables.end() };
    }
}

bool partitioned_sstable_set::store_as_unleveled(const shared_sstable& sst) const {
    return _use_level_metadata && sst->get_sstable_level() == 0;
}

dht::ring_position partitioned_sstable_set::to_ring_position(const compatible_ring_position_or_view& crp) {
    // Ring position views, representing bounds of sstable intervals are
    // guaranteed to have key() != nullptr;
    const auto& pos = crp.position();
    return dht::ring_position(pos.token(), *pos.key());
}

dht::partition_range partitioned_sstable_set::to_partition_range(const interval_type& i) {
    return dht::partition_range::make(
            {to_ring_position(i.lower()), boost::icl::is_left_closed(i.bounds())},
            {to_ring_position(i.upper()), boost::icl::is_right_closed(i.bounds())});
}

dht::partition_range partitioned_sstable_set::to_partition_range(const dht::ring_position_view& pos, const interval_type& i) {
    auto lower_bound = [&] {
        if (pos.key()) {
            return dht::partition_range::bound(dht::ring_position(pos.token(), *pos.key()),
                    pos.is_after_key() == dht::ring_position_view::after_key::no);
        } else {
            return dht::partition_range::bound(dht::ring_position(pos.token(), pos.get_token_bound()), true);
        }
    }();
    auto upper_bound = dht::partition_range::bound(to_ring_position(i.lower()), !boost::icl::is_left_closed(i.bounds()));
    return dht::partition_range::make(std::move(lower_bound), std::move(upper_bound));
}

partitioned_sstable_set::partitioned_sstable_set(schema_ptr schema, bool use_level_metadata)
        : _schema(std::move(schema))
        , _use_level_metadata(use_level_metadata) {
}

std::unique_ptr<sstable_set_impl> partitioned_sstable_set::clone() const {
    return std::make_unique<partitioned_sstable_set>(*this);
}

std::vector<shared_sstable> partitioned_sstable_set::select(const dht::partition_range& range) const {
    auto ipair = query(range);
    auto b = std::move(ipair.first);
    auto e = std::move(ipair.second);
    value_set result;
    while (b != e) {
        boost::copy(b++->second, std::inserter(result, result.end()));
    }
    auto r = _unleveled_sstables;
    r.insert(r.end(), result.begin(), result.end());
    return r;
}

void partitioned_sstable_set::insert(shared_sstable sst) {
    if (store_as_unleveled(sst)) {
        _unleveled_sstables.push_back(std::move(sst));
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.add({make_interval(*sst), value_set({sst})});
    }
}

void partitioned_sstable_set::erase(shared_sstable sst) {
    if (store_as_unleveled(sst)) {
        _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
    } else {
        _leveled_sstables_change_cnt++;
        _leveled_sstables.subtract({make_interval(*sst), value_set({sst})});
    }
}

bool partitioned_sstable_set::empty() const {
    return _unleveled_sstables.empty() && _leveled_sstables.empty();
}

class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    schema_ptr _schema;
    const std::vector<shared_sstable>& _unleveled_sstables;
    const interval_map_type& _leveled_sstables;
    const uint64_t& _leveled_sstables_change_cnt;
    uint64_t _last_known_leveled_sstables_change_cnt;
    map_iterator _it;
    // Only to back the dht::ring_position_view returned from select().
    dht::ring_position _next_position;
private:
    dht::ring_position_view next_position(map_iterator it) {
        if (it == _leveled_sstables.end()) {
            _next_position = dht::ring_position::max();
            return dht::ring_position_view::max();
        } else {
            _next_position = partitioned_sstable_set::to_ring_position(it->first.lower());
            return dht::ring_position_view(_next_position, dht::ring_position_view::after_key(!boost::icl::is_left_closed(it->first.bounds())));
        }
    }
    static bool is_before_interval(const compatible_ring_position_or_view& crp, const interval_type& interval) {
        if (boost::icl::is_left_closed(interval.bounds())) {
            return crp < interval.lower();
        } else {
            return crp <= interval.lower();
        }
    }
    void maybe_invalidate_iterator(const compatible_ring_position_or_view& crp) {
        if (_last_known_leveled_sstables_change_cnt != _leveled_sstables_change_cnt) {
            _it = _leveled_sstables.lower_bound(interval_type::closed(crp, crp));
            _last_known_leveled_sstables_change_cnt = _leveled_sstables_change_cnt;
        }
    }
public:
    incremental_selector(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables,
                         const uint64_t& leveled_sstables_change_cnt)
        : _schema(std::move(schema))
        , _unleveled_sstables(unleveled_sstables)
        , _leveled_sstables(leveled_sstables)
        , _leveled_sstables_change_cnt(leveled_sstables_change_cnt)
        , _last_known_leveled_sstables_change_cnt(leveled_sstables_change_cnt)
        , _it(leveled_sstables.begin())
        , _next_position(dht::ring_position::min()) {
    }
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view> select(const dht::ring_position_view& pos) override {
        auto crp = compatible_ring_position_or_view(*_schema, pos);
        auto ssts = _unleveled_sstables;
        using namespace dht;

        maybe_invalidate_iterator(crp);

        while (_it != _leveled_sstables.end()) {
            if (boost::icl::contains(_it->first, crp)) {
                ssts.insert(ssts.end(), _it->second.begin(), _it->second.end());
                return std::make_tuple(partitioned_sstable_set::to_partition_range(_it->first), std::move(ssts), next_position(std::next(_it)));
            }
            // We don't want to skip current interval if pos lies before it.
            if (is_before_interval(crp, _it->first)) {
                return std::make_tuple(partitioned_sstable_set::to_partition_range(pos, _it->first), std::move(ssts), next_position(_it));
            }
            _it++;
        }
        return std::make_tuple(partition_range::make_open_ended_both_sides(), std::move(ssts), ring_position_view::max());
    }
};

time_series_sstable_set::time_series_sstable_set(schema_ptr schema)
    : _schema(std::move(schema))
    , _sstables(make_lw_shared<container_t>(position_in_partition::less_compare(*_schema))) {}

time_series_sstable_set::time_series_sstable_set(const time_series_sstable_set& s)
    : _schema(s._schema)
    , _sstables(make_lw_shared(*s._sstables)) {}

std::unique_ptr<sstable_set_impl> time_series_sstable_set::clone() const {
    return std::make_unique<time_series_sstable_set>(*this);
}

std::vector<shared_sstable> time_series_sstable_set::select(const dht::partition_range& range) const {
    return boost::copy_range<std::vector<shared_sstable>>(*_sstables | boost::adaptors::map_values);
}

// O(log n)
void time_series_sstable_set::insert(shared_sstable sst) {
    auto pos = sst->min_position();
    _sstables->emplace(pos, std::move(sst));
}

// O(n) worst case, but should be close to O(log n) most of the time
void time_series_sstable_set::erase(shared_sstable sst) {
    auto [first, last] = _sstables->equal_range(sst->min_position());
    auto it = std::find_if(first, last,
            [&sst] (const std::pair<position_in_partition, shared_sstable>& p) { return sst == p.second; });
    if (it != last) {
        _sstables->erase(it);
    }
}

bool time_series_sstable_set::empty() const {
    return _sstables->empty();
}

std::unique_ptr<incremental_selector_impl> time_series_sstable_set::make_incremental_selector() const {
    struct selector : public incremental_selector_impl {
        const time_series_sstable_set& _set;

        selector(const time_series_sstable_set& set) : _set(set) {}

        virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_view>
        select(const dht::ring_position_view&) override {
            return std::make_tuple(dht::partition_range::make_open_ended_both_sides(), _set.select(), dht::ring_position_view::max());
        }
    };

    return std::make_unique<selector>(*this);
}

// Queue of readers of sstables in a time_series_sstable_set,
// returning readers in order of the sstables' min_position()s.
//
// Skips sstables that don't pass the supplied filter.
// Guarantees that the filter will be called at most once for each sstable;
// exactly once after all sstables are iterated over.
//
// The readers are created lazily on-demand using the supplied factory function.
class min_position_reader_queue : public position_reader_queue {
    using container_t = time_series_sstable_set::container_t;
    using value_t = container_t::value_type;

    schema_ptr _schema;
    lw_shared_ptr<const container_t> _sstables;

    // Iterates over sstables in order of min_position().
    // Invariant: _it == _end or filter(it->second) == true
    container_t::const_iterator _it;
    const container_t::const_iterator _end;

    position_in_partition::tri_compare _cmp;

    std::function<flat_mutation_reader(sstable&)> _create_reader;
    std::function<bool(const sstable&)> _filter;

    flat_mutation_reader create_reader(sstable& sst) {
        return _create_reader(sst);
    }

    bool filter(const sstable& sst) const {
        return _filter(sst);
    }

public:
    min_position_reader_queue(schema_ptr schema,
            lw_shared_ptr<const time_series_sstable_set::container_t> sstables,
            std::function<flat_mutation_reader(sstable&)> create_reader,
            std::function<bool(const sstable&)> filter)
        : _schema(std::move(schema))
        , _sstables(std::move(sstables))
        , _it(_sstables->begin())
        , _end(_sstables->end())
        , _cmp(*_schema)
        , _create_reader(std::move(create_reader))
        , _filter(std::move(filter))
    {
        while (_it != _end && !this->filter(*_it->second)) {
            ++_it;
        }
    }

    virtual ~min_position_reader_queue() override = default;

    // Open sstable readers to all sstables with smallest min_position() from the set
    // {S: filter(S) and prev_min_pos < S.min_position() <= bound}, where `prev_min_pos` is the min_position()
    // of the sstables returned from last non-empty pop() or -infinity if no sstables were previously returned,
    // and `filter` is the filtering function provided when creating the queue.
    //
    // Note that there may be multiple returned sstables (all with the same position) or none.
    //
    // Note that S.min_position() is global for sstable S; if the readers are used to inspect specific partitions,
    // the minimal positions in these partitions might actually all be greater than S.min_position().
    virtual std::vector<reader_and_upper_bound> pop(position_in_partition_view bound) override {
        if (empty(bound)) {
            return {};
        }

        // by !empty(bound) and `_it` invariant:
        //      _it != _end, _it->first <= bound, and filter(*_it->second) == true
        assert(_cmp(_it->first, bound) <= 0);
        // we don't assert(filter(*_it->second)) due to the requirement that `filter` is called at most once for each sstable

        // Find all sstables with the same position as `_it` (they form a contiguous range in the container).
        auto next = std::find_if(std::next(_it), _end, [this] (const value_t& v) { return _cmp(v.first, _it->first) != 0; });

        // We'll return all sstables in the range [_it, next) which pass the filter
        std::vector<reader_and_upper_bound> ret;
        do {
            // loop invariant: filter(*_it->second) == true
            ret.emplace_back(create_reader(*_it->second), _it->second->max_position());
            // restore loop invariant
            do {
                ++_it;
            } while (_it != next && !filter(*_it->second));
        } while (_it != next);

        // filter(*_it->second) wasn't called yet since the inner `do..while` above checks _it != next first
        // restore the `_it` invariant before returning
        while (_it != _end && !filter(*_it->second)) {
            ++_it;
        }

        return ret;
    }

    // Is the set of sstables {S: filter(S) and prev_min_pos < S.min_position() <= bound} empty?
    // (see pop() for definition of `prev_min_pos`)
    virtual bool empty(position_in_partition_view bound) const override {
        return _it == _end || _cmp(_it->first, bound) > 0;
    }
};

std::unique_ptr<position_reader_queue> time_series_sstable_set::make_min_position_reader_queue(
        std::function<flat_mutation_reader(sstable&)> create_reader,
        std::function<bool(const sstable&)> filter) const {
    return std::make_unique<min_position_reader_queue>(_schema, _sstables, std::move(create_reader), std::move(filter));
}

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables, _leveled_sstables_change_cnt);
}

std::unique_ptr<sstable_set_impl> compaction_strategy_impl::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<bag_sstable_set>();
}

std::unique_ptr<sstable_set_impl> leveled_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<partitioned_sstable_set>(std::move(schema));
}

std::unique_ptr<sstable_set_impl> time_window_compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return std::make_unique<time_series_sstable_set>(std::move(schema));
}

sstable_set make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata) {
    return sstable_set(std::make_unique<partitioned_sstable_set>(schema, use_level_metadata), schema);
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(schema),
            schema);
}

using sstable_reader_factory_type = std::function<flat_mutation_reader(shared_sstable&, const dht::partition_range& pr)>;

static logging::logger irclogger("incremental_reader_selector");

// Incremental selector implementation for combined_mutation_reader that
// selects readers on-demand as the read progresses through the token
// range.
class incremental_reader_selector : public reader_selector {
    const dht::partition_range* _pr;
    lw_shared_ptr<const sstable_set> _sstables;
    tracing::trace_state_ptr _trace_state;
    std::optional<sstable_set::incremental_selector> _selector;
    std::unordered_set<int64_t> _read_sstable_gens;
    sstable_reader_factory_type _fn;

    flat_mutation_reader create_reader(shared_sstable sst) {
        tracing::trace(_trace_state, "Reading partition range {} from sstable {}", *_pr, seastar::value_of([&sst] { return sst->get_filename(); }));
        return _fn(sst, *_pr);
    }

public:
    explicit incremental_reader_selector(schema_ptr s,
            lw_shared_ptr<const sstable_set> sstables,
            const dht::partition_range& pr,
            tracing::trace_state_ptr trace_state,
            sstable_reader_factory_type fn)
        : reader_selector(s, pr.start() ? pr.start()->value() : dht::ring_position_view::min())
        , _pr(&pr)
        , _sstables(std::move(sstables))
        , _trace_state(std::move(trace_state))
        , _selector(_sstables->make_incremental_selector())
        , _fn(std::move(fn)) {

        irclogger.trace("{}: created for range: {} with {} sstables",
                fmt::ptr(this),
                *_pr,
                _sstables->all()->size());
    }

    incremental_reader_selector(const incremental_reader_selector&) = delete;
    incremental_reader_selector& operator=(const incremental_reader_selector&) = delete;

    incremental_reader_selector(incremental_reader_selector&&) = delete;
    incremental_reader_selector& operator=(incremental_reader_selector&&) = delete;

    virtual std::vector<flat_mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        irclogger.trace("{}: {}({})", fmt::ptr(this), __FUNCTION__, seastar::lazy_deref(pos));

        auto readers = std::vector<flat_mutation_reader>();

        do {
            auto selection = _selector->select(_selector_position);
            _selector_position = selection.next_position;

            irclogger.trace("{}: {} sstables to consider, advancing selector to {}", fmt::ptr(this), selection.sstables.size(),
                    _selector_position);

            readers = boost::copy_range<std::vector<flat_mutation_reader>>(selection.sstables
                    | boost::adaptors::filtered([this] (auto& sst) { return _read_sstable_gens.emplace(sst->generation()).second; })
                    | boost::adaptors::transformed([this] (auto& sst) { return this->create_reader(sst); }));
        } while (!_selector_position.is_max() && readers.empty() && (!pos || dht::ring_position_tri_compare(*_s, *pos, _selector_position) >= 0));

        irclogger.trace("{}: created {} new readers", fmt::ptr(this), readers.size());

        // prevents sstable_set::incremental_selector::_current_sstables from holding reference to
        // sstables when done selecting.
        if (_selector_position.is_max()) {
            _selector.reset();
        }

        return readers;
    }

    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        _pr = &pr;

        auto pos = dht::ring_position_view::for_range_start(*_pr);
        if (dht::ring_position_tri_compare(*_s, pos, _selector_position) >= 0) {
            return create_new_readers(pos);
        }

        return {};
    }
};

// The returned function uses the bloom filter to check whether the given sstable
// may have a partition given by the ring position `pos`.
//
// Returning `false` means the sstable doesn't have such a partition.
// Returning `true` means it may, i.e. we don't know whether or not it does.
//
// Assumes the given `pos` and `schema` are alive during the function's lifetime.
static std::predicate<const sstable&> auto
make_pk_filter(const dht::ring_position& pos, const schema& schema) {
    return [&pos, key = key::from_partition_key(schema, *pos.key()), cmp = dht::ring_position_comparator(schema)] (const sstable& sst) {
        return cmp(pos, sst.get_first_decorated_key()) >= 0 &&
               cmp(pos, sst.get_last_decorated_key()) <= 0 &&
               sst.filter_has_key(key);
    };
}

// Filter out sstables for reader using bloom filter
static std::vector<shared_sstable>
filter_sstable_for_reader_by_pk(std::vector<shared_sstable>&& sstables, const schema& schema, const dht::ring_position& pos) {
    auto filter = [_filter = make_pk_filter(pos, schema)] (const shared_sstable& sst) { return !_filter(*sst); };
    sstables.erase(boost::remove_if(sstables, filter), sstables.end());
    return sstables;
}

// Filter out sstables for reader using sstable metadata that keeps track
// of a range for each clustering component.
static std::vector<shared_sstable>
filter_sstable_for_reader_by_ck(std::vector<shared_sstable>&& sstables, column_family& cf, const schema_ptr& schema,
        const query::partition_slice& slice) {
    // no clustering filtering is applied if schema defines no clustering key or
    // compaction strategy thinks it will not benefit from such an optimization,
    // or the partition_slice includes static columns.
    if (!schema->clustering_key_size() || !cf.get_compaction_strategy().use_clustering_key_filter() || slice.static_columns.size()) {
        return sstables;
    }

    ::cf_stats* stats = cf.cf_stats();
    stats->clustering_filter_count++;
    stats->sstables_checked_by_clustering_filter += sstables.size();

    auto ck_filtering_all_ranges = slice.get_all_ranges();
    // fast path to include all sstables if only one full range was specified.
    // For example, this happens if query only specifies a partition key.
    if (ck_filtering_all_ranges.size() == 1 && ck_filtering_all_ranges[0].is_full()) {
        stats->clustering_filter_fast_path_count++;
        stats->surviving_sstables_after_clustering_filter += sstables.size();
        return sstables;
    }

    auto skipped = std::partition(sstables.begin(), sstables.end(), [&ranges = ck_filtering_all_ranges] (const shared_sstable& sst) {
        return sst->may_contain_rows(ranges);
    });
    sstables.erase(skipped, sstables.end());
    stats->surviving_sstables_after_clustering_filter += sstables.size();

    return sstables;
}

flat_mutation_reader
sstable_set_impl::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_list& sstl) const
{
    const auto& pos = pr.start()->value();
    auto selected_sstables = filter_sstable_for_reader_by_pk(select(pr), *schema, pos);

    selected_sstables.erase(boost::remove_if(selected_sstables, [&sstl] (const shared_sstable& sst) { return !sstl.contains(sst); }), selected_sstables.end());

    auto num_sstables = selected_sstables.size();
    if (!num_sstables) {
        return make_empty_flat_reader(schema, permit);
    }
    auto readers = boost::copy_range<std::vector<flat_mutation_reader>>(
        filter_sstable_for_reader_by_ck(std::move(selected_sstables), *cf, schema, slice)
        | boost::adaptors::transformed([&] (const shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pos, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->make_reader(schema, permit, pr, slice, pc, trace_state, fwd);
        })
    );

    // If filter_sstable_for_reader_by_ck filtered any sstable that contains the partition
    // we want to emit partition_start/end if no rows were found,
    // to prevent https://github.com/scylladb/scylla/issues/3552.
    //
    // Use `flat_mutation_reader_from_mutations` with an empty mutation to emit
    // the partition_start/end pair and append it to the list of readers passed
    // to make_combined_reader to ensure partition_start/end are emitted even if
    // all sstables actually containing the partition were filtered.
    auto num_readers = readers.size();
    if (num_readers != num_sstables) {
        readers.push_back(flat_mutation_reader_from_mutations(permit, {mutation(schema, *pos.key())}, slice, fwd));
    }
    sstable_histogram.add(num_readers);
    return make_combined_reader(schema, std::move(permit), std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader
time_series_sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr,
        const sstable_list& sstl) const {
    const auto& pos = pr.start()->value();
    // First check if the optimized algorithm for TWCS single partition queries can be applied.
    // Multiple conditions must be satisfied:
    // 1. The sstables must be sufficiently modern so they contain the min/max column metadata.
    // 2. The schema cannot have static columns, since we're going to be opening new readers
    //    into new sstables in the middle of the partition query. TWCS sstables will usually pass
    //    this condition.
    // 3. The sstables cannot have partition tombstones for the same reason as above.
    //    TWCS sstables will usually pass this condition.
    using sst_entry = std::pair<position_in_partition, shared_sstable>;
    if (schema->has_static_columns()
            || std::any_of(_sstables->begin(), _sstables->end(),
                [&sstl] (const sst_entry& e) {
                    return sstl.contains(e.second) && (e.second->get_version() < sstable_version_types::md
                        || e.second->may_have_partition_tombstones());
    })) {
        // Some of the conditions were not satisfied so we use the standard query path.
        return sstable_set_impl::create_single_key_sstable_reader(
                cf, std::move(schema), std::move(permit), sstable_histogram,
                pr, slice, pc, std::move(trace_state), fwd_sm, fwd_mr, sstl);
    }

    auto pk_filter = make_pk_filter(pos, *schema);
    auto it = std::find_if(_sstables->begin(), _sstables->end(), [&] (const sst_entry& e) { return sstl.contains(e.second) && pk_filter(*e.second); });
    if (it == _sstables->end()) {
        // No sstables contain data for the queried partition.
        return make_empty_flat_reader(std::move(schema), std::move(permit));
    }

    auto& stats = *cf->cf_stats();
    stats.clustering_filter_count++;

    auto ck_filter = [ranges = slice.get_all_ranges()] (const sstable& sst) { return sst.may_contain_rows(ranges); };
    {
        auto next = std::find_if(it, _sstables->end(), [&] (const sst_entry& e) { return sstl.contains(e.second) && ck_filter(*e.second); });
        stats.sstables_checked_by_clustering_filter += std::distance(it, next);
        it = next;
    }
    if (it == _sstables->end()) {
        // Some sstables passed the partition key filter, but none passed the clustering key filter.
        // However, we still have to emit a partition (even though it will be empty) so we don't fool the cache
        // into thinking this partition doesn't exist in any sstable (#3552).
        return flat_mutation_reader_from_mutations(std::move(permit), {mutation(schema, *pos.key())}, slice, fwd_sm);
    }

    auto create_reader = [schema, permit, &pr, &slice, &pc, trace_state, fwd_sm] (sstable& sst) {
        return sst.make_reader(schema, permit, pr, slice, pc, trace_state, fwd_sm);
    };

    // We're going to pass this filter into min_position_reader_queue. The queue guarantees that
    // the filter is going to be called at most once for each sstable and exactly once after
    // the queue is exhausted. We use that fact to gather statistics.
    auto filter = [pk_filter = std::move(pk_filter), ck_filter = std::move(ck_filter), &stats]
        (const sstable& sst) {
            if (pk_filter(sst)) {
                return true;
            }

            ++stats.sstables_checked_by_clustering_filter;
            if (ck_filter(sst)) {
                ++stats.surviving_sstables_after_clustering_filter;
                return true;
            }

            return false;
    };

    return make_clustering_combined_reader(
            std::move(schema), std::move(permit), fwd_sm,
            make_min_position_reader_queue(std::move(create_reader), std::move(filter)));
}

flat_mutation_reader
sstable_set::create_single_key_sstable_reader(
        column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    assert(pr.is_singular() && pr.start()->value().has_key());
    return _all->version().create_single_key_sstable_reader(cf, std::move(schema),
            std::move(permit), sstable_histogram, pr, slice, pc, std::move(trace_state), fwd, fwd_mr, *_all);
}

flat_mutation_reader
sstable_set::make_range_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator) const
{
    auto reader_factory_fn = [s, permit, &slice, &pc, trace_state, fwd, fwd_mr, &monitor_generator]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        return sst->make_reader(s, permit, pr, slice, pc, trace_state, fwd, fwd_mr, monitor_generator(sst));
    };
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

flat_mutation_reader
sstable_set::make_local_shard_sstable_reader(
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator) const
{
    auto reader_factory_fn = [s, permit, &slice, &pc, trace_state, fwd, fwd_mr, &monitor_generator]
            (shared_sstable& sst, const dht::partition_range& pr) mutable {
        flat_mutation_reader reader = sst->make_reader(s, permit, pr, slice, pc,
                trace_state, fwd, fwd_mr, monitor_generator(sst));
        if (sst->is_shared()) {
            auto filter = [&s = *s](const dht::decorated_key& dk) -> bool {
                return dht::shard_of(s, dk.token()) == this_shard_id();
            };
            reader = make_filtering_reader(std::move(reader), std::move(filter));
        }
        return reader;
    };
    return make_combined_reader(s, std::move(permit), std::make_unique<incremental_reader_selector>(s,
                    shared_from_this(),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

flat_mutation_reader make_restricted_range_sstable_reader(
        lw_shared_ptr<sstable_set> sstables,
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor_generator& monitor_generator)
{
    auto ms = mutation_source([sstables=std::move(sstables), &monitor_generator] (
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return sstables->make_range_sstable_reader(std::move(s), std::move(permit), pr, slice, pc,
                std::move(trace_state), fwd, fwd_mr, monitor_generator);
    });
    return make_restricted_flat_reader(std::move(ms), std::move(s), std::move(permit), pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
}

} // namespace sstables
