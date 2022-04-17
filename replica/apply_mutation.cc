/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/apply_mutation.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"
#include "log.hh"

extern logging::logger dblog;

namespace replica {

apply_mutation::apply_mutation(const mutation& m)
    : _schema(m.schema())
    , _mp(&m)
{}

apply_mutation::apply_mutation(mutation&& m)
    : _schema(m.schema())
    , _m(make_foreign(std::make_unique<const mutation>(std::move(m))))
    , _mp(_m.get())
{}

apply_mutation::apply_mutation(schema_ptr s, const frozen_mutation& fm)
    : _schema(std::move(s))
    , _fmp(&fm)
{}

apply_mutation::apply_mutation(schema_ptr s, frozen_mutation&& fm)
    : _schema(std::move(s))
    , _fm(make_foreign(std::make_unique<const frozen_mutation>(std::move(fm))))
    , _fmp(_fm.get())
{}

apply_mutation::apply_mutation(apply_mutation&& o) noexcept // not really noexcept, but required for database::apply_stage
    : _schema(std::move(o._schema))
    , _m(std::move(o._m))
    , _mp(std::exchange(o._mp, nullptr))
    , _fm(std::move(o._fm))
    , _fmp(std::exchange(o._fmp, nullptr))
{}

apply_mutation::~apply_mutation() {}

const mutation& apply_mutation::get_mutation() {
    if (!_mp) {
        _m = make_foreign(std::make_unique<const mutation>(_fmp->unfreeze(schema())));
        _mp = _m.get();
    }
    return *_mp;
}

const frozen_mutation& apply_mutation::get_frozen_mutation() {
    if (!_fmp) {
        _fm = make_foreign(std::make_unique<const frozen_mutation>(freeze(*_mp)));
        _fmp = _fm.get();
    }
    return *_fmp;
}

mutation apply_mutation::take_mutation() && {
    if (_m) {
        // extract unique_ptr<mutation> if available
        auto p = _m.release();
        _mp = nullptr;
        return std::move(*const_cast<mutation*>(p.get()));
    }
    if (_mp) {
        // copy const mutation
        auto m = mutation(*_mp);
        _mp = nullptr;
        return m;
    }
    // get mutation by unfreezing the frozen_mutation
    return _fmp->unfreeze(schema());
}

utils::UUID apply_mutation::column_family_id() const noexcept {
    return _mp ? _mp->column_family_id() : _fmp->column_family_id();
}

unsigned apply_mutation::shard_of() const {
    return visit(
        [this] (const mutation& m) {
            return dht::shard_of(*schema(), m.token());
        },
        [] (schema_ptr s, const frozen_mutation& fm) {
            return dht::shard_of(*s, dht::get_token(*s, fm.key()));
        }
    );
}

std::ostream& operator<<(std::ostream& os, const apply_mutation& am) {
    // am.visit() doesn't work here
    // since it thinks it needs to copy the ostream.
    if (am._mp) {
        return os << *am._mp;
    } else {
        return os << am._fmp->pretty_printer(am.schema());
    }
}

void apply_mutation::upgrade(schema_ptr new_schema) {
    if (*_schema == *new_schema) {
        return;
    }
    std::optional<mutation> m_opt;
    mutation* mp = nullptr;
    if (_mp) {
        if (_m) {
            // upgrade in-place if possible
            mp = const_cast<mutation*>(_m.get());
        } else {
            // copy const mutation
            m_opt.emplace(*_mp);
            mp = &*m_opt;
        }
    } else {
        // get mutation by unfreezing the frozen_mutation
        m_opt.emplace(_fmp->unfreeze(schema()));
        mp = &*m_opt;
    }
    mp->upgrade(new_schema);
    if (m_opt) {
        _m = make_foreign(std::make_unique<const mutation>(std::move(*m_opt)));
        _mp = _m.get();
    }
    // cleanup frozen_mutation
    _fm.reset();
    _fmp = nullptr;
    set_schema(std::move(new_schema));
}

} // namespace replica
