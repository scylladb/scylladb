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
 *
 * Copyright (C) 2020 ScyllaDB
 */

#include <any>
#include <seastar/core/sstring.hh>
#include <seastar/core/reactor.hh>
#include "log.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

namespace gms {

static logging::logger logger("features");

feature_service::feature_service(feature_config cfg) : _config(cfg) {
}

feature_config feature_config_from_db_config(db::config& cfg) {
    feature_config fcfg;

    if (cfg.enable_sstables_mc_format()) {
        fcfg.enable_sstables_mc_format = true;
    }
    if (cfg.enable_user_defined_functions()) {
        if (!cfg.check_experimental(db::experimental_features_t::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
        fcfg.enable_user_defined_functions = true;
    }

    if (cfg.check_experimental(db::experimental_features_t::CDC)) {
        fcfg.enable_cdc = true;
    }

    if (cfg.check_experimental(db::experimental_features_t::LWT)) {
        fcfg.enable_lwt = true;
    }

    return fcfg;
}

future<> feature_service::stop() {
    return make_ready_future<>();
}

void feature_service::register_feature(feature* f) {
    _registered_features.emplace(f->name(), std::vector<feature*>()).first->second.emplace_back(f);
}

void feature_service::unregister_feature(feature* f) {
    auto&& fsit = _registered_features.find(f->name());
    if (fsit == _registered_features.end()) {
        return;
    }
    auto&& fs = fsit->second;
    auto it = std::find(fs.begin(), fs.end(), f);
    if (it != fs.end()) {
        fs.erase(it);
    }
}


void feature_service::enable(const sstring& name) {
    if (auto it = _registered_features.find(name); it != _registered_features.end()) {
        for (auto&& f : it->second) {
            f->enable();
        }
    }
}

feature::feature(feature_service& service, sstring name, bool enabled)
        : _service(&service)
        , _name(name)
        , _enabled(enabled) {
    _service->register_feature(this);
    if (_enabled) {
        _pr.set_value();
    }
}

feature::~feature() {
    if (_service) {
        _service->unregister_feature(this);
    }
}

feature& feature::operator=(feature&& other) {
    _service->unregister_feature(this);
    _service = std::exchange(other._service, nullptr);
    _name = other._name;
    _enabled = other._enabled;
    _pr = std::move(other._pr);
    _s = std::move(other._s);
    _service->register_feature(this);
    return *this;
}

void feature::enable() {
    if (!_enabled) {
        if (engine().cpu_id() == 0) {
            logger.info("Feature {} is enabled", name());
        }
        _enabled = true;
        _pr.set_value();
        _s();
    }
}

} // namespace gms
