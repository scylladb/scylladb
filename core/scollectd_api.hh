/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef CORE_SCOLLECTD_API_HH_
#define CORE_SCOLLECTD_API_HH_

#include "core/scollectd.hh"

namespace scollectd {

struct collectd_value {
    union {
        double _d;
        uint64_t _ui;
        int64_t _i;
    } u;
    scollectd::data_type _type;
    collectd_value()
            : _type(data_type::GAUGE) {
    }
    collectd_value(data_type t, uint64_t i)
            : _type(t) {
        u._ui = i;
    }

    collectd_value& operator=(const collectd_value& c) = default;

    collectd_value& operator+=(const collectd_value& c) {
        *this = *this + c;
        return *this;
    }

    collectd_value operator+(const collectd_value& c) {
        collectd_value res(*this);
        switch (_type) {
        case data_type::GAUGE:
            res.u._d += c.u._d;
            break;
        case data_type::DERIVE:
            res.u._i += c.u._i;
            break;
        default:
            res.u._ui += c.u._ui;
            break;
        }
        return res;
    }
};

std::vector<collectd_value> get_collectd_value(
        const scollectd::type_instance_id& id);

std::vector<scollectd::type_instance_id> get_collectd_ids();

}

#endif /* CORE_SCOLLECTD_API_HH_ */
