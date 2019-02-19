/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "service_level_statement.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

uint32_t service_level_statement::get_bound_terms() const {
    return 0;
}

bool service_level_statement::depends_on_keyspace(
        const sstring &ks_name) const {
    return false;
}

bool service_level_statement::depends_on_column_family(
        const sstring &cf_name) const {
    return false;
}

void service_level_statement::validate(
        service::storage_proxy &,
        const service::client_state &state) const {
}

future<> service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return make_ready_future<>();
}

}
}
