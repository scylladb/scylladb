/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastarx.hh"
#include "cql3/statements/list_service_level_statement.hh"
#include "cql3/column_identifier.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"
#include "utils/overloaded_functor.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

list_service_level_statement::list_service_level_statement(sstring service_level, bool describe_all) :
    _service_level(service_level), _describe_all(describe_all) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::list_service_level_statement::prepare(
        data_dictionary::database db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<list_service_level_statement>(*this));
}

future<> list_service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DESCRIBE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &,
        std::optional<service::group0_guard> guard) const {

    static auto make_column = [] (sstring name, const shared_ptr<const abstract_type> type) {
        return make_lw_shared<column_specification>(
                "QOS",
                "service_levels",
                ::make_shared<column_identifier>(std::move(name), true),
                type);
    };

    std::vector<lw_shared_ptr<column_specification>> metadata({make_column("service_level", utf8_type),
        make_column("timeout", duration_type),
        make_column("workload_type", utf8_type),
        make_column("shares", int32_type),
    });
    if (_describe_all) {
        metadata.push_back(make_column("percentage of all service level shares", utf8_type));
    }

    return make_ready_future().then([this, &state] () {
                                  if (_describe_all) {
                                      return state.get_service_level_controller().get_distributed_service_levels(qos::query_context::user);
                                  } else {
                                      return state.get_service_level_controller().get_distributed_service_level(_service_level);
                                  }
                              })
            .then([this, metadata = std::move(metadata)] (qos::service_levels_info sl_info) {
                auto d = [] (const qos::service_level_options::timeout_type& duration) -> bytes_opt {
                    return std::visit(overloaded_functor{
                        [&] (const qos::service_level_options::unset_marker&) {
                            return bytes_opt();
                        },
                        [&] (const qos::service_level_options::delete_marker&) {
                            return bytes_opt();
                        },
                        [&] (const lowres_clock::duration& d) -> bytes_opt {
                            auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
                            return duration_type->decompose(cql_duration(months_counter{0}, days_counter{0}, nanoseconds_counter{nanos}));
                        },
                    }, duration);
                };
                auto dd = [] <typename T> (const std::variant<qos::service_level_options::unset_marker, qos::service_level_options::delete_marker, T>& v) -> bytes_opt {
                    return std::visit(overloaded_functor{
                        [&] (const qos::service_level_options::unset_marker&) {
                            return bytes_opt();
                        },
                        [&] (const qos::service_level_options::delete_marker&) {
                            return bytes_opt();
                        },
                        [&] (const T& v) -> bytes_opt {
                            return data_type_for<T>()->decompose(v);
                        },
                    }, v);
                };
                auto get_shares_value = [] (const std::variant<qos::service_level_options::unset_marker, qos::service_level_options::delete_marker, int32_t>& shares) {
                    if (std::holds_alternative<int32_t>(shares)) {
                        return std::get<int32_t>(shares);
                    } else {
                        return qos::service_level_controller::default_shares;
                    }
                };

                int32_t sum_of_shares = 0;
                if (_describe_all) {
                    for (auto &&[_, slo]: sl_info) {
                        sum_of_shares += get_shares_value(slo.shares);
                    }
                }

                auto rs = std::make_unique<result_set>(metadata);
                for (auto &&[sl_name, slo] : sl_info) {
                    bytes_opt workload = slo.workload == qos::service_level_options::workload_type::unspecified
                            ? bytes_opt()
                            : utf8_type->decompose(qos::service_level_options::to_string(slo.workload));

                    auto row = std::vector<bytes_opt>{
                            utf8_type->decompose(sl_name),
                            d(slo.timeout),
                            workload,
                            dd(slo.shares)};
                    if (_describe_all) {
                        row.push_back(utf8_type->decompose(
                                fmt::format("{:.2f}%", 100.0f * get_shares_value(slo.shares) / sum_of_shares)
                        ));
                    }
                    rs->add_row(std::move(row));
                }

                auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
                return ::static_pointer_cast<cql_transport::messages::result_message>(rows);
            });
}
}
}
