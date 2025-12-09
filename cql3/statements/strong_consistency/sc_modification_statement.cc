#include "sc_modification_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/raft/strong_consistency/sc_storage_proxy.hh"

namespace cql3::statements::strong_consistency {
static logging::logger logger("sc_modification_statement");

sc_modification_statement::sc_modification_statement(shared_ptr<base_statement> statement)
    : cql_statement_opt_metadata(&timeout_config::write_timeout)
    , _statement(std::move(statement))
{
}

using result_message = cql_transport::messages::result_message;

future<shared_ptr<result_message>> sc_modification_statement::execute(query_processor& qp, service::query_state& qs, 
    const query_options& options, std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

future<shared_ptr<result_message>> sc_modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    auto json_cache = base_statement::json_cache_opt{};
    const auto keys = _statement->build_partition_keys(options, json_cache);
    if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }
    if (_statement->requires_read()) {
        throw exceptions::invalid_request_exception("Strongly consistent updates don't support data prefetch");
    }

    auto timeout = db::timeout_clock::now() + _statement->get_timeout(qs.get_client_state(), options);
    auto [proxy, holder] = qp.acquire_sc_storage_proxy();
    co_await proxy.get().mutate(*_statement->s, keys[0].start()->value().token(), [&](api::timestamp_type ts) {
        auto muts = _statement->get_mutations(qp, options, timeout, false, ts, qs, json_cache, keys);
        if (!muts.available()) {
            on_internal_error(logger, "get_mutations must return a resolved future");
        }
        return std::move(muts.get());
    });

    co_return seastar::make_shared<result_message::void_message>();
}

future<> sc_modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _statement->check_access(qp, state);
}

uint32_t sc_modification_statement::get_bound_terms() const {
    return _statement->get_bound_terms();
}

bool sc_modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _statement->depends_on(ks_name, cf_name);
}
}
