#include "sc_select_statement.hh"

namespace cql3::statements::strong_consistency {
static logging::logger logger("sc_select_statement");

using result_message = cql_transport::messages::result_message;

future<::shared_ptr<result_message>> sc_select_statement::do_execute(query_processor& qp,
        service::query_state& state, 
        const query_options& options) const
{
    throw exceptions::invalid_request_exception("not implemented");
}
}