#include "cql3/cql_statement.hh"
#include "cql3/statements/select_statement.hh"

namespace cql3::statements::strong_consistency {

class sc_select_statement : public cql3::statements::select_statement {
    using result_message = cql_transport::messages::result_message;

public:
    using select_statement::select_statement;

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
        service::query_state& state, const query_options& options) const override;
};

}