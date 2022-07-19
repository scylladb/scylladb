namespace db {
namespace functions {
class function_name {
    sstring keyspace;
    sstring name;
};
}
}
namespace query {
struct forward_request {
    struct aggregation_info {
        db::functions::function_name name;
        std::vector<sstring> column_names;
    };
    enum class reduction_type : uint8_t {
        count,
        aggregate
    };

    std::vector<query::forward_request::reduction_type> reduction_types;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_clock::time_point timeout;

    std::optional<std::vector<query::forward_request::aggregation_info>> aggregation_infos [[version 5.1]];
};

struct forward_result {
    std::vector<bytes_opt> query_results;
};

verb forward_request(query::forward_request, std::optional<tracing::trace_info>) -> query::forward_result;
}
