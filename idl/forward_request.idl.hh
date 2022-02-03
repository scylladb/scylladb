namespace query {
struct forward_request {
    enum class reduction_type : uint8_t {
        count,
    };
    std::vector<query::forward_request::reduction_type> reduction_types;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_clock::time_point timeout;
};

struct forward_result {
    std::vector<bytes_opt> query_results;
};

verb forward_request(query::forward_request, std::optional<tracing::trace_info>) -> query::forward_result;
}
