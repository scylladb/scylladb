#include "parallel_aggregations.hh"

namespace parallel_aggregations {

std::ostream& operator<<(std::ostream& out, const reduction_type& r) {
    out << "reduction_type{";
    switch (r) {
        case reduction_type::count:
            out << "count";
            break;
        case reduction_type::aggregate:
            out << "aggregate";
            break;
    }
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, const aggregation_info& a) {
    return out << "aggregation_info{"
        << ", name=" << a.name
        << ", column_names=[" << join(",", a.column_names) << "]"
        << "}";
}

std::ostream& operator<<(std::ostream& out, const forward_request& r) {
    auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(r.timeout).time_since_epoch().count();

    out << "forward_request{"
        << "reductions=[" << join(",", r.reduction_types) << "]";
    if(r.aggregation_infos) {
        out << ", aggregation_infos=[" << join(",", r.aggregation_infos.value()) << "]";
    }
    return out << ", cmd=" << r.cmd
        << ", pr=" << r.pr
        << ", cl=" << r.cl
        << ", timeout(ms)=" << ms << "}";
}

std::ostream& operator<<(std::ostream& out, const forward_result::printer& p) {
    if (p.functions.size() != p.res.query_results.size()) {
        return out << "[malformed forward_result (" << p.res.query_results.size()
            << " results, " << p.functions.size() << " aggregates)]";
    }

    out << "[";
    for (size_t i = 0; i < p.functions.size(); i++) {
        auto& return_type = p.functions[i]->return_type();
        out << return_type->to_string(bytes_view(*p.res.query_results[i]));

        if (i + 1 < p.functions.size()) {
            out << ", ";
        }
    }
    return out << "]";
}


} // namespace parallel_aggregations
