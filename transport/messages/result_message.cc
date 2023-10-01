
/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "result_message.hh"
#include "cql3/cql_statement.hh"
#include <seastar/core/print.hh>
#include <fmt/std.h>

namespace cql_transport::messages {

std::ostream& operator<<(std::ostream& os, const result_message::void_message& msg) {
    fmt::print(os, "{{result_message::void}}");
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::bounce_to_shard& msg) {
    fmt::print(os, "{{result_message::bounce_to_shard {}}}", msg.move_to_shard());
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::exception& msg) {
    fmt::print(os, "{{result_message::exception {}}}", msg.get_exception());
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::set_keyspace& msg) {
    fmt::print(os, "{{result_message::set_keyspace {}}}", msg.get_keyspace());
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::prepared::thrift& msg) {
    fmt::print(os, "{{result_message::prepared::thrift {:d}}}", msg.get_id());
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::prepared::cql& msg) {
    fmt::print(os, "{{result_message::prepared::cql {}}}", to_hex(msg.get_id()));
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::schema_change& msg) {
    // FIXME: format contents
    fmt::print(os, "{{result_message::prepared::schema_change {:p}}}", (void*)msg.get_change().get());
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message::rows& msg) {
    os << "{result_message::rows ";
    struct visitor {
        std::ostream& _os;
        void start_row() { _os << "{row: "; }
        void accept_value(managed_bytes_view_opt value) {
            if (!value) {
                _os << " null";
                return;
            }
            _os << " ";
            for (auto fragment : fragment_range(*value)) {
                _os << fragment;
            }
        }
        void end_row() { _os << "}"; }
    };
    msg.rs().visit(visitor { os });
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const result_message& msg) {
    class visitor : public result_message::visitor {
        std::ostream& _os;
    public:
        explicit visitor(std::ostream& os) : _os(os) {}
        void visit(const result_message::void_message& m) override { _os << m; };
        void visit(const result_message::set_keyspace& m) override { _os << m; };
        void visit(const result_message::prepared::cql& m) override { _os << m; };
        void visit(const result_message::prepared::thrift& m) override { _os << m; };
        void visit(const result_message::schema_change& m) override { _os << m; };
        void visit(const result_message::rows& m) override { _os << m; };
        void visit(const result_message::bounce_to_shard& m) override { _os << m; };
        void visit(const result_message::exception& m) override { _os << m; };
    };
    visitor print_visitor{os};
    msg.accept(print_visitor);
    return os;
}

void result_message::visitor_base::visit(const result_message::exception& ex) {
    ex.throw_me();
}

result_message::prepared::prepared(cql3::statements::prepared_statement::checked_weak_ptr prepared, bool support_lwt_opt)
        : _prepared(std::move(prepared))
        , _metadata(
            _prepared->bound_names,
            _prepared->partition_key_bind_indices,
            support_lwt_opt ? _prepared->statement->is_conditional() : false)
        , _result_metadata{extract_result_metadata(_prepared->statement)}
{
}

::shared_ptr<const cql3::metadata> result_message::prepared::extract_result_metadata(::shared_ptr<cql3::cql_statement> statement) {
    return statement->get_result_metadata();
}

}
