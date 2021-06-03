
/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "result_message.hh"
#include <seastar/core/print.hh>

namespace cql_transport::messages {

std::ostream& operator<<(std::ostream& os, const result_message::void_message& msg) {
    return fmt_print(os, "{{result_message::void}}");
}

std::ostream& operator<<(std::ostream& os, const result_message::bounce_to_shard& msg) {
    return fmt_print(os, "{{result_message::bounce_to_shard {}}}", msg.move_to_shard());
}

std::ostream& operator<<(std::ostream& os, const result_message::set_keyspace& msg) {
    return fmt_print(os, "{{result_message::set_keyspace {}}}", msg.get_keyspace());
}

std::ostream& operator<<(std::ostream& os, const result_message::prepared::thrift& msg) {
    return fmt_print(os, "{{result_message::prepared::thrift {:d}}}", msg.get_id());
}

std::ostream& operator<<(std::ostream& os, const result_message::prepared::cql& msg) {
    return fmt_print(os, "{{result_message::prepared::cql {}}}", to_hex(msg.get_id()));
}

std::ostream& operator<<(std::ostream& os, const result_message::schema_change& msg) {
    // FIXME: format contents
    return fmt_print(os, "{{result_message::prepared::schema_change {:p}}}", (void*)msg.get_change().get());
}

std::ostream& operator<<(std::ostream& os, const result_message::rows& msg) {
    os << "{result_message::rows ";
    struct visitor {
        std::ostream& _os;
        void start_row() { _os << "{row: "; }
        void accept_value(std::optional<query::result_bytes_view> value) {
            if (!value) {
                _os << " null";
                return;
            }
            _os << " ";
            using boost::range::for_each;
            for_each(*value, [this] (bytes_view fragment) {
                _os << fragment;
            });
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
    };
    visitor print_visitor{os};
    msg.accept(print_visitor);
    return os;
}

}
