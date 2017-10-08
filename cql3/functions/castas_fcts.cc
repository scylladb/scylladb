/*
 * Copyright (C) 2017 ScyllaDB
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

#include "castas_fcts.hh"
#include "cql3/functions/native_scalar_function.hh"

namespace cql3 {
namespace functions {

namespace {

using bytes_opt = std::experimental::optional<bytes>;

class castas_function_for : public cql3::functions::native_scalar_function {
    castas_fctn _func;
public:
    castas_function_for(data_type to_type,
                        data_type from_type,
                        castas_fctn func)
            : native_scalar_function("castas" + to_type->as_cql3_type()->to_string(), to_type, {from_type})
            , _func(func) {
    }
    virtual bool is_pure() override {
        return true;
    }
    virtual void print(std::ostream& os) const override {
        os << "cast(" << _arg_types[0]->name() << " as " << _return_type->name() << ")";
    }
    virtual bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override {
        auto from_type = arg_types()[0];
        auto to_type = return_type();

        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        auto val_from = from_type->deserialize(*val);
        auto val_to = _func(val_from);
        return to_type->decompose(val_to);
    }
};

shared_ptr<function> make_castas_function(data_type to_type, data_type from_type, castas_fctn func) {
    return ::make_shared<castas_function_for>(std::move(to_type), std::move(from_type), std::move(func));
}

} /* Anonymous Namespace */

shared_ptr<function> castas_functions::get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args, schema_ptr s) {
    if (provided_args.size() != 1) {
        throw exceptions::invalid_request_exception("Invalid CAST expression");
    }
    auto from_type = provided_args[0]->get_type();
    auto from_type_key = from_type;
    if (from_type_key->is_reversed()) {
        from_type_key = dynamic_cast<const reversed_type_impl&>(*from_type).underlying_type();
    }

    auto f = get_castas_fctn(to_type, from_type_key);
    return make_castas_function(to_type, from_type, f);
}

}
}
