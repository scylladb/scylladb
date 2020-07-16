/*
 * Copyright 2020 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <iostream>
#include <boost/lexical_cast.hpp>
#include "error.hh"

std::ostream& alternator::detail::operator<<(std::ostream& os, exception_type t) {
    switch (t) {
        default:
        case exception_type::ValidationException: return os << "ValidationException";
        case exception_type::ResourceNotFoundException: return os << "ResourceNotFoundException"; 
        case exception_type::AccessDeniedException: return os << "AccessDeniedException";
        case exception_type::InvalidSignatureException: return os << "InvalidSignatureException";
    }
}

alternator::api_error::api_error(detail::exception_type type, std::string msg, status_type http_code)
    : api_error(boost::lexical_cast<std::string>(type), std::move(msg), http_code)
{}
