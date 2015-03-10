//
// mime_types.hpp
// ~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2013 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef HTTP_MIME_TYPES_HH
#define HTTP_MIME_TYPES_HH

#include "core/sstring.hh"

namespace httpd {

namespace mime_types {

/**
 * Convert a file extension into a MIME type.
 *
 * @param extension the file extension
 * @return the mime type as a string
 */
const char* extension_to_type(const sstring& extension);

} // namespace mime_types

} // namespace httpd

#endif // HTTP_MIME_TYPES_HH
