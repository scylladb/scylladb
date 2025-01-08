/*
 * Copyright (C) 2024 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "db/extensions.hh"

namespace encryption {

using base_error = db::extension_storage_exception;

class permission_error : public db::extension_storage_permission_error {
public:
    using mybase = db::extension_storage_permission_error;
    using mybase::mybase;
};

class configuration_error : public db::extension_storage_misconfigured {
public:
    using mybase = db::extension_storage_misconfigured;
    using mybase::mybase;
};

class service_error : public base_error {
public:
    using base_error::base_error;
};

class missing_resource_error : public db::extension_storage_resource_unavailable {
public:
    using mybase = db::extension_storage_resource_unavailable;
    using mybase::mybase;
};

// #4970 - not 100% correct, but network errors are 
// generally intermittent/recoverable. Mark as a non-isolating
// error.
class network_error : public missing_resource_error {
public:
    using missing_resource_error::missing_resource_error;
};

class malformed_response_error : public service_error {
public:
    using service_error::service_error;
};

}

