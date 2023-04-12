/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "role_manager.hh"
#include "seastar/core/thread.hh"

namespace auth {

future<> role_manager::notify_attribute_set(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value) {
    return seastar::async([this, role_name, attribute_name, attribute_value] {
        _subscribers.thread_for_each([role_name, attribute_name, attribute_value] (role_configuration_change_subscriber* subscriber) {
            subscriber->on_attribute_set(role_name, attribute_name, attribute_value).get();
        });
    });
}

future<> role_manager::notify_attribute_removed(std::string_view role_name, std::string_view attribute_name) {
    return seastar::async([this, role_name, attribute_name] {
        _subscribers.thread_for_each([role_name, attribute_name] (role_configuration_change_subscriber* subscriber) {
            subscriber->on_attribute_removed(role_name, attribute_name).get();
        });
    });
}

future<> role_manager::notify_role_granted(std::string_view grantee_name, std::string_view role_name) {
    return seastar::async([this, grantee_name, role_name] {
        _subscribers.thread_for_each([grantee_name, role_name] (role_configuration_change_subscriber* subscriber) {
            subscriber->on_role_granted(grantee_name, role_name).get();
        });
    });
}

future<> role_manager::notify_role_revoked(std::string_view revokee_name, std::string_view role_name) {
    return seastar::async([this, revokee_name, role_name] {
        _subscribers.thread_for_each([revokee_name, role_name] (role_configuration_change_subscriber* subscriber) {
            subscriber->on_role_revoked(revokee_name, role_name).get();
        });
    });
}

void role_manager::register_subscriber(role_configuration_change_subscriber *subscriber) {
    _subscribers.add(subscriber);
}

future<> role_manager::unregister_subscriber(role_configuration_change_subscriber *subscriber) {
    return _subscribers.remove(subscriber);
}

}