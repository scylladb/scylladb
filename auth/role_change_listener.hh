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

#pragma once

namespace auth {

/**
 * Interface on which interested parties can be notified if a specific role changed.
 */
class role_change_listener {
public:
    virtual ~role_change_listener()
    { }

    /**
     * Called when a given role is altered.
     *
     * @param role_name name of the altered role.
     */
    virtual future<> on_alter_role(std::string_view role_name) = 0;

    /**
     * Called when a given role is granted to somebody.
     *
     * @param role_name name of the granted role.
     * @param grantee receiver of the new role.
     */
    virtual future<> on_grant_role(std::string_view role_name, std::string_view grantee) = 0;

    /**
     * Called when a given role is granted to somebody.
     *
     * @param role_name name of the granted role.
     * @param revokee entity which loses the role.
     */
    virtual future<> on_revoke_role(std::string_view role_name, std::string_view revokee) = 0;
};

}
