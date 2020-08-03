/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#pragma once

#include <memory>
#include <stdexcept>

#include "bytes.hh"
#include "ent/ldap/ldap_connection.hh"
#include "standard_role_manager.hh"

namespace auth {

/// Queries an LDAP server for roles.
///
/// Since LDAP grants and revokes roles, calling grant() and revoke() is disallowed.
///
/// We query LDAP for a list of a particular user's roles, and the results must match roles that exist in the
/// database.  Furthermore, the user must have already authenticated to Scylla, meaning it, too, exists in the
/// database.  Therefore, some of the role_manager functionality is provided by a standard_role_manager under
/// the hood.  For example, listing all roles or checking if the user can login cannot currently be determined
/// by querying LDAP, so they are delegated to the standard_role_manager.
class ldap_role_manager : public role_manager {
    standard_role_manager _std_mgr;
    // Pointer because it's initialized in start(), it's reset on connection problems, and its non-const
    // method search() is invoked from our const method query_granted().
    std::unique_ptr<ldap_connection> _conn;
    seastar::sstring _query_template; ///< LDAP URL dictating which query to make.
    seastar::sstring _target_attr; ///< LDAP entry attribute containing the Scylla role name.
    seastar::sstring _bind_name; ///< Username for LDAP simple bind.
    seastar::sstring _bind_password; ///< Password for LDAP simple bind.
    size_t _retries; ///< Count of LDAP reconnect retries so far.
  public:
    ldap_role_manager(
            sstring_view query_template, ///< LDAP query template as described in Scylla documentation.
            sstring_view target_attr, ///< LDAP entry attribute containing the Scylla role name.
            sstring_view bind_name, ///< LDAP bind credentials.
            sstring_view bind_password, ///< LDAP bind credentials.
            cql3::query_processor& qp, ///< Passed to standard_role_manager.
            ::service::migration_manager& mm ///< Passed to standard_role_manager.
    );

    /// Retrieves LDAP configuration entries from qp and invokes the other constructor.  Required by
    /// class_registrator<role_manager>.
    ldap_role_manager(cql3::query_processor& qp, ::service::migration_manager& mm);

    /// Thrown when query-template parsing fails.
    struct url_error : public std::runtime_error {
        using runtime_error::runtime_error;
    };

    std::string_view qualified_java_name() const noexcept override;

    const resource_set& protected_resources() const override;

    future<> start() override;

    future<> stop() override;

    future<> create(std::string_view, const role_config&) const override;

    future<> drop(std::string_view) const override;

    future<> alter(std::string_view, const role_config_update&) const override;

    future<> grant(std::string_view, std::string_view) const override;

    future<> revoke(std::string_view, std::string_view) const override;

    future<role_set> query_granted(std::string_view, recursive_role_query) const override;

    future<role_set> query_all() const override;

    future<bool> exists(std::string_view) const override;

    future<bool> is_superuser(std::string_view) const override;

    future<bool> can_login(std::string_view) const override;

    future<std::optional<sstring>> get_attribute(std::string_view, std::string_view) const override;

    future<role_manager::attribute_vals> query_attribute_for_all(std::string_view) const override;

    future<> set_attribute(std::string_view, std::string_view, std::string_view) const override;

    future<> remove_attribute(std::string_view, std::string_view) const override;

    static constexpr size_t retry_limit = 3; ///< Limit for number of LDAP reconnect retries.

  private:
    /// Connects to the LDAP server indicated by _query_template and executes LDAP bind using _bind_name and
    /// _bind_password.
    future<> connect();

    /// If _retries is under retry_limit, attempts to reconnect to LDAP.
    future<> reconnect();

    /// Releases _conn's managed object, closes it, and deletes it in the future.
    void reset_connection();

    /// Macro-expands _query_template, returning the result.
    sstring get_url(std::string_view user) const;
};

} // namespace auth