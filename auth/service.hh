/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <memory>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/authenticated_user.hh"
#include "auth/permission.hh"
#include "auth/permissions_cache.hh"
#include "seastarx.hh"

namespace cql3 {
class query_processor;
}

namespace db {
class config;
}

namespace service {
class migration_manager;
class migration_listener;
}

namespace auth {

class authenticator;
class authorizer;

struct service_config final {
    static service_config from_db_config(const db::config&);

    sstring authorizer_java_name;
    sstring authenticator_java_name;
};

class service final {
    permissions_cache_config _permissions_cache_config;
    std::unique_ptr<permissions_cache> _permissions_cache;

    cql3::query_processor& _qp;

    ::service::migration_manager& _migration_manager;

    std::unique_ptr<authorizer> _authorizer;

    std::unique_ptr<authenticator> _authenticator;

    // Only one of these should be registered, so we end up with some unused instances. Not the end of the world.
    std::unique_ptr<::service::migration_listener> _migration_listener;

    future<> _stopped;
    seastar::abort_source _as;

public:
    service(
            permissions_cache_config,
            cql3::query_processor&,
            ::service::migration_manager&,
            std::unique_ptr<authorizer>,
            std::unique_ptr<authenticator>);

    service(
            permissions_cache_config,
            cql3::query_processor&,
            ::service::migration_manager&,
            const service_config&);

    future<> start();

    future<> stop();

    future<bool> is_existing_user(const sstring& name) const;

    future<bool> is_super_user(const sstring& name) const;

    future<> insert_user(const sstring& name, bool is_superuser);

    future<> delete_user(const sstring& name);

    future<permission_set> get_permissions(::shared_ptr<authenticated_user>, data_resource) const;

    authenticator& underlying_authenticator() {
        return *_authenticator;
    }

    const authenticator& underlying_authenticator() const {
        return *_authenticator;
    }

    authorizer& underlying_authorizer() {
        return *_authorizer;
    }

    const authorizer& underlying_authorizer() const {
        return *_authorizer;
    }

private:
    future<bool> has_existing_users() const;

    bool should_create_metadata() const;

    future<> create_metadata_if_missing();
};

future<bool> is_super_user(const service&, const authenticated_user&);

}
