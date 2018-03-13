/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#ifndef APPS_SEASTAR_THRIFT_HANDLER_HH_
#define APPS_SEASTAR_THRIFT_HANDLER_HH_

#include "Cassandra.h"
#include "auth/service.hh"
#include "database.hh"
#include "core/distributed.hh"
#include "cql3/query_processor.hh"
#include <memory>

std::unique_ptr<::cassandra::CassandraCobSvIfFactory> create_handler_factory(distributed<database>& db, distributed<cql3::query_processor>& qp, auth::service&);

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
