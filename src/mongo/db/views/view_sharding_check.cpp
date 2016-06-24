/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/views/view_sharding_check.h"

#include "mongo/db/catalog/database.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/s/chunk_version.h"

namespace mongo {

ViewShardingCheck::ViewShardingCheck(OperationContext* opCtx,
                                     const Database* db,
                                     const ViewDefinition* view)
    : _viewMayBeSharded(false) {
    invariant(opCtx);
    invariant(db);
    invariant(view);

    if (ClusterRole::ShardServer == serverGlobalParams.clusterRole) {
        auto resolvedView = db->getViewCatalog()->resolveView(opCtx, view->fullViewNs());

        const auto& sourceNs = std::get<0>(resolvedView);
        const auto sourceColIsSharded = collectionIsSharded(opCtx, sourceNs);
        const auto canAcceptWrites =
            repl::ReplicationCoordinator::get(opCtx->getClient()->getServiceContext())
                ->canAcceptWritesForDatabase(db->name());

        _viewMayBeSharded = !canAcceptWrites || sourceColIsSharded;
        if (_viewMayBeSharded) {
            _resolvedViewNamespace = sourceNs;
            _resolvedViewDefinition = std::get<1>(resolvedView);
        }
    }
}


bool ViewShardingCheck::canRunOnMongod() {
    return !_viewMayBeSharded;
}

void ViewShardingCheck::appendResolvedView(BSONObjBuilder& result) {
    BSONObjBuilder viewBob;
    viewBob.append("ns", _resolvedViewNamespace);
    viewBob.append("pipeline", _resolvedViewDefinition);

    result.append("resolvedView", viewBob.obj());
}

bool ViewShardingCheck::collectionIsSharded(OperationContext* opCtx, const std::string& ns) {
    const ChunkVersion unsharded(0, 0, OID());
    return !(ShardingState::get(opCtx)->getVersion(ns).isWriteCompatibleWith(unsharded));
}

}  // namespace mongo
