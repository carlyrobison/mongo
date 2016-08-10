// view_catalog.cpp: In-memory data structures for view resolution

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/views/view_catalog.h"

#include <map>
#include <memory>
#include <tuple>

#include "mongo/base/string_data.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/db/views/view.h"
#include "mongo/util/log.h"

namespace {
bool enableViews = false;
}  // namespace

namespace mongo {
ExportedServerParameter<bool, ServerParameterType::kStartupOnly> enableViewsParameter(
    ServerParameterSet::getGlobal(), "enableViews", &enableViews);

const std::uint32_t ViewCatalog::kMaxViewDepth = 20;

ViewCatalog::ViewCatalog(OperationContext* txn, Database* database) : _db(database) {
    Collection* systemViews = database->getCollection(database->getSystemViewsName());
    if (!systemViews)
        return;

    auto cursor = systemViews->getCursor(txn);
    while (auto record = cursor->next()) {
        RecordData& data = record->data;

        // Check the document is valid BSON with only the expected fields.
        fassertStatusOK(40185, validateBSON(data.data(), data.size()));
        BSONObj viewDef = data.toBson();

        // Make sure we fail when new fields get added to the definition, so we fail safe in case
        // of future format upgrades.
        for (const BSONElement& e : viewDef) {
            std::string name(e.fieldName());
            fassert(40186, name == "_id" || name == "viewOn" || name == "pipeline" || name == "timeseries" || name == "compressed");
        }
        NamespaceString viewName(viewDef["_id"].str());
        fassert(40187, viewName.db() == database->name());
        _viewMap[viewDef["_id"].str()] = std::make_shared<ViewDefinition>(
            viewName.db(), viewName.coll(), viewDef["viewOn"].str(), viewDef["pipeline"].Obj(), viewDef["timeseries"].trueValue(), viewDef["compressed"].trueValue());
    }
}

Status ViewCatalog::createView(OperationContext* txn,
                               const NamespaceString& viewName,
                               const std::string& viewOn,
                               const BSONObj& pipeline,
                               bool timeseries,
                               bool compressed) {
    uassert(40188, "View support not enabled", enableViews);
    NamespaceString viewNss(viewName.db(), viewOn);
    if (lookup(StringData(viewName.ns()))) {
        LOG(3) << "VIEWS: Attempted to create a duplicate view";
        return Status(ErrorCodes::NamespaceExists, "Namespace already exists");
    }
    BSONObj viewDef = BSON("_id" << viewName.ns() << "viewOn" << viewOn << "pipeline" << pipeline << "timeseries" << timeseries << "compressed" << compressed);
    Collection* systemViews = _db->getOrCreateCollection(txn, _db->getSystemViewsName());
    OpDebug* opDebug = nullptr;
    bool enforceQuota = false;
    systemViews->insertDocument(txn, viewDef, opDebug, enforceQuota);

    BSONObj ownedPipeline = pipeline.getOwned();
    txn->recoveryUnit()->onCommit([this, viewName, viewOn, ownedPipeline, timeseries, compressed]() {
        _viewMap[viewName.ns()] =
            std::make_shared<ViewDefinition>(viewName.db(), viewName.coll(), viewOn, ownedPipeline, timeseries, compressed);
    });
    return Status::OK();
}

void ViewCatalog::dropView(OperationContext* txn, const NamespaceString& viewName) {
    bool requireIndex = false;
    Collection* systemViews = _db->getCollection(_db->getSystemViewsName());
    if (!systemViews)
        return;
    RecordId id = Helpers::findOne(txn, systemViews, BSON("_id" << viewName.ns()), requireIndex);
    if (!id.isNormal())
        return;

    OpDebug* opDebug = nullptr;
    systemViews->deleteDocument(txn, id, opDebug);

    txn->recoveryUnit()->onCommit([this, viewName]() { this->_viewMap.erase(viewName.ns()); });
}

ViewDefinition* ViewCatalog::lookup(StringData ns) {
    ViewMap::const_iterator it = _viewMap.find(ns);
    if (it != _viewMap.end() && it->second) {
        return it->second.get();
    }
    return nullptr;
}

std::tuple<std::string, std::vector<BSONObj>> ViewCatalog::resolveView(OperationContext* txn,
                                                                       StringData ns) {
    LOG(3) << "VIEWS: attempting to resolve " << ns;

    std::string backingNs = ns.toString();
    std::vector<BSONObj> newPipeline;

    for (auto i = ViewCatalog::kMaxViewDepth; i > 0; i--) {
        auto numberOfAttempts = ViewCatalog::kMaxViewDepth - i;
        LOG(3) << "VIEWS: resolution attempt #" << numberOfAttempts;
        ViewDefinition* view = lookup(backingNs);
        if (!view) {
            return std::tie(backingNs, newPipeline);
        }

        backingNs = view->fullViewOnNs();

        std::vector<BSONObj> oldPipeline = view->pipeline();
        newPipeline.insert(newPipeline.begin(), oldPipeline.begin(), oldPipeline.end());
    }

    uasserted(ErrorCodes::ViewRecursionLimitExceeded, "view depth too deep or view cycle detected");
    MONGO_UNREACHABLE;
}
}  // namespace mongo
