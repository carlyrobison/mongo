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
#include <mutex>
#include <tuple>

#include "mongo/base/string_data.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/views/view.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {
ViewCatalog* viewCatalogSingleton = new ViewCatalog();
} // namespace

const std::uint32_t ViewCatalog::kMaxViewDepth = 20;

ViewCatalog* ViewCatalog::getInstance() {
    invariant(viewCatalogSingleton);
    return viewCatalogSingleton;
}

Status ViewCatalog::createView(OperationContext* txn,
                               std::string dbName,
                               std::string viewName,
                               std::string backingViewName,
                               BSONObj& pipeline) {
    std::string ns = generateViewNamespace(dbName, viewName);
    if (ViewCatalog::lookup(StringData(ns))) {
        LOG(3) << "VIEWS: Attempted to create a duplicate view";
        return Status(ErrorCodes::NamespaceExists, "Namespace already exists");
    }

    _viewMap[ns] = new ViewDefinition(dbName, viewName, backingViewName, pipeline);
    return Status::OK();
}

std::string ViewCatalog::generateViewNamespace(StringData dbName, StringData viewName) {
    return dbName.toString() + "." + viewName.toString();
}


ViewDefinition* ViewCatalog::lookup(StringData ns) {

    ViewMap::const_iterator it = _viewMap.find(ns);
    if (it != _viewMap.end() && it->second) {
        return it->second;
    }
    return nullptr;
}

ViewDefinition* ViewCatalog::lookup(StringData dbName, StringData viewName) {
    return lookup(generateViewNamespace(dbName, viewName));
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
        // log() << "Resolving view: " << view->toString();
        backingNs = view->fullBackingViewNs();

        std::vector<BSONObj> oldPipeline = view->pipeline();
        newPipeline.insert(newPipeline.end(), oldPipeline.begin(), oldPipeline.end());
    }

    uasserted(ErrorCodes::ViewRecursionLimitExceeded, "view depth too deep or view cycle detected");
    MONGO_UNREACHABLE;
}
}  // namespace mongo
