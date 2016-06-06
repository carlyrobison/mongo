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

BSONObj convertToAggregation(StringData backingNs, const BSONObj& pipeline) {
    BSONObjBuilder builder;

    invariant(!backingNs.empty() && !pipeline.isEmpty());
    builder.append("aggregate", backingNs.toString());
    builder.appendArray("pipeline", pipeline);

    return builder.obj();
}
}

const std::uint32_t ViewCatalog::kMaxViewDepth = 20;

ViewCatalog* ViewCatalog::getInstance() {
    invariant(viewCatalogSingleton);
    return viewCatalogSingleton;
}

Status ViewCatalog::createView(OperationContext* txn,
                               StringData ns,
                               StringData backingNs,
                               const BSONObj& pipeline) {
    if (ViewCatalog::lookup(txn, ns)) {
        LOG(3) << "VIEWS: Attempted to create a duplicate view";
        return Status(ErrorCodes::NamespaceExists, "Namespace already exists");
    }

    // Parse the pipeline, then steal its list of DocumentSources.
    std::string errmsg;
    errmsg.reserve(1024);
    auto parsedPipeline =
        Pipeline::parseCommand(errmsg, convertToAggregation(backingNs, pipeline), nullptr);
    if (!errmsg.empty()) {
        uasserted(40140, errmsg);
    }

    // Steal the list of sources for the pipeline. This may be empty (for example, performing a
    // $match with no specifier.
    auto parsedSources = parsedPipeline->getSources();

    _viewMap[ns.toString()] = stdx::make_unique<ViewDefinition>(ns, backingNs, parsedSources);
    return Status::OK();
}

ViewDefinition* ViewCatalog::lookup(OperationContext* txn, StringData ns) {
    auto result = _viewMap.find(ns.toString());
    if (result == _viewMap.end())
        return nullptr;
    else
        return (result->second).get();
}

std::tuple<std::string, boost::intrusive_ptr<Pipeline>> ViewCatalog::resolveView(
    OperationContext* txn, StringData ns, boost::intrusive_ptr<Pipeline> pipeline) {
    LOG(3) << "VIEWS: attempting to resolve " << ns;

    StringData backingNs = ns;
    boost::intrusive_ptr<Pipeline> newPipeline = pipeline;

    for (auto i = ViewCatalog::kMaxViewDepth; i > 0; i--) {
        auto numberOfAttempts = ViewCatalog::kMaxViewDepth - i;
        LOG(3) << "VIEWS: resolution attempt #" << numberOfAttempts;
        ViewDefinition* view = lookup(txn, backingNs);
        if (!view) {
            std::string backingNsString = backingNs.toString();
            return std::tie(backingNsString, newPipeline);
        }

        backingNs = view->backingNs();
        newPipeline = view->concatenate(newPipeline);
    }

    uasserted(ErrorCodes::ViewRecursionLimitExceeded, "view depth too deep or view cycle detected");
    MONGO_UNREACHABLE;
}
}  // namespace mongo
