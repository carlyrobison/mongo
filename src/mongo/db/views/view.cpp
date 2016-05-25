// view.cpp: Non-materialized views

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

#include "mongo/db/views/view.h"

#include <boost/intrusive_ptr.hpp>
#include <memory>

#include "mongo/base/string_data.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/util/log.h"

namespace mongo {

ViewDefinition::ViewDefinition(StringData ns,
                               StringData backingNs,
                               const Pipeline::SourceContainer& aggSources)
    : _ns(ns.toString()), _backingNs(backingNs.toString()), _pipeline(aggSources) {
    LOG(3) << "VIEWS: Constructed a new view " << ns << " on namespace " << backingNs;
}

boost::intrusive_ptr<Pipeline> ViewDefinition::concatenate(boost::intrusive_ptr<Pipeline> other) {
    invariant(!_pipeline.empty());

    // Make a copy of the source pipeline.
    Pipeline::SourceContainer newContainer(_pipeline);

    LOG(3) << "VIEWS: Appending the following sources to _pipeline";
    for (auto docSource : other->getSources()) {
        LOG(3) << "VIEWS: " << docSource->getSourceName();
        newContainer.push_back(docSource);
    }

    LOG(3) << "VIEWS: Iterating through concatenated pipeline";
    for (auto docSource : newContainer) {
        LOG(3) << "VIEWS: " << docSource->getSourceName();
    }

    // Construct the result from the new pipeline, reattaching to other's ExpressionContext.
    boost::intrusive_ptr<Pipeline> result(new Pipeline(newContainer, other->getContext()));
    return result;
}
}  // namespace mongo
