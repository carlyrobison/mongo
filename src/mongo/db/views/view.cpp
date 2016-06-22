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

ViewDefinition::ViewDefinition(StringData dbName,
                               StringData viewName,
                               StringData viewOn,
                               const BSONObj& pipeline,
                               bool timeseries)
    : _dbName(dbName.toString()),
      _viewName(viewName.toString()),
      _backingViewName(viewOn.toString()),
      _timeseries = timeseries {

    for (BSONElement e : pipeline) {
        _pipeline.push_back(e.Obj().getOwned());
    }
}

void ViewDefinition::changeBackingNs(std::string newNs) {
    _backingViewName = newNs;
}

void ViewDefinition::changePipeline(const BSONObj& pipeline) {
    _pipeline.clear();
    for (BSONElement e : pipeline) {
        BSONObj value = e.Obj();
        _pipeline.push_back(value.copy());
    }
}

BSONObj ViewDefinition::pipelineToViewAggregation(const std::string resolvedViewNs,
                                                  const std::vector<BSONObj>& resolvedViewPipeline,
                                                  const BSONObj& cmdObj) {
    BSONObjBuilder b;
    std::vector<BSONObj> pipeline;
    auto colName = NamespaceString(resolvedViewNs).coll().toString();

    for (auto&& item : resolvedViewPipeline) {
        pipeline.push_back(item);
    }

    for (BSONElement e : cmdObj) {
        StringData fieldName = e.fieldNameStringData();
        if (fieldName == "pipeline") {
            BSONObj p = e.embeddedObject();
            for (BSONElement el : p) {
                pipeline.push_back(el.Obj().getOwned());
            }

            b.append("pipeline", pipeline);
        } else if (fieldName == "aggregate") {
            b.append("aggregate", colName);
        } else {
            b.append(e);
        }
    }

    return b.obj();
}

}  // namespace mongo
