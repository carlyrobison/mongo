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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/views/view_transform.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/log.h"


namespace mongo {

bool ViewTransform::isValidOnView(const BSONObj& cmdObj) {
    // TODO - implement
    return true;
}

BSONObj ViewTransform::findToViewAggregation(const BSONObj& cmdObj, bool hasExplain) {
    return findToViewAggregation(std::string(), std::vector<BSONElement>(), cmdObj, hasExplain);
}

BSONObj ViewTransform::findToViewAggregation(const std::string resolvedViewNs,
                                             const std::vector<BSONElement>& resolvedViewPipeline,
                                             const BSONObj& cmdObj,
                                             bool hasExplain) {
    // Options that we do not support
    if (cmdObj.getBoolField("singleBatch") || cmdObj.hasField("hint") ||
        cmdObj.hasField("maxScan") || cmdObj.hasField("max") || cmdObj.hasField("min") ||
        cmdObj.hasField("returnKey") || cmdObj.hasField("tailable") ||
        cmdObj.hasField("showRecordId") || cmdObj.hasField("snapshot") ||
        cmdObj.hasField("oplogReplay") || cmdObj.hasField("noCursorTimeut") ||
        cmdObj.hasField("awaitData") || cmdObj.hasField("allowPartialResults")) {
        return BSONObj();
    }

    // Build the pipeline
    BSONObjBuilder b;
    std::vector<BSONObj> pipeline;
    auto colName =
        resolvedViewNs.empty() ? cmdObj["find"].str() : NamespaceString(resolvedViewNs).coll();

    for (auto&& item : resolvedViewPipeline) {
        pipeline.push_back(item.Obj());
    }

    if (cmdObj.hasField("filter")) {
        BSONObj value = cmdObj.getObjectField("filter");
        // We do not support these operators
        // if (!isValidQuery(cmd)) {
        //     return BSONObj();
        // }
        pipeline.push_back(BSON("$match" << value));
    }
    if (cmdObj.hasField("sort")) {
        BSONObj value = cmdObj.getObjectField("sort");
        if (value.hasField("$natural")) {
            // Do not support $natural
            return BSONObj();
        }
        pipeline.push_back(BSON("$sort" << value));
    }
    if (cmdObj.hasField("skip")) {
        pipeline.push_back(BSON("$skip" << cmdObj.getIntField("skip")));
    }
    if (cmdObj.hasField("limit")) {
        pipeline.push_back(BSON("$limit" << cmdObj.getIntField("limit")));
    }
    if (cmdObj.hasField("projection")) {
        BSONObj value = cmdObj.getObjectField("projection");
        if (!value.isEmpty()) {
            bool hasOutputField = false;
            for (BSONElement e : value) {
                const char* fieldName = e.fieldName();
                if (StringData(fieldName) != "_id" && e.numberInt() == 1) {
                    hasOutputField = true;
                    break;
                }
            }
            if (!hasOutputField) {
                return BSONObj();
            }
            pipeline.push_back(BSON("$project" << value));
        }
        for (BSONElement e : value) {
            // Only support simple 0 or 1 projection
            const char* fieldName = e.fieldName();
            if (fieldName[e.fieldNameSize() - 2] == '$') {
                return BSONObj();
            }
            if (!e.isNumber()) {
                return BSONObj();
            }
        }
    }

    b.append("aggregate", colName);
    b.append("pipeline", pipeline);

    if (cmdObj.hasField("batchSize")) {
        b.append("cursor", BSON("batchSize" << cmdObj.getIntField("batchSize")));
    } else {
        b.append("cursor", BSONObj());
    }
    if (hasExplain) {
        b.append("explain", true);
    }
    if (cmdObj.hasField("maxTimeMS")) {
        b.append("maxTimeMS", cmdObj.getIntField("maxTimeMS"));
    }

    return b.obj();
}

BSONObj ViewTransform::pipelineToViewAggregation(const std::string resolvedViewNs,
                                                 const std::vector<BSONObj>& resolvedViewPipeline,
                                                 const BSONObj& cmdObj) {
    BSONObjBuilder b;
    std::vector<BSONObj> pipeline;
    auto colName = NamespaceString(resolvedViewNs).coll();

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

BSONObj ViewTransform::countToViewAggregation(const BSONObj& resolvedView,
                                              const BSONObj& cmdObj,
                                              bool hasExplain) {
    // TODO: Implement
    return BSONObj();
}

}  // namespace mongo
