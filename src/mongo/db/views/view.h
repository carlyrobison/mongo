// view.h: Interface for views

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

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <memory>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

class Pipeline;
class BSONObj;

// Represents a "view"; that is, a visible subset of a collection or another view.
class ViewDefinition {
public:
    ViewDefinition(StringData dbName,
                   StringData viewName,
                   StringData viewOn,
                   const BSONObj& pipeline);

    void operator=(const ViewDefinition& other) {
        _dbName = other._dbName;
        _viewName = other._viewName;
        _backingViewName = other._backingViewName;
        _pipeline = other._pipeline;
    }

    StringData name() const {
        return StringData(_viewName);
    }

    StringData viewOn() const {
        return StringData(_backingViewName);
    }

    StringData db() const {
        return StringData(_dbName);
    }

    std::string fullViewNs() const {
        return _dbName + "." + _viewName;
    }

    std::string fullViewOnNs() const {
        return _dbName + "." + _backingViewName;
    }

    const std::vector<BSONObj>& pipeline() const {
        return _pipeline;
    }

    void changeBackingNs(std::string newNs);

    void changePipeline(const BSONObj& pipeline);

    static BSONObj getAggregateCommand(std::string rootNs,
                                       BSONObj& cmd,
                                       std::vector<BSONObj> pipeline);

    // Just for debugging right now
    std::string toString() {
        str::stream ss;
        ss << "{name: " << _viewName << " options: {view: " << _backingViewName << ", pipeline: [";
        for (size_t i = 0; i < _pipeline.size(); i++) {
            ss << _pipeline[i].toString();
            if (i != _pipeline.size() - 1) {
                ss << ",";
            }
        }
        ss << "]}}";
        return ss;
    }

private:
    stdx::mutex _mutex;
    std::string _dbName;
    std::string _viewName;  // The namespace of the view.
    std::string
        _backingViewName;  // The namespace of the view/collection upon which the view is based.
    std::vector<BSONObj> _pipeline;
};
}  // namespace mongo
