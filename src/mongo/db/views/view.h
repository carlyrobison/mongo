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

namespace mongo {

class Pipeline;
class BSONObj;

// Represents a "view"; that is, a visible subset of a collection or another view.
class ViewDefinition {
public:
    ViewDefinition(StringData ns, StringData backingNs, BSONObj& pipeline);

    StringData ns() const {
        return StringData(_ns);
    }

    StringData backingNs() const {
        return StringData(_backingNs);
    }

    const std::vector<BSONObj>& pipeline() const {
        return _pipeline;
    }

    static BSONObj getAggregateCommand(std::string rootNs,
                                       BSONObj& cmd,
                                       std::vector<BSONObj> pipeline);

    std::string toString() {
        std::string s;
        for (auto& item : _pipeline) {
            s += item.jsonString();
        }
        return _ns + "    " + _backingNs + "   " + s;
    }

private:
    std::string _ns;         // The namespace of the view.
    std::string _backingNs;  // The namespace of the view/collection upon which the view is based.
    std::vector<BSONObj> _pipeline;
};
}  // namespace mongo
