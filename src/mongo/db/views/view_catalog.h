// view_catalog.h: In-memory data structures for view resolution

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

#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/views/view.h"
#include "mongo/util/string_map.h"

namespace mongo {
class Database;

// In-memory data structure that holds view definitions
class ViewCatalog {
public:
    // TODO(SERVER-23700): Make this a unique_ptr once StringMap supports move-only types.
    typedef StringMap<std::shared_ptr<ViewDefinition>> ViewMap;
    static const std::uint32_t kMaxViewDepth;

    ViewCatalog(OperationContext* txn, Database* database);

    ViewMap::const_iterator begin() const {
        return _viewMap.begin();
    }

    ViewMap::const_iterator end() const {
        return _viewMap.end();
    }

    /**
     * Create a new view.
     */
    Status createView(OperationContext* txn,
                      const NamespaceString& viewName,
                      const std::string& viewOn,
                      const BSONObj& pipeline);

    /**
     * Drop a view.
     */
    void dropView(OperationContext* txn, const NamespaceString& viewName);

    /**
     * Look up the namespace in the view catalog, returning a pointer to a View definition, or
     * nullptr if it doesn't exist. Note that the caller does not own the pointer.
     *
     * @param ns The full namespace string of the view.
     * @returns A bare pointer to a view definition if ns is a valid view with a backing namespace.
     */
    ViewDefinition* lookup(StringData ns);

    /**
     * Resolve the views on the given namespace, transforming the pipeline appropriately.
     *
     * @returns A pair containing the fully-qualified namespace and pipeline for an aggregation.
     */
    std::tuple<std::string, std::vector<BSONObj>> resolveView(OperationContext* txn, StringData ns);

private:
    Database* _db;
    ViewMap _viewMap;
};
}  // namespace mongo
