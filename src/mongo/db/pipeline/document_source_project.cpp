/**
 * Copyright 2011 (c) 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source.h"

#include <boost/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/db/pipeline/parsed_aggregation_projection.h"

namespace mongo {

using boost::intrusive_ptr;
using parsed_aggregation_projection::ParsedAggregationProjection;
using parsed_aggregation_projection::ProjectionType;

REGISTER_DOCUMENT_SOURCE_ALIAS(project, DocumentSourceProject::createFromBson);

std::vector<intrusive_ptr<DocumentSource>> DocumentSourceProject::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(15969, "$project specification must be an object", elem.type() == Object);

    std::unique_ptr<ParsedAggregationProjection> _parsedProject = ParsedAggregationProjection::create(elem.Obj());

    if (_parsedProject->getType() == ProjectionType::kInclusion) {
        // Stop looking for further dependencies later in the pipeline, since anything that is not
        // explicitly included or added in this projection will not exist after this stage, so would
        // be pointless to include in our dependencies.
        return {new DocumentSourceSingleDocumentTransformation(expCtx, std::move(_parsedProject), "$project", DocumentSource::EXHAUSTIVE_FIELDS)};
    }
    return {new DocumentSourceSingleDocumentTransformation(expCtx, std::move(_parsedProject), "$project", DocumentSource::SEE_NEXT)};
};

}  // namespace mongo
