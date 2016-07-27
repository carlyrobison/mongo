/**
 * Copyright 2016 (c) 10gen Inc.
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

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/parsed_add_fields.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

using boost::intrusive_ptr;
using parsed_aggregation_projection::ParsedAddFields;

DocumentSourceAddFields::DocumentSourceAddFields(const intrusive_ptr<ExpressionContext>& expCtx,
                                                 std::unique_ptr<ParsedAddFields> parsedAddFields)
    : DocumentSource(expCtx), _parsedAddFields(std::move(parsedAddFields)) {}

REGISTER_DOCUMENT_SOURCE(addFields, DocumentSourceAddFields::createFromBson);

const char* DocumentSourceAddFields::getSourceName() const {
    return "$addFields";
}

boost::optional<Document> DocumentSourceAddFields::getNext() {
    pExpCtx->checkForInterrupt();

    // Get the next input document.
    boost::optional<Document> input = pSource->getNext();
    if (!input) {
        return boost::none;
    }

    // Apply and return the document with added fields.
    return _parsedAddFields->applyProjection(*input);
}

Pipeline::SourceContainer::iterator DocumentSourceAddFields::optimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    invariant(*itr == this);

    auto nextSkip = dynamic_cast<DocumentSourceSkip*>((*std::next(itr)).get());
    auto nextLimit = dynamic_cast<DocumentSourceLimit*>((*std::next(itr)).get());

    if (nextSkip || nextLimit) {
        std::swap(*itr, *std::next(itr));
        return itr == container->begin() ? itr : std::prev(itr);
    }
    return std::next(itr);
}

intrusive_ptr<DocumentSource> DocumentSourceAddFields::optimize() {
    _parsedAddFields->optimize();
    return this;
}

void DocumentSourceAddFields::dispose() {
    _parsedAddFields.reset();
}

Value DocumentSourceAddFields::serialize(bool explain) const {
    return Value(Document{{getSourceName(), _parsedAddFields->serialize(explain)}});
}

intrusive_ptr<DocumentSource> DocumentSourceAddFields::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {

    // Confirm that the stage was called with an object.
    uassert(40247,
            str::stream() << "$addFields specification stage must be an object, got "
                          << typeName(elem.type()),
            elem.type() == Object);

    // Create the AddFields aggregation stage.
    return new DocumentSourceAddFields(expCtx, ParsedAddFields::create(elem.Obj()));
}

DocumentSource::GetDepsReturn DocumentSourceAddFields::getDependencies(DepsTracker* deps) const {
    // We will definitely need the fields that we use in computed expressions.
    _parsedAddFields->addDependencies(deps);
    // For the rest of the fields, see if the next stage needs them.
    return SEE_NEXT;
}

void DocumentSourceAddFields::doInjectExpressionContext() {
    _parsedAddFields->injectExpressionContext(pExpCtx);
}
}
