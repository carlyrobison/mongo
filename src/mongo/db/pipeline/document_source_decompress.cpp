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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault
#include "mongo/util/log.h"

#include "mongo/platform/basic.h"

#include "mongo/db/ftdc/decompressor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

using boost::intrusive_ptr;
using std::string;
using std::vector;

/** Helper class to decompress documents from a single compressed one. */
class DocumentSourceDecompress::Decompressor {
public:
    Decompressor(const FieldPath& decompressPath);
    /** Reset the decompressor to unwind a new document. */
    void resetDocument(const Document& document);

    /**
     * @return the next document decompressed from the document provided to resetDocument().
     *
     * Returns boost::none if there are no more decompressed documents.
     */
    boost::optional<Document> getNext();

private:
    // Path to the array to unwind.
    const FieldPath _decompressPath;

    std::vector<BSONObj> _docsToReturn;
};

DocumentSourceDecompress::Decompressor::Decompressor(const FieldPath& decompressPath)
    : _decompressPath(decompressPath) {}

void DocumentSourceDecompress::Decompressor::resetDocument(const Document& document) {
    // Decompress the next batch.
    Value compressedData = document.getNestedField(_decompressPath);
    log() << "compressed data: " << compressedData;

    ConstDataRange buf = compressedData.getBinData();
    auto swBuf = FTDCDecompressor().uncompress(buf);
    uassert(40278, swBuf.getStatus().reason(), swBuf.isOK());
    _docsToReturn = swBuf.getValue();
}

boost::optional<Document> DocumentSourceDecompress::Decompressor::getNext() {
    if (_docsToReturn.empty()) {
        return boost::none;
    }

    Document doc = Document(_docsToReturn.back());
    _docsToReturn.pop_back();
    log() << "document: " << doc;
    return doc;
}

DocumentSourceDecompress::DocumentSourceDecompress(const intrusive_ptr<ExpressionContext>& expCtx,
                                                   const FieldPath& fieldPath)
    : DocumentSource(expCtx),
      _decompressPath(fieldPath),
      _decompressor(new Decompressor(fieldPath)) {}

REGISTER_DOCUMENT_SOURCE(decompress, DocumentSourceDecompress::createFromBson);

const char* DocumentSourceDecompress::getSourceName() const {
    return "$decompress";
}

boost::optional<Document> DocumentSourceDecompress::getNext() {
    pExpCtx->checkForInterrupt();

    boost::optional<Document> out = _decompressor->getNext();
    while (!out) {
        // No more objects to decompress, so load another batch in.
        boost::optional<Document> input = pSource->getNext();
        if (!input)
            return boost::none;  // input exhausted

        // Try to extract an output document from the new input document.
        _decompressor->resetDocument(*input);
        out = _decompressor->getNext();
    }

    return out;
}

// redo this
Value DocumentSourceDecompress::serialize(bool explain) const {
    return Value(
        Document{{getSourceName(), Document{{"path", _decompressPath.fullPathWithPrefix()}}}});
}

DocumentSource::GetDepsReturn DocumentSourceDecompress::getDependencies(DepsTracker* deps) const {
    deps->fields.insert(_decompressPath.fullPath());
    return EXHAUSTIVE_FIELDS;
}

intrusive_ptr<DocumentSource> DocumentSourceDecompress::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    // $decompress accepts "{$decompress: '$path'}" syntax.
    uassert(40282,
            str::stream()
                << "expected a string or an object as specification for $decompress stage, got "
                << typeName(elem.type()),
            elem.type() == String);
    std::string prefixedPathString = elem.str();
    uassert(40281, "no path specified to $decompress stage", !prefixedPathString.empty());

    uassert(40280,
            str::stream() << "path option to $decompress stage should be prefixed with a '$': "
                          << prefixedPathString,
            prefixedPathString[0] == '$');
    string pathString(Expression::removeFieldPrefix(prefixedPathString));
    return new DocumentSourceDecompress(pExpCtx, pathString);
}

}  // namespace mongo
