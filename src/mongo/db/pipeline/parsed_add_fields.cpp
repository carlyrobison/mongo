/**
 *    Copyright (C) 2016 MongoDB, Inc.
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
#include "mongo/util/log.h"

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/parsed_add_fields.h"

#include <algorithm>

namespace mongo {

namespace parsed_aggregation_projection {

using std::string;
using std::unique_ptr;

//
// ComputedNode
//
ComputedNode::ComputedNode(std::string pathToNode) : InclusionNode(pathToNode) {}

void ComputedNode::serialize(MutableDocument* output, bool explain) const {
    for (auto&& field : _orderToProcessAdditionsAndChildren) {
        log() << "Processing field: " << field;
        auto childIt = _children.find(field);
        if (childIt != _children.end()) {
            log() << "Field is a child.";
            MutableDocument subDoc;
            childIt->second->serialize(&subDoc, explain);
            output->addField(field, subDoc.freezeToValue());
        } else {
            log() << "Assuming field is an expression?: " << (_expressions.end() != _expressions.find(field));
            auto expressionIt = _expressions.find(field);
            invariant(expressionIt != _expressions.end());
            output->addField(field, expressionIt->second->serialize(explain));
        }
    }
}

ComputedNode* ComputedNode::addOrGetChild(std::string field) {
    auto child = getChild(field);
    return child ? child : addChild(field);
}

ComputedNode* ComputedNode::getChild(string field) const {
    auto childIt = _children.find(field);
    return childIt == _children.end() ? nullptr : childIt->second.get();
}

ComputedNode* ComputedNode::addChild(string field) {
    invariant(!str::contains(field, "."));
    _orderToProcessAdditionsAndChildren.push_back(field);
    auto childPath = FieldPath::getFullyQualifiedPath(_pathToNode, field);
    auto insertedPair = _children.emplace(
        std::make_pair(std::move(field), stdx::make_unique<ComputedNode>(std::move(childPath))));
    return insertedPair.first->second.get();
}

//
// ParsedAddFields
//

std::unique_ptr<ParsedAddFields> ParsedAddFields::create(const BSONObj& spec) {
    // addFields has a projection type of kComputed, but that is not necessary to create it.

    std::unique_ptr<ParsedAddFields> parsedAddFields =
        stdx::make_unique<ParsedAddFields>();

    // Actually parse the specification.
    parsedAddFields->parse(spec);
    return parsedAddFields;
}

void ParsedAddFields::parse(const BSONObj& spec, const VariablesParseState& variablesParseState) {
    log() << "Parsing spec: " << spec;
    for (auto elem : spec) {
        log() << "elem: " << elem;
        auto fieldName = elem.fieldNameStringData();

        switch (elem.type()) {
            case BSONType::Object: {
                // This is either an expression, or a nested specification.
                if (parseObjectAsExpression(fieldName, elem.Obj(), variablesParseState)) {
                    // It was an expression.
                    break;
                }

                // The field name might be a dotted path. If so, we need to keep adding children
                // to our tree until we create a child that represents that path.
                auto remainingPath = FieldPath(elem.fieldName());
                auto child = _root.get();
                while (remainingPath.getPathLength() > 1) {
                    child = child->addOrGetChild(remainingPath.getFieldName(0));
                    remainingPath = remainingPath.tail();
                }
                // It is illegal to construct an empty FieldPath, so the above loop ends one
                // iteration too soon. Add the last path here.
                child = child->addOrGetChild(remainingPath.fullPath());
                parseSubObject(elem.Obj(), variablesParseState, child);
                break;
            }
            default: {
                // This is a literal or regular value.
                _root->addComputedField(FieldPath(elem.fieldName()),
                                        Expression::parseOperand(elem, variablesParseState));
            }
        }
    }
}

Document ParsedAddFields::applyProjection(Document inputDoc, Variables* vars) const {
    // All expressions will be evaluated in the context of the input document, before any
    // transformations have been applied.
    vars->setRoot(inputDoc);

    MutableDocument output(inputDoc);
    _root->addComputedFields(&output, vars);

    // Always pass through the metadata.
    output.copyMetaDataFrom(inputDoc);
    return output.freeze();
}

bool ParsedAddFields::parseObjectAsExpression(StringData pathToObject,
                                              const BSONObj& objSpec,
                                              const VariablesParseState& variablesParseState) {
    if (objSpec.firstElementFieldName()[0] == '$') {
        // This is an expression like {$add: [...]}. We have already verified that it has only one
        // field.
        invariant(objSpec.nFields() == 1);
        _root->addComputedField(pathToObject.toString(),
                                Expression::parseExpression(objSpec, variablesParseState));
        return true;
    }
    return false;
}

void ParsedAddFields::parseSubObject(const BSONObj& subObj,
                                     const VariablesParseState& variablesParseState,
                                     ComputedNode* node) {
    log() << "Parsing sub obj: " << subObj;
    for (auto elem : subObj) {
        invariant(elem.fieldName()[0] != '$');
        // Dotted paths in a sub-object have already been disallowed in
        // ParsedAggregationProjection's parsing.
        invariant(elem.fieldNameStringData().find('.') == std::string::npos);

        switch (elem.type()) {
            case BSONType::Object: {
                // This is either an expression, or a nested specification.
                auto fieldName = elem.fieldNameStringData().toString();
                if (parseObjectAsExpression(
                        FieldPath::getFullyQualifiedPath(node->getPath(), fieldName),
                        elem.Obj(),
                        variablesParseState)) {
                    break;
                }
                auto child = node->addOrGetChild(fieldName);
                parseSubObject(elem.Obj(), variablesParseState, child);
                break;
            }
            default: {
                // This is a literal or regular value.
                node->addComputedField(FieldPath(elem.fieldName()),
                                       Expression::parseOperand(elem, variablesParseState));
            }
        }
    }
}

}  // namespace parsed_aggregation_projection
}  // namespace mongo
