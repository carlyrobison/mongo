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

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/parsed_aggregation_projection.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/stdx/memory.h"

namespace mongo {

class FieldPath;
class Value;

namespace parsed_aggregation_projection {

/**
 * A ParsedAddFields represents a parsed form of the raw BSON specification for the AddFields
 * stage.
 *
 * This class is mostly a wrapper around an AdditionNode tree. It contains logic to parse a
 * specification object into the corresponding AdditionNode tree, but defers most execution logic
 * to the underlying tree.
 */
class ParsedAddFields : public ParsedAggregationProjection {
public:
    ParsedAddFields() : ParsedAggregationProjection(), _root(new InclusionNode()) {}

    /**
     * Creates the data needed to perform an AddFields.
     * Overwrites the create() method in ParsedAggregationProjection
     */
    static std::unique_ptr<ParsedAddFields> create(const BSONObj& spec);

    ProjectionType getType() const final {
        return ProjectionType::kComputed;
    }

    /**
     * Parses the addFields specification given by 'spec', populating internal data structures.
     */
    void parse(const BSONObj& spec) final {
        VariablesIdGenerator idGenerator;
        VariablesParseState variablesParseState(&idGenerator);
        parse(spec, variablesParseState);
        _variables = stdx::make_unique<Variables>(idGenerator.getIdCount());
    }

    /**
     * Serialize the addition.
     */
    Document serialize(bool explain = false) const final {
        MutableDocument output;
        _root->serialize(&output, explain);
        return output.freeze();
    }

    /**
     * Optimize any computed expressions.
     */
    void optimize() final {
        _root->optimize();
    }

    void injectExpressionContext(const boost::intrusive_ptr<ExpressionContext>& expCtx) final {
        _root->injectExpressionContext(expCtx);
    }

    void addDependencies(DepsTracker* deps) const final {
        _root->addDependencies(deps);
    }

    /**
     * Add the specified fields to 'inputDoc'.
     *
     * Computed fields will be added in the order in which they were specified to the $addFields
     * stage.
     *
     * Arrays will be traversed, with any dotted/nested exclusions or computed fields applied to
     * each element in the array.
     */
    Document applyProjection(Document inputDoc) const final {
        _variables->setRoot(inputDoc);
        return applyProjection(inputDoc, _variables.get());
    }

    Document applyProjection(Document inputDoc, Variables* vars) const;

private:
    /**
     * Parses 'spec' to determine which fields to add.
     */
    void parse(const BSONObj& spec, const VariablesParseState& variablesParseState);

    /**
     * Attempts to parse 'objSpec' as an expression like {$add: [...]}. Adds a computed field to
     * '_root' and returns true if it was successfully parsed as an expression. Returns false if it
     * was not an expression specification.
     *
     * Throws an error if it was determined to be an expression specification, but failed to parse
     * as a valid expression.
     */
    bool parseObjectAsExpression(StringData pathToObject,
                                 const BSONObj& objSpec,
                                 const VariablesParseState& variablesParseState);

    /**
     * Traverses 'subObj' and parses each field. Adds any computed fields at this level
     * to 'node'.
     */
    void parseSubObject(const BSONObj& subObj,
                        const VariablesParseState& variablesParseState,
                        InclusionNode* node);

    // The AdditionNode tree does most of the execution work once constructed.
    std::unique_ptr<InclusionNode> _root;

    // This is needed to give the expressions knowledge about the context in which they are being
    // executed.
    std::unique_ptr<Variables> _variables;
};
}  // namespace parsed_aggregation_projection
}  // namespace mongo
