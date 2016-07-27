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

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/parsed_add_fields.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace parsed_aggregation_projection {
namespace {
using std::vector;

template <typename T>
BSONObj wrapInLiteral(const T& arg) {
    return BSON("$literal" << arg);
}

// Verify that ParsedAddFields requires an object with valid expressions as input.
TEST(ParsedAddFieldsSpec, ThrowsWhenParsingAnInvalidExpression) {
    ParsedAddFields addition;
    ASSERT_THROWS(addition.parse(BSON("a" << BSON("$gt" << BSON("bad"
                                                                << "arguments")))),
                  UserException);
}

// Verify that empty object specifications are allowed as inputs
TEST(ParsedAddFieldsDeps, DoesNotRejectEmptyObjectForAddFieldsSpec) {
    ParsedAddFields addition;
    addition.parse(BSONObj());

    DepsTracker deps;
    addition.addDependencies(&deps);

    ASSERT_EQ(deps.fields.size(), 0UL);
}

// Verify that replaced fields are not included as dependencies.
TEST(ParsedAddFieldsDeps, RemovesReplaceFieldsFromDependencies) {
    ParsedAddFields addition;
    addition.parse(BSON("a" << true));

    DepsTracker deps;
    addition.addDependencies(&deps);

    ASSERT_EQ(deps.fields.size(), 0UL);
    ASSERT_EQ(deps.fields.count("_id"), 0UL);  // Not explicitly included
    ASSERT_EQ(deps.fields.count("a"), 0UL);    // set to true
}

// Verify that adding nested fields keeps the top-level field as a dependency.
TEST(ParsedAddFieldsDeps, IncludesTopLevelFieldInDependenciesWhenAddingNestedFields) {
    ParsedAddFields addition;
    addition.parse(BSON("x.y" << true));

    DepsTracker deps;
    addition.addDependencies(&deps);

    ASSERT_EQ(deps.fields.size(), 1UL);
    ASSERT_EQ(deps.fields.count("_id"), 0UL); // Not explicitly included
    ASSERT_EQ(deps.fields.count("x.y"), 0UL); // Set to true
    ASSERT_EQ(deps.fields.count("x"), 1UL); // Top-level field included
}

// Verify that fields that an expression depends on are added to the dependencies.
TEST(ParsedAddFieldsDeps, AddsDependenciesForComputedFields) {
    ParsedAddFields addition;
    addition.parse(BSON("x.y"
                        << "$z"
                        << "a"
                        << "$b"));

    DepsTracker deps;
    addition.addDependencies(&deps);

    ASSERT_EQ(deps.fields.size(), 3UL);
    ASSERT_EQ(deps.fields.count("_id"), 0UL); // Not explicitly included
    ASSERT_EQ(deps.fields.count("z"), 1UL); // Needed by the ExpressionFieldPath for x.y.
    ASSERT_EQ(deps.fields.count("x"), 1UL); // Preserves top-level field, for structure.
    ASSERT_EQ(deps.fields.count("a"), 0UL); // Replaced, so omitted.
    ASSERT_EQ(deps.fields.count("b"), 1UL); // Needed by the ExpressionFieldPath for a.
}

// Verify that the serialization produces the correct output: converting numbers and literals to
// their corresponding $const form.
TEST(ParsedAddFieldsSerialize, SerializesToCorrectForm) {
    ParsedAddFields addition;
    addition.parse(fromjson("{a: {$add: ['$a', 2]}, b: {d: 3}, 'x.y': {$literal: 4}}"));

    auto expectedSerialization = Document(
        fromjson("{a: {$add: [\"$a\", {$const: 2}]}, b: {d: {$const: 3}}, x: {y: {$const: 4}}}"));

    // Should be the same if we're serializing for explain or for internal use.
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(false));
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(true));
}

// Verify that serialize treats the _id field as any other field: including when explicity included,
// excluded otherwise. We add this check because it is different behavior from $project, yet they
// derive from the same parent class. If the parent class were to change, this test would fail.
TEST(ParsedAddFieldsSerialize, AddsIdToSerializeOnlyWhenExplicitlyIncluded) {
    ParsedAddFields addition;
    addition.parse(BSON("_id" << false));

    // Adds explicit "_id" setting field, serializes expressions.
    auto expectedSerialization = Document(fromjson("{_id: {$const: false}}"));

    // Should be the same if we're serializing for explain or for internal use.
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(false));
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(true));

    addition.parse(BSON("a" << true));

    // Does not include "_id" setting field.
    expectedSerialization = Document(fromjson("{a: {$const: true}}"));

    // Should be the same if we're serializing for explain or for internal use.
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(false));
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(true));
}

// Verify that 
TEST(ParsedAddFieldsOptimize, ShouldOptimizeTopLevelExpressions) {
    ParsedAddFields addition;
    addition.parse(BSON("a" << BSON("$add" << BSON_ARRAY(1 << 2))));

    addition.optimize();

    auto expectedSerialization = Document{{"a", Document{{"$const", 3}}}};

    // Should be the same if we're serializing for explain or for internal use.
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(false));
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(true));
}

TEST(ParsedAddFieldsOptimize, ShouldOptimizeNestedExpressions) {
    ParsedAddFields addition;
    addition.parse(BSON("a.b" << BSON("$add" << BSON_ARRAY(1 << 2))));

    addition.optimize();

    auto expectedSerialization = Document{{"a", Document{{"b", Document{{"$const", 3}}}}}};

    // Should be the same if we're serializing for explain or for internal use.
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(false));
    ASSERT_DOCUMENT_EQ(expectedSerialization, addition.serialize(true));
}

//
// Top-level only.
//

TEST(ParsedAddFieldsExecutionTest, ShouldIncludeTopLevelField) {
    ParsedAddFields addition;
    addition.parse(BSON("c" << 3));

    // Specified field is not in the document
    auto result = addition.applyProjection(Document{{"a", 1}, {"b", 2}});
    auto expectedResult = Document{{"a", 1}, {"b", 2}, {"c", 3}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Specified field is the only field in the document.
    result = addition.applyProjection(Document{{"c", 1}});
    expectedResult = Document{{"c", 3}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Specified field is one of the fields in the document
    result = addition.applyProjection(Document{{"c", 1}, {"b", 2}});
    expectedResult = Document{{"c", 3}, {"b", 2}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // There are no fields in the document.
    result = addition.applyProjection(Document{});
    expectedResult = Document{{"c", 3}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldAddComputedTopLevelField) {
    ParsedAddFields addition;
    addition.parse(BSON("newField" << wrapInLiteral("computedVal")));
    auto result = addition.applyProjection(Document{});
    auto expectedResult = Document{{"newField", "computedVal"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Computed field should replace existing field.
    result = addition.applyProjection(Document{{"newField", "preExisting"}});
    expectedResult = Document{{"newField", "computedVal"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldKeepOriginalDocWithEmptyAddFields) {
    ParsedAddFields addition;
    addition.parse(BSONObj());
    auto result = addition.applyProjection(Document{{"a", 1}});
    auto expectedResult = Document{{"a", 1}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldKeepFieldsInOrderOfInputDoc) {
    ParsedAddFields addition;
    addition.parse(BSONObj());
    auto inputDoc = Document{{"second", 1}, {"first", 0}, {"third", 2}};
    auto result = addition.applyProjection(inputDoc);
    ASSERT_DOCUMENT_EQ(result, inputDoc);
}

TEST(ParsedAddFieldsExecutionTest, ShouldAddNewFieldsAfterExistingComputedFieldsInOrderSpecified) {
    ParsedAddFields addition;
    addition.parse(BSON("firstComputed" << wrapInLiteral("FIRST") << "secondComputed"
                                        << wrapInLiteral("SECOND")));
    auto result = addition.applyProjection(Document{{"first", 0}, {"second", 1}, {"third", 2}});
    auto expectedResult = Document{{"first", 0},
                                   {"second", 1},
                                   {"third", 2},
                                   {"firstComputed", "FIRST"},
                                   {"secondComputed", "SECOND"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldIncludeExistingFields) {
    ParsedAddFields addition;
    addition.parse(BSON("a" << true));
    auto result = addition.applyProjection(Document{{"_id", "ID"}, {"a", 1}, {"b", 2}});
    auto expectedResult = Document{{"_id", "ID"}, {"a", true}, {"b", 2}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Should leave the "_id" in the same place as in the original document.
    result = addition.applyProjection(Document{{"a", 1}, {"b", 2}, {"_id", "ID"}});
    expectedResult = Document{{"a", true}, {"b", 2}, {"_id", "ID"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldIncludeExistingFieldsWithComputedFields) {
    ParsedAddFields addition;
    addition.parse(BSON("newField" << wrapInLiteral("computedVal")));
    auto result = addition.applyProjection(Document{{"_id", "ID"}, {"a", 1}});
    auto expectedResult = Document{{"_id", "ID"}, {"a", 1}, {"newField", "computedVal"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldReplaceIdWithComputedId) {
    ParsedAddFields addition;
    addition.parse(BSON("_id" << wrapInLiteral("newId")));
    auto result = addition.applyProjection(Document{{"a", 1}, {"_id", "ID"}});
    auto expectedResult = Document{{"a", 1}, {"_id", "newId"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

//
// Projections with nested fields.
//

TEST(ParsedAddFieldsExecutionTest, ShouldAddSimpleDottedFieldToSubDoc) {
    ParsedAddFields addition;
    addition.parse(BSON("a.b" << true));

    // More than one field in sub document.
    auto result = addition.applyProjection(Document{{"a", Document{{"b", 1}, {"c", 2}}}});
    auto expectedResult = Document{{"a", Document{{"b", true}, {"c", 2}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Specified field is the only field in the sub document.
    result = addition.applyProjection(Document{{"a", Document{{"b", 1}}}});
    expectedResult = Document{{"a", Document{{"b", true}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Specified field is not present in the sub document.
    result = addition.applyProjection(Document{{"a", Document{{"c", 1}}}});
    expectedResult = Document{{"a", Document{{"c", 1}, {"b", true}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // There are no fields in sub document.
    result = addition.applyProjection(Document{{"a", Document{}}});
    expectedResult = Document{{"a", Document{{"b", true}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldCreateSubDocIfDottedAddedFieldDoesNotExist) {
    ParsedAddFields addition;
    addition.parse(BSON("sub.target" << true));

    // Should add the path if it doesn't exist.
    auto result = addition.applyProjection(Document{});
    auto expectedResult = Document{{"sub", Document{{"target", true}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Should replace the second part of the path if that part already exists.
    result = addition.applyProjection(Document{{"sub", "notADocument"}});
    expectedResult = Document{{"sub", Document{{"target", true}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

// SERVER-25200 TODO: make this agree with $set
TEST(ParsedAddFieldsExecutionTest, ShouldApplyDottedAdditionToEachElementInArray) {
    ParsedAddFields addition;
    addition.parse(BSON("a.b" << true));

    vector<Value> nestedValues = {Value(1),
                                  Value(Document{}),
                                  Value(Document{{"b", 1}}),
                                  Value(Document{{"b", 1}, {"c", 2}}),
                                  Value(vector<Value>{}),
                                  Value(vector<Value>{Value(1), Value(Document{{"c", 1}})})};

    // Adds the field {b: true} to every object in the array. Recurses on non-empty nested
    // arrays. TODO: Is this the desired behavior?
    vector<Value> expectedNestedValues = {
        Value(Document{{"b", true}}),
        Value(Document{{"b", true}}),
        Value(Document{{"b", true}}),
        Value(Document{{"b", true}, {"c", 2}}),
        Value(vector<Value>{}),
        Value(vector<Value>{Value(Document{{"b", true}}), Value(Document{{"c", 1}, {"b", true}})})};
    auto result = addition.applyProjection(Document{{"a", nestedValues}});
    auto expectedResult = Document{{"a", expectedNestedValues}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldCreateNestedSubDocumentsAllTheWayToAddedField) {
    ParsedAddFields addition;
    addition.parse(BSON("a.b.c.d" << wrapInLiteral("computedVal")));

    // Should add the path if it doesn't exist.
    auto result = addition.applyProjection(Document{});
    auto expectedResult =
        Document{{"a", Document{{"b", Document{{"c", Document{{"d", "computedVal"}}}}}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    // Should replace non-documents with documents.
    result = addition.applyProjection(Document{{"a", Document{{"b", "other"}}}});
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldAddSubFieldsOfId) {
    ParsedAddFields addition;
    addition.parse(BSON("_id.X" << true << "_id.Z" << wrapInLiteral("NEW")));
    auto result = addition.applyProjection(Document{{"_id", Document{{"X", 1}, {"Y", 2}}}});
    auto expectedResult = Document{{"_id", Document{{"X", true}, {"Y", 2}, {"Z", "NEW"}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldAllowMixedNestedAndDottedFields) {
    ParsedAddFields addition;
    // Include all of "a.b", "a.c", "a.d", and "a.e".
    // Add new computed fields "a.W", "a.X", "a.Y", and "a.Z".
    addition.parse(BSON("a.b" << true << "a.c" << true << "a.W" << wrapInLiteral("W") << "a.X"
                              << wrapInLiteral("X")
                              << "a"
                              << BSON("d" << true << "e" << true << "Y" << wrapInLiteral("Y") << "Z"
                                          << wrapInLiteral("Z"))));
    auto result = addition.applyProjection(
        Document{{"a", Document{{"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}, {"f", "f"}}}});
    auto expectedResult = Document{{"a",
                                    Document{{"b", true},
                                             {"c", true},
                                             {"d", true},
                                             {"e", true},
                                             {"f", "f"},
                                             {"W", "W"},
                                             {"X", "X"},
                                             {"Y", "Y"},
                                             {"Z", "Z"}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, ShouldApplyNestedAddedFieldsInOrderSpecified) {
    ParsedAddFields addition;
    addition.parse(BSON("a" << wrapInLiteral("FIRST") << "b.c" << wrapInLiteral("SECOND")));
    auto result = addition.applyProjection(Document{});
    auto expectedResult = Document{{"a", "FIRST"}, {"b", Document{{"c", "SECOND"}}}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

TEST(ParsedAddFieldsExecutionTest, AddedFieldReplacingExistingShouldAppearWithOriginalFields) {
    ParsedAddFields addition;
    addition.parse(BSON("b" << wrapInLiteral("NEW")));
    auto result = addition.applyProjection(Document{{"b", 1}, {"a", 1}});
    auto expectedResult = Document{{"b", "NEW"}, {"a", 1}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);

    result = addition.applyProjection(Document{{"a", 1}, {"b", 4}});
    expectedResult = Document{{"a", 1}, {"b", "NEW"}};
    ASSERT_DOCUMENT_EQ(result, expectedResult);
}

//
// Misc.
//

TEST(ParsedAddFieldsExecutionTest, ShouldAlwaysKeepMetadataFromOriginalDoc) {
    ParsedAddFields addition;
    addition.parse(BSON("a" << true));

    MutableDocument inputDocBuilder(Document{{"a", 1}});
    inputDocBuilder.setRandMetaField(1.0);
    inputDocBuilder.setTextScore(10.0);
    Document inputDoc = inputDocBuilder.freeze();

    auto result = addition.applyProjection(inputDoc);

    MutableDocument expectedDoc(Document{{"a", true}});
    expectedDoc.copyMetaDataFrom(inputDoc);
    ASSERT_DOCUMENT_EQ(result, expectedDoc.freeze());
}

}  // namespace
}  // namespace parsed_aggregation_projection
}  // namespace mongo
