// Tests aggregation on views for proper pipeline concatenation and semantics.

(function() {
    "use strict";

    // For arrayEq and orderedArrayEq.
    load("jstests/aggregation/extras/utils.js");

    var viewsDB = db.getSiblingDB("views_aggregation");
    assert.commandWorked(viewsDB.dropDatabase());

    // Helper functions.
    let assertAggResultEq = function(cmd, expected, ordered) {
        let res = viewsDB.runCommand(cmd);
        assert.commandWorked(res);
        let cursor = new DBCommandCursor(db.getMongo(), res, 5);

        let arr = cursor.toArray();

        print("Expected:");
        printjson(expected);
        print("Got:");
        printjson(arr);

        if (typeof(ordered) === "undefined" || !ordered)
            assert(orderedArrayEq(arr, expected));
        else
            assert(arrayEq(arr, expected));
    };

    // Populate a collection with some test data.
    let allDocuments = [];
    allDocuments.push({_id: "New York", state: "NY", pop: 7});
    allDocuments.push({_id: "Newark", state: "NJ", pop: 3});
    allDocuments.push({_id: "Palo Alto", state: "CA", pop: 10});
    allDocuments.push({_id: "San Francisco", state: "CA", pop: 4});
    allDocuments.push({_id: "Trenton", state: "NJ", pop: 5});

    let coll = viewsDB.coll;
    let bulk = coll.initializeUnorderedBulkOp();
    allDocuments.forEach(function(doc) {
        bulk.insert(doc);
    });
    assert.writeOK(bulk.execute());

    // Create views on the data.
    assert.commandWorked(
        viewsDB.runCommand({create: "identityView", viewOn: "coll", pipeline: [{$match: {}}]}));
    assert.commandWorked(viewsDB.runCommand({
        create: "noIdView",
        viewOn: "coll",
        pipeline: [{$match: {}}, {$project: {_id: 0, state: 1, pop: 1}}]
    }));

    // Find all documents with empty aggregations.
    assertAggResultEq({aggregate: "identityView", cursor: {}}, allDocuments);
    assertAggResultEq({aggregate: "identityView", pipeline: [{$match: {}}], cursor: {}},
                      allDocuments);

    // An aggregation still works on a view that strips _id.
    assertAggResultEq({aggregate: "noIdView", pipeline: [{$match: {state: "NY"}}], cursor: {}},
                      [{state: "NY", pop: 7}]);
}());
