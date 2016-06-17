// Tests the find command on views.

(function() {
    "use strict";

    // For arrayEq and orderedArrayEq.
    load("jstests/aggregation/extras/utils.js");

    var viewsDB = db.getSiblingDB("views_find");
    assert.commandWorked(viewsDB.dropDatabase());

    // Helper functions.
    let assertFindResultEq = function(cmd, expected, ordered) {
        let res = viewsDB.runCommand(cmd);
        assert.commandWorked(res);
        let cursor = new DBCommandCursor(db.getMongo(), res, 5);

        if (typeof(ordered) === "undefined" || !ordered)
            assert(orderedArrayEq(cursor.toArray(), expected));
        else
            assert(arrayEq(cursor.toArray(), expected));
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

    // Filters and "simple" projections.
    assertFindResultEq({find: "identityView"}, allDocuments, false /* unordered comparison */
                       );
    assertFindResultEq({find: "identityView", filter: {state: "NJ"}, projection: {_id: 1}},
                       [{_id: "Trenton"}, {_id: "Newark"}]);

    // A view that projects out the _id should still work with the find command.
    assertFindResultEq({find: "noIdView", filter: {state: "NY"}, projection: {pop: 1}}, [{pop: 7}]);

    // Only simple 0 or 1 projections are allowed on views.
    assert.commandFailed(viewsDB.runCommand(
        {find: "identityView", projection: {$elemMatch: {state: "NY", pop: {$gt: 5}}}}));

    // Sort, limit and batchSize.
    assertFindResultEq(
        {find: "identityView", sort: {_id: 1}}, allDocuments, true /* ordered comparison */
        );
    assertFindResultEq(
        {find: "identityView", limit: 1, batchSize: 1, sort: {_id: 1}, projection: {_id: 1}},
        [{_id: "New York"}]);

    // Negative batch size and limit should fail.
    assert.commandFailed(viewsDB.runCommand({find: "identityView", batchSize: -1}));
    assert.commandFailed(viewsDB.runCommand({find: "identityView", limit: -1}));

    // Views support find with explain.
    assert.commandWorked(viewsDB.identityView.find().explain());
}());
