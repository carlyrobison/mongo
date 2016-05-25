// Tests basic functionality of read-only, non-materialized views.

(function() {
    "use strict";
    load("jstests/aggregation/extras/utils.js");

    var coll = db.getSiblingDB("views_basic").coll;

    // Insert some control documents.
    var bulk = coll.initializeUnorderedBulkOp();
    bulk.insert({user: "foo", state: "CA"});
    bulk.insert({user: "bar", state: "NY"});
    bulk.insert({user: "qux", state: "NY"});
    assert.writeOK(bulk.execute());

    // TODO: Test view creation.
    // assert.commandWorked(db.runCommand(
    //    {create: "new_york", view: "views_basic.coll", pipeline: [{$match: {state: "NY"}}]}));
    // TODO: Shell helper version:
    // assert.commandWorked(db.createView("new_york", {view: "views_basic.coll", pipeline: [{$match:
    // {state: "NY"}}]});

    // Perform a simple count.
    var newYorkView = db.getSiblingDB("views_basic").new_york;
    // assert.eq(2, newYorkView.count());

    // Test aggregation pipeline concatenation.
    var result =
        newYorkView
            .aggregate([{$project: {_id: 0, user: 1, state: 1}}, {$sort: {user: 1}}, {$limit: 1}])
            .toArray();
    assert(arrayEq(result, [{user: "bar", state: "NY"}]));
}());
