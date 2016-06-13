// Test the creation of views with various options. Once created, views should also be accessible
// via listCollections and the like.

(function() {
    "use strict";

    // For arrayEq.
    load("jstests/aggregation/extras/utils.js");

    var viewsDB = db.getSiblingDB("views_creation");
    viewsDB.dropDatabase();
    assert.eq(0, viewsDB.getCollectionNames().length);

    // Create a collection for test purposes.
    assert.commandWorked(viewsDB.runCommand( { create: "collection" } ));

    var pipe = [{ $match: {} }];

    // Create a "regular" view on a collection.
    assert.commandWorked(viewsDB.runCommand( { create: "view", view: "collection", pipeline: pipe } ));

    // Cannot create a view with an invalid aggregation pipeline.
//    assert.commandFailed(viewsDB.runCommand( { create: "badPipeline1", view: "collection", pipeline: { x: 1 } } ));
//    assert.commandFailed(viewsDB.runCommand( { create: "badPipeline2", view: "collection", pipeline: [ { $limit: 1 }, { $indexStats: {} } ] } ));

    // Views should be listed along with collections, searchable via listCollections, etc.
    assert.eq(2, viewsDB.getCollectionNames().length);
    var res = viewsDB.runCommand( { listCollections: 1, filter: { "options.view": { $exists: true } } } );
    assert.commandWorked(res);
    assert(arrayEq(res.cursor.firstBatch, [{ name: "view", options: { view: "collection", pipeline: pipe } }]));

    // Create a view on a non-existent collection.
    assert.commandWorked(viewsDB.runCommand( { create: "viewOnEmpty", view: "nonexistent", pipeline: pipe } ));

    // Create a view but don't specify a pipeline; this should default to something sane.
    // assert.commandWorked(viewsDB.runCommand( { create: "viewWithDefaultPipeline", view: "collection" } ));

    // Specifying a pipeline but no view namespace must fail.
    assert.commandFailed(viewsDB.runCommand( { create: "viewNoViewNamespace", pipeline: pipe } ));

    // Create a view on another view.
    assert.commandWorked(viewsDB.runCommand( { create: "viewOnView", view: "view", pipeline: pipe } ));

    // Creating a view that induces a cycle of views must fail.
    // assert.commandWorked(viewsDB.runCommand( { create: "viewCycle1", view: "viewCycle2", pipeline: pipe } ));
    // assert.commandFailed(viewsDB.runCommand( { create: "viewCycle2", view: "viewCycle1", pipeline: pipe } ));

    // Cannot create a view on reserved system collections (e.g. the oplog).
    // assert.commandFailed(viewsDB.runCommand( { create: "viewOnOplog", view: "local.oplog.rs", pipeline: pipe } ));

    // View names are constrained to the same limitations as collection names.
    assert.commandFailed(viewsDB.runCommand( { create: "", view: "emptyStringViewName", pipeline: pipe } ));
    assert.commandFailed(viewsDB.runCommand( { create: "system.local.new", view: "collection", pipeline: pipe } ));
    assert.commandFailed(viewsDB.runCommand( { create: "dollar$", view: "collection", pipeline: pipe } ));
}());
