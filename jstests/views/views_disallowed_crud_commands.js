// Tests that inserts, updates and deletes fail on read-only views.

(function() {
    "use strict";

    var viewsDB = db.getSiblingDB("views_writes");
    assert.commandWorked(viewsDB.dropDatabase());

    // Create a view on a non-existent collection.
    assert.commandWorked(
        viewsDB.runCommand({create: "view", viewOn: "nonexistent", pipeline: [{$match: {}}]}));

    // Cannot do read-only operations on a view.
    assert.writeError(viewsDB.view.insert({a: 1}));
    assert.writeError(viewsDB.view.update({a: 1}, {$set: {a: 2}}));
    assert.writeError(viewsDB.view.remove({a: 1}));
    assert.commandFailed(viewsDB.runCommand({findAndModify: "view", query: {a: 1}, remove: true}));
    assert.commandFailed(
        viewsDB.runCommand({findAndModify: "view", query: {a: 1}, update: {$set: {a: 2}}}));

    // MapReduce is not supported.
    assert.commandFailed(viewsDB.runCommand(
        {mapReduce: "view", map: function() {}, reduce: function(key, vals) {}, out: "out"}));

    // Geo queries are unsupported.
    assert.commandFailed(viewsDB.runCommand({
        geoSearch: "view",
        search: {},
        near: [-50, 37],
    }));
    assert.commandFailed(viewsDB.runCommand(
        {geoNear: "view", near: {type: "Point", coordinates: [-50, 37]}, spherical: true}));
}());
