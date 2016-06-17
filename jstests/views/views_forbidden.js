// Test general commands not supported on views.

(function() {
    "use strict";

    var viewsDB = db.getSiblingDB("views_forbidden");
    assert.commandWorked(viewsDB.dropDatabase());

    // Create a view on a non-existent collection.
    assert.commandWorked(
        viewsDB.runCommand({create: "view", viewOn: "nonexistent", pipeline: [{$match: {}}]}));

    assert.commandFailed(viewsDB.runCommand({createIndexes: "view", indexes: [{a: 1}]}));
    assert.commandFailed(viewsDB.runCommand({listIndexes: "view"}));
    assert.commandFailed(viewsDB.runCommand({touch: "view", data: true}));
    assert.commandFailed(viewsDB.runCommand({touch: "view", index: true}));

    // TODO: These fail only because of "ns doesn't exist" errors.
    assert.commandFailed(viewsDB.runCommand({dropIndexes: "view"}));
    assert.commandFailed(viewsDB.runCommand({validate: "view"}));
    assert.commandFailed(viewsDB.runCommand({compact: "view"}));
    assert.commandFailed(viewsDB.runCommand({reIndex: "view"}));
}());
