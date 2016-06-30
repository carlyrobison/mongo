// Confirms proper behavior when reading from a view that is based on a sharded collection.

(function() {
    "use strict";

    var st = new ShardingTest({name: "views_sharded", shards: 2});

    var mongos = st.s;
    var config = mongos.getDB("config");
    var db = mongos.getDB(jsTestName());
    db.dropDatabase();

    var coll = db.getCollection("coll");

    assert.commandWorked(config.adminCommand({enableSharding: db.getName()}));
    st.ensurePrimaryShard(db.getName(), "shard0000");
    assert.commandWorked(config.adminCommand({shardCollection: coll.getFullName(), key: {a: 1}}));

    assert.commandWorked(mongos.adminCommand({split: coll.getFullName(), middle: {a: 50}}));
    assert.commandWorked(
        db.adminCommand({moveChunk: coll.getFullName(), find: {a: 25}, to: "shard0001"}));

    for (var i = 0; i < 10; ++i) {
        coll.insert({a: i});
    }

    assert.commandWorked(db.createView("view", coll.getName(), [{$match: {a: {$gte: 5}}}]));
    var view = db.getCollection("view");

    assert.eq(3, view.find({a: {$lte: 7}}).itcount());
    assert.eq(3, view.aggregate([{$match: {a: {$lte: 7}}}]).itcount());
    assert.eq([5, 6, 7], view.distinct("a", {a: {$lte: 7}}).sort());
    assert.eq(3, view.count({a: {$lte: 7}}));

    // TODO: Add explain tests
})();
