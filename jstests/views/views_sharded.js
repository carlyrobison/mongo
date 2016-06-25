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

    for (var i = 0; i < 20; ++i) {
        coll.insert({a: i % 2});
    }

    assert.commandWorked(db.createView("even_numbers", coll.getName(), [{$match: {a: 0}}]));
    var view = db.getCollection("even_numbers");

    assert.eq(10, view.find({}).itcount());
    assert.eq(10, view.aggregate([{$match: {a: {$gte: 0}}}]).itcount());
    assert.eq([0], view.distinct("a", {a: {$gte: 0}}));

    // TODO: Fix count
    // assert.eq(10, view.count({a: {$gte: 0}}));

    // TODO: Add explain tests
})();
