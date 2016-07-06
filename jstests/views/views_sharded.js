// Confirms proper behavior when reading from a view that is based on a sharded collection.

(function() {
    "use strict";

    var st = new ShardingTest(
        {name: "views_sharded", shards: 2, other: {shardOptions: {setParameter: "enableViews=1"}}});

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

    //
    // find
    //
    assert.eq(3, view.find({a: {$lte: 7}}).itcount());

    var result = db.runCommand({explain: {find: "view", filter: {a: {$lte: 7}}}});
    assert.commandWorked(result);
    assert(result.hasOwnProperty("shards"), tojson(result));

    //
    // aggregate
    //
    assert.eq(3, view.aggregate([{$match: {a: {$lte: 7}}}]).itcount());

    var result =
        db.runCommand({aggregate: "view", pipeline: [{$match: {a: {$lte: 7}}}], explain: true});
    assert.commandWorked(result);
    assert(result.hasOwnProperty("shards"), tojson(result));

    //
    // count
    //
    assert.eq(3, view.count({a: {$lte: 7}}));

    result = db.runCommand({explain: {count: "view", query: {a: {$lte: 7}}}});
    assert.commandWorked(result);
    assert(result.hasOwnProperty("shards"), tojson(result));

    //
    // distinct
    //
    result = db.runCommand({distinct: "view", key: "a", query: {a: {$lte: 7}}});
    assert.commandWorked(result);
    assert.eq([5, 6, 7], result.result[0].a.sort());

    result = db.runCommand({explain: {distinct: "view", key: "a", query: {a: {$lte: 7}}}});
    assert.commandWorked(result);
    assert(result.hasOwnProperty("shards"), tojson(result));
})();
