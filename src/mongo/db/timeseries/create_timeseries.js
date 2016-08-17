/**
 * Verify that creating a bunch of timeseries collections works.
 */

(function() {
    "use strict";

    const dbName = "timeseries";
    const nDocs = 10000;

    function insertDocs(db, collName) {
    	// Insert a bunch of documents.
    	const coll = db.getCollection(collName);
        for (let i = 0; i < nDocs; i+= 10) {
            assert.writeOK(coll.insert({"_id": new Date(i), "val": i}));
        }
    }

    function printColl(db, collName) {
    	print(db.getCollection(collName).find());
    }

    function spec(db, collName, compressed, cacheSize, millis, timeField, backingCollName) {
    	return {
    		"create": collName,
    		"timeseries": {
    			"compressed": compressed,
    			"cache_size": cacheSize,
    			"millis_in_batch": millis,
    			"time_field": timeField,
    			"backing_coll_name": backingCollName,
    		},
    	}
    }

    function doExecutionTest(conn) {
    	// Setup
    	const db = conn.getDB(dbName);
    	db.dropDatabase();

    	// Make a default TS
    	assert.commandWorked(db.runCommand({"create": "coll1", "timeseries"}));
    	insertDocs(db, "coll1");
    	printColl(db, "coll1");
    	printColl(db, "coll1_timeseries");

    	
    }

    // Test against the standalone started by resmoke.py.
    let conn = db.getMongo();
    doExecutionTest(conn);
    print("Success! Standalone execution use case test for $addFields passed.");

    // Test against a sharded cluster.
    let st = new ShardingTest({shards: 2});
    doExecutionTest(st.s0);
    st.stop();
    print("Success! Sharding use case test for $addFields passed.");

}());
