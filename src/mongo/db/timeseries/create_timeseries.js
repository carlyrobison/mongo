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
        for (let i = 0; i < nDocs; i+= 100) {
            assert.writeOK(coll.insert({"_id": new Date(i), "val": i}));
        }
    }

    function insertFewerDocs(db, collName) {
        // Insert a bunch of documents.
        const coll = db.getCollection(collName);
        for (let i = 0; i < 10000; i+= 100) {
            let doc = {"_id": new Date(i), "val": i};
            assert.writeOK(coll.insert(doc));
        }
    }


    function insertDocsDateField(db, collName) {
    	// Insert a bunch of documents.
    	const coll = db.getCollection(collName);
        for (let i = 0; i < nDocs; i+= 100) {
            assert.writeOK(coll.insert({"date": new Date(i), "val": i}));
        }
    }

    function spec(db, collName, compressed, cacheSize, millis, timeField, backingCollName) {
    	return {
    		"create": collName,
    		"timeseries": {
    			"compressed": compressed,
    			"cache_size": cacheSize,
    			"millis_in_batch": millis,
    			"time_field": timeField,
    			"backing_name": backingCollName,
    		},
    	}
    }

    function doExecutionTest(conn) {
    	// Setup
    	const db = conn.getDB(dbName);
    	db.dropDatabase();

    	// Make a default TS
    	assert.commandWorked(db.runCommand({"create": "coll1", "timeseries": {}}));
    	insertDocs(db, "coll1");
        print("Successful inserts into default timeseries collection");

		// Make a compressed collection
    	assert.commandWorked(db.runCommand(spec(db, "coll2", true, 4, 1000, "_id", "coll2_timeseries")));
    	insertFewerDocs(db, "coll2");
        print("Successful inserts into compressed timeseries collection");

    	// Make a custom collection
    	assert.commandWorked(db.runCommand(spec(db, "coll3", false, 20, 10, "date", "test3")));
    	insertDocsDateField(db, "coll3");
        print("Successful inserts into custom named collection");
    }

    // Test against the standalone started by resmoke.py.
    let conn = db.getMongo();
    doExecutionTest(conn);
    print("Success! Standalone execution use case test for creation of timeseries passed.");

}());
