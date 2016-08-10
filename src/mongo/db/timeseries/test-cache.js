// This test assesses the cache eviction policy.

conn = new Mongo();
db = conn.getDB("timeseriesview");

// Drop the old database, this will also drop old views
db.dropDatabase();

// Create the timeseries collection
// New TS collection can now have any name, except ones that are <existing collection>_timeseries
db.runCommand({"create": "tsv", "timeseries": "true"});

// Insert some dates
db.tsv.insert({"_id": new Date(2975), "val": "pis87"});
db.tsv.insert({"_id": new Date(2420), "val": "op4qy"});
db.tsv.insert({"_id": new Date(526), "val": "leky4"});
db.tsv.insert({"_id": new Date(2118), "val": "eu8a4"});
db.tsv.insert({"_id": new Date(9655), "val": "xd2ii"});
db.tsv.insert({"_id": new Date(915), "val": "dznto"});
db.tsv.insert({"_id": new Date(5098), "val": "jyk1v"});
db.tsv.insert({"_id": new Date(6902), "val": "c0cx6"});

// Extract some dates
db.tsv.find({"_id": new Date(2118)});
db.tsv.find({"_id": new Date(9655)});
db.tsv.find({"_id": new Date(915)});
db.tsv.find({"_id": new Date(2975)});
db.tsv.find({"_id": new Date(2420)});
db.tsv.find({"_id": new Date(526)});

// Should not have been flushed yet.
db.tsv.find({"_id": new Date(5098)});
db.tsv.find({"_id": new Date(6902)});