conn = new Mongo();
db = conn.getDB("timeseriesview");

// Drop the old database, this will also drop old views
db.dropDatabase();

// Insert new data
db.data.insert([{}]);

// Create the view
db.runCommand({"create": "timeseriesview", "view": "data", "pipeline": [{"$unwind": "$docs"}]});

// Insert some dates
db.timeseriesview.insert({"_id": new Date(2118), "val": "eu8a4"});
db.timeseriesview.insert({"_id": new Date(9655), "val": "xd2ii"});
db.timeseriesview.insert({"_id": new Date(915), "val": "dznto"});
db.timeseriesview.insert({"_id": new Date(5098), "val": "jyk1v"});
db.timeseriesview.insert({"_id": new Date(6902), "val": "c0cx6"});
db.timeseriesview.insert({"_id": new Date(2975), "val": "pis87"});
db.timeseriesview.insert({"_id": new Date(2420), "val": "op4qy"});
db.timeseriesview.insert({"_id": new Date(526), "val": "leky4"});

// Extract some dates
db.timeseriesview.find({"_id": new Date(2118)});
db.timeseriesview.find({"_id": new Date(9655)});
db.timeseriesview.find({"_id": new Date(915)});
db.timeseriesview.find({"_id": new Date(5098)});
db.timeseriesview.find({"_id": new Date(6902)});
db.timeseriesview.find({"_id": new Date(2975)});
db.timeseriesview.find({"_id": new Date(2420)});
db.timeseriesview.find({"_id": new Date(526)});

// Try to extract some that don't exist
db.timeseriesview.find({"_id": new Date(6014)});

// Try to insert/extract some with invalid types
db.timeseriesview.insert({"_id": 6058, "val": 6610});
db.timeseriesview.find({"_id": 6058});