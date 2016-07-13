conn = new Mongo();
db = conn.getDB("timeseriesview"); // can now randomly generate this

// Drop the old database, this will also drop old views
db.dropDatabase();

// Insert new data
// db.data.insert([{}]);

// Create the view
// view can now have any name, except ones that <existing collection>_timeseries
db.runCommand({"create": "tsv", "timeseries": "true"});

// Insert some dates
db.tsv.insert({"_id": new Date(2118), "val": "eu8a4"});
db.tsv.insert({"_id": new Date(9655), "val": "xd2ii"});
db.tsv.insert({"_id": new Date(915), "val": "dznto"});
db.tsv.insert({"_id": new Date(5098), "val": "jyk1v"});
db.tsv.insert({"_id": new Date(6902), "val": "c0cx6"});
db.tsv.insert({"_id": new Date(2975), "val": "pis87"});
db.tsv.insert({"_id": new Date(2420), "val": "op4qy"});
db.tsv.insert({"_id": new Date(526), "val": "leky4"});

// Extract some dates
db.tsv.find({"_id": new Date(2118)});
db.tsv.find({"_id": new Date(9655)});
db.tsv.find({"_id": new Date(915)});
db.tsv.find({"_id": new Date(5098)});
db.tsv.find({"_id": new Date(6902)});
db.tsv.find({"_id": new Date(2975)});
db.tsv.find({"_id": new Date(2420)});
db.tsv.find({"_id": new Date(526)});

// Try to extract some that don't exist
db.tsv.find({"_id": new Date(6014)});

// Try to insert/extract some with invalid types
db.tsv.insert({"_id": 6058, "val": 6610});
db.tsv.find({"_id": 6058});

//console.log(db.tsv.find({}));
