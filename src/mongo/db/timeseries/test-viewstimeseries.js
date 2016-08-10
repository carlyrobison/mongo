// Testing whether a view can use a secondary index. The answer is no.

conn = new Mongo();
db = conn.getDB("viewstest");

// Drop the old database, this will also drop old views
db.dropDatabase();

// Insert new data
db.data.insert([
    {
      "_id": "15:58",
      "docs": [{"_id": 492, "val": 914}, {"_id": 89, "val": 503}, {"_id": 633, "val": 793}]
    },
    {
      "_id": "15:59",
      "docs": [
          {"_id": 832, "val": 1},
          {"_id": 332, "val": 511},
          {"_id": 654, "val": 244},
      ]
    },
    {
      "_id": "16:00",
      "docs": [
          {"_id": 412, "val": 980},
          {"_id": 415, "val": 883},
          {"_id": 269, "val": 18},
      ]
    }
]);

// Create the view
db.runCommand({"create": "docsview", "view": "data", "pipeline": [{"$unwind": "$docs"}]});

// Display the view so far
db.docsview.find().pretty();

// Find a bigger document
db.docsview.aggregate([{"$match": {"_id": "15:58"}}]).pretty();

// Find a certain document. Should not use an index scan at this point
db.docsview.explain().aggregate(
    [{"$match": {"docs._id": 412}}, {"$project": {"docs": 1, "_id": 0}}]);

// Create a secondary index.
db.data.createIndex({"docs._id": 1});

// Find a certain document. Should now use an index scan BUT DOESN'T
db.docsview.explain().aggregate(
    [{"$match": {"docs._id": 412}}, {"$project": {"docs": 1, "_id": 0}}]);

db.docsview.find({"docs._id": 412}).hint({"docs._id": 1});
