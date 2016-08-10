// restart mongod with views enabled
./mongod --setParameter enableViews=1
./mongo

// do before demo
use ts
db.dropDatabase()
db.runCommand({"create": "test", "timeseries": true})
for (let i = 0; i < 1000; ++i) db.test.insert({"_id": new Date(i*100), "val": i*100})

// demo for schema
db.test.find()

db.test_timeseries.findOne()

// demo inserting
db.test.insert({"_id": new Date(8102016), "val": "hello this is a demo"});
db.test.insert({"_id": new Date(8202016), "val": "this is a demo"});
db.test.insert({"_id": new Date(8302016), "val": "is a demo"});

// we can find just as we inserted
db.test.find({"_id": new Date(8102016)})

db.test_timeseries.findOne({"_id": 8102})


// Now use compression
db.runCommand({"create": "example", "timeseries": true, "compressed": true})

db.example.insert({"_id": new Date(2665), "val": 2665});
db.example.insert({"_id": new Date(2664), "val": 2664});
db.example.insert({"_id": new Date(2663), "val": 2663});
db.example.insert({"_id": new Date(3663), "val": 3663});
db.example.insert({"_id": new Date(1663), "val": 1663});

// see what we inserted
db.example.find()

db.example_timeseries.find()

// insert some more
db.example.insert({"_id": new Date(2662), "val": 2662});
db.example.insert({"_id": new Date(1662), "val": 1662});
db.example.insert({"_id": new Date(3662), "val": 3662});

// can see that it *adds to* the backing collection
db.example.find()

db.example_timeseries.find()
