// Insert many documents into a 

load("src/mongo/db/timeseries/utils.js");

(function() {

	Random.setRandomSeed();

	var SCALAR = 50;
	var NUMDOCS = 100000;

	function _genRandomDoc(i) {
		return {
			_id: new Date(Random.srand()),
			value: Random.srand(),
			//str: generateRandomString(256),
		};
	}

	function _genDoc(i) {
		return {
			_id: new Date(i),
			value: Random.srand(),
			//str: generateRandomString(256),
		}
	}

	function _allDocs(n, scale) {
		var list = [];
		for (var i = 0; i < n; i+= scale) {
			list.push(_genDoc(i));
		}
		return list;
	}

	function runRSTest(testcase, nDocs, docGenerator, collName, bulk, is_ts) {
		var rst = new ReplSetTest({name: 'testSet', nodes: 3, nodeOptions: {}});
		rst.startSet({});
		rst.initiate();
		var rstConn = new Mongo(rst.getURL());

		var db = rstConn.getDB('test');

		if (is_ts) {
			assert.commandWorked(db.runCommand({create: collName, timeseries: true}));
		} else {
			assert.commandWorked(db.runCommand({create: collName}));
		}

		var coll = db.getCollection(collName);

		if (bulk) {
			var bulk = db.getCollection(collName).initializeUnorderedBulkOp();
		}

		var testResults = {};
		testResults['name'] = testcase;

		// Make sure replication is up to date.
		rst.awaitReplication();
		rst.awaitSecondaries(); // not sure why I'm including this
		print("-- up to date --");

		for (var i = 0; i < nDocs; i+= SCALAR) {
			var doc = docGenerator(i);
			if (bulk) {
				bulk.insert(doc);
			} else {
				db[collName].insert(doc);
			}
		}

		// Insert last document
		print("@@START@@");
		testResults['start-repl'] = new Date();

		var doc = docGenerator(nDocs);
		if (bulk) {
			bulk.insert(doc);
		} else {
			db[collName].insert(doc);
		}

		if (bulk) {
			bulk.execute();
		}

		// Wait for the last insert/op to propagate
		rst.awaitReplication();

		testResults['end'] = new Date();
		print("@@END@@");
		testResults['duration'] = testResults['end'] - testResults['start'];

		print("@@@RESULTS_START@@@");
    	print(JSON.stringify(testResults));
    	print("@@@RESULTS_END@@@");

    	db.printReplicationInfo();
		db.printSlaveReplicationInfo();
		rstConn.stopSet();

		return {
			testName: testResults['name'],
			duration: testResults['duration'],
		};
	}

	function runInsertTestFromList(testcase, nDocs, docGenerator, collName, bulk, is_ts) {
		if (is_ts) {
			assert.commandWorked(db.runCommand({create: collName, timeseries: true}));
		} else {
			assert.commandWorked(db.runCommand({create: collName}));
		}

		if (bulk) {
			var bulk = db[collName].initializeUnorderedBulkOp();
		}

		var list = docGenerator(nDocs, SCALAR);
		print("-- done generating docs --");

		var testResults = {};
		testResults['name'] = testcase;
		print("@@START@@");
		testResults['start'] = new Date();

		list.forEach( function(doc){
			if (bulk) {
				bulk.insert(doc);
			} else {
				db[collName].insert(doc);
			}
		} );

		if (bulk) {
			bulk.execute();
		}

		testResults['end'] = new Date();
		print("@@END@@");
		testResults['duration'] = testResults['end'] - testResults['start'];

		print("@@@RESULTS_START@@@");
    	print(JSON.stringify(testResults));
    	print("@@@RESULTS_END@@@");

		return {
			testName: testResults['name'],
			duration: testResults['duration'],
		};
	}

	function runInsertTest(testcase, nDocs, docGenerator, collName, is_ts) {
		if (is_ts) {
			assert.commandWorked(db.runCommand({create: collName, timeseries: true}));
		} else {
			assert.commandWorked(db.runCommand({create: collName}));
		}

		var coll = db[collName];

		var testResults = {};
		testResults['name'] = testcase;


		print("@@START@@");
		testResults['start'] = new Date();

		for (var i = 0; i < nDocs; i+= SCALAR) {
			var doc = docGenerator(i);
			coll.insert(doc);
		}

		testResults['end'] = new Date();

		print("@@END@@");
		testResults['duration'] = testResults['end'] - testResults['start'];

		print("@@@RESULTS_START@@@");
    	print(JSON.stringify(testResults));
    	print("@@@RESULTS_END@@@");

		return {
			testName: testResults['name'],
			duration: testResults['duration'],
		};
	}

	var res = [];

	db.dropDatabase();
	//db.setLogLevel(2);

	//res.push(runInsertTest("regular-sequential", NUMDOCS, _genDoc, "data2", false, false));
	//res.push(runInsertTest("ts-sequential", NUMDOCS, _genDoc, "tsv2", false, true));

	res.push(runInsertTestFromList("regular-sequential-pregenerated", NUMDOCS, _allDocs, "data3", false, false));
	res.push(runInsertTestFromList("ts-sequential-pregenerated", NUMDOCS, _allDocs, "tsv3", false, true));

	res.push(runInsertTest("regular-sequential-replication", NUMDOCS, _genDoc, "data4", false, false));
	res.push(runInsertTest("ts-sequential-replication", NUMDOCS, _genDoc, "tsv4", false, true));

	//res.push(runInsertTest("regular-random", NUMDOCS, _genRandomDoc, "data", false, false));
	//res.push(runInsertTest("ts-random", NUMDOCS, _genRandomDoc, "tsv", false, true));

	for (var i = 0; i < 6; i++){
		print(JSON.stringify(res[i]));
	}
	//print(JSON.stringify(res));

	print("Finished all tests\n");

}());