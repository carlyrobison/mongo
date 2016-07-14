// Insert many documents into a 

load("utils.js");


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

	function runInsertTest(testcase, nDocs, docGenerator, collName, bulk, is_ts) {
		if (is_ts) {
			assert.commandWorked(db.runCommand({create: collName, timeseries: true}));
		} else {
			assert.commandWorked(db.runCommand({create: collName}));
		}

		if (bulk) {
			var bulk = db[collName].initializeUnorderedBulkOp();
		}

		var testResults = {};
		testResults['name'] = testcase;
		print("@@START@@");
		testResults['start'] = new Date();

		for (var i = 0; i < nDocs; i+= SCALAR) {
			var doc = docGenerator(i);
			if (bulk) {
				bulk.insert(doc);
			} else {
				db[collName].insert(doc);
			}
		}

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

	var res = [];

	db.dropDatabase();

	res.push(runInsertTest("regular-sequential", NUMDOCS, _genDoc, "data2", false, false));
	res.push(runInsertTest("ts-sequential", NUMDOCS, _genDoc, "tsv2", false, true));

	res.push(runInsertTest("regular-random", NUMDOCS, _genRandomDoc, "data", false, false));
	res.push(runInsertTest("ts-random", NUMDOCS, _genRandomDoc, "tsv", false, true));

	//db.runCommand({"create": "data2"});
	//res.push(runInsertTest("regular-random-bulk", NUMDOCS, _genRandomDoc, "data2", true));
	//res.push(runInsertTest("regular-sequential-bulk", NUMDOCS, _genDoc, "data2", true));

	//db.runCommand({"create": "tsv2", "timeseries": "true"});
	//res.push(runInsertTest("ts-random-bulk", NUMDOCS, _genRandomDoc, "tsv", true));
	//res.push(runInsertTest("ts-sequential-bulk", NUMDOCS, _genDoc, "tsv", true));

	for (var i = 0; i < 4; i++){
		print(JSON.stringify(res[i]));
	}
	//print(JSON.stringify(res));

	print("Finished all tests\n");

}());