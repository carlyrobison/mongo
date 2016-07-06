// timeseries.cpp

/**
*    Copyright (C) 2016 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"
#include "mongo/db/timeseries/timeseries.h"
#include "mongo/util/log.h"

namespace mongo {

TimeSeriesBatch::TimeSeriesBatch(const BSONObj& batchDocument) {
    BSONObj batchDoc = batchDocument.getOwned();
    // Create a map entry for each doc in the docs subarray.
    for (const BSONElement& elem : batchDoc["_docs"].Array()) {
        BSONObj obj = elem.Obj();
        BSONElement docId = obj.getField("_id");
        // TODO: check that docId is really a date.
        Date_t date = docId.Date();
        _docs[date] = elem.Obj();
    }

    // Create the batch Id from the first batch document
    _batchId = batchDoc["_id"].numberLong();
}

TimeSeriesBatch::TimeSeriesBatch(batchIdType batchId) {
    _batchId = batchId;
}

void TimeSeriesBatch::insert(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // internally copies if necessary

    /* Adds this document to the map of documents, based on date. */
    Date_t date = newDoc.getField("_id").Date();
    uassert(ErrorCodes::DuplicateKeyValue, "Cannot insert a document that already exists.",
		_docs.find(date) == _docs.end());
    _docs[date] = newDoc;
}

void TimeSeriesBatch::update(const BSONObj& doc) {
	BSONObj newDoc = doc.getOwned(); // internally copies if necessary

	/* Confirms that the timestamp already exists */
	Date_t date = newDoc.getField("_id").Date();
	massert(ErrorCodes::NoSuchKey, "Cannot update a document that doesn't exist.",
		_docs.find(date) != _docs.end());
	_docs[date] = newDoc;
}

BSONObj TimeSeriesBatch::retrieveBatch() {
    // create the BSON array
    BSONArrayBuilder arrayBuilder;

    std::map<Date_t, BSONObj>::iterator i;

    // Add values (documents) to the bson array
    for (i = _docs.begin(); i != _docs.end(); ++i) {
        arrayBuilder.append(i->second);
    }

    // build BSON object iteself
    BSONObjBuilder builder;
    builder.append("_id", _batchId);
    builder.append("_docs", arrayBuilder.arr());

    return builder.obj();
}

BSONObj TimeSeriesBatch::retrieve(const Date_t& time) {
	uassert(ErrorCodes::NoSuchKey, "Cannot retrieve a document that doesn't exist.",
		_docs.find(time) != _docs.end());

    return _docs[time];
}

void TimeSeriesBatch::remove(const Date_t& time) {
	uassert(ErrorCodes::NoSuchKey, "Cannot delete a document that doesn't exist.",
		_docs.find(time) != _docs.end());

	_docs.erase(time);
}

batchIdType TimeSeriesBatch::_thisBatchId() {
    return _batchId;
}

// bool TimeSeriesBatch::save(StringData db, StringData coll) {
//     // Step 1: Construct the txn
//     OperationContext txn = getGlobalServiceContext()->makeOperationContext(&cc());
//     // Step 2: Construct the command and its arguments
//     Command *save = Command::findCommand("update");
//     std::string errmsg;
//     BSONObjBuilder ReplyBob;
//     BSONObj upsertCmd = _constructUpsertCommand(coll);

//     log() << txn;
//     //bool result = save->run(txn, db, upsertCmd, 0, errmsg, inPlaceReplyBob);

//     // Step 3: Construct the response
//     //Command::appendCommandStatus(ReplyBob, result, errmsg);
//     ReplyBob.doneFast();

//     //BSONObjBuilder metadataBob;
//     //appendOpTimeMetadata(txn, request, &metadataBob);
//     //replyBuilder->setMetadata(metadataBob.done());

//     //return result;
//     return true;
// }

// BSONObj TimeSeriesBatch::_constructUpsertCommand(StringData coll) {
//     BSONObjBuilder cmdBuilder;
//     cmdBuilder.append("update", coll);

//     BSONObjBuilder updateObj;

//     // Create query
//     BSONObjBuilder query;
//     query.append("_id", _batchId);
//     updateObj.append("q", query.obj());
//     // Add the other parts
//     updateObj.append("u", retrieveBatch());
//     updateObj.append("multi", false);
//     updateObj.append("upsert", true);

//     BSONArrayBuilder updateArray;
//     updateArray.append(updateObj.obj());
//     cmdBuilder.append("updates", updateArray.arr());
//     cmdBuilder.append("ordered", true);

//     BSONObj newCmd = cmdBuilder.obj();
//     log() << "newcmd: " << newCmd;

//     return newCmd;
// }


TimeSeriesCache::TimeSeriesCache(StringData db, StringData coll) {
    _db = db;
    _coll = coll;
}

void TimeSeriesCache::insert(const BSONObj& doc) {
	Date_t date = doc.getField("_id").Date();

    batchIdType batchId = _getBatchId(date);

    // Check if the batch exists. If it doesn't, make one. If it does, use the existing one.
    if (_cache.find(batchId) == _cache.end()) {
    	// Batch does not exist, create one.
    	TimeSeriesBatch batch(batchId);
    	_cache[batchId] = batch;
    }
    
    TimeSeriesBatch& batch = _cache[batchId];

    batch.insert(doc);
}

void TimeSeriesCache::loadBatch(const BSONObj& doc) {
    batchIdType batchId = doc["_id"].numberLong();

    // Check that the batch id doesn't already exist in memory.
    uassert(40155, "Cannot load a batch that already exists.",
        _cache.find(batchId) == _cache.end());

    _cache[batchId] = TimeSeriesBatch(doc);
}

void TimeSeriesCache::update(const BSONObj& doc) {
	Date_t date = doc.getField("_id").Date();

	batchIdType batchId = _getBatchId(date);

	// Assert that the batch exists.
	uassert(BATCH_NONEXISTENT, "Cannot update a document in a batch that does not exist",
		_cache.find(batchId) != _cache.end());
    
    TimeSeriesBatch& batch = _cache[batchId];

    batch.update(doc);
}

BSONObj TimeSeriesCache::retrieve(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot retrieve from a batch that doesn't exist.",
    	_cache.find(batchId) != _cache.end());

    TimeSeriesBatch& batch = _cache[batchId];

    return batch.retrieve(time);
}

BSONObj TimeSeriesCache::retrieveBatch(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot retrieve a batch that doesn't exist.",
    	_cache.find(batchId) != _cache.end());

    TimeSeriesBatch& batch = _cache[batchId];

    return batch.retrieveBatch();
}

void TimeSeriesCache::remove(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot delete from a batch that doesn't exist.",
    	_cache.find(batchId) != _cache.end());

    TimeSeriesBatch& batch = _cache[batchId];

    return batch.remove(time);
}

void TimeSeriesCache::removeBatch(const Date_t& time) {
	batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot delete a batch that doesn't exist.",
    	_cache.find(batchId) != _cache.end());

    // TODO: gracefully close the batch, i.e. freeing memory we used?
    _cache.erase(batchId);
}

bool TimeSeriesCache::saveBatch(const Date_t& time){
    batchIdType batchId = _getBatchId(time);

    // Assert that the batch exists
    uassert(BATCH_NONEXISTENT, "Cannot save a batch that doesn't exist.",
        _cache.find(batchId) != _cache.end());

    //TimeSeriesBatch& batch = _cache[batchId];

    // Assert that the batch manager has a collection to save to
    //massert(0000, (_db != NULL) && (_coll != NULL));

    //return batch.save(_db, _coll);
    return false;
}

batchIdType TimeSeriesCache::evictBatch() {
    // Step 1: Choose the least recently used batch to evict.
    massert(000, "Empty cache", !_lruQueue.empty());

    // Step 2: save the batch to the underlying collection

    // Return the batchIdType
    return 0;
}

batchIdType TimeSeriesCache::_getBatchId(const Date_t& time) {
	long long millis = time.toMillisSinceEpoch();

	batchIdType id = millis / NUM_MILLIS_IN_BATCH;

	return id;
}

}
