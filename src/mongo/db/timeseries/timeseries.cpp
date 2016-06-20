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

#include "mongo/platform/basic.h"

#include "mongo/db/timeseries/timeseries.h"

namespace mongo {


TimeSeriesBatch::TimeSeriesBatch(const BSONObj& batchDoc) {
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

void TimeSeriesBatchManager::insert(const BSONObj& doc) {
	Date_t date = doc.getField("_id").Date();

    batchIdType batchId = _getBatchId(date);

    // Check if the batch exists. If it doesn't, make one. If it does, use the existing one.
    if (_loadedBatches.find(batchId) == _loadedBatches.end()) {
    	// Batch does not exist, create one.
    	TimeSeriesBatch batch(batchId);
    	_loadedBatches[batchId] = batch;
    }
    
    TimeSeriesBatch& batch = _loadedBatches[batchId];

    batch.insert(doc);
}

void TimeSeriesBatchManager::update(const BSONObj& doc) {
	Date_t date = doc.getField("_id").Date();

	batchIdType batchId = _getBatchId(date);

	// Assert that the batch exists.
	uassert(BATCH_NONEXISTENT, "Cannot update a document in a batch that does not exist",
		_loadedBatches.find(batchId) != _loadedBatches.end());
    
    TimeSeriesBatch& batch = _loadedBatches[batchId];

    batch.update(doc);
}

BSONObj TimeSeriesBatchManager::retrieve(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot retrieve from a batch that doesn't exist.",
    	_loadedBatches.find(batchId) != _loadedBatches.end());

    TimeSeriesBatch& batch = _loadedBatches[batchId];

    return batch.retrieve(time);
}

BSONObj TimeSeriesBatchManager::retrieveBatch(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot retrieve a batch that doesn't exist.",
    	_loadedBatches.find(batchId) != _loadedBatches.end());

    TimeSeriesBatch& batch = _loadedBatches[batchId];

    return batch.retrieveBatch();
}

void TimeSeriesBatchManager::remove(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot delete from a batch that doesn't exist.",
    	_loadedBatches.find(batchId) != _loadedBatches.end());

    TimeSeriesBatch& batch = _loadedBatches[batchId];

    return batch.remove(time);
}

void TimeSeriesBatchManager::removeBatch(const Date_t& time) {
	batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot delete a batch that doesn't exist.",
    	_loadedBatches.find(batchId) != _loadedBatches.end());

    // TODO: gracefully close the batch, i.e. freeing memory we used?
    _loadedBatches.erase(batchId);
}

batchIdType TimeSeriesBatchManager::_getBatchId(const Date_t& time) {
	long long millis = time.toMillisSinceEpoch();

	batchIdType id = millis / NUM_MILLIS_IN_BATCH;

	return id;
}
}
