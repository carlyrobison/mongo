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

#include "mongo/util/assert_util.h"
#include "mongo/bson/bsonobjbuilder.h"

#include "mongo/db/ops/update_request.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/ops/update_result.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/ftdc/config.h"
#include "mongo/db/ftdc/decompressor.h"


#include "mongo/db/dbhelpers.h"

namespace mongo {

TimeSeriesBatch::TimeSeriesBatch(const BSONObj& batchDocument) {
    BSONObj batchDoc = batchDocument.getOwned();
    // log() << "Creating batch from input doc: " << batchDoc;
    // Create a map entry for each doc in the docs subarray.
    for (const BSONElement& elem : batchDoc["_docs"].Array()) {
        BSONObj obj = elem.Obj();
        BSONElement docId = obj.getField("_id");
        // TODO: check that docId is really a date.
        Date_t date = docId.Date();
        _docs.emplace(date, obj.getOwned());
    }

    // Create the batch Id from the batch document
    _batchId = batchDoc["_id"].numberLong();
}

TimeSeriesBatch::TimeSeriesBatch(batchIdType batchId) {
    _batchId = batchId;
}

std::string TimeSeriesBatch::toString(bool includeTime) const {
    StringBuilder s;
    s << ("Batch number: ");
    s << (_batchId);
    for (auto i = _docs.cbegin(); i != _docs.cend(); ++i) {
        s << (",\t");
        if (includeTime) {
                    s << (i->first.toString());
        s << (", ");
        }
        s << (i->second.toString());
    }
    return s.str();
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
    log() << "Retrieving object of batch: " << toString();

    // create the BSON array
    BSONArrayBuilder arrayBuilder;

    // Add values (documents) to the bson array
    for (auto i = _docs.cbegin(); i != _docs.cend(); ++i) {
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

bool TimeSeriesBatch::save(OperationContext* txn, const NamespaceString& nss) {
    //Create the update request
    UpdateRequest request(nss);
    request.setQuery(BSON("_id" << _batchId));
    request.setUpdates(retrieveBatch());
    request.setUpsert(true);

    // get the database NEEDS SERVERONLY
    AutoGetOrCreateDb autoDb(txn, nss.db(), MODE_IX);

    // update!
    UpdateResult result = ::mongo::update(txn, autoDb.getDb(), request);

    // See what we got
    //log() << "result of save: " << result;
    //massert(result.numMatched == 1);

    return true;
}

TimeSeriesCompressor::TimeSeriesCompressor() {
    FTDCConfig config;
    _compressor = new FTDCCompressor(&config);
}

TimeSeriesCompressor::TimeSeriesCompressor(const BSONObj& batchDocument) {
    BSONObj batchDoc = batchDocument.getOwned();

    // it will have a format of
    // _id: (batch id)
    // _docs: (compressed data buffer as bson)

    // 1. Decompress the batch.
    BSONElement compressedData = batchDocument.getField("_docs");
    log() << "compressed data: " << compressedData;
    int len;
    const char* bData = compressedData.binData(len);
    log() << "bin data: " << bData;

    ConstDataRange buf(bData, len);
    auto swBuf = FTDCDecompressor().uncompress(buf);
    uassert(40201, swBuf.getStatus().reason(), swBuf.isOK());
    auto _docsToReturn = swBuf.getValue();

    // Create the batch Id from the batch document
    _batchId = batchDoc["_id"].numberLong();

    FTDCConfig config;
    _compressor = new FTDCCompressor(&config);

    // Put 'em all back in
    for (auto&& doc : _docsToReturn) {
        _compressor->addSample(doc, doc.getField("_id").Date());
    }
}

TimeSeriesCompressor::TimeSeriesCompressor(batchIdType batchId) {
    _batchId = batchId;

    FTDCConfig config;
    _compressor = new FTDCCompressor(&config);
}

std::string TimeSeriesCompressor::toString(bool includeTime) const {
    StringBuilder s;
    s << ("Batch number: ");
    s << (_batchId);
    return s.str();
}

void TimeSeriesCompressor::insert(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // internally copies if necessary
    Date_t date = newDoc.getField("_id").Date();

    /* Adds this document to the map of documents, based on date. */
    auto st = _compressor->addSample(doc, date);
    uassert(40195, "Insert was not OK", st.isOK());
    // uassertStatusOK
    if (std::get<1>(st.getValue().get()) == FTDCCompressor::CompressorState::kCompressorFull) {
        log() << "WE NEED TO SAVE!!!";
    }
}

BSONObj TimeSeriesCompressor::retrieveBatch() {
    // build BSON object iteself
    log() << "retrieving batch: " << toString();
    BSONObjBuilder builder;
    builder.append("_id", _batchId);
    log() << "Id appended";
    StatusWith<std::tuple<ConstDataRange, Date_t>> swBuf = _compressor->getCompressedSamples();
    log() << "got compressed samples? " << swBuf.isOK();
    uassert(40196, "Retrieval was not OK", swBuf.isOK());
    ConstDataRange dataRange = std::get<0>(swBuf.getValue());
    //log() << "got const data range shouldn't be null " << (dataRange.data() != nullptr);
    builder.appendBinData("_docs", dataRange.length(), BinDataType::BinDataGeneral, dataRange.data());
    log() << builder.asTempObj();
    return builder.obj();
}

batchIdType TimeSeriesCompressor::_thisBatchId() {
    return _batchId;
}

bool TimeSeriesCompressor::save(OperationContext* txn, const NamespaceString& nss) {
    //Create the update request
    UpdateRequest request(nss);
    request.setQuery(BSON("_id" << _batchId));
    request.setUpdates(retrieveBatch());
    request.setUpsert(true);

    // get the database NEEDS SERVERONLY
    AutoGetOrCreateDb autoDb(txn, nss.db(), MODE_IX);

    // update!
    UpdateResult result = ::mongo::update(txn, autoDb.getDb(), request);

    // See what we got
    //log() << "result of save: " << result;
    //massert(result.numMatched == 1);

    return true;
}

TimeSeriesCache::TimeSeriesCache(const NamespaceString& nss) {
    log() << "NamespaceString: " << nss.ns();
    _nss = nss;
}

std::string TimeSeriesCache::toString(bool printBatches) const {
    StringBuilder s;
    s << ("Namespace: ");
    s << (_nss.ns());
    for (auto i = _cache.cbegin(); i != _cache.cend(); ++i) {
        s << (",\t");
        s << (i->first);
        if (printBatches) {
            s << ("\t");
            s << (i->second.toString());
        }
    }
    return s.str();
}

void TimeSeriesCache::insert(OperationContext* txn, const BSONObj& doc, bool persistent) {
    log() << "Inserting into cache";
	Date_t date = doc.getField("_id").Date();
    batchIdType batchId = _getBatchId(date);

    // Check if the batch exists in the cache. If it doesn't, try to find it. If it does, use the existing one.
    if (_cache.find(batchId) == _cache.end()) {
        log() << "Batch not in the cache";
        // Batch does not exist in the cache. Try to load it from the underlying collection.
        auto loadedBatch = findBatch(txn, batchId);

        log() << "Loaded the batch: " << loadedBatch;

        if (!loadedBatch.isEmpty()) {
            log() << "Nonempty batch";
            TimeSeriesCompressor newBatch(loadedBatch.getOwned());
            //TimeSeriesCompressor newBatch(loadedBatch);

            addToCache(txn, newBatch);
            massert(40194, "Loaded batch must be what we got from collection", newBatch.retrieveBatch() == loadedBatch);
        } else {
            log() << "Batch not in collection, making a new one";
            //Sparkling new time batch
            TimeSeriesCompressor newBatch(batchId);
            log() << "New batch created";
            addToCache(txn, newBatch);
            log() << "Done adding to cache";
        }
    } else { // Simply add it to the LRU cache
        log() << "Batch already in cache";
        addToLRUList(batchId);
    }

    massert(40193, "TimeSeriesCache::insert | Still no batch to insert into", _cache.find(batchId) != _cache.end());

    _cache[batchId].insert(doc);

    if (persistent) {
        _cache[batchId].save(txn, _nss);
    }

}

BSONObj TimeSeriesCache::findBatch(OperationContext* txn, batchIdType batchId) {
    // Check that the batch id doesn't already exist in memory.
    massert(40189, "Cannot find a batch that already exists in the cache.",
        _cache.find(batchId) == _cache.end());

    //log() << "Finding batch. <Mario voice> Here we go!";
    BSONObjBuilder query;
    query.append("_id", batchId);
    AutoGetCollectionForRead autoColl(txn, _nss);

     // check getCollection syntax
    BSONObj result;
    // what does "set your db SavedContext first" mean?
    Helpers::findOne(txn, autoColl.getCollection(), query.obj().getOwned(), result, true);
    //bool success = Helpers::findOne(txn, autoColl.getCollection(), query.obj().getOwned(), result, true);

    //log() << "Find success? " << success << ", with result " << result;
    // addToCache(TimeSeriesCompressor(doc));
    return result;
}

BSONObj TimeSeriesCache::retrieveBatch(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

	// Assert that the batch exists.
    uassert(BATCH_NONEXISTENT, "Cannot retrieve a batch that doesn't exist.",
    	_cache.find(batchId) != _cache.end());

    TimeSeriesCompressor& batch = _cache[batchId];

    addToLRUList(batchId);

    return batch.retrieveBatch();
}

batchIdType TimeSeriesCache::evictBatch(OperationContext* txn) {
    // Step 1: Choose the least recently used batch to evict.
    massert(40190, "Timeseries cache is empty; cannot evict from an empty cache",
        !_lruList.empty());

    /* Save the batch to the underlying collection */
    batchIdType toEvict = _lruList.front();
    //log() << "namespace: " << _nss.ns() << " evicting: " << toEvict;
    _cache[toEvict].save(txn, _nss);

    //log() << "Save OK";
    _cache.erase(toEvict);
    //log() << "Erased from cache too";
    _lruList.pop_front();

    return toEvict;
}

void TimeSeriesCache::addToLRUList(batchIdType batchId) {
    /* Remove the batchId from the list if it's already in there.
     * This is O(n) but n is 4 */
    removeFromLRUList(batchId);
    /* Add the batchId to the back (most recently used) */
    _lruList.push_back(batchId);

    //log() << "Added " << batchId << " to LRU list";
}

void TimeSeriesCache::removeFromLRUList(batchIdType batchId) {
    _lruList.remove(batchId);
}

bool TimeSeriesCache::needsEviction() {
    /* Only allow 4 batches */
    return (_lruList.size() >= 2);
}

void TimeSeriesCache::dropFromCache(batchIdType batchId) {
    /* Assert that the batch exists in the cache */
    massert(40191, "Batch must exist in the cache", _cache.find(batchId) != _cache.end());

    /* Drop the batch from the cache */
    _cache.erase(batchId);

    /* Remove the batch from the LRU list */
    removeFromLRUList(batchId);
}

/* Adds to the cache, updates the LRU list, determines if eviction needed... */
void TimeSeriesCache::addToCache(OperationContext* txn, TimeSeriesCompressor& batch) {
    batchIdType batchId = batch._thisBatchId();

    massert(40192, "Batch is already in the cache", _cache.find(batchId) == _cache.end());

    if (needsEviction()) { // make space in the cache
        LOG(2) << "Eviction required";
        LOG(2) << "Evicted batch: " << evictBatch(txn);
        evictBatch(txn);
    }

    _cache.emplace(batchId, batch); // add the batch to the cache
    addToLRUList(batchId);

    LOG(2) << "Added batch number: " << batchId << " to the cache";
}


batchIdType TimeSeriesCache::_getBatchId(const Date_t& time) {
	long long millis = time.toMillisSinceEpoch();

	batchIdType id = millis / NUM_MILLIS_IN_BATCH;

	return id;
}

}
