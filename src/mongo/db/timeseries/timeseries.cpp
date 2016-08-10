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

#include <algorithm>
#include <memory>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/ftdc/config.h"
#include "mongo/db/ftdc/decompressor.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_request.h"
#include "mongo/db/ops/update_result.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "mongo/db/dbhelpers.h"

namespace mongo {

Batch::Batch(const BSONObj& batchDocument, bool compressed)
    : _compressed(compressed), _ftdcConfig() {
    BSONObj batchDoc = batchDocument.getOwned();
    // Create the batch Id from the batch document
    _batchId = batchDoc["_id"].numberLong();

    if (compressed) {
        _compressor = stdx::make_unique<FTDCCompressor>(&_ftdcConfig);
        // 1. Decompress the batch.
        BSONElement compressedData = batchDoc.getField("_docs");
        log() << "compressed data: " << compressedData;
        int len;
        const char* bData = compressedData.binData(len);
        log() << "bin data: " << bData;

        ConstDataRange buf(bData, len);
        auto swBuf = FTDCDecompressor().uncompress(buf);
        uassert(40201, swBuf.getStatus().reason(), swBuf.isOK());
        auto _docsToReturn = swBuf.getValue();

        FTDCConfig config;
        _compressor = new FTDCCompressor(&config);

        // Put 'em all back in
        for (auto&& doc : _docsToReturn) {
            _compressor->addSample(doc, doc.getField("_id").Date());
        }
    }
    // Create a map entry for each doc in the docs subarray.
    for (const BSONElement& elem : batchDoc["_docs"].Array()) {
        BSONObj obj = elem.Obj();
        BSONElement docId = obj.getField("_id");
        // TODO: check that docId is really a date.
        Date_t date = docId.Date();
        _docs.emplace(date, obj.getOwned());
        _batchId = batchDoc["_id"].numberLong();
    }
}

Batch::Batch(batchIdType batchId, bool compressed)
    : _batchId(batchId), _compressed(compressed), _ftdcConfig() {
    if (compressed) {
        _compressor = stdx::make_unique<FTDCCompressor>(&_ftdcConfig);
    }
}

std::string Batch::toString(bool includeTime) const {
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

void Batch::insert(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // internally copies if necessary
    /* Adds this document to the map of documents, based on date. */
    Date_t date = newDoc.getField("_id").Date();

    if (_compressed) {
        // Compress this document
        auto st = _compressor->addSample(newDoc, date);
        massert(40276, "Insert was not OK", st.isOK());
        if (std::get<1>(st.getValue().get()) == FTDCCompressor::CompressorState::kCompressorFull) {
            // TODO: Force a save and eviction from the cache. Do this when evicting by size since
            // it'll likely be similar logic.
            log() << "Compressor is full";
        }
        return;
    }

    uassert(ErrorCodes::DuplicateKeyValue,
            "Cannot insert a document that already exists.",
            _docs.find(date) == _docs.end());
    _docs[date] = newDoc;
    _needsFlush = false;
}

void Batch::update(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // internally copies if necessary

    /* Confirms that the timestamp already exists */
    Date_t date = newDoc.getField("_id").Date();
    massert(ErrorCodes::NoSuchKey,
            "Cannot update a document that doesn't exist.",
            _docs.find(date) != _docs.end());
    _docs[date] = newDoc;
}

BSONObj Batch::retrieveBatch() {
    if (_compressed) {
        BSONObjBuilder builder;
        builder.append("_id", _batchId);
        StatusWith<std::tuple<ConstDataRange, Date_t>> swBuf = _compressor->getCompressedSamples();
        massert(40275, "Could not retrieve compressed timeseries batch", swBuf.isOK());
        ConstDataRange dataRange = std::get<0>(swBuf.getValue());
        builder.appendBinData(
            "_docs", dataRange.length(), BinDataType::BinDataGeneral, dataRange.data());
        _compressor.reset();
        return builder.obj();
    }

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

BSONObj Batch::retrieve(const Date_t& time) {
    uassert(ErrorCodes::NoSuchKey,
            "Cannot retrieve a document that doesn't exist.",
            _docs.find(time) != _docs.end());

    return _docs[time];
}

void Batch::remove(const Date_t& time) {
    uassert(ErrorCodes::NoSuchKey,
            "Cannot delete a document that doesn't exist.",
            _docs.find(time) != _docs.end());

    _docs.erase(time);
}

batchIdType Batch::_thisBatchId() {
    return _batchId;
}

// Must have exclusive lock on this collection.
bool Batch::save(OperationContext* txn, const NamespaceString& nss) {
    // Create the update request
    UpdateRequest request(nss);
    request.setQuery(BSON("_id" << _batchId));
    request.setUpdates(retrieveBatch());
    request.setUpsert(true);

    // get the database NEEDS SERVERONLY
    log() << "TimeSeriesCache::Batch::save obtaining lock";
    Database* db = dbHolder().get(txn, nss.db());
    log() << "TimeSeriesCache::Batch::save lock obtained";

    // update!
    UpdateResult result = ::mongo::update(txn, db, request);
    log() << "Updated";

    // See what we got
    // log() << "result of save: " << result;
    // massert(result.numMatched == 1);

    return true;
}

bool Batch::checkIfNeedsFlushAndReset() {
    bool oldValue = _needsFlush;
    _needsFlush = true;  // Until insert occurs;
    return oldValue;
}


TimeSeriesCache::TimeSeriesCache(const NamespaceString& nss, bool compressed)
    : _nss(nss), _compressed(compressed) {}

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

void TimeSeriesCache::insert(OperationContext* txn, const BSONObj& doc) {
    stdx::lock_guard<stdx::mutex> guard(_lock);
    Date_t date = doc.getField("_id").Date();
    batchIdType batchId = _getBatchId(date);

    // Check if the batch exists in the cache. If it doesn't, try to find it. If it does, use the
    // existing one.
    if (_cache.find(batchId) == _cache.end()) {
        ensureFree(txn);
        // Batch does not exist in the cache. Try to load it from the underlying collection.
        BSONObj loadedBatchObj = findBatch(txn, batchId);
        if (loadedBatchObj.isEmpty()) {
            _cache.emplace(std::piecewise_construct,
                std::forward_as_tuple(batchId),
                std::forward_as_tuple(batchId, _compressed));
        } else {
            _cache.emplace(std::piecewise_construct,
                std::forward_as_tuple(batchId),
                std::forward_as_tuple(loadedBatchObj, _compressed));
        }
        addToLRUList(batchId);
    } else {  // Simply add it to the LRU cache
        // log() << "Batch already in cache";
        addToLRUList(batchId);
    }

    massert(40270,
            "TimeSeriesCache::insert | Still no batch to insert into",
            _cache.find(batchId) != _cache.end());

    _cache[batchId].insert(doc);
}

BSONObj TimeSeriesCache::findBatch(OperationContext* txn, batchIdType batchId) {
    // Check that the batch id doesn't already exist in memory.
    massert(40271,
            "Cannot find a batch that already exists in the cache.",
            _cache.find(batchId) == _cache.end());

    // log() << "Finding batch. <Mario voice> Here we go!";
    BSONObjBuilder query;
    query.append("_id", batchId);
    log() << "Getting collection for read";
    AutoGetCollectionForRead autoColl(txn, _nss);
    log() << "Got collection for read";

    // check getCollection syntax
    BSONObj result;
    // what does "set your db SavedContext first" mean?
    Helpers::findOne(txn, autoColl.getCollection(), query.obj().getOwned(), result, true);
    // bool success = Helpers::findOne(txn, autoColl.getCollection(), query.obj().getOwned(),
    // result, true);

    // log() << "Find success? " << success << ", with result " << result;
    // addToCache(TimeSeriesBatch(doc));
    return result;
}

void TimeSeriesCache::update(const BSONObj& doc) {
    Date_t date = doc.getField("_id").Date();

    log() << "Update unsupported.";

    batchIdType batchId = _getBatchId(date);

    // Assert that the batch exists.
    uassert(BATCH_NONEXISTENT,
            "Cannot update a document in a batch that does not exist",
            _cache.find(batchId) != _cache.end());

    Batch& batch = _cache[batchId];

    batch.update(doc);
}

BSONObj TimeSeriesCache::retrieve(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

    log() << "Why are you retrieving from the cache? Use the find() path!";

    // Assert that the batch exists.
    uassert(BATCH_NONEXISTENT,
            "Cannot retrieve from a batch that doesn't exist.",
            _cache.find(batchId) != _cache.end());

    Batch& batch = _cache[batchId];

    addToLRUList(batchId);

    return batch.retrieve(time);
}

BSONObj TimeSeriesCache::retrieveBatch(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

    // Assert that the batch exists.
    uassert(BATCH_NONEXISTENT,
            "Cannot retrieve a batch that doesn't exist.",
            _cache.find(batchId) != _cache.end());

    Batch& batch = _cache[batchId];

    addToLRUList(batchId);

    return batch.retrieveBatch();
}

void TimeSeriesCache::remove(const Date_t& time) {
    batchIdType batchId = _getBatchId(time);

    // Assert that the batch exists.
    uassert(BATCH_NONEXISTENT,
            "Cannot delete from a batch that doesn't exist.",
            _cache.find(batchId) != _cache.end());

    Batch& batch = _cache[batchId];

    batch.remove(time);

    addToLRUList(batchId);
}

bool TimeSeriesCache::saveToCollection() {
    return true;
}

void TimeSeriesCache::ensureFree(OperationContext* txn) {
    if (_lruList.size() < 2) return;
    // Choose the least recently used batch to evict.
    batchIdType toEvict = _lruList.front();
    log() << "namespace: " << _nss.ns() << " evicting: " << toEvict;
    _cache[toEvict].save(txn, _nss);

    log() << "Save OK";
    _cache.erase(toEvict);
    log() << "Erased from cache too";
    _lruList.pop_front();
}

void TimeSeriesCache::addToLRUList(batchIdType batchId) {
    /* Remove the batchId from the list if it's already in there.
     * This is O(n) but n is 4 */
    removeFromLRUList(batchId);
    /* Add the batchId to the back (most recently used) */
    _lruList.push_back(batchId);

    // log() << "Added " << batchId << " to LRU list";
}

void TimeSeriesCache::removeFromLRUList(batchIdType batchId) {
    _lruList.remove(batchId);
}

batchIdType TimeSeriesCache::_getBatchId(const Date_t& time) {
    long long millis = time.toMillisSinceEpoch();

    batchIdType id = millis / NUM_MILLIS_IN_BATCH;

    return id;
}

void TimeSeriesCache::flushIfNecessary(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> guard(_lock);
    std::list<batchIdType> toRemove;
    for (auto& batchEntry : _cache) {
        Batch& batch = batchEntry.second;
        bool needsFlush = batch.checkIfNeedsFlushAndReset();
        if (needsFlush) {
            log() << "Flushing one batch in " << _nss;
            toRemove.push_back(batchEntry.first);
        }
    }

    for (batchIdType& batchId : toRemove) {
        _cache[batchId].save(txn, _nss);
        auto lruIter = std::find(_lruList.begin(), _lruList.end(), batchId);
        _lruList.erase(lruIter);
        _cache.erase(batchId);
    }
}

}  // namespace mongo