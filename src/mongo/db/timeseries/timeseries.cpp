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

TimeSeriesCache::Batch::Batch(const BSONObj& batchDocument, bool compressed, std::string timeField)
    : _compressed(compressed), _timeField(timeField) {
    BSONObj batchDoc = batchDocument.getOwned();
    // Create the batch Id from the batch document
    _batchId = batchDoc["_id"].numberLong();

    log() << "creating batch from existing";

    if (compressed) {
        FTDCConfig ftdcConfig;
        _compressor = stdx::make_unique<FTDCCompressor>(&ftdcConfig);
        // Decompress the batch.
        int len;
        const char* bData = batchDoc.getField("_docs").binData(len);

        ConstDataRange buf(bData, len);
        auto swBuf = FTDCDecompressor().uncompress(buf);
        uassert(40279, swBuf.getStatus().reason(), swBuf.isOK());
        auto _docsToReturn = swBuf.getValue();

        // Put 'em all back in
        for (auto&& doc : _docsToReturn) {
            _compressor->addSample(doc, doc.getField(_timeField).Date());
        }
    } else {
        // Create a map entry for each doc in the docs subarray.
        for (const BSONElement& elem : batchDoc["_docs"].Array()) {
            BSONObj obj = elem.Obj();
            Date_t date = obj.getField(_timeField).Date();
            _docs.emplace(date, obj.getOwned());
        }
    }
}

TimeSeriesCache::Batch::Batch(BatchIdType batchId, bool compressed, std::string timeField)
    : _batchId(batchId), _compressed(compressed), _timeField(timeField)  {
    if (compressed) {
        FTDCConfig ftdcConfig;
        _compressor = stdx::make_unique<FTDCCompressor>(&ftdcConfig);
    }
}

std::string TimeSeriesCache::Batch::toString() const {
    StringBuilder s;
    s << ("Batch number: ");
    s << (_batchId);
    return s.str();
}

void TimeSeriesCache::Batch::insert(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // internally copies if necessary
    // Adds this document to the map of documents, based on date.
    Date_t date = newDoc.getField(_timeField).Date();

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

void TimeSeriesCache::Batch::update(const BSONObj& doc) {
    BSONObj newDoc = doc.getOwned();  // Internally copies if necessary.
    Date_t date = newDoc.getField(_timeField).Date();
    massert(ErrorCodes::NoSuchKey, "Cannot update a document that doesn't exist.",
        _docs.find(date) != _docs.end());
    _docs[date] = newDoc;
}

BSONObj TimeSeriesCache::Batch::asBSONObj() {
    BSONObjBuilder builder;
    builder.append("_id", _batchId);
    if (_compressed) {
        StatusWith<std::tuple<ConstDataRange, Date_t>> swBuf = _compressor->getCompressedSamples();
        massert(40275, "Could not retrieve compressed timeseries batch", swBuf.isOK());
        ConstDataRange dataRange = std::get<0>(swBuf.getValue());
        builder.appendBinData(
            "_docs", dataRange.length(), BinDataType::BinDataGeneral, dataRange.data());
        _compressor.reset();
    } else {
        // create the BSON array
        BSONArrayBuilder arrayBuilder;

        // Add values (documents) to the bson array
        for (auto i = _docs.cbegin(); i != _docs.cend(); ++i) {
            arrayBuilder.append(i->second);
        }

        // add the docs
        builder.append("_docs", arrayBuilder.arr());
    }

    return builder.obj();
}

BSONObj TimeSeriesCache::Batch::retrieve(const Date_t& time) {
    uassert(ErrorCodes::NoSuchKey,
            "Cannot retrieve a document that doesn't exist.",
            _docs.find(time) != _docs.end());

    return _docs[time];
}

void TimeSeriesCache::Batch::remove(const Date_t& time) {
    uassert(ErrorCodes::NoSuchKey,
            "Cannot delete a document that doesn't exist.",
            _docs.find(time) != _docs.end());

    _docs.erase(time);
}

// Must have exclusive lock on this collection.
bool TimeSeriesCache::Batch::save(OperationContext* txn, const NamespaceString& nss) {
    // Create the update request
    UpdateRequest request(nss);
    request.setQuery(BSON("_id" << _batchId));
    request.setUpdates(asBSONObj());
    request.setUpsert(true);

    Database* db = dbHolder().get(txn, nss.db());
    UpdateResult result = ::mongo::update(txn, db, request);
    return true;
}

bool TimeSeriesCache::Batch::checkIfNeedsFlushAndReset() {
    bool oldValue = _needsFlush;
    _needsFlush = true;  // Until insert occurs;
    return oldValue;
}

TimeSeriesCache::TimeSeriesCache(const NamespaceString& nss, const BSONObj&
 options)
    : _nss(nss) {
        // Set up options
        if (options.hasField("compressed") && options.getField("compressed").Bool()) {
            _compressed = true;
        }
        if (options.hasField("cache_size")) {
            _cacheSize = options.getField("cache_size").Number();
        }
        if (options.hasField("millis_in_batch")) {
            _millisInBatch = options.getField("millis_in_batch").Number();
        }
        if (options.hasField("time_field")) {
            _timeField = options.getField("time_field").String();
        }
    }

void TimeSeriesCache::insert(OperationContext* txn, const BSONObj& doc) {
    stdx::lock_guard<stdx::mutex> guard(_lock);
    Date_t date = doc.getField(_timeField).Date();
    BatchIdType batchId = _getBatchId(date);
    Batch& batch = _getOrCreateBatch(txn, batchId);
    batch.insert(doc);
}

void TimeSeriesCache::update(OperationContext* txn, const BSONObj& doc) {
    Date_t date = doc.getField(_timeField).Date();
    BatchIdType batchId = _getBatchId(date);
    Batch& batch = _getOrCreateBatch(txn, batchId);
    batch.update(doc);
}

void TimeSeriesCache::remove(OperationContext* txn, const Date_t& date) {
    BatchIdType batchId = _getBatchId(date);
    invariant(_cache.find(batchId) != _cache.end());
    _cache.at(batchId).remove(date);
    _addToLRUList(batchId);
}

void TimeSeriesCache::flushIfNecessary(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> guard(_lock);
    std::list<BatchIdType> toRemove;
    for (auto& batchEntry : _cache) {
        Batch& batch = batchEntry.second;
        bool needsFlush = batch.checkIfNeedsFlushAndReset();
        if (needsFlush) {
            toRemove.push_back(batchEntry.first);
        }
    }

    for (BatchIdType& batchId : toRemove) {
        _cache.at(batchId).save(txn, _nss);
        // Keep flushed things in the cache for the future.
        auto lruIter = std::find(_lruList.begin(), _lruList.end(), batchId);
        _lruList.erase(lruIter);
        _cache.erase(batchId);
    }
}

BSONObj TimeSeriesCache::_findBatch(OperationContext* txn, BatchIdType batchId) {
    // Check that the batch id doesn't already exist in memory.
    massert(40271,
            "Cannot find a batch that already exists in the cache.",
            _cache.find(batchId) == _cache.end());

    AutoGetCollectionForRead autoColl(txn, _nss);
    BSONObj result;
    Helpers::findOne(txn, autoColl.getCollection(), BSON("_id" << batchId), result, true);
    return result;
}

BSONObj TimeSeriesCache::_retrieveBatch(const Date_t& time) {
    BatchIdType batchId = _getBatchId(time);
    // Assert that the batch exists.
    invariant(_cache.find(batchId) != _cache.end());
    _addToLRUList(batchId);
    return _cache.at(batchId).asBSONObj();
}

TimeSeriesCache::Batch& TimeSeriesCache::_getOrCreateBatch(OperationContext* txn,
                                                           BatchIdType batchId) {
    if (_cache.find(batchId) != _cache.end()) {
        return _cache.at(batchId);
    }

    // The batch is not in our cache, so ensure there is room to add it.
    if (_lruList.size() >= _cacheSize) {
        // Choose the least recently used batch to evict.
        BatchIdType toEvict = _lruList.front();
        _cache.at(toEvict).save(txn, _nss);
        _cache.erase(toEvict);
        _lruList.pop_front();
    }

    // Attempt to retrieve it from the collection.
    BSONObj batchObj = _findBatch(txn, batchId);
    if (batchObj.isEmpty()) {
        _cache.emplace(std::piecewise_construct,
                       std::forward_as_tuple(batchId),
                       std::forward_as_tuple(batchId, _compressed, _timeField));
    } else {
        _cache.emplace(std::piecewise_construct,
                       std::forward_as_tuple(batchId),
                       std::forward_as_tuple(batchObj, _compressed, _timeField));
    }
    _addToLRUList(batchId);
    return _cache.at(batchId);
}

void TimeSeriesCache::_addToLRUList(BatchIdType batchId) {
    /* Remove the batchId from the list if it's already in there.
     * This is O(n) but n is 4 */
    _removeFromLRUList(batchId);
    /* Add the batchId to the back (most recently used) */
    _lruList.push_back(batchId);
}

void TimeSeriesCache::_removeFromLRUList(BatchIdType batchId) {
    _lruList.remove(batchId);
}

BatchIdType TimeSeriesCache::_getBatchId(const Date_t& time) {
    return time.toMillisSinceEpoch() / _millisInBatch;
}

}  // namespace mongo