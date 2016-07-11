// timeseries.h

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

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/time_support.h"
#include "mongo/util/assert_util.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/namespace_string.h"

//#include "mongo/db/service_context.h"
//#include "mongo/db/concurrency/write_conflict_exception.h"
//#include "mongo/db/catalog/collection.h"
//#include "mongo/db/curop.h"
//#include "mongo/db/commands.h"
//#include "mongo/db/client.h"
//#include "mongo/db/query/query_request.h"
//#include "mongo/db/db_raii.h"

//#include "mongo/db/dbhelpers.h"

#include <string>
#include <assert.h>

#pragma once

namespace mongo {

using mongo::ErrorCodes;

typedef long long batchIdType;
class OperationContext;

static const batchIdType NUM_MILLIS_IN_BATCH = 1000;

// Error code for a batch that doesn't exist.
static const int BATCH_NONEXISTENT = 40154;

/**
 * Represents a single time series batch.
 *
 * It will support CRUD operations for documents falling in this time series interval.
 * The document it represents is as follows:
 * {
 *     _id: <id containing timestamp>,
 *     docs: [{}, {}, ...]
 * }
 */
class TimeSeriesBatch {
public:
    /**
     * Sets the current batch id and initializes the map.
     */
    TimeSeriesBatch(batchIdType batchId);

    /**
     * Constructs a batch object from the bson document.
     */
    TimeSeriesBatch(const BSONObj& batchDocument);

    TimeSeriesBatch() = default;

    /**
     * Inserts a document into the time series DB.
     * Assume that the document's ID is a time.
     */
    void insert(const BSONObj& doc);

    /**
     * Updates a document in the time series DB.
     * Updates the whole document; doesn't check to update just the fields that don't exist already.
     * Asserts that the document already exists.
     */
    void update(const BSONObj& doc);

    /* Retrieves the single BSONObj for the batch document. */
    BSONObj retrieveBatch() const;

    /* Retrieves a batch document for a given timestamp */
    BSONObj retrieve(const Date_t& time);

    /* Deletes a document from this batch. */
    void remove(const Date_t& time);

    /* Reports this batch's batch Id */
    batchIdType _thisBatchId() const;

    /* Saves a specific batch to a collection */
    bool save(OperationContext* txn, const std::string& ns) const;

    /* Assuming this is the deconstructor. Saves the contents of the buffer
     * (on disk?) and disappears */
    // ~TimeSeriesBatch();


private:
    batchIdType _batchId;

    /* batch should own the docs so use emplace */
    std::map<Date_t, BSONObj> _docs;

};


/**
 * Manages multiple time series batches in memory.
 */
class TimeSeriesCache {
public:
    TimeSeriesCache() = default;

    TimeSeriesCache(NamespaceString nss);

    /* Inserts a document into the corresponding batch.
     * Creates the batch if necessary. */
    void insert(OperationContext* txn, const BSONObj& doc, bool persistent = false);

    /* Loads a batch into the cache and the cache list */
    void loadBatch(const BSONObj& doc);

    /* Updates a document in the corresponding batch.
     * Checks for space? Updates LRU list */
    void update(const BSONObj& doc);

    /* Gets the document corresponding to that date and time.
     * Updates LRU list */
    BSONObj retrieve(const Date_t& time);

    /* Updates LRU list */
    BSONObj retrieveBatch(const Date_t& time);

    /* Removes the document at that time.
     * Decrements the space? */
    void remove(const Date_t& time);

    /* Removes a specific batch from the collection. Deletes it from the cache,
     * and the cache list, decrements cache size. */
    void removeBatch(const Date_t& time);

    /**
     * Saves all of the batches at once to the backing collection. Saving consists of:
     * Saving to the collection
     * Waiting until durable????
     */
    bool saveToCollection();

    // When trying to get rid of TS Cache, save everything to the collection
    // ~TimeSeriesCache();

private:
    /* Cache methods that should not be able to be called by external classes */

    /* Evicts a batch and saves it to the backing collection in doing so.
     * Returns the batch Id of the cache that was evicted. Uses an LRU algorithm
     * to determine which batch to evict. */
    batchIdType evictBatch(OperationContext* txn);

    /* Adds a batch to the LRU list. Checks if the cache needs to evict after
     * this operation, and does so if needed. */
    void addToLRUList(batchIdType batchId);

    /* Removes a batch from the LRU list. */
    void removeFromLRUList(batchIdType batchId);

    /* Determines if adding something of this size needs an eviction */
    bool needsEviction();

    /* Drops a batch from the cache */
    void dropFromCache(batchIdType batchId);

    /* Adds to the cache, updates the LRU list, determines if eviction needed... */
    void addToCache(OperationContext* txn, const TimeSeriesBatch& batch);

    /* Queue for LRU part of cache: least recently used is at the front, we add
     * new elements to the back */
    std::list<batchIdType> _lruList;

private:
    /* Converts a date to the corresponding batch id number */
    batchIdType _getBatchId(const Date_t& time);

    /* Map of batch IDs to TSbatches */
    /* cache should own the batch so use emplace */
    std::map<batchIdType, TimeSeriesBatch> _cache;

    // Namespace of underlying collection
    NamespaceString _nss;
};

}  // namespace mongo