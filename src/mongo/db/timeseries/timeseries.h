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

#include <string>
#include <assert.h>
#include <queue>

#pragma once

namespace mongo {

using mongo::ErrorCodes;

typedef long long batchIdType;

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
    BSONObj retrieveBatch();

    /* Retrieves a batch document for a given timestamp */
    BSONObj retrieve(const Date_t& time);

    /* Deletes a document from this batch. */
    void remove(const Date_t& time);

    /* Reports this batch's batch Id */
    batchIdType _thisBatchId();

    /* Saves to a collection */
    // bool save(StringData db, StringData coll);

    /* Assuming this is the deconstructor. Saves the contents of the buffer
     * (on disk?) and disappears */
    // ~TimeSeriesBatch();


private:
    batchIdType _batchId;

    /* batch should own the docs so use emplace */
    std::map<Date_t, BSONObj> _docs;

    // BSONObj _constructUpsertCommand(StringData collName);
};


/**
 * Manages multiple time series batches in memory.
 */
class TimeSeriesCache {
public:
    TimeSeriesCache() = default;

    TimeSeriesCache(StringData db, StringData coll);

    /* Inserts a document into the corresponding batch */
    void insert(const BSONObj& doc);

    /* Loads a batch */
    void loadBatch(const BSONObj& doc);

    /* Updates a document in the corresponding batch */
    void update(const BSONObj& doc);

    /* Gets the document corresponding to that date and time */
    BSONObj retrieve(const Date_t& time);

    BSONObj retrieveBatch(const Date_t& time);

    /* Removes the document at that time */
    void remove(const Date_t& time);

    void removeBatch(const Date_t& time);

    /**
     * Saves a specific batch to the backing collection
     */
    bool saveBatch(const Date_t& time);

    // When trying to get rid of TS Cache, save everything to the collection
    // ~TimeSeriesCache();

private:
    /* Eviction methods that should not be able to be called by external classes */

    /* Evicts a batch and saves it to the backing collection in doing so.
     * Returns the batch Id of the cache that was evicted. Uses an LRU algorithm */
    batchIdType evictBatch();

    /* Linked list for LRU part of cache */
    std::queue<batchIdType> _lruQueue;


private:
    /* Converts a date to the corresponding batch id number */
    batchIdType _getBatchId(const Date_t& time);

    /* Map of batch IDs to TSbatches */
    /* cache should own the batch so use emplace */
    std::map<batchIdType, TimeSeriesBatch> _cache;

    // Namespace of underlying collection
    StringData _db = NULL;
    StringData _coll = NULL;
};

}  // namespace mongo