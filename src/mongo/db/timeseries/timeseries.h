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

#pragma once

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/ftdc/compressor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/time_support.h"

#include <assert.h>
#include <list>
#include <map>
#include <string>

namespace mongo {

class OperationContext;
using BatchIdType = long long;
/**
 * Manages multiple time series batches in memory. This class is thread safe.
 */
class TimeSeriesCache {
public:
    TimeSeriesCache(const NamespaceString& nss, const BSONObj& options);

    /**
     * Inserts a document into the corresponding batch.
     */
    void insert(OperationContext* txn, const BSONObj& doc);

    /**
     * Updates a document in the corresponding batch. TODO: integrate and test.
     */
    void update(OperationContext* txn, const BSONObj& doc);

    /**
     * Removes the document at that time. TODO: integrate and test.
     */
    void remove(OperationContext* txn, const Date_t& time);

    /**
     * Should be called periodically by the TimeSeriesCacheMonitor thread to notify if inactive
     * batches should be flushed.
     */
    void flushIfNecessary(OperationContext* txn);

    /**
     * Should be called when mongod is shutting down.
     */
    void flushAll(OperationContext* txn);

private:
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
    class Batch {
    public:
        /**
         * Sets the current batch id and initializes the map.
         */
        Batch(BatchIdType batchId, bool compressed, std::string timeField);

        /**
         * Constructs a batch object from the bson document.
         */
        Batch(const BSONObj& batchDocument, bool compressed, std::string timeField);

        /**
         * Prints a string. For debugging.
         */
        std::string toString() const;

        /**
         * Inserts a document into the time series DB.
         * Assume that the document's ID is a time.
         */
        void insert(const BSONObj& doc);

        /**
         * Updates a document in the time series DB.
         * Updates the whole document; doesn't check to update just the fields that don't exist.
         * Asserts that the document already exists.
         */
        void update(const BSONObj& doc);

        /**
         * Retrieves the BSONObj for the entire batch document.
         */
        BSONObj asBSONObj();

        /**
         * Retrieves a batch document for a given timestamp.
         */
        BSONObj retrieve(const Date_t& time);

        /**
         * Deletes a document from this batch.
         */
        void remove(const Date_t& time);

        /**
         * Saves a specific batch to a collection.
         */
        bool save(OperationContext* txn, const NamespaceString& nss);

        /**
         * Returns true if this batch should be flushed and sets _needsFlush to true.
         */
        bool checkIfNeedsFlushAndReset();

    private:
        BatchIdType _batchId;
        std::map<Date_t, BSONObj> _docs;
        std::unique_ptr<FTDCCompressor> _compressor;
        bool _needsFlush = false;
        FTDCConfig _ftdcConfig;

        // options
        bool _compressed;
        std::string _timeField;
    };

    /**
     * Loads a batch into the cache and the cache list.
     */
    BSONObj _findBatch(OperationContext* txn, BatchIdType batchId);

    /**
     * Updates LRU list.
     */
    BSONObj _retrieveBatch(const Date_t& date);

    /**
     * Ensures there is an available entry in the batch map, and evicts a batch if necessary
     */
    Batch& _getOrCreateBatch(OperationContext* txn, BatchIdType batchId);

    /**
     * Adds a batch to the LRU list. Checks if the cache needs to evict after this operation, and
     * does so if needed.
     */
    void _addToLRUList(BatchIdType batchId);

    /**
     * Removes a batch from the LRU list.
     */
    void _removeFromLRUList(BatchIdType batchId);

    /**
     * Converts a date to the corresponding batch id number.
     */
    BatchIdType _getBatchId(const Date_t& date);

    std::map<BatchIdType, Batch> _cache;  // Map of batch IDs to TSbatches
    std::list<BatchIdType> _lruList;
    stdx::mutex _lock;
    NamespaceString _nss;  // Namespace of underlying collection

    // options with defaults
    bool _compressed = false;
    unsigned int _cacheSize = 1;
    long long _millisInBatch = 1000;
    std::string _timeField = "_id";

};

}  // namespace mongo
