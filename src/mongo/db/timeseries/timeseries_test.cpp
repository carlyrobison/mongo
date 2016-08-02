// timeseries_test.cpp

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

#include "mongo/db/timeseries/timeseries.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/db/operation_context_noop.h"

#include <stdio.h>
#include <stdlib.h>

namespace mongo {

// Tests inserting documents into one batch document.
// TEST(TimeSeries, SingleBatchInsert) {
//     TimeSeriesBatch batch(0);

//     // Insert documents over one second.
//     for (int i = 0; i < 1000; i++) {
//         BSONObjBuilder builder;
//         // Construct BSONObj with _id as a timestamp for now.
//         // Later we'll add support for having it only partially a timestamp.
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i);
//         batch.insert(builder.obj());
//     }

//     // Check inserting a duplicate.
//     BSONObjBuilder builder;
//     builder.append("_id", Date_t::fromMillisSinceEpoch(987));
//     builder.append("num", "hello");
//     ASSERT_THROWS_CODE(batch.insert(builder.obj()), 
//         UserException, ErrorCodes::DuplicateKeyValue);

//     // Retrieve batch document for this second.
//     BSONObj batchDoc = batch.retrieveBatch();

//     // Check that the batch document has the form:
//     // { _docs: [{}, {}, {}, ... ] }
//     BSONElement docs = batchDoc.getField("_docs");

//     // If eoo is true, then "docs" did not exist in the document.
//     ASSERT_FALSE(docs.eoo());

//     std::vector<BSONElement> subDocuments = docs.Array();

//     // Each batch should have 1000 documents.
//     ASSERT_EQUALS(subDocuments.size(), 1000U);

//     // Check num of each sub document.
//     for (int i = 0; i < 1000; i++) {
//         ASSERT_EQUALS(subDocuments[i]["num"].Int(), i);

//         // Ensure that we can also retrieve this doc.
//         BSONObj retrievedDoc = batch.retrieve(Date_t::fromMillisSinceEpoch(i));
//         ASSERT_EQUALS(retrievedDoc["num"].Int(), i);
//     }

//     // Try to retrieve a key that doesn't exist
//     ASSERT_THROWS_CODE(batch.retrieve(Date_t::fromMillisSinceEpoch(1004)),
//         UserException, ErrorCodes::NoSuchKey);
// }

// // Tests constructing a batch from BSON.
// TEST(TimeSeries, BatchLoading) {
//     BSONArrayBuilder arrayBuilder;
//     //for (int i = 0; i < 1000; i++) {
//     for (int i = 0; i < 10; i++) {
//         BSONObjBuilder docBuilder;
//         docBuilder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         docBuilder.append("num", i);
//         arrayBuilder.append(docBuilder.obj());
//     }

//     // batchDoc looks like {_id: 0, _docs: [{}, {}, {}, ...]}
//     BSONObj batchDoc = BSON("_id" << 0 << "_docs" << arrayBuilder.arr());
//     TimeSeriesBatch batch(batchDoc);

//     // Check that the BSONObj the batch constructs is the same as the one we used
//     // to construct it.
//     ASSERT_EQUALS(batch.retrieveBatch().getField("_id").numberLong(), 0);
//     ASSERT_EQUALS(batch.retrieveBatch()["_docs"], batchDoc["_docs"]);
// }

// // Tests inserting documents into many batch documents.
// TEST(TimeSeries, BatchManagerInsert) {
//     TimeSeriesCache manager;
//     // Insert documents over the span of five seconds.
//     for (int i = 0; i < 4000; i++) {
//         BSONObjBuilder builder;
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i);
//         manager.insert(builder.obj());
//     }

//     // Check retrieval
//     for (int j = 0; j < 4; j++) {
//         for (int i = 0; i < 1000; i++) {
//             int t = (j * 1000) + i;
//             Date_t date = Date_t::fromMillisSinceEpoch(t);
//             BSONObj retrievedDoc = manager.retrieve(date);
//             ASSERT_EQUALS(retrievedDoc["num"].Int(), t);
//             ASSERT_EQUALS(manager.retrieveBatch(date).getField("_id").numberLong(), j);
//         }
//         // NOTE: only do j up to 4 because that's the size of the cache.
//     }

//     // Check that we can't retrieve or retrieve from a batch that doesn't exist.
//     ASSERT_THROWS_CODE(manager.retrieveBatch(Date_t::fromMillisSinceEpoch(6000)),
//         UserException, BATCH_NONEXISTENT);
//     ASSERT_THROWS_CODE(manager.retrieve(Date_t::fromMillisSinceEpoch(6000)),
//         UserException, BATCH_NONEXISTENT);
// }

// TEST(TimeSeries, Update) {
//     TimeSeriesCache manager;

//     // Insert some documents
//     for (int i = 0; i < 900; i++) {
//         BSONObjBuilder builder;
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i);
//         manager.insert(builder.obj());
//     }

//     // Update some documents
//     for (int i = 0; i < 500; i++) {
//         BSONObjBuilder builder;
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i * 2);
//         manager.update(builder.obj());
//     }

//     // Try and update some documents that don't exist
//     for (int i = 900; i < 1000; i++) {
//         BSONObjBuilder builder;
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i);
//         // ASSERT_THROWS_CODE(manager.update(builder.obj()), DatabaseException, ErrorCodes::NoSuchKey);
//     }

//     // Check that updates were successful
//     for (int i = 0; i < 500; i++) {
//         Date_t date = Date_t::fromMillisSinceEpoch(i);
//         BSONObj retrievedDoc = manager.retrieve(date);
//         ASSERT_EQUALS(retrievedDoc["num"].Int(), i * 2);
//     }

//     // Check that the updates did not mess up things we did not update
//     for (int i = 500; i < 900; i++) {
//         Date_t date = Date_t::fromMillisSinceEpoch(i);
//         BSONObj retrievedDoc = manager.retrieve(date);
//         ASSERT_EQUALS(retrievedDoc["num"].Int(), i);
//     }

//     // Check that we can't update a batch that doesn't exist
//     BSONObjBuilder builder;
//     builder.append("_id", Date_t::fromMillisSinceEpoch(2000));
//     builder.append("num", "should never see this");
//     ASSERT_THROWS_CODE(manager.update(builder.obj()), UserException, BATCH_NONEXISTENT);

// }

// TEST(TimeSeries, Remove) {
//     TimeSeriesCache manager;

//     // Insert some documents
//     for (int i = 0; i < 900; i++) {
//         BSONObjBuilder builder;
//         builder.append("_id", Date_t::fromMillisSinceEpoch(i));
//         builder.append("num", i);
//         manager.insert(builder.obj());
//     }

//     // Remove some documents
//     for (int i = 0; i < 500; i++) {
//         manager.remove(Date_t::fromMillisSinceEpoch(i));
//     }

//     // Try and remove some documents that don't exist/check that removal was successful
//     for (int i = 0; i < 500; i++) {
//         ASSERT_THROWS_CODE(manager.remove(Date_t::fromMillisSinceEpoch(i)),
//             UserException, ErrorCodes::NoSuchKey);
//     }

//     // Check that the removals did not mess up things we did not remove
//     for (int i = 500; i < 900; i++) {
//         Date_t date = Date_t::fromMillisSinceEpoch(i);
//         BSONObj retrievedDoc = manager.retrieve(date);
//         ASSERT_EQUALS(retrievedDoc["num"].Int(), i);
//     }

//     // Check that we can't remove/remove from a batch that doesn't exist
//     ASSERT_THROWS_CODE(manager.remove(Date_t::fromMillisSinceEpoch(2000)),
//         UserException, BATCH_NONEXISTENT);
//     ASSERT_THROWS_CODE(manager.removeBatch(Date_t::fromMillisSinceEpoch(2000)),
//         UserException, BATCH_NONEXISTENT);

//     // Check that the removals did not mess up things we did not remove
//     for (int i = 500; i < 900; i++) {
//         Date_t date = Date_t::fromMillisSinceEpoch(i);
//         BSONObj retrievedDoc = manager.retrieve(date);
//         ASSERT_EQUALS(retrievedDoc["num"].Int(), i);
//     }

//     // TODO:
//     // Remove the rest of the documents
//     // Check that the batch still exists
//     // Remove the batch
//     // Check that the batch does not exist anymore

// }

TEST(TimeSeries, Cache) {
    log() << "hello there";
    OperationContextNoop txn;
    NamespaceString nss("timeseriewview", "tsv");
    TimeSeriesCache cache(nss);

    // Insert some documents
    for (int i = 0; i < 5000; i += 100) {
        BSONObjBuilder builder;
        builder.append("_id", Date_t::fromMillisSinceEpoch(i));
        builder.append("num", i);
        cache.insert(&txn, builder.obj());
    }

    // Insert randomly
    for (int i = 0; i < 100; i++) {
        BSONObjBuilder builder;
        int j = rand() % 10000;
        builder.append("_id", Date_t::fromMillisSinceEpoch(j));
        builder.append("num", j);
        cache.insert(&txn, builder.obj(), false);
    }

}

}  // namespace mongo