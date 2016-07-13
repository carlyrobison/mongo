// timeseriestests.cpp : timeseries.{h,cpp} unit tests.
//

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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <string>

#include "mongo/dbtests/dbtests.h"
#include "mongo/db/namespace_string.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/time_support.h"
#include "mongo/base/string_data.h"
#include "mongo/util/log.h"

namespace TimeseriesTests {

using std::string;

/** A missing field is represented as null in a btree index. */
class TimeseriesBase {
public:
    TimeseriesBase() {
        // Make nss
        NamespaceString nss(StringData("timeseries-timeseries"), StringData("test"));
        _tsCache = TimeSeriesCache(nss);
    }

protected:
    insertI(int i) {
        BSONObjBuilder builder;
        builder.append("_id", Date_t::fromMillisSinceEpoch(i));
        builder.append("num", i);
        cache.insert(_txnPtr, builder.obj());
    }

    const ServiceContext::UniqueOperationContext _txnPtr = cc().makeOperationContext();
    OperationContext& _txn = *_txnPtr;
};

/** Try inserting stuff into a TS collection */
class SimpleInsert : public TimeseriesBase {
public:
    void run() {
        for (int i = 0; i < 100; i++) {
            insertI(i);
        }
    }
};

class All : public Suite {
public:
    All() : Suite("timeseries") {}

    void setupTests() {
        add<SimpleInsert>();

    }
};

SuiteInstance<All> myall;

}  // namespace NamespaceTests
