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

#include "mongo/db/timeseries/timeseries_monitor.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

namespace mongo {
const auto getTimeseriesCacheMonitor = ServiceContext::declareDecoration<TimeSeriesCacheMonitor>();

MONGO_EXPORT_SERVER_PARAMETER(timeseriesCacheMonitorSleepSecs, int, 1);

TimeSeriesCacheMonitor& TimeSeriesCacheMonitor::get(ServiceContext* ctx) {
    return getTimeseriesCacheMonitor(ctx);
}
void TimeSeriesCacheMonitor::run() {
    Client::initThread(name().c_str());
    AuthorizationSession::get(cc())->grantInternalAuthorization();

    while (!inShutdown()) {
        sleepsecs(timeseriesCacheMonitorSleepSecs);
        const ServiceContext::UniqueOperationContext txnPtr = cc().makeOperationContext();
        OperationContext& txn = *txnPtr;
        std::unordered_set<std::string> removed;
        for (const std::string& ns : _timeseriesNssCache) {
            log() << "Looking at " << ns;
            NamespaceString nss(ns);
            AutoGetCollectionOrTimeseries autoColl(&txn, nss, LockMode::MODE_IX);
            if (!autoColl.isTimeseries()) {
                removed.insert(ns);
                continue;
            }
            autoColl.getTimeseriesCache()->flushIfNecessary(&txn);
        }
        _timeseriesNssCache.erase(removed.begin(), removed.end());
    }
}

void TimeSeriesCacheMonitor::registerView(const NamespaceString& viewNss) {
    _timeseriesNssCache.insert(viewNss.toString());
}

}  // namespace mongo