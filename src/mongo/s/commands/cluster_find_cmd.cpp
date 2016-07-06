/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include <boost/optional.hpp>

#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/stats/counters.h"
#include "mongo/s/commands/strategy.h"
#include "mongo/s/query/cluster_find.h"
#include "mongo/s/query/cluster_view_util.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::vector;

const char kTermField[] = "term";

/**
 * Implements the find command on mongos.
 */
class ClusterFindCmd : public Command {
    MONGO_DISALLOW_COPYING(ClusterFindCmd);

public:
    ClusterFindCmd() : Command("find") {}


    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool slaveOk() const final {
        return false;
    }

    bool slaveOverrideOk() const final {
        return true;
    }

    bool maintenanceOk() const final {
        return false;
    }

    bool adminOnly() const final {
        return false;
    }

    bool shouldAffectCommandCounter() const final {
        return false;
    }

    void help(std::stringstream& help) const final {
        help << "query for documents";
    }

    /**
     * In order to run the find command, you must be authorized for the "find" action
     * type on the collection.
     */
    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
        NamespaceString nss(parseNs(dbname, cmdObj));
        auto hasTerm = cmdObj.hasField(kTermField);
        return AuthorizationSession::get(client)->checkAuthForFind(nss, hasTerm);
    }

    Status explain(OperationContext* txn,
                   const std::string& dbname,
                   const BSONObj& cmdObj,
                   ExplainCommon::Verbosity verbosity,
                   const rpc::ServerSelectionMetadata& serverSelectionMetadata,
                   BSONObjBuilder* out) const final {
        const string fullns = parseNs(dbname, cmdObj);
        const NamespaceString nss(fullns);
        if (!nss.isValid()) {
            return {ErrorCodes::InvalidNamespace,
                    str::stream() << "Invalid collection name: " << nss.ns()};
        }

        // Parse the command BSON to a QueryRequest.
        bool isExplain = true;
        auto qr = QueryRequest::makeFromFindCommand(std::move(nss), cmdObj, isExplain);
        if (!qr.isOK()) {
            return qr.getStatus();
        }

        auto result = Strategy::explainFind(
            txn, cmdObj, *qr.getValue(), verbosity, serverSelectionMetadata, out);

        if (result == ErrorCodes::ViewMustRunOnMongos) {
            auto viewDef = ClusterViewDecoration::getResolvedView(txn);
            invariant(!viewDef.isEmpty());

            auto aggCmd = convertViewToAgg(
                txn, viewDef["ns"].valueStringData(), viewDef["pipeline"].Array(), cmdObj, true);

            if (aggCmd.isOK()) {
                out->resetToEmpty();
                Command* c = Command::findCommand("aggregate");
                int queryOptions = 0;  // TODO: Is this correct?
                std::string errMsg;
                bool retval = c->run(txn, dbname, aggCmd.getValue(), queryOptions, errMsg, *out);

                if (retval) {
                    return Status::OK();
                }

                BSONObj tmp = out->asTempObj();
                return {ErrorCodes::fromInt(tmp["code"].numberInt()), tmp["errmsg"]};
            }

            return aggCmd.getStatus();
        }

        return result;
    }

    StatusWith<BSONObj> convertViewToAgg(OperationContext* txn,
                                         StringData resolvedViewNs,
                                         const std::vector<BSONElement>& resolvedViewPipeline,
                                         const BSONObj& cmdObj,
                                         bool isExplain) const {
        NamespaceString nss(resolvedViewNs);

        auto qrStatus = QueryRequest::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!qrStatus.isOK()) {
            return qrStatus.getStatus();
        }

        auto& qr = qrStatus.getValue();

        auto statusWithCQ =
            CanonicalQuery::canonicalize(txn, std::move(qr), ExtensionsCallbackNoop());
        if (!statusWithCQ.isOK()) {
            return statusWithCQ.getStatus();
        }
        std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        auto& queryRequest = cq->getQueryRequest();
        Status viewValidationStatus = queryRequest.validateForView();
        if (!viewValidationStatus.isOK()) {
            return viewValidationStatus;
        }

        return queryRequest.asExpandedViewAggregation("", resolvedViewPipeline);
    }

    bool run(OperationContext* txn,
             const std::string& dbname,
             BSONObj& cmdObj,
             int options,
             std::string& errmsg,
             BSONObjBuilder& result) final {

        auto ok = executeFind(txn, dbname, cmdObj, options, errmsg, result);

        if (!ok) {
            BSONObj tmp = result.asTempObj();
            if (tmp.hasField("code")) {  // TODO - can we assume 'code' exists?
                auto codeElement = tmp.getField("code");
                auto code = codeElement.Int();

                if (code == ErrorCodes::ViewMustRunOnMongos) {
                    auto viewDef = ClusterViewDecoration::getResolvedView(txn);
                    invariant(!viewDef.isEmpty());

                    auto aggCmd = convertViewToAgg(txn,
                                                   viewDef["ns"].valueStringData(),
                                                   viewDef["pipeline"].Array(),
                                                   cmdObj,
                                                   false);
                    if (aggCmd.isOK()) {
                        result.resetToEmpty();
                        Command* c = Command::findCommand("aggregate");
                        bool retval =
                            c->run(txn, dbname, aggCmd.getValue(), options, errmsg, result);
                        return retval;
                    }

                    return appendCommandStatus(result, aggCmd.getStatus());
                }
            }
        }

        return ok;
    }

private:
    bool executeFind(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        // We count find command as a query op.
        globalOpCounters.gotQuery();

        const NamespaceString nss(parseNs(dbname, cmdObj));
        if (!nss.isValid()) {
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace,
                                        str::stream() << "Invalid collection name: " << nss.ns()});
        }

        const bool isExplain = false;
        auto qr = QueryRequest::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!qr.isOK()) {
            return appendCommandStatus(result, qr.getStatus());
        }

        auto cq =
            CanonicalQuery::canonicalize(txn, std::move(qr.getValue()), ExtensionsCallbackNoop());
        if (!cq.isOK()) {
            return appendCommandStatus(result, cq.getStatus());
        }

        // Extract read preference. If no read preference is specified in the query, will we pass
        // down a "primaryOnly" or "secondary" read pref, depending on the slaveOk setting.
        auto readPref =
            ClusterFind::extractUnwrappedReadPref(cmdObj, options & QueryOption_SlaveOk);
        if (!readPref.isOK()) {
            return appendCommandStatus(result, readPref.getStatus());
        }

        // Do the work to generate the first batch of results. This blocks waiting to get responses
        // from the shard(s).
        std::vector<BSONObj> batch;
        auto cursorId = ClusterFind::runQuery(txn, *cq.getValue(), readPref.getValue(), &batch);
        if (!cursorId.isOK()) {
            return appendCommandStatus(result, cursorId.getStatus());
        }

        // Build the response document.
        CursorResponseBuilder firstBatch(/*firstBatch*/ true, &result);
        for (const auto& obj : batch) {
            firstBatch.append(obj);
        }
        firstBatch.done(cursorId.getValue(), nss.ns());
        return true;
    }

} cmdFindCluster;

}  // namespace
}  // namespace mongo
