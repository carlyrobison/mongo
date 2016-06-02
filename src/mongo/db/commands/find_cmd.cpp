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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/base/disallow_copying.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/service_context.h"
#include "mongo/db/matcher/extensions_callback_real.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/find.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/stats/counters.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {

const char kTermField[] = "term";

BSONObj convertToAggregate(const BSONObj& cmd, bool hasExplain) {
    BSONObjBuilder b;
    
    std::vector<BSONObj> pipeline;

    // Do not support single batch
    if (cmd.getBoolField("singleBatch") || cmd.hasField("hint") || 
        cmd.hasField("maxScan") || cmd.hasField("max") || cmd.hasField("min") ||
        cmd.hasField("returnKey") || cmd.hasField("tailable") || cmd.hasField("showRecordId") ||
        cmd.hasField("snapshot") || cmd.hasField("oplogReplay") || cmd.hasField("noCursorTimeut") ||
        cmd.hasField("awaitData") || cmd.hasField("allowPartialResults")) {
        return BSONObj();
    }
    
    // Build the pipeline
    if (cmd.hasField("filter")) {
        BSONObj value = cmd.getObjectField("filter");
        if (value.hasField("$where") || value.hasField("geo") || 
            value.hasField("$elemMatch") || value.hasField("loc")) {
            // Do not support $where, $near, or $elemMatch
            return BSONObj();
        }
        pipeline.push_back(BSON("$match" << value));
    }
    if (cmd.hasField("sort")) {
        BSONObj value = cmd.getObjectField("sort");
        if (value.hasField("$natural")) {
            // Do not support $natural
            return BSONObj();
        }
        pipeline.push_back(BSON("$sort" << value));
    }
    if (cmd.hasField("skip")) {
        pipeline.push_back(BSON("$skip" << cmd.getIntField("skip")));
    }
    if (cmd.hasField("limit")) {
        pipeline.push_back(BSON("$limit" << cmd.getIntField("limit")));
    }
    if (cmd.hasField("projection")) {
        BSONObj value = cmd.getObjectField("projection");
        if (!value.isEmpty()) {
            pipeline.push_back(BSON("$project" << value));
        }
        for (BSONElement e: value) {
            // Only support simple 0 or 1 projection
            const char * fieldName = e.fieldName();
            // log() << fieldName << "size: " << e.fieldNameSize() << "last char: " << fieldName[e.fieldNameSize() - 1];
            if (fieldName[e.fieldNameSize() - 2] == '$') {
                return BSONObj();
            }
            if (!e.isNumber()) {
                return BSONObj();
            }
        }   
    }

    b.append("aggregate", cmd["find"].str());
    b.append("pipeline", pipeline);

    if (cmd.hasField("batchSize")) {
        b.append("cursor", BSON("batchSize" << cmd.getIntField("batchSize")));
    }
    else {
        b.append("cursor", BSONObj());
    }
    if (hasExplain) {
        b.append("explain", true);
    }
    if (cmd.hasField("maxTimeMS")) {
        b.append("maxTimeMS", cmd.getIntField("maxTimeMS"));
    }
   
    return b.obj();
}

}  // namespace

/**
 * A command for running .find() queries.
 */
class FindCmd : public Command {
    MONGO_DISALLOW_COPYING(FindCmd);

public:
    FindCmd() : Command("find") {}


    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool slaveOk() const override {
        return false;
    }

    bool slaveOverrideOk() const override {
        return true;
    }

    bool maintenanceOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return false;
    }

    bool supportsReadConcern() const final {
        return true;
    }

    void help(std::stringstream& help) const override {
        help << "query for documents";
    }

    LogicalOp getLogicalOp() const override {
        return LogicalOp::opQuery;
    }

    std::size_t reserveBytesForReply() const override {
        return FindCommon::kInitReplyBufferSize;
    }

    /**
     * A find command does not increment the command counter, but rather increments the
     * query counter.
     */
    bool shouldAffectCommandCounter() const override {
        return false;
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) override {
        NamespaceString nss(parseNs(dbname, cmdObj));
        auto hasTerm = cmdObj.hasField(kTermField);
        return AuthorizationSession::get(client)->checkAuthForFind(nss, hasTerm);
    }

    Status explain(OperationContext* txn,
                   const std::string& dbname,
                   const BSONObj& cmdObj,
                   ExplainCommon::Verbosity verbosity,
                   const rpc::ServerSelectionMetadata&,
                   BSONObjBuilder* out) const override {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        if (!nss.isValid()) {
            return {ErrorCodes::InvalidNamespace,
                    str::stream() << "Invalid collection name: " << nss.ns()};
        }

        // Parse the command BSON to a LiteParsedQuery.
        const bool isExplain = true;
        auto lpqStatus = LiteParsedQuery::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!lpqStatus.isOK()) {
            return lpqStatus.getStatus();
        }

        // Finish the parsing step by using the LiteParsedQuery to create a CanonicalQuery.

        ExtensionsCallbackReal extensionsCallback(txn, &nss);
        auto statusWithCQ =
            CanonicalQuery::canonicalize(txn, std::move(lpqStatus.getValue()), extensionsCallback);
        if (!statusWithCQ.isOK()) {
            return statusWithCQ.getStatus();
        }
        std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        AutoGetCollectionForRead ctx(txn, nss);
        // The collection may be NULL. If so, getExecutor() should handle it by returning
        // an execution tree with an EOFStage.
        Collection* collection = ctx.getCollection();

        /* Collection does not exist - check for a view */
        if (!collection) {
            BSONObj explainCmd = convertToAggregate(cmdObj, true);
            if (!explainCmd.isEmpty()) {
                Command *c = Command::findCommand("aggregate");
                std::string errMsg;
                bool retVal = c->run(txn, dbname, explainCmd, 0, errMsg, *out);
                if (retVal) {
                    return Status::OK();
                }
            }
        }
        
        // We have a parsed query. Time to get the execution plan for it.
        auto statusWithPlanExecutor =
            getExecutorFind(txn, collection, nss, std::move(cq), PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return statusWithPlanExecutor.getStatus();
        }
        std::unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        // Got the execution tree. Explain it.
        Explain::explainStages(exec.get(), verbosity, out);
        return Status::OK();
    }

    /**
     * Runs a query using the following steps:
     *   --Parsing.
     *   --Acquire locks.
     *   --Plan query, obtaining an executor that can run it.
     *   --Generate the first batch.
     *   --Save state for getMore, transferring ownership of the executor to a ClientCursor.
     *   --Generate response to send to the client.
     */
    bool run(OperationContext* txn,
             const std::string& dbname,
             BSONObj& cmdObj,
             int options,
             std::string& errmsg,
             BSONObjBuilder& result) override {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        if (!nss.isValid() || nss.isCommand() || nss.isSpecialCommand()) {
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace,
                                        str::stream() << "Invalid collection name: " << nss.ns()});
        }

        // Although it is a command, a find command gets counted as a query.
        globalOpCounters.gotQuery();

        if (txn->getClient()->isInDirectClient()) {
            return appendCommandStatus(
                result,
                Status(ErrorCodes::IllegalOperation, "Cannot run find command from eval()"));
        }

        // Parse the command BSON to a LiteParsedQuery.
        const bool isExplain = false;
        auto lpqStatus = LiteParsedQuery::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!lpqStatus.isOK()) {
            return appendCommandStatus(result, lpqStatus.getStatus());
        }

        auto& lpq = lpqStatus.getValue();

        // Validate term before acquiring locks, if provided.
        if (auto term = lpq->getReplicationTerm()) {
            auto replCoord = repl::ReplicationCoordinator::get(txn);
            Status status = replCoord->updateTerm(txn, *term);
            // Note: updateTerm returns ok if term stayed the same.
            if (!status.isOK()) {
                return appendCommandStatus(result, status);
            }
        }

        // Fill out curop information.
        //
        // We pass negative values for 'ntoreturn' and 'ntoskip' to indicate that these values
        // should be omitted from the log line. Limit and skip information is already present in the
        // find command parameters, so these fields are redundant.
        const int ntoreturn = -1;
        const int ntoskip = -1;
        beginQueryOp(txn, nss, cmdObj, ntoreturn, ntoskip);

        // Finish the parsing step by using the LiteParsedQuery to create a CanonicalQuery.
        ExtensionsCallbackReal extensionsCallback(txn, &nss);
        auto statusWithCQ = CanonicalQuery::canonicalize(txn, std::move(lpq), extensionsCallback);
        if (!statusWithCQ.isOK()) {
            return appendCommandStatus(result, statusWithCQ.getStatus());
        }
        std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        // Acquire locks. 
        boost::optional<AutoGetCollectionForRead> ctx;
        ctx.emplace(txn, nss);
        Collection* collection = ctx->getCollection();
        
        // Collection does not exist. Check for a view instead
        if (collection) {
            BSONObj match = convertToAggregate(cmdObj, false);
            log() << cmdObj.jsonString();
            if (!match.isEmpty()) {
                log() << match.jsonString();
                ctx = boost::none;
                Command *c = Command::findCommand("aggregate");
                bool retval = c->run(txn, dbname, match, options, errmsg, result);
                return retval;
            }
        }

        // Get the execution plan for the query.
        auto statusWithPlanExecutor =
            getExecutorFind(txn, collection, nss, std::move(cq), PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return appendCommandStatus(result, statusWithPlanExecutor.getStatus());
        }

        std::unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        {
            stdx::lock_guard<Client>(*txn->getClient());
            CurOp::get(txn)->setPlanSummary_inlock(Explain::getPlanSummary(exec.get()));
        }

        if (!collection) {
            // No collection. Just fill out curop indicating that there were zero results and
            // there is no ClientCursor id, and then return.
            const long long numResults = 0;
            const CursorId cursorId = 0;
            endQueryOp(txn, collection, *exec, numResults, cursorId);
            appendCursorResponseObject(cursorId, nss.ns(), BSONArray(), &result);
            return true;
        }

        const LiteParsedQuery& pq = exec->getCanonicalQuery()->getParsed();

        // Stream query results, adding them to a BSONArray as we go.
        CursorResponseBuilder firstBatch(/*isInitialResponse*/ true, &result);
        BSONObj obj;
        PlanExecutor::ExecState state = PlanExecutor::ADVANCED;
        long long numResults = 0;
        while (!FindCommon::enoughForFirstBatch(pq, numResults) &&
               PlanExecutor::ADVANCED == (state = exec->getNext(&obj, NULL))) {
            // If we can't fit this result inside the current batch, then we stash it for later.
            if (!FindCommon::haveSpaceForNext(obj, numResults, firstBatch.bytesUsed())) {
                exec->enqueue(obj);
                break;
            }

            // Add result to output buffer.
            firstBatch.append(obj);
            numResults++;
        }

        // Throw an assertion if query execution fails for any reason.
        if (PlanExecutor::FAILURE == state || PlanExecutor::DEAD == state) {
            firstBatch.abandon();
            error() << "Plan executor error during find command: " << PlanExecutor::statestr(state)
                    << ", stats: " << Explain::getWinningPlanStats(exec.get());

            return appendCommandStatus(result,
                                       Status(ErrorCodes::OperationFailed,
                                              str::stream()
                                                  << "Executor error during find command: "
                                                  << WorkingSetCommon::toStatusString(obj)));
        }

        // Before saving the cursor, ensure that whatever plan we established happened with the
        // expected collection version
        auto css = CollectionShardingState::get(txn, nss);
        css->checkShardVersionOrThrow(txn);

        // Set up the cursor for getMore.
        CursorId cursorId = 0;
        if (shouldSaveCursor(txn, collection, state, exec.get())) {
            // Register the execution plan inside a ClientCursor. Ownership of the PlanExecutor is
            // transferred to the ClientCursor.
            //
            // First unregister the PlanExecutor so it can be re-registered with ClientCursor.
            exec->deregisterExec();

            // Create a ClientCursor containing this plan executor. We don't have to worry about
            // leaking it as it's inserted into a global map by its ctor.
            ClientCursor* cursor =
                new ClientCursor(collection->getCursorManager(),
                                 exec.release(),
                                 nss.ns(),
                                 txn->recoveryUnit()->isReadingFromMajorityCommittedSnapshot(),
                                 pq.getOptions(),
                                 pq.getFilter());
            cursorId = cursor->cursorid();

            invariant(!exec);
            PlanExecutor* cursorExec = cursor->getExecutor();

            // State will be restored on getMore.
            cursorExec->saveState();
            cursorExec->detachFromOperationContext();

            cursor->setLeftoverMaxTimeMicros(txn->getRemainingMaxTimeMicros());
            cursor->setPos(numResults);

            // Fill out curop based on the results.
            endQueryOp(txn, collection, *cursorExec, numResults, cursorId);
        } else {
            endQueryOp(txn, collection, *exec, numResults, cursorId);
        }

        // Generate the response object to send to the client.
        firstBatch.done(cursorId, nss.ns());
        return true;
    }

} findCmd;

}  // namespace mongo
