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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"


#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/count.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/range_preserver.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::stringstream;

bool isValidQuery(const BSONObj& o) {
    // log() << "Query: " << o.jsonString();
    for (BSONElement e: o) {
        // log() << "Element: " << e;
        if (e.type() == Object || e.type() == Array) {
            if (!isValidQuery(e.Obj())) {
                return false;
            }
        } else {
            StringData fieldName = e.fieldNameStringData();
            if (fieldName == "$where" || fieldName == "$elemMatch" || 
                fieldName == "geo" || fieldName == "loc") {
                return false;
            }
        }
    }
    return true;
}

BSONObj convertToAggregate(const BSONObj& cmd, bool hasExplain) {
    log() << "Converting to aggregate command: " << cmd.jsonString();
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
    if (cmd.hasField("query")) {
        BSONObj value = cmd.getObjectField("query");
        if (!isValidQuery(value)) {
            return BSONObj();
        }
        pipeline.push_back(BSON("$match" << value));
    }
    if (cmd.hasField("skip")) {
        pipeline.push_back(BSON("$skip" << cmd.getIntField("skip")));
    }
    if (cmd.hasField("limit")) {
        int value = cmd.getIntField("limit");
        if (value < 0) value = -value;
        pipeline.push_back(BSON("$limit" << value));
    }

    pipeline.push_back(BSON("$group" << BSON("_id" << BSONNULL << "count" << BSON("$sum" << 1))));

    b.append("aggregate", cmd["count"].str());
    b.append("pipeline", pipeline);

    if (cmd.hasField("maxTimeMS")) {
        b.append("maxTimeMS", cmd.getIntField("maxTimeMS"));
    }

    return b.obj();
}

/**
 * Implements the MongoD side of the count command.
 */
class CmdCount : public Command {
public:
    CmdCount() : Command("count") {}
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    virtual bool slaveOk() const {
        // ok on --slave setups
        return repl::getGlobalReplicationCoordinator()->getSettings().isSlave();
    }
    virtual bool slaveOverrideOk() const {
        return true;
    }
    virtual bool maintenanceOk() const {
        return false;
    }
    virtual bool adminOnly() const {
        return false;
    }
    bool supportsReadConcern() const final {
        return true;
    }
    virtual void help(stringstream& help) const {
        help << "count objects in collection";
    }
    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::find);
        out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
    }

    virtual Status explain(OperationContext* txn,
                           const std::string& dbname,
                           const BSONObj& cmdObj,
                           ExplainCommon::Verbosity verbosity,
                           const rpc::ServerSelectionMetadata&,
                           BSONObjBuilder* out) const {
        auto request = CountRequest::parseFromBSON(dbname, cmdObj);
        if (!request.isOK()) {
            return request.getStatus();
        }

        // Acquire the db read lock.
        AutoGetCollectionForRead ctx(txn, request.getValue().getNs());
        Collection* collection = ctx.getCollection();

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(txn,
                                                       collection,
                                                       request.getValue(),
                                                       true,  // explain
                                                       PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return statusWithPlanExecutor.getStatus();
        }

        unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        Explain::explainStages(exec.get(), verbosity, out);
        return Status::OK();
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        auto request = CountRequest::parseFromBSON(dbname, cmdObj);
        if (!request.isOK()) {
            return appendCommandStatus(result, request.getStatus());
        }

        log() << cmdObj.jsonString();

        const NamespaceString nss(parseNs(dbname, cmdObj)); 

        // Are we counting on a view?
        // if (ViewCatalog::getInstance()->lookup(txn, nss.ns())) {
        //     log() << "Look up on a view";
        //     BSONObj agg = convertToAggregate(cmdObj, false);
        //     if (!agg.isEmpty()) {
        //         Command *c = Command::findCommand("aggregate");
        //         bool retval = c->run(txn, dbname, agg, options, errmsg, result);
        //         return retval;
        //     }
        // }

        // Acquire locks.
        boost::optional<AutoGetCollectionForRead> ctx;
        ctx.emplace(txn, nss);
        Collection* collection = ctx->getCollection();

        if (collection) {
            // log() << "Look up on a view";
            BSONObj agg = convertToAggregate(cmdObj, false);
            if (!agg.isEmpty()) {
                ctx = boost::none;
                Command *c = Command::findCommand("aggregate");
                bool retval = c->run(txn, dbname, agg, options, errmsg, result);
                return retval;
            }
        }

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(txn,
                                                       collection,
                                                       request.getValue(),
                                                       false,  // !explain
                                                       PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return appendCommandStatus(result, statusWithPlanExecutor.getStatus());
        }

        unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        // Store the plan summary string in CurOp.
        auto curOp = CurOp::get(txn);
        {
            stdx::lock_guard<Client> lk(*txn->getClient());
            curOp->setPlanSummary_inlock(Explain::getPlanSummary(exec.get()));
        }

        Status execPlanStatus = exec->executePlan();
        if (!execPlanStatus.isOK()) {
            return appendCommandStatus(result, execPlanStatus);
        }

        PlanSummaryStats summaryStats;
        Explain::getSummaryStats(*exec, &summaryStats);
        if (collection) {
            collection->infoCache()->notifyOfQuery(txn, summaryStats.indexesUsed);
        }
        curOp->debug().setPlanSummaryMetrics(summaryStats);

        if (curOp->shouldDBProfile(curOp->elapsedMillis())) {
            BSONObjBuilder execStatsBob;
            Explain::getWinningPlanStats(exec.get(), &execStatsBob);
            curOp->debug().execStats = execStatsBob.obj();
        }

        // Plan is done executing. We just need to pull the count out of the root stage.
        invariant(STAGE_COUNT == exec->getRootStage()->stageType());
        CountStage* countStage = static_cast<CountStage*>(exec->getRootStage());
        const CountStats* countStats =
            static_cast<const CountStats*>(countStage->getSpecificStats());

        result.appendNumber("n", countStats->nCounted);
        return true;
    }

} cmdCount;

}  // namespace
}  // namespace mongo
