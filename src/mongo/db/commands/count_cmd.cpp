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
#include "mongo/db/views/view_sharding_check.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::stringstream;

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

    ReadWriteType getReadWriteType() const {
        return ReadWriteType::kRead;
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

        /* Check if this is running on a view */
        bool isExplain = true;
        auto qr = QueryRequest::makeFromCountRequest(request.getValue(), isExplain);

        const NamespaceString nss(parseNs(dbname, cmdObj));

        // Acquire the db read lock.
        AutoGetCollectionOrViewForRead ctx(txn, request.getValue().getNs());
        Collection* collection = ctx.getCollection();

        // Are we counting on a view?
        if (auto view = ctx.getView()) {
            ViewShardingCheck viewShardingCheck(txn, ctx.getDb(), view);
            if (!viewShardingCheck.canRunOnMongod()) {
                viewShardingCheck.appendResolvedView(*out);

                Status status({ErrorCodes::ViewMustRunOnMongos,
                               str::stream() << "Command on view must be executed by mongos"});
                appendCommandStatus(*out, status);
                return status;
            }

            ctx.unlock();

            auto query = qr.get();
            Status viewValidationStatus = query->validateForView();
            if (!viewValidationStatus.isOK()) {
                return viewValidationStatus;
            }
            std::string errmsg;
            BSONObj agg = qr->asAggregationCommand("count");
            Command* c = Command::findCommand("aggregate");
            try {
                c->run(txn, dbname, agg, 0, errmsg, *out);
            } catch (DBException& e) {
                auto errorCode = e.getCode();
                if (errorCode == ErrorCodes::InvalidPipelineOperator) {
                    return {ErrorCodes::InvalidPipelineOperator,
                            str::stream() << "Unsupported in view pipeline: " << e.what()};
                }
                return e.toStatus();
            }
            return Status::OK();
        }


        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(
            txn, collection, request.getValue(), std::move(qr), PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return statusWithPlanExecutor.getStatus();
        }

        unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        Explain::explainStages(exec.get(), collection, verbosity, out);
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

        bool isExplain = false;
        auto qr = QueryRequest::makeFromCountRequest(request.getValue(), isExplain);

        const NamespaceString nss(parseNs(dbname, cmdObj));

        // Acquire locks.
        AutoGetCollectionOrViewForRead ctx(txn, nss);
        Collection* collection = ctx.getCollection();

        // Are we counting on a view?
        if (auto view = ctx.getView()) {
            ViewShardingCheck viewShardingCheck(txn, ctx.getDb(), view);
            if (!viewShardingCheck.canRunOnMongod()) {
                viewShardingCheck.appendResolvedView(result);

                return appendCommandStatus(
                    result,
                    {ErrorCodes::ViewMustRunOnMongos,
                     str::stream() << "Command on view must be executed by mongos"});
            }

            ctx.unlock();
            auto query = qr.get();
            Status viewValidationStatus = query->validateForView();
            if (!viewValidationStatus.isOK()) {
                return appendCommandStatus(result, viewValidationStatus);
            }
            BSONObj agg = query->asAggregationCommand("count");
            Command* c = Command::findCommand("aggregate");
            try {
                c->run(txn, dbname, agg, options, errmsg, result);
            } catch (DBException& e) {
                auto errorCode = e.getCode();
                if (errorCode == ErrorCodes::InvalidPipelineOperator) {
                    return appendCommandStatus(
                        result,
                        {ErrorCodes::InvalidPipelineOperator,
                         str::stream() << "Unsupported in view pipeline: " << e.what()});
                }
                return appendCommandStatus(result, e.toStatus());
            }
            return true;
        }


        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(
            txn, collection, request.getValue(), std::move(qr), PlanExecutor::YIELD_AUTO);
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
