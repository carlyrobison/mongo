/**
 * Copyright (c) 2011-2014 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <cctype>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/pipeline_proxy.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/views/view.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/db/views/view_sharding_check.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {
// BSONObj concatenate(BSONObj b1, BSONObj b2) {
//     BSONObjBuilder b;
//     std::vector<BSONObj> pipeline;
//     for (BSONElement e: b2.getObjectField("pipeline")) {
//         pipeline.push_back(e.Obj());
//     }
//     for (BSONElement e: b2.getObjectField("pipeline")) {
//         pipeline.push_back(e.Obj());
//     }
//     return b.append("pipeline", pipeline).obj();
// }
}

using boost::intrusive_ptr;
using std::endl;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unique_ptr;
using stdx::make_unique;

/**
 * Returns true if we need to keep a ClientCursor saved for this pipeline (for future getMore
 * requests).  Otherwise, returns false.
 */
static bool handleCursorCommand(OperationContext* txn,
                                const string& ns,
                                ClientCursorPin* pin,
                                PlanExecutor* exec,
                                const BSONObj& cmdObj,
                                BSONObjBuilder& result) {
    ClientCursor* cursor = pin ? pin->c() : NULL;
    if (pin) {
        invariant(cursor);
        invariant(cursor->getExecutor() == exec);
        invariant(cursor->isAggCursor());
    }

    const long long defaultBatchSize = 101;  // Same as query.
    long long batchSize;
    uassertStatusOK(Command::parseCommandCursorOptions(cmdObj, defaultBatchSize, &batchSize));

    // can't use result BSONObjBuilder directly since it won't handle exceptions correctly.
    BSONArrayBuilder resultsArray;
    BSONObj next;
    for (int objCount = 0; objCount < batchSize; objCount++) {
        // The initial getNext() on a PipelineProxyStage may be very expensive so we don't
        // do it when batchSize is 0 since that indicates a desire for a fast return.
        PlanExecutor::ExecState state;
        if ((state = exec->getNext(&next, NULL)) == PlanExecutor::IS_EOF) {
            // make it an obvious error to use cursor or executor after this point
            cursor = NULL;
            exec = NULL;
            break;
        }

        uassert(34426,
                "Plan executor error during aggregation: " + WorkingSetCommon::toStatusString(next),
                PlanExecutor::ADVANCED == state);

        // If adding this object will cause us to exceed the message size limit, then we stash it
        // for later.
        if (!FindCommon::haveSpaceForNext(next, objCount, resultsArray.len())) {
            exec->enqueue(next);
            break;
        }

        resultsArray.append(next);
    }

    // NOTE: exec->isEOF() can have side effects such as writing by $out. However, it should
    // be relatively quick since if there was no pin then the input is empty. Also, this
    // violates the contract for batchSize==0. Sharding requires a cursor to be returned in that
    // case. This is ok for now however, since you can't have a sharded collection that doesn't
    // exist.
    const bool canReturnMoreBatches = pin;
    if (!canReturnMoreBatches && exec && !exec->isEOF()) {
        // msgasserting since this shouldn't be possible to trigger from today's aggregation
        // language. The wording assumes that the only reason pin would be null is if the
        // collection doesn't exist.
        msgasserted(
            17391,
            str::stream() << "Aggregation has more results than fit in initial batch, but can't "
                          << "create cursor since collection "
                          << ns
                          << " doesn't exist");
    }

    if (cursor) {
        // If a time limit was set on the pipeline, remaining time is "rolled over" to the
        // cursor (for use by future getmore ops).
        cursor->setLeftoverMaxTimeMicros(txn->getRemainingMaxTimeMicros());

        CurOp::get(txn)->debug().cursorid = cursor->cursorid();

        // Cursor needs to be in a saved state while we yield locks for getmore. State
        // will be restored in getMore().
        exec->saveState();
        exec->detachFromOperationContext();
    } else {
        CurOp::get(txn)->debug().cursorExhausted = true;
    }

    const long long cursorId = cursor ? cursor->cursorid() : 0LL;
    appendCursorResponseObject(cursorId, ns, resultsArray.arr(), &result);

    return static_cast<bool>(cursor);
}


class PipelineCommand : public Command {
public:
    PipelineCommand() : Command(Pipeline::commandName) {}  // command is called "aggregate"

    // Locks are managed manually, in particular by DocumentSourceCursor.
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return Pipeline::aggSupportsWriteConcern(cmd);
    }

    virtual bool slaveOk() const {
        return false;
    }

    virtual bool slaveOverrideOk() const {
        return true;
    }

    bool supportsReadConcern() const final {
        return true;
    }

    ReadWriteType getReadWriteType() const {
        return ReadWriteType::kRead;
    }

    virtual void help(stringstream& help) const {
        help << "{ pipeline: [ { $operator: {...}}, ... ]"
             << ", explain: <bool>"
             << ", allowDiskUse: <bool>"
             << ", cursor: {batchSize: <number>}"
             << " }" << endl
             << "See http://dochub.mongodb.org/core/aggregation for more details.";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
        return Pipeline::checkAuthForCommand(client, dbname, cmdObj);
    }

    virtual bool run(OperationContext* txn,
                     const string& db,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        std::string ns = parseNs(db, cmdObj);
        if (nsToCollectionSubstring(ns).empty()) {
            errmsg = "missing collection name";
            return false;
        }

        const NamespaceString nss(ns);

        intrusive_ptr<ExpressionContext> pCtx = new ExpressionContext(txn, nss);
        pCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";

        /* try to parse the command; if this fails, then we didn't run */
        intrusive_ptr<Pipeline> pPipeline = Pipeline::parseCommand(errmsg, cmdObj, pCtx);
        if (!pPipeline.get())
            return false;

        // This is outside of the if block to keep the object alive until the pipeline is finished.
        BSONObj parsed;
        if (kDebugBuild && !pPipeline->isExplain() && !pCtx->inShard) {
            // Make sure all operations round-trip through Pipeline::toBson() correctly by
            // reparsing every command in debug builds. This is important because sharded
            // aggregations rely on this ability.  Skipping when inShard because this has
            // already been through the transformation (and this unsets pCtx->inShard).
            parsed = pPipeline->serialize().toBson();
            pPipeline = Pipeline::parseCommand(errmsg, parsed, pCtx);
            verify(pPipeline);
        }

        unique_ptr<ClientCursorPin> pin;  // either this OR the exec will be non-null
        unique_ptr<PlanExecutor> exec;
        auto curOp = CurOp::get(txn);
        {
            // Acquire locks.
            // This will throw if the sharding version for this connection is out of date. The
            // lock must be held continuously from now until we have we created both the output
            // ClientCursor and the input executor. This ensures that both are using the same
            // sharding version that we synchronize on here. This is also why we always need to
            // create a ClientCursor even when we aren't outputting to a cursor. See the comment
            // on ShardFilterStage for more details.
            AutoGetCollectionOrViewForRead ctx(txn, ns);
            Collection* collection = ctx.getCollection();

            // If this is a view, resolve it.
            if (auto view = ctx.getView()) {
                ViewShardingCheck viewShardingCheck(txn, ctx.getDb(), view);
                if (!viewShardingCheck.canRunOnMongod()) {
                    viewShardingCheck.appendResolvedView(result);

                    return appendCommandStatus(
                        result,
                        {ErrorCodes::ViewMustRunOnMongos,
                         str::stream() << "Command on view must be executed by mongos"});
                }

                auto newAggregation = ctx.getDb()->getViewCatalog()->resolveView(txn, ns);
                std::string rootNs = std::get<0>(newAggregation);
                std::vector<BSONObj> viewPipeline = std::get<1>(newAggregation);
                // TODO: Use AggregationRequest object for transformation when available.
                BSONObj viewCmd =
                    ViewDefinition::pipelineToViewAggregation(rootNs, viewPipeline, cmdObj);

                LOG(2) << "VIEWS: Final pipeline: " << viewCmd.jsonString();
                ctx.unlock();
                return this->run(txn, db, viewCmd, options, errmsg, result);
            }

            // If the pipeline does not have a user-specified collation, set it from the
            // collection default.
            if (pPipeline->getContext()->collation.isEmpty() && collection &&
                collection->getDefaultCollator()) {
                pPipeline->setCollator(collection->getDefaultCollator()->clone());
            }

            // This does mongod-specific stuff like creating the input PlanExecutor and adding
            // it to the front of the pipeline if needed.
            std::shared_ptr<PlanExecutor> input =
                PipelineD::prepareCursorSource(txn, collection, nss, pPipeline, pCtx);
            pPipeline->stitch();

            // Create the PlanExecutor which returns results from the pipeline. The WorkingSet
            // ('ws') and the PipelineProxyStage ('proxy') will be owned by the created
            // PlanExecutor.
            auto ws = make_unique<WorkingSet>();
            auto proxy = make_unique<PipelineProxyStage>(txn, pPipeline, input, ws.get());

            auto statusWithPlanExecutor = (NULL == collection)
                ? PlanExecutor::make(
                      txn, std::move(ws), std::move(proxy), nss.ns(), PlanExecutor::YIELD_MANUAL)
                : PlanExecutor::make(
                      txn, std::move(ws), std::move(proxy), collection, PlanExecutor::YIELD_MANUAL);
            invariant(statusWithPlanExecutor.isOK());
            exec = std::move(statusWithPlanExecutor.getValue());

            {
                auto planSummary = Explain::getPlanSummary(exec.get());
                stdx::lock_guard<Client>(*txn->getClient());
                curOp->setPlanSummary_inlock(std::move(planSummary));
            }

            if (collection) {
                PlanSummaryStats stats;
                Explain::getSummaryStats(*exec, &stats);
                collection->infoCache()->notifyOfQuery(txn, stats.indexesUsed);
            }

            if (collection) {
                const bool isAggCursor = true;  // enable special locking behavior
                ClientCursor* cursor =
                    new ClientCursor(collection->getCursorManager(),
                                     exec.release(),
                                     nss.ns(),
                                     txn->recoveryUnit()->isReadingFromMajorityCommittedSnapshot(),
                                     0,
                                     cmdObj.getOwned(),
                                     isAggCursor);
                pin.reset(new ClientCursorPin(collection->getCursorManager(), cursor->cursorid()));
                // Don't add any code between here and the start of the try block.
            }

            // At this point, it is safe to release the collection lock.
            // - In the case where we have a collection: we will need to reacquire the
            //   collection lock later when cleaning up our ClientCursorPin.
            // - In the case where we don't have a collection: our PlanExecutor won't be
            //   registered, so it will be safe to clean it up outside the lock.
            invariant(!exec || !collection);
        }

        try {
            // Unless set to true, the ClientCursor created above will be deleted on block exit.
            bool keepCursor = false;

            const bool isCursorCommand = !cmdObj["cursor"].eoo();

            // Use of the aggregate command without specifying to use a cursor is deprecated.
            // Applications should migrate to using cursors. Cursors are strictly more useful than
            // outputting the results as a single document, since results that fit inside a single
            // BSONObj will also fit inside a single batch.
            //
            // We occasionally log a deprecation warning.
            if (!isCursorCommand) {
                RARELY {
                    warning()
                        << "Use of the aggregate command without the 'cursor' "
                           "option is deprecated. See "
                           "http://dochub.mongodb.org/core/aggregate-without-cursor-deprecation.";
                }
            }

            // If both explain and cursor are specified, explain wins.
            if (pPipeline->isExplain()) {
                result << "stages" << Value(pPipeline->writeExplainOps());
            } else if (isCursorCommand) {
                keepCursor = handleCursorCommand(txn,
                                                 nss.ns(),
                                                 pin.get(),
                                                 pin ? pin->c()->getExecutor() : exec.get(),
                                                 cmdObj,
                                                 result);
            } else {
                pPipeline->run(result);
            }

            if (!pPipeline->isExplain()) {
                PlanSummaryStats stats;
                Explain::getSummaryStats(pin ? *pin->c()->getExecutor() : *exec.get(), &stats);
                curOp->debug().setPlanSummaryMetrics(stats);
                curOp->debug().nreturned = stats.nReturned;
            }

            // Clean up our ClientCursorPin, if needed.  We must reacquire the collection lock
            // in order to do so.
            if (pin) {
                // We acquire locks here with DBLock and CollectionLock instead of using
                // AutoGetCollectionForRead.  AutoGetCollectionForRead will throw if the
                // sharding version is out of date, and we don't care if the sharding version
                // has changed.
                Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
                Lock::CollectionLock collLock(txn->lockState(), nss.ns(), MODE_IS);
                if (keepCursor) {
                    pin->release();
                } else {
                    pin->deleteUnderlying();
                }
            }
        } catch (...) {
            // On our way out of scope, we clean up our ClientCursorPin if needed.
            if (pin) {
                Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
                Lock::CollectionLock collLock(txn->lockState(), nss.ns(), MODE_IS);
                pin->deleteUnderlying();
            }
            throw;
        }
        // Any code that needs the cursor pinned must be inside the try block, above.

        return true;
    }
} cmdPipeline;

}  // namespace mongo
