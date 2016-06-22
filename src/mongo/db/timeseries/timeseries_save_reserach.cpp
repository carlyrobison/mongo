    // Step 1: Construct the txn
    // Client* client;
    // auto txn = client->makeOperationContext();
    // const ServiceContext::UniqueOperationContext txn = cc().makeOperationContext();

    // mongo::OperationContext txn = getGlobalServiceContext()->makeOperationContext(&cc());
    // Step 2: Construct the command and its arguments
    // Command *save = Command::findCommand("update");
    // std::string errmsg;
    // BSONObjBuilder ReplyBob;
    // BSONObj upsertCmd = _constructUpsertCommand(coll);

    // log() << txn;
    // bool result = save->run(&txn, db.toString(), upsertCmd, 0, errmsg, ReplyBob);

    // Step 3: Construct the response
    // Command::appendCommandStatus(ReplyBob, result, errmsg);
    // ReplyBob.doneFast();

    //BSONObjBuilder metadataBob;
    //appendOpTimeMetadata(txn, request, &metadataBob);
    //replyBuilder->setMetadata(metadataBob.done());

    //return result;
    // batch.ns is the namespace string

    // const ServiceContext::UniqueOperationContext _txnPtr = cc().makeOperationContext();
    // OperationContext& _txn = *_txnPtr;
    // DBDirectClient db;

    //         ASSERT(db.createCollection(ns()));
    //     {
    //         BSONObjBuilder b;
    //         b.genOID();
    //         b.append("name", "Tom");
    //         b.append("rating", 0);
    //         db.insert(ns(), b.obj());
    //     }

    //     BSONObjBuilder cmd;
    //     cmd.appendSymbol("findAndModify", nsColl());  // Use Symbol for SERVER-16260
    //     cmd.append("update", BSON("$inc" << BSON("score" << 1)));
    //     cmd.append("new", true);

    //     BSONObj result;
    //     bool ok = db.runCommand(nsDb(), cmd.obj(), result);
    //     log() << result.jsonString();
    //     ASSERT(ok);
    //     // TODO(kangas) test that Tom's score is 1

    // verify(_mongod);
    // verify(_tempNs.size() == 0);

    // DBClientBase* conn = _mongod->directClient();
    // bool ok = conn->runCommand(_outputNs.db().toString(), cmd.done(), info);

    // OpDebug* const nullOpDebug = nullptr;
    // toCollection->insertDocument(
    //             txn, objToClone.value(), nullOpDebug, true, txn->writesAreReplicated());

    // Change ns here
    AutoGetCollection autoColl(txn, batch.ns, MODE_IX);
    Collection* const collection = autoColl.getCollection();
    uassert(ErrorCodes::NamespaceNotFound,
        str::stream() << "Couldn't find collection " << nss.ns(),
        collection);

    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        AutoGetCollection collectionWriteGuard(txn, nss, MODE_X);
        auto collection = collectionWriteGuard.getCollection();
        if (!collection) {
            return {ErrorCodes::NamespaceNotFound,
                    str::stream() << "unable to write operations to oplog at " << nss.ns()
                                  << ": collection not found. Did you drop it?"};
        }

        WriteUnitOfWork wunit(txn);
        OpDebug* const nullOpDebug = nullptr;
        auto status = collection->insertDocuments(txn, ops.begin(), ops.end(), nullOpDebug, false);
        if (!status.isOK()) {
            return status;
        }
        wunit.commit();

    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "writeOpsToOplog", nss.ns());

    {
        WriteUnitOfWork wuow(_opCtx.get());
        OpDebug* const nullOpDebug = nullptr;
        const bool enforceQuota = true;
        ASSERT_OK(collection->insertDocument(
            _opCtx.get(),
            BSON("_id" << 0 << "a" << 5 << "b" << BSON_ARRAY(1 << 2 << 3)),
            nullOpDebug,
            enforceQuota));
        wuow.commit();
    }

    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        const auto fromOptions =
            fromCollection->getCatalogEntry()->getCollectionOptions(txn).toBSON();
        OldClientContext ctx(txn, toNs);
        BSONObjBuilder spec;
        spec.appendBool("capped", true);
        spec.append("size", size);
        if (temp)
            spec.appendBool("temp", true);
        spec.appendElementsUnique(fromOptions);

        WriteUnitOfWork wunit(txn);
        Status status = userCreateNS(txn, ctx.db(), toNs, spec.done());
        if (!status.isOK())
            return status;
        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "cloneCollectionAsCapped", fromNs);

    /* Make a retry loop */
    // Status: duplicat id

            // WriteUnitOfWork wunit(txn);
            // OpDebug* const nullOpDebug = nullptr;
            // Status status = toCollection->insertDocument(
            //     txn, objToClone.value(), nullOpDebug, true, txn->writesAreReplicated());
            // wunit.commit();

    /* write_ops_exec */
    //     auto exec = uassertStatusOK(
    //     getExecutorUpdate(txn, &curOp.debug(), collection->getCollection(), &parsedUpdate));

    // {
    //     stdx::lock_guard<Client>(*txn->getClient());
    //     CurOp::get(txn)->setPlanSummary_inlock(Explain::getPlanSummary(exec.get()));
    // }

    // uassertStatusOK(exec->executePlan());

    // Status result = collection.getCollection()->;
    //return result;

    /* find_and_modify.cpp */

    /* db/exec/update.cpp */
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        WriteUnitOfWork wunit(getOpCtx());
        invariant(_collection);
        const bool enforceQuota = !request->isGod();
        uassertStatusOK(_collection->insertDocument(
            getOpCtx(), newObj, _params.opDebug, enforceQuota, request->isFromMigration()));

        // Technically, we should save/restore state here, but since we are going to return
        // immediately after, it would just be wasted work.
        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(getOpCtx(), "upsert", _collection->ns().ns());


BSONObj TimeSeriesBatch::_constructUpsertCommand(StringData coll) {
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("update", coll);

    BSONObjBuilder updateObj;

    // Create query
    BSONObjBuilder query;
    query.append("_id", _batchId);
    updateObj.append("q", query.obj());
    // Add the other parts
    updateObj.append("u", retrieveBatch());
    updateObj.append("multi", false);
    updateObj.append("upsert", true);

    BSONArrayBuilder updateArray;
    updateArray.append(updateObj.obj());
    cmdBuilder.append("updates", updateArray.arr());
    cmdBuilder.append("ordered", true);

    BSONObj newCmd = cmdBuilder.obj();
    log() << "newcmd: " << newCmd;

    return newCmd;
}

bool TimeSeriesCache::saveToCollection(){
    // Check that we have a backing collection
    massert(000, "No backing collection for timeseries", _nss != NULL);

    // Create the context
    const ServiceContext::UniqueOperationContext txnPtr = cc().makeOperationContext();
    OperationContext& txn = *txnPtr;

    std::vector<BSONObj> docs;
    // for each batch in the cache, add it to the list of docs
    for(auto const & cachePair : _cache) {
        docs.push_back(cachePair.second.retrieveBatch());
    }

    // Try to save all batches to the collection
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        AutoGetCollection autoColl(&txn, _nss, MODE_IX);
        auto collection = autoColl.getCollection();

        uassert(ErrorCodes::NamespaceNotFound,
                str::stream() << "Couldn't find collection " << _nss.ns(),
                collection);

        WriteUnitOfWork wunit(&txn);
        OpDebug* const nullOpDebug = nullptr;
        auto status = collection->insertDocuments(&txn, docs.begin(), docs.end(), nullOpDebug, false);
        if (!status.isOK()) {
            log() << status;
            return false;
        }
        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(&txn, "Timeseries: save to backing collection", _nss.ns());

    return true;
}


