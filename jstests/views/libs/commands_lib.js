/*
 * Declaratively-defined tests for views for all database commands. This contains an array of test
 * definitions as well as shared test logic for running them.
 *
 * The example test
 *
 *      {
 *          testName: "views_insert",
 *          command: {insert: "views_insert"},
 *          expectFailure: true
 *      }
 *
 * runs the command {insert: "views_insert"} and expects it to fail.
 */

var viewsCommandTests = {
    _configsvrAddShard: {skip: true},
    _getUserCacheGeneration: {skip: true},
    _hashBSONElement: {skip: true},
    _isSelf: {skip: true},
    _mergeAuthzCollections: {skip: true},
    _migrateClone: {skip: true},
    _recvChunkAbort: {skip: true},
    _recvChunkCommit: {skip: true},
    _recvChunkStart: {skip: true},
    _recvChunkStatus: {skip: true},
    _transferMods: {skip: true},
    aggregate: {command: {aggregate: "view"}},
    appendOplogNote: {skip: true},
    applyOps: {command: {applyOps: [{op: "i", o: {_id: 1}, ns: "test.view"}]}, expectFailure: true},
    authSchemaUpgrade: {skip: true},
    authenticate: {skip: true},
    availableQueryOptions: {command: {availableQueryOptions: 1}},
    buildInfo: {command: {buildInfo: 1}},
    // TODO fails with "collection test.view does not exist" (code 26)
    captrunc: {command: {captrunc: "view", n: 2, inc: false}, expectFailure: true},
    checkShardingIndex: {command: {checkShardingIndex: 1}, skip: true},
    // TODO
    cleanupOrphaned: {command: {cleanupOrphaned: 1}, skip: true},
    // TODO
    clone: {command: {clone: ""}, skip: true},
    // TODO skipping because of src/mongo/db/cloner.cpp:150. Also would need to set up two mongods.
    cloneCollection: {command: {cloneCollection: "test.views"}, skip: true},
    cloneCollectionAsCapped: {
        command: {cloneCollectionAsCapped: "view", toCollection: "testcapped", size: 10240},
        expectFailure: true
    },
    collMod: {command: {collMod: "view", viewOn: "other"}},
    // TODO
    collStats: {command: {collStats: "view"}, expectFailure: true},
    compact: {command: {compact: "view"}, expectFailure: true},
    configureFailPoint: {skip: true},
    connPoolStats: {command: {connPoolStats: 1}, skipStandalone: 1},
    connPoolSync: {command: {connPoolSync: 1}, skipStandalone: 1},
    connectionStatus: {command: {connectionStatus: 1}},
    // TODO: decide
    convertToCapped: {command: {convertToCapped: "view"}, expectFailure: true},
    // TODO: decide. note that this will say that it succeeded but actually is a no-op due to the
    // lack of an actual collection.
    copydb: {skip: true},
    copydbgetnonce: {command: {copydbgetnonce: 1}, runOnDb: "admin", skip: true},
    // TODO: ask spencer
    copydbsaslstart:
        {command: {copydbsaslstart: 1, mechanism: "PLAIN"}, runOnDb: "admin", skip: true},
    count: {command: {count: "view"}},
    create: {command: {create: "viewOnView", viewOn: "view"}},
    createIndexes: {
        command: {createIndexes: "view", indexes: [{key: {x: 1}, name: "x_1"}]},
        expectFailure: true
    },
    createRole: {command: {createRole: "testrole", privileges: [], roles: []}},
    createUser: {command: {createUser: "testuser", pwd: "testpass", roles: []}},
    currentOp: {command: {currentOp: 1}, runOnDb: "admin"},
    currentOpCtx: {command: {currentOpCtx: 1}},
    // TODO: decide. it fails for now
    dataSize: {command: {dataSize: "test.view"}, expectFailure: true},
    // TODO: decide. note that running this returns ok: 1, but the hash for the view is not present.
    dbHash: {command: {dbHash: 1}},
    // TODO: Note: dbstats should show views. right now it only shows collections
    dbStats: {command: {dbStats: 1}},
    delete: {command: {delete: "view", deletes: [{q: {x: 1}, limit: 1}]}, expectFailure: true},
    diagLogging: {command: {diagLogging: 1}, runOnDb: "admin"},
    distinct: {command: {distinct: "view", key: "x"}},
    driverOIDTest: {command: {driverOIDTest: ObjectId("576951e357e2d6b424bef27b")}},
    drop: {command: {drop: "view"}},
    dropAllRolesFromDatabase: {command: {dropAllRolesFromDatabase: 1}},
    dropAllUsersFromDatabase: {command: {dropAllUsersFromDatabase: 1}},
    dropDatabase: {command: {dropDatabase: 1}},
    // TODO: fails due to ns not found
    dropIndexes: {command: {dropIndexes: "view"}, expectFailure: true},
    dropRole: {
        command: {dropRole: "testrole"},
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        }
    },
    dropUser: {
        command: {dropUser: "testuser"},
        setup: function(conn) {
            conn.runCommand({createUser: "testuser", pwd: "testpass", roles: []});
        }
    },
    // TODO: this fails as expected but with an awful backtrace
    emptycapped: {command: {emptycapped: "view"}, expectFailure: true},
    eval: {command: {eval: function() {}}},
    explain: {command: {explain: {count: "view"}}},
    features: {command: {features: 1}},
    filemd5: {command: {filemd5: ObjectId("5769628e57e2d6b424bef27c"), root: "fs"}},
    find: {command: {find: "view"}},
    findAndModify:
        {command: {findAndModify: "view", query: {x: 1}, remove: true}, expectFailure: true},
    forceerror: {command: {forceerror: 1}, expectFailure: true},
    fsync: {command: {fsync: 1, async: true}, runOnDb: "admin"},
    fsyncUnlock: {
        command: {fsyncUnlock: 1},
        runOnDb: "admin",
        setup: function(conn) {
            conn.adminCommand({fsync: 1, lock: true});
        }
    },
    geoNear: {
        command: {geoNear: "view", near: {type: "Point", coordnates: [-50, 37]}, spherical: true},
        expectFailure: true
    },
    geoSearch: {command: {geoSearch: "view", search: {}, near: [50, -37]}, expectFailure: true},
    getCmdLineOpts: {command: {getCmdLineOpts: 1}, runOnDb: "admin"},
    getLastError: {command: {getLastError: 1}},
    getLog: {command: {getLog: "startupWarnings"}, runOnDb: "admin"},
    // TODO: This fails because it wants a getmore on a collection
    getMore: {command: {getMore: NumberLong(1), collection: "view"}, expectFailure: true},
    getParameter: {command: {getParameter: "*"}, runOnDb: "admin"},
    getPrevError: {command: {getPrevError: 1}},
    getShardMap: {command: {getShardMap: 1}, runOnDb: "admin", skipStandalone: true},
    // TODO Fails with "namespace is a view, not a collection"
    getShardVersion:
        {command: {getShardVersion: "test.view"}, runOnDb: "admin", expectFailure: true},
    getnonce: {command: {getnonce: 1}},
    godinsert: {skip: true},
    grantPrivilegesToRole: {
        command: {
            grantPrivilegesToRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        }
    },
    grantRolesToRole: {
        command: {grantRolesToRole: "testrole", roles: ["read"]},
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        }
    },
    grantRolesToUser: {
        command: {grantRolesToUser: "testuser", roles: ["read"]},
        setup: function(conn) {
            conn.runCommand({createUser: "testuser", pwd: "testpass", roles: []});
        }
    },
    // TODO should write a fuller jstest for this
    group: {command: {group: {ns: "test.view", key: "x", $reduce: function() {}, initial: {}}}},
    handshake: {command: {handshake: 1}, skip: true},
    hostInfo: {command: {hostInfo: 1}},
    insert: {command: {insert: "view", documents: [{x: 1}]}, expectFailure: true},
    invalidateUserCache: {command: {invalidateUserCache: 1}, runOnDb: "admin"},
    isMaster: {command: {isMaster: 1}},
    journalLatencyTest: {skip: true},
    killCursors: {command: {killCursors: "views", cursors: [NumberLong(0)]}},
    killOp: {command: {killOp: 1, op: 0}, runOnDb: "admin"},
    listCollections: {command: {listCollections: 1}},
    listCommands: {command: {listCommands: 1}},
    listDatabases: {command: {listDatabases: 1}, runOnDb: "admin"},
    // TODO: Fails because it is not a collection
    listIndexes: {command: {listIndexes: "view"}, expectFailure: true},
    lockInfo: {command: {lockInfo: 1}, runOnDb: "admin"},
    logRotate: {command: {logRotate: 1}, runOnDb: "admin"},
    logout: {command: {logout: 1}},
    makeSnapshot: {skip: true},
    mapReduce: {
        command: {mapReduce: "view", map: function() {}, reduce: function() {}, out: "out"},
        expectFailure: true
    },
    "mapreduce.shardedfinish": {command: {"mapreduce.shardedfinish": 1}, skip: true},
    // TODO: decide
    mergeChunks:
        {command: {mergeChunks: "view", bounds: [{x: 0}, {x: 10}]}, runOnDb: "admin", skip: true},
    // TODO: decide
    moveChunk: {command: {moveChunk: 1}, skip: true},
    // TODO: errors because "namespace is a view, not a collection"
    parallelCollectionScan: {command: {parallelCollectionScan: "view"}, expectFailure: true},
    ping: {command: {ping: 1}},
    // TODO: all of the plan cache commands error because "namespace is a view, not a collection"
    planCacheClear: {command: {planCacheClear: "view"}, expectFailure: true},
    planCacheClearFilters: {command: {planCacheClearFilters: "view"}, expectFailure: true},
    planCacheListFilters: {command: {planCacheListFilters: "view"}, expectFailure: true},
    planCacheListPlans: {command: {planCacheListPlans: "view"}, expectFailure: true},
    planCacheListQueryShapes: {command: {planCacheListQueryShapes: "view"}, expectFailure: true},
    planCacheSetFilter: {command: {planCacheSetFilter: "view"}, expectFailure: true},
    // -----
    profile: {command: {profile: -1}},
    // TODO: errors because "ns not found"
    reIndex: {command: {reIndex: "view"}, expectFailure: true},
    // TODO: errors because "source namespace does not exist (code 26)"
    renameCollection: {
        command: {renameCollection: "test.view", to: "test.otherview"},
        runOnDb: "admin",
        expectFailure: true
    },
    // Probably fine: fails with "namespace is a view, not a collection"
    repairCursor: {command: {repairCursor: "view"}, expectFailure: true},
    repairDatabase: {command: {repairDatabase: 1}},
    // XXX: Skipping all repl commands
    replSetElect: {command: {replSetElect: 1}, skip: true},
    replSetFreeze: {command: {replSetFreeze: 1}, skip: true},
    replSetFresh: {command: {replSetFresh: 1}, skip: true},
    replSetGetConfig: {command: {replSetGetConfig: 1}, skip: true},
    replSetGetRBID: {command: {replSetGetRBID: 1}, skip: true},
    replSetGetStatus: {command: {replSetGetStatus: 1}, skip: true},
    replSetHeartbeat: {command: {replSetHeartbeat: 1}, skip: true},
    replSetInitiate: {command: {replSetInitiate: 1}, skip: true},
    replSetMaintenance: {command: {replSetMaintenance: 1}, skip: true},
    replSetReconfig: {command: {replSetReconfig: 1}, skip: true},
    replSetRequestVotes: {command: {replSetRequestVotes: 1}, skip: true},
    replSetStepDown: {command: {replSetStepDown: 1}, skip: true},
    replSetSyncFrom: {command: {replSetSyncFrom: 1}, skip: true},
    replSetTest: {skip: true},
    replSetUpdatePosition: {command: {replSetUpdatePosition: 1}, skip: true},
    // ---
    resetError: {command: {resetError: 1}},
    resync: {command: {resync: 1}, skip: true},
    revokePrivilegesFromRole: {
        command: {
            revokePrivilegesFromRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        }
    },
    revokeRolesFromRole: {command: {revokeRolesFromRole: 1}, skip: true},
    revokeRolesFromUser: {command: {revokeRolesFromUser: 1}, skip: true},
    // Skipping because it needs a role
    rolesInfo: {command: {rolesInfo: 1}, skip: true},
    saslContinue: {command: {saslContinue: 1}, skip: true},
    saslStart: {command: {saslStart: 1}, skip: true},
    serverStatus: {command: {serverStatus: 1}},
    setCommittedSnapshot: {skip: 1},
    setParameter: {command: {setParameter: 1}, skip: true},
    setShardVersion: {command: {setShardVersion: 1}, skip: true},
    shardConnPoolStats: {command: {shardConnPoolStats: 1}},          // skippable
    shardingState: {command: {shardingState: 1}, runOnDb: "admin"},  // skippable
    shutdown: {skip: true},
    sleep: {skip: true},
    // TODO: Decide on sharding commands
    splitChunk: {command: {splitChunk: 1}, expectFailure: true, skip: true},
    splitVector: {command: {splitVector: 1}, expectFailure: true, skip: true},
    stageDebug: {
        command: {
            stageDebug: "view",
            plan: {
                ixscan: {
                    args: {
                        keyPattern: {x: 1},
                        startKey: {"": 20},
                        endKey: {},
                        endKeyInclusive: true,
                        direction: -1
                    }
                }
            }
        },
        expectFailure: true
    },
    top: {command: {top: "view"}, runOnDb: "admin"},
    touch: {command: {touch: "view", data: true}, expectFailure: true},
    unsetSharding: {command: {unsetSharding: 1}, skip: true},
    update:
        {command: {update: "view", updates: [{q: {x: 1}, u: {$set: {x: 2}}}]}, expectFailure: true},
    updateRole: {
        command: {
            updateRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        }
    },
    // TODO
    updateUser: {command: {updateUser: 1}, skip: true},
    usersInfo: {command: {usersInfo: 1}},
    // TODO: This fails for now, but we want it to work
    validate: {command: {validate: "view"}, expectFailure: true},
    whatsmyuri: {command: {whatsmyuri: 1}}
};

var viewsCommandUtils = {
    genericSetup: function(conn) {
        assert.commandWorked(conn.dropDatabase());
        assert.commandWorked(conn.runCommand({create: "view", viewOn: "collection"}));
        assert.writeOK(conn.collection.insert({x: 1}));
    },

};

/**
 * @return true if the connection is to a mongos; false otherwise
 */
var isMongos = function(conn) {
    var res = conn.getSiblingDB("admin").runCommand({isdbgrid: 1});
    return (res.ok == 1 && res.isdbgrid == 1);
};
