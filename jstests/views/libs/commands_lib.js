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

// Pre-written reasons for skipping a test.
let isAnInternalCommand = "internal, non-user-facing command";
let isUnrelated = "is unrelated and/or does not interact with namespaces";
let needsToFailWithViewsErrorCode = "TO DO: needs to fail with a views-specific error code";
let needsTriage = "TO DO: needs triage";

var viewsCommandTests = {
    _configsvrAddShard: {skip: isAnInternalCommand},
    _getUserCacheGeneration: {skip: isAnInternalCommand},
    _hashBSONElement: {skip: isAnInternalCommand},
    _isSelf: {skip: isAnInternalCommand},
    _mergeAuthzCollections: {skip: isAnInternalCommand},
    _migrateClone: {skip: isAnInternalCommand},
    _recvChunkAbort: {skip: isAnInternalCommand},
    _recvChunkCommit: {skip: isAnInternalCommand},
    _recvChunkStart: {skip: isAnInternalCommand},
    _recvChunkStatus: {skip: isAnInternalCommand},
    _transferMods: {skip: isAnInternalCommand},
    aggregate: {skip: "tested in views/views_aggregation.js"},
    appendOplogNote: {skip: isUnrelated},
    applyOps: {
        command: {applyOps: [{op: "i", o: {_id: 1}, ns: "test.view"}]},
        expectFailure: true,
        skip: needsToFailWithViewsErrorCode
    },
    authSchemaUpgrade: {skip: isUnrelated},
    authenticate: {skip: isUnrelated},
    availableQueryOptions: {skip: isUnrelated},
    buildInfo: {skip: isUnrelated},
    captrunc: {
        command: {captrunc: "view", n: 2, inc: false},
        expectFailure: true,
        skip: needsToFailWithViewsErrorCode
    },
    checkShardingIndex: {skip: isUnrelated},
    cleanupOrphaned: {command: {cleanupOrphaned: 1}, skip: needsTriage},
    clone: {
        skip: "TO DO: need to write a separate test for clone, that cloning a db clones the views"
    },
    cloneCollection:
        {command: {cloneCollection: "test.views"}, skip: needsToFailWithViewsErrorCode},
    cloneCollectionAsCapped: {
        command: {cloneCollectionAsCapped: "view", toCollection: "testcapped", size: 10240},
        expectFailure: true,
        skip: needsToFailWithViewsErrorCode
    },
    collMod: {command: {collMod: "view", viewOn: "other"}},
    collStats: {command: {collStats: "view"}, skip: "TO DO: need to fix this command to work"},
    compact: {command: {compact: "view"}, expectFailure: true, skip: needsToFailWithViewsErrorCode},
    configureFailPoint: {skip: isUnrelated},
    connPoolStats: {skip: isUnrelated},
    connPoolSync: {skip: isUnrelated},
    connectionStatus: {skip: isUnrelated},
    convertToCapped: {command: {convertToCapped: "view"}, skip: needsToFailWithViewsErrorCode},
    copydb: {skip: "TO DO: write a separate test that copydb copies the views"},
    copydbgetnonce: {skip: isUnrelated},
    copydbsaslstart: {skip: isUnrelated},
    count: {skip: "tested in views/views_basic.js"},
    create: {skip: "tested in views/views_creation.js"},
    createIndexes: {
        command: {createIndexes: "view", indexes: [{key: {x: 1}, name: "x_1"}]},
        expectFailure: true
    },
    createRole: {
        command: {createRole: "testrole", privileges: [], roles: []},
        setup: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    createUser: {
        command: {createUser: "testuser", pwd: "testpass", roles: []},
        setup: function(conn) {
            conn.runCommand({dropAllUsersFromDatabase: 1});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllUsersFromDatabase: 1});
        }
    },
    currentOp: {skip: isUnrelated},
    currentOpCtx: {skip: isUnrelated},
    dataSize: {
        command: {dataSize: "test.view"},
        expectFailure: true,
    },
    dbHash: {
        command: {dbHash: 1},
        skip:
            "TO DO: decide whether or not to make dbHash work, or error if a view is specified. Whatever is easier."
    },
    dbStats:
        {command: {dbStats: 1}, skip: "TO DO: dbStats should show views as well as collections"},
    delete: {command: {delete: "view", deletes: [{q: {x: 1}, limit: 1}]}, expectFailure: true},
    diagLogging: {skip: isUnrelated},
    distinct: {command: {distinct: "view", key: "x"}},
    driverOIDTest: {skip: isUnrelated},
    drop: {command: {drop: "view"}},
    dropAllRolesFromDatabase: {skip: isUnrelated},
    dropAllUsersFromDatabase: {skip: isUnrelated},
    dropDatabase: {command: {dropDatabase: 1}},
    dropIndexes: {
        command: {dropIndexes: "view"},
        expectFailure: true,
        skip: "TO DO: this should either fail with a views-specific error OR be a no-op"
    },
    dropRole: {
        command: {dropRole: "testrole"},
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    dropUser: {skip: isUnrelated},
    emptycapped:
        {command: {emptycapped: "view"}, expectFailure: true, skip: needsToFailWithViewsErrorCode},
    eval: {skip: "command is deprecated"},
    explain: {command: {explain: {count: "view"}}},
    features: {skip: isUnrelated},
    filemd5: {skip: isUnrelated},
    find: {skip: "tested in views/views_find.js"},
    findAndModify: {skip: "tested in views/disallowed_crud_commands.js"},
    forceerror: {skip: isUnrelated},
    fsync: {skip: isUnrelated},
    fsyncUnlock: {skip: isUnrelated},
    geoNear: {skip: "tested in views/disallowed_crud_commands.js"},
    geoSearch: {skip: "tested in views/disallowed_crud_commands.js"},
    getCmdLineOpts: {skip: isUnrelated},
    getLastError: {skip: isUnrelated},
    getLog: {skip: isUnrelated},
    getMore: {
        command: {getMore: NumberLong(1), collection: "view"},
        expectFailure: true,
        skip:
            "TO DO: we should support this. Cursors from operations on views should be cursors on the view namespace"
    },
    getParameter: {skip: isUnrelated},
    getPrevError: {skip: isUnrelated},
    getShardMap: {skip: isUnrelated},
    getShardVersion: {
        command: {getShardVersion: "test.view"},
        runOnDb: "admin",
        expectFailure: true,
        skip: "TO DO: this currently fails with a views error, but should it work? Talk to sharding"
    },
    getnonce: {skip: isUnrelated},
    godinsert: {skip: isAnInternalCommand},
    grantPrivilegesToRole: {
        command: {
            grantPrivilegesToRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    grantRolesToRole: {
        command: {grantRolesToRole: "testrole", roles: ["read"]},
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    grantRolesToUser: {
        command: {grantRolesToUser: "testuser", roles: ["read"]},
        setup: function(conn) {
            conn.runCommand({createUser: "testuser", pwd: "testpass", roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllUsersFromDatabase: 1});
        },
        skip: "TO DO: need a more sophisticated authentication test for views"
    },
    group: {
        command: {group: {ns: "test.view", key: "x", $reduce: function() {}, initial: {}}},
        skip: needsToFailWithViewsErrorCode
    },
    handshake: {skip: isUnrelated},
    hostInfo: {skip: isUnrelated},
    insert: {skip: "tested in views/views_disallowed_crud_commands.js"},
    invalidateUserCache: {skip: isUnrelated},
    isMaster: {skip: isUnrelated},
    journalLatencyTest: {skip: isUnrelated},
    killCursors: {
        command: {killCursors: "views", cursors: [NumberLong(0)]},
        skip: "TO DO: this should behave the same as getMore"
    },
    killOp: {skip: isUnrelated},
    listCollections: {skip: "tested in views/views_creation.js"},
    listCommands: {skip: isUnrelated},
    listDatabases: {skip: isUnrelated},
    listIndexes: {
        command: {listIndexes: "view"},
        expectFailure: true,
        skip: "TO DO: either needs to fail with a views error code OR be a no-op"
    },
    lockInfo: {skip: isUnrelated},
    logRotate: {skip: isUnrelated},
    logout: {skip: isUnrelated},
    makeSnapshot: {skip: isAnInternalCommand},
    mapReduce: {skip: "tested in views/disallowed_crud_commands.js"},
    "mapreduce.shardedfinish": {skip: isAnInternalCommand},
    mergeChunks: {
        command: {mergeChunks: "view", bounds: [{x: 0}, {x: 10}]},
        runOnDb: "admin",
        skip: needsToFailWithViewsErrorCode
    },
    moveChunk: {command: {moveChunk: 1}, skip: needsToFailWithViewsErrorCode},
    parallelCollectionScan: {command: {parallelCollectionScan: "view"}, expectFailure: true},
    ping: {command: {ping: 1}},
    planCacheClear: {command: {planCacheClear: "view"}, expectFailure: true},
    planCacheClearFilters: {command: {planCacheClearFilters: "view"}, expectFailure: true},
    planCacheListFilters: {command: {planCacheListFilters: "view"}, expectFailure: true},
    planCacheListPlans: {command: {planCacheListPlans: "view"}, expectFailure: true},
    planCacheListQueryShapes: {command: {planCacheListQueryShapes: "view"}, expectFailure: true},
    planCacheSetFilter: {command: {planCacheSetFilter: "view"}, expectFailure: true},
    profile: {skip: isUnrelated},
    reIndex: {
        command: {reIndex: "view"},
        expectFailure: true,
        skip: "TO DO: either needs to fail with a views error code OR allow it with a no-op"
    },
    renameCollection: {
        command: {renameCollection: "test.view", to: "test.otherview"},
        runOnDb: "admin",
        expectFailure: true,
        skip: "TO DO: it fails now but we need to support this"
    },
    repairCursor: {command: {repairCursor: "view"}, expectFailure: true},
    repairDatabase: {command: {repairDatabase: 1}},
    replSetElect: {skip: isAnInternalCommand},
    replSetFreeze: {skip: isAnInternalCommand},
    replSetFresh: {skip: isAnInternalCommand},
    replSetGetConfig: {skip: isAnInternalCommand},
    replSetGetRBID: {skip: isAnInternalCommand},
    replSetGetStatus: {skip: isAnInternalCommand},
    replSetHeartbeat: {skip: isAnInternalCommand},
    replSetInitiate: {skip: isAnInternalCommand},
    replSetMaintenance: {skip: isAnInternalCommand},
    replSetReconfig: {skip: isAnInternalCommand},
    replSetRequestVotes: {skip: isAnInternalCommand},
    replSetStepDown: {skip: isAnInternalCommand},
    replSetSyncFrom: {skip: isAnInternalCommand},
    replSetTest: {skip: isAnInternalCommand},
    replSetUpdatePosition: {skip: isAnInternalCommand},
    resetError: {skip: isUnrelated},
    resync: {skip: isUnrelated},
    revokePrivilegesFromRole: {
        command: {
            revokePrivilegesFromRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    revokeRolesFromRole: {skip: isUnrelated},
    revokeRolesFromUser: {skip: isUnrelated},
    // Skipping because it needs a role
    rolesInfo: {skip: isUnrelated},
    saslContinue: {skip: isUnrelated},
    saslStart: {skip: isUnrelated},
    serverStatus: {
        command: {serverStatus: 1},
        skip:
            "TO DO: ensure that if there's collection-related stats in serverStatus, views show up as well (when applicable)"
    },
    setCommittedSnapshot: {skip: isAnInternalCommand},
    setParameter: {skip: isUnrelated},
    setShardVersion: {skip: isUnrelated},
    shardConnPoolStats: {skip: isUnrelated},
    shardingState: {skip: isUnrelated},
    shutdown: {skip: isUnrelated},
    sleep: {skip: isUnrelated},
    splitChunk:
        {command: {splitChunk: 1}, expectFailure: true, skip: needsToFailWithViewsErrorCode},
    splitVector:
        {command: {splitVector: 1}, expectFailure: true, skip: needsToFailWithViewsErrorCode},
    stageDebug: {skip: isAnInternalCommand},
    top: {
        command: {top: "view"},
        runOnDb: "admin",
        skip: "TO DO: need to check output for views stats"
    },
    touch: {
        command: {touch: "view", data: true},
        expectFailure: true,
        skip: needsToFailWithViewsErrorCode
    },
    unsetSharding: {skip: isAnInternalCommand},
    update: {skip: "tested in views/views_disallowed_crud_commands.js"},
    updateRole: {
        command: {
            updateRole: "testrole",
            privileges: [{resource: {db: "test", collection: "view"}, actions: ["find"]}]
        },
        setup: function(conn) {
            conn.runCommand({createRole: "testrole", privileges: [], roles: []});
        },
        teardown: function(conn) {
            conn.runCommand({dropAllRolesFromDatabase: 1});
        }
    },
    updateUser: {skip: isUnrelated},
    usersInfo: {skip: isUnrelated},
    validate: {
        command: {validate: "view"},
        skip: "TO DO: we want validate on a view to validate the pipeline"
    },
    whatsmyuri: {skip: isUnrelated}
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
