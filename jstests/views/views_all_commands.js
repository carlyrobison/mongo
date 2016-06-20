(function() {
    "use strict";

    load("jstests/views/libs/commands_lib.js");

    // Helper function for all failing tests.
    let assertCommandOrWriteFailed = function(res) {
        if (res.writeErrors !== undefined)
            assert.neq(0, res.writeErrors.length);
        else
            assert.commandFailed(res);
    };

    // Obtain a list of all commands.
    let res = db.runCommand({listCommands: 1});
    assert.commandWorked(res);

    let commands = res.commands;
    for (let command in commands) {
        if (!commands.hasOwnProperty(command))
            continue;

        let test = viewsCommandTests[command];
        assert(test !== undefined,
               "Coverage failure: must explicitly define a views test for command: " + command);

        // Tests can be explicitly skipped if they don't deal with namespaces.
        if (test.skip) {
            print("Skipping over views test for command: " + command);
            continue;
        }

        let dbName = (test.runOnDb === undefined) ? "test" : test.runOnDb;
        let dbHandle = db.getSiblingDB(dbName);

        // Skip tests depending on sharding configuration.
        if (test.skipSharded && isMongos(dbHandle))
            continue;
        if (test.skipStandalone && !isMongos(dbHandle))
            continue;

        // Perform test setup.
        viewsCommandUtils.genericSetup(dbHandle);
        if (test.setup !== undefined)
            test.setup(dbHandle);

        // Execute the command. The print statement is necessary because in the event of a failure,
        // there is no way to determine what command was actually running.
        print("Running test for command: " + command);

        if (test.expectFailure)
            assertCommandOrWriteFailed(dbHandle.runCommand(test.command));
        else
            assert.commandWorked(dbHandle.runCommand(test.command));

        if (test.teardown !== undefined)
            test.teardown(dbHandle);
    }
}());
