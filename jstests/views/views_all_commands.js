(function() {
    "use strict";

    load("jstests/views/libs/commands_lib.js");

    // Helper function for all failing tests.
    let assertCommandOrWriteFailed = function(res) {
        if (res.writeErrors !== undefined)
            assert.neq(0, res.writeErrors.length);
        else {
            assert.commandFailed(res);
            switch (res.code) {
                case 160:  // ViewRecursionLimitExceeded
                case 161:  // CommandNotSupportedOnView
                case 162:  // OptionNotSupportedOnView
                case 164:  // ViewMustRunOnMongos
                    break;
                default:
                    assert(
                        false,
                        "Expected command to fail with a views-specific error code, but instead failed with " +
                            res.code);
            }
        }
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
               "Coverage failure: must explicitly define a views test for " + command);

        // Tests can be explicitly skipped. Print the name of the skipped test, as well as the
        // reason why.
        if (test.skip !== undefined) {
            print("Skipping " + command + ": " + test.skip);
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
        print("Testing " + command);

        if (test.expectFailure)
            assertCommandOrWriteFailed(dbHandle.runCommand(test.command));
        else
            assert.commandWorked(dbHandle.runCommand(test.command));

        if (test.teardown !== undefined)
            test.teardown(dbHandle);
    }
}());
