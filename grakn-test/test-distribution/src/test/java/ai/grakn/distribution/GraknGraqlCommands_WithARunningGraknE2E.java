/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.distribution;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static ai.grakn.distribution.DistributionE2EConstants.assertGraknRunning;
import static ai.grakn.distribution.DistributionE2EConstants.assertGraknStopped;
import static ai.grakn.distribution.DistributionE2EConstants.assertZipExists;
import static ai.grakn.distribution.DistributionE2EConstants.unzipGrakn;
import static ai.grakn.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Contains tests which must be ran against a running Grakn instance.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class GraknGraqlCommands_WithARunningGraknE2E {

    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertGraknStopped();
        assertZipExists();
        unzipGrakn();
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    /**
     * test 'graql console' and 'define person sub entity;' from inside the console
     */
    @Test
    public void graql_shouldBeAbleToExecuteQuery_fromRepl() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;\n";

        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .command("./graql", "console", "-k", randomKeyspace).execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    /**
     * test "graql console -e 'define person sub entity;"
     */
    @Test
    public void graql_shouldBeAbleToExecuteQuery_fromArgument() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;";

        String output = commandExecutor.command("./graql", "console", "-k", randomKeyspace, "-e", graql).execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    /**
     * test 'grakn server clean' 'y' while grakn is running
     */
    @Test
    public void grakn_GraknServerCleanShouldNotBeAllowed_IfGraknIsRunning() throws IOException, InterruptedException, TimeoutException {
        String userInput = "y";
        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();

        assertThat(output, containsString("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup."));
    }

    /**
     * test 'grakn server status' when started
     */
    @Test
    public void grakn_testPrintStatus_whenCurrentlyRunning() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: RUNNING"), containsString("Engine: RUNNING")));
    }
}