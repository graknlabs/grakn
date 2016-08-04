/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.api.shell;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GraqlShellTest {

    private Function<String, MindmapsGraph> graphFactory;

    private String providedNamespace;
    private String expectedVersion = "graql-9.9.9";

    @Before
    public void setUp() {
        graphFactory = namespace -> {
            providedNamespace = namespace;
            return MindmapsTestGraphFactory.newEmptyGraph();
        };
    }

    @Test
    public void testStartAndExitShell() throws IOException {
        // Assert simply that the shell starts and terminates without errors
        assertTrue(testShell("exit\n").endsWith(">>> exit\n"));
    }

    @Test
    public void testHelpOption() throws IOException {
        String result = testShell("", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("graql.sh"), containsString("-e"),
                        containsString("--execute <arg>"), containsString("query to execute")
                )
        );
    }

    @Test
    public void testVersionOption() throws IOException {
        String result = testShell("", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    @Test
    public void testDefaultNamespace() throws IOException {
        testShell("");
        assertEquals("mindmaps", providedNamespace);
    }

    @Test
    public void testSpecifiedNamespace() throws IOException {
        testShell("", "-n", "myspace");
        assertEquals("myspace", providedNamespace);
    }

    @Test
    public void testExecuteOption() throws IOException {
        String result = testShell("", "-e", "match $x isa role-type ask");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    @Test
    public void testMatchQuery() throws IOException {
        String[] result = testShell("match $x isa type\nexit").split("\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x isa type", result[4]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws IOException {
        String result = testShell("match $x isa relation-type ask\n");
        assertThat(result, containsString("False"));
    }

    @Test
    public void testInsertQuery() throws IOException {
        String result = testShell(
                "match $x isa entity-type ask\ninsert my-type isa entity-type\nmatch $x isa entity-type ask\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    @Test
    public void testInsertOutput() throws IOException {
        String[] result = testShell("insert a-type isa entity-type; thingy isa a-type\n").split("\n");

        // Expect ten lines output - four for the license, one for the query, four results and a new prompt
        assertEquals(10, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[4]);
        assertEquals(">>> ", result[9]);

        assertThat(
                Arrays.toString(Arrays.copyOfRange(result, 5, 9)),
                allOf(containsString("a-type"), containsString("entity-type"), containsString("thingy"))
        );
    }

    @Test
    public void testAutocomplete() throws IOException {
        String result = testShell("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString("type"), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    @Test
    public void testAutocompleteShellCommand() throws IOException {
        String result = testShell("\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    @Test
    public void testAutocompleteFill() throws IOException {
        String result = testShell("match $x isa typ\t\n");
        assertThat(result, containsString("\"relation-type\""));
    }

    @Test
    public void testReasoner() throws IOException {
        String result = testShell(
                "insert man isa entity-type; person isa entity-type;\n" +
                "insert 'felix' isa man;\n" +
                "match $x isa person;\n" +
                "insert my-rule isa inference-rule lhs {match $x isa man;} rhs {match $x isa person;};\n" +
                "match $x isa person;\n"
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        int matchCount = 0;
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match $x isa person")) {

                if (matchCount == 0) {
                    // First 'match' result is before rule is added, so should have no results
                    assertFalse(results[i + 1].contains("felix"));
                } else {
                    // Second 'match' result is after rule is added, so should have a result
                    assertTrue(results[i + 1].contains("felix"));
                }

                matchCount ++;
            }
        }

        assertEquals(2, matchCount);
    }

    @Test
    public void testInvalidQuery() throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie isa entity-type; moon isa movie; europa isa moon\n", err);

        assertThat(err.toString(), allOf(containsString("moon"), containsString("not"), containsString("type")));
    }

    private String testShell(String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream err, String... args) throws IOException {
        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream pout = new PrintStream(bout);
        PrintStream perr = new PrintStream(err);

        GraqlShell.runShell(args, graphFactory, expectedVersion, in, pout, perr);

        pout.flush();
        perr.flush();

        return bout.toString();
    }
}
