/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.migration.export;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.migration.export.Main;
import ai.grakn.test.EngineContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.GraphLoader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class GraphWriterMainTest {

    private static String keyspace;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Rule
    public final SystemOutRule sysOut = new SystemOutRule().enableLog();

    @Rule
    public final SystemErrRule sysErr = new SystemErrRule().enableLog();

    @BeforeClass
    public static void loadMovieGraph() {
        keyspace = GraphLoader.randomKeyspace();
        try(GraknTx graph = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)){
            MovieGraph.get().accept(graph);
            graph.commit();
        }
    }

    @Test
    public void exportCalledWithOntologyFlag_DataPrintedToSystemOut(){
        run("export", "-u", engine.uri(), "-ontology", "-keyspace", keyspace);

        assertThat(sysOut.getLog(), containsString("sub entity"));
    }

    @Test
    public void exportCalledWithDataFlag_DataPrintedToSystemOutTest(){
        run("export", "-u", engine.uri(), "-data", "-keyspace", keyspace);

        assertThat(sysOut.getLog(), containsString("isa movie"));
    }
    
    @Test
    public void exportCalledWithNoArgs_HelpMessagePrintedToSystemOut(){
        run("export", "ontology");

        assertThat(sysOut.getLog(), containsString("usage: migration.sh"));
    }

    @Test
    public void exportCalledWithHelpFlag_HelpMessagePrintedToSystemOut(){
        run("export", "-h");

        assertThat(sysOut.getLog(), containsString("usage: migration.sh"));
    }

    @Test
    public void exportCalledWithIncorrectURI_ErrorIsPrintedToSystemErr(){
        run("export", "-u", engine.uri().substring(1), "-data", "-keyspace", keyspace);

        assertThat(sysErr.getLog(), containsString("Could not connect to Grakn Engine. Have you run 'grakn.sh start'?"));
    }

    private void run(String... args){
        Main.main(args);
    }
}
