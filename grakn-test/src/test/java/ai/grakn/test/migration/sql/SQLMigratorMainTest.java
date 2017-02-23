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

package ai.grakn.test.migration.sql;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.migration.sql.Main;
import ai.grakn.test.EngineContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.SQLException;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.setupExample;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.DRIVER;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.PASS;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.URL;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.USER;

public class SQLMigratorMainTest {

    private final String templateFile = getFile("sql", "pets/template.gql").getAbsolutePath();
    private final String query = "SELECT * FROM pet";
    private Connection connection;
    private GraknGraphFactory factory;
    private GraknGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup() throws SQLException {
        factory = engine.factoryWithNewKeyspace();
        graph = factory.getGraph();
        connection = setupExample(graph, "pets");
    }

    @After
    public void stop() throws SQLException {
        connection.close();
    }

    @Test
    public void sqlMainTest(){
        runAndAssertDataCorrect("sql", "-t", templateFile,
                "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query, "-k", graph.getKeyspace());
    }

    @Test
    public void sqlMainNoKeyspace(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Keyspace missing (-k)");
        run("sql", "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile);
    }

    @Test
    public void sqlMainNoUserTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No username specified (-user)");
        run("sql", "-pass", PASS, "-location", URL, "-q", query, "-t", templateFile, "-k", graph.getKeyspace());
    }

    @Test
    public void sqlMainNoPassTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No password specified (-pass)");
        run("sql", "-t", templateFile, "-driver", DRIVER, "-location", URL, "-user", USER, "-q", query, "-k", graph.getKeyspace());
    }

    @Test
    public void sqlMainNoURLTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No db specified (-location)");
        run("sql", "-driver", DRIVER, "-q", query, "-t", templateFile);
    }

    @Test
    public void sqlMainNoQueryTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No SQL query specified (-query)");
        run("sql", "-t", templateFile, "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", graph.getKeyspace());
    }

    @Test
    public void sqlMainNoTemplateTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        run("sql", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
    }

    @Test
    public void sqlMainTemplateNoExistTest(){
        exception.expect(RuntimeException.class);
        run("sql", "-t", templateFile + "wrong", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-q", query);
    }

    @Test
    public void sqlMainPropertiesTest() throws SQLException {
        connection.close();
        graph = factory.getGraph(); //Reopen transaction
        connection = setupExample(graph, "pokemon");

        String configurationFile = getFile("sql", "pokemon/migration.yaml").getAbsolutePath();

        run("sql", "-driver", DRIVER, "-location", URL,
                "-pass", PASS, "-user", USER, "-k", graph.getKeyspace(),
                "-c", configurationFile);

        graph = factory.getGraph(); //Reopen transaction
        assertPokemonGraphCorrect(graph);
    }

    private void run(String... args){
        Main.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);
        graph = factory.getGraph(); //Reopen transaction
        assertPetGraphCorrect(graph);
    }

}
