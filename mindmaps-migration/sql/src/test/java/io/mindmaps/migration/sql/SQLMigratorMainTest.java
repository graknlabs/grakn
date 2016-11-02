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

package io.mindmaps.migration.sql;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SQLMigratorMainTest {

    private Namer namer = new Namer() {};
    private MindmapsGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup() throws SQLException {
        String graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);
        SQLMigratorUtil.setupExample("simple");
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void sqlMainMissingDriverTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No driver specified (-driver)");
        runAndAssertDataCorrect(new String[]{"sql"});
    }

    @Test
    public void sqlMainMissingURLTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No db specified (-database)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER});
    }

    @Test
    public void sqlMainMissingUserTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No username specified (-user)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL});
    }

    @Test
    public void sqlMainMissingPassTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("No password specified (-pass)");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER});
    }

    @Test
    public void unknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        runAndAssertDataCorrect(new String[]{ "-whale", ""});
    }

    @Test
    public void sqlMainDifferentGraphNameTest(){
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER, "-pass", SQLMigratorUtil.PASS,
                "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void sqlMainDistributedLoaderTest(){
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", SQLMigratorUtil.USER, "-pass", SQLMigratorUtil.PASS,
                "-uri", "localhost:4567", "-keyspace", graph.getKeyspace()});
    }

    @Test
    public void sqlMainThrowableTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Wrong user name or password [28000-192]");
        runAndAssertDataCorrect(new String[]{"-driver", SQLMigratorUtil.DRIVER, "-database", SQLMigratorUtil.URL, "-user", "none", "-pass", SQLMigratorUtil.PASS,});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

        Entity alex = graph.getEntity("USERS-2");
        assertNotNull(alex);

        assertResourceRelationExists("NAME", "alex", alex, "USERS");
        assertResourceRelationExists("EMAIL", "alex@yahoo.com", alex, "USERS");
        assertResourceRelationExists("ID", 2L, alex, "USERS");

        Entity alexandra = graph.getEntity("USERS-4");
        assertNotNull(alexandra);

        assertResourceRelationExists("NAME", "alexandra", alexandra, "USERS");
        assertResourceRelationExists("ID", 4L, alexandra, "USERS");
    }

    private void assertResourceRelationExists(String type, Object value, Entity owner, String tableName){
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(namer.resourceName(tableName, type)) &&
                        resource.getValue().equals(value)));
    }
}
