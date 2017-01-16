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

package ai.grakn.test.migration.json;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.migration.json.Main;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.getProperties;
import static ai.grakn.test.migration.MigratorTestUtils.getProperty;
import static ai.grakn.test.migration.MigratorTestUtils.getResource;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static junit.framework.TestCase.assertEquals;

public class JsonMigratorMainTest {

    private final String dataFile = getFile("json", "simple-schema/data.json").getAbsolutePath();
    private final String templateFile = getFile("json", "simple-schema/template.gql").getAbsolutePath();

    private GraknGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Before
    public void setup() {
        graph = engine.getNewGraph();
        load(graph, getFile("json", "simple-schema/schema.gql"));
    }

    @Test
    public void jsonMigratorMainTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace());
    }

    @Test
    public void jsonMainDistributedLoaderTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-keyspace", graph.getKeyspace(), "-uri", "localhost:4567");
    }

    @Test
    public void jsonMainNoArgsTest() {
        run("json");
    }

    @Test
    public void jsonMainNoTemplateFileNameTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Template file missing (-t)");
        run("-input", "");
    }

    @Test
    public void jsonMainUnknownArgumentTest(){
        exception.expect(RuntimeException.class);
        exception.expectMessage("Unrecognized option: -whale");
        run("-whale", "");
    }

    @Test
    public void jsonMainNoDataFileExistsTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile + "wrong", "-template", templateFile + "wrong");
    }

    @Test
    public void jsonMainNoTemplateFileExistsTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile, "-template", templateFile + "wrong");
    }

    @Test
    public void jsonMainBatchSizeArgumentTest(){
        runAndAssertDataCorrect("-input", dataFile, "-template", templateFile, "-batch", "100", "-keyspace", graph.getKeyspace());
    }

    @Test
    public void jsonMainThrowableTest(){
        exception.expect(RuntimeException.class);
        run("-input", dataFile, "-template", templateFile, "-batch", "hello");
    }

    private void run(String... args){
        Main.main(args);
    }

    private void runAndAssertDataCorrect(String... args){
        run(args);

        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();
        Entity address = getProperty(graph, person, "has-address").asEntity();
        Entity streetAddress = getProperty(graph, address, "address-has-street").asEntity();

        Resource number = getResource(graph, streetAddress, "number").asResource();
        assertEquals(21L, number.getValue());

        Collection<Instance> phoneNumbers = getProperties(graph, person, "has-phone");
        assertEquals(2, phoneNumbers.size());
    }
}
