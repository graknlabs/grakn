/*
 * GraknDB - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Research Ltd
 *
 * GraknDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GraknDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GraknDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.migration.json;

import com.google.common.io.Files;
import io.grakn.GraknGraph;
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.Resource;
import io.grakn.engine.GraknEngineServer;
import io.grakn.engine.GraknEngineServer;
import io.grakn.engine.util.ConfigProperties;
import io.grakn.exception.GraknValidationException;
import io.grakn.factory.GraphFactory;
import io.grakn.graql.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static io.grakn.migration.json.JsonMigratorUtil.getFile;
import static io.grakn.migration.json.JsonMigratorUtil.getProperties;
import static io.grakn.migration.json.JsonMigratorUtil.getProperty;
import static io.grakn.migration.json.JsonMigratorUtil.getResource;
import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;

public class JsonMigratorMainTest {

    private final String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    private GraknGraph graph;

    private final String dataFile = getFile("simple-schema/data.json").getAbsolutePath();;
    private final String templateFile = getFile("simple-schema/template.gql").getAbsolutePath();

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        GraknEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        GraknEngineServer.stop();
    }

    @Before
    public void setup(){
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        load(getFile("simple-schema/schema.gql"));
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void jsonMigratorMainTest(){
        String[] args = {
                "-data", dataFile,
                "-template", templateFile,
                "-graph", GRAPH_NAME
        };

        runAndAssertDataCorrect(args);
    }

    @Test
    public void jsonMainDistributedLoaderTest(){
        String[] args = {
                "-data", dataFile,
                "-template", templateFile,
                "-engine", "0.0.0.0"
        };

        runAndAssertDataCorrect(args);
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainNoDataFileNameTest(){
        runAndAssertDataCorrect(new String[]{"json"});
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainNoTemplateFileNameTest(){
        runAndAssertDataCorrect(new String[]{"-data", ""});
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainUnknownArgumentTest(){
        runAndAssertDataCorrect(new String[]{"-whale", ""});
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainNoDataFileExistsTest(){
        runAndAssertDataCorrect(new String[]{"-data", dataFile + "wrong", "-template", templateFile + "wrong"});
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainNoTemplateFileExistsTest(){
        runAndAssertDataCorrect(new String[]{"-data", dataFile, "-template", templateFile + "wrong"});
    }

    @Test
    public void jsonMainBatchSizeArgumentTest(){
        runAndAssertDataCorrect(new String[]{"-data", dataFile, "-template", templateFile, "-batch", "100"});
    }

    @Test(expected = RuntimeException.class)
    public void jsonMainThrowableTest(){
        runAndAssertDataCorrect(new String[]{"-data", dataFile, "-template", templateFile, "-batch", "hello"});
    }

    private void runAndAssertDataCorrect(String[] args){
        Main.main(args);

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

    // common class
    private void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException |GraknValidationException e){
            throw new RuntimeException(e);
        }
    }

}
