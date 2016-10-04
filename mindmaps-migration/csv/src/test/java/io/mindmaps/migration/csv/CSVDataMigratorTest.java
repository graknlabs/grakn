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

package io.mindmaps.migration.csv;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Graql;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.mindmaps.graql.Graql.var;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVDataMigratorTest {

    private String GRAPH_NAME;

    private MindmapsGraph graph;
    private BlockingLoader loader;

    private static CSVSchemaMigrator schemaMigrator;
    private static CSVDataMigrator dataMigrator;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();

        schemaMigrator = new CSVSchemaMigrator();
        dataMigrator = new CSVDataMigrator();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup(){
        GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        loader = new BlockingLoader(GRAPH_NAME);
        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void icijEntityDataTest() throws IOException {

        schemaMigrator.configure("entity", parser("icij/Entities.csv")).migrate(loader);
        dataMigrator.configure("entity", parser("icij/Entities.csv")).migrate(loader);

        // test a entity
        Entity thing = Graql.withGraph(graph).match(var("x").isa("entity")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("name-resource", thing);
        assertResourceRelationExists("country_codes-resource", thing);
    }

    @Test
    public void icijAddressDataTest() throws IOException {
        schemaMigrator.configure("address", parser("icij/Addresses.csv")).migrate(loader);
        dataMigrator.configure("address", parser("icij/Addresses.csv")).migrate(loader);

        // test an address
        Entity thing = Graql.withGraph(graph).match(var("x").isa("address")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("countries-resource", thing);
    }

    @Test
    public void icijOfficerDataTest() throws IOException, InterruptedException {
        schemaMigrator.configure("officer", parser("icij/Officers.csv")).migrate(loader);
        dataMigrator.configure("officer", parser("icij/Officers.csv")).migrate(loader);

//        // test an officer
        Entity thing = Graql.withGraph(graph).match(var("x").isa("officer")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("name-resource", thing);

        thing = Graql.withGraph(graph).match(var("x").isa("officer").has("name-resource", "JOHN OLIVER")).iterator().next().get("x").asEntity();
        assertResourceRelationExists("country_codes-resource", thing);
    }

    @Test
    public void icijIntermediaryDataTest() throws IOException {
        schemaMigrator.configure("intermediary", parser("icij/Intermediaries.csv")).migrate(loader);
        dataMigrator.configure("intermediary", parser("icij/Intermediaries.csv")).migrate(loader);

        // test an intermediary
        Entity thing = Graql.withGraph(graph).match(var("x").isa("intermediary")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("countries-resource", thing);
        assertResourceRelationExists("status-resource", thing);
    }

    private void assertResourceRelationExists(String type, Entity owner){
        System.out.println(owner);
        System.out.println(owner.resources());
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(type)));
    }

    private CSVParser parser(String fileName){
        File file = new File(CSVSchemaMigratorTest.class.getClassLoader().getResource(fileName).getPath());

        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(file.toURI().toURL(),
                    StandardCharsets.UTF_8, CSVFormat.DEFAULT.withHeader());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return csvParser;
    }
}
