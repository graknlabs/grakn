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

package io.mindmaps.loader;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.api.CommitLogController;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Var;
import io.mindmaps.util.ConfigProperties;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static io.mindmaps.graql.Graql.insert;

public class BlockingLoaderTest {

    String graphName;
    BlockingLoader loader;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(BlockingLoaderTest.class);


    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);

    }

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        loader = new BlockingLoader(graphName);
        new CommitLogController();
    }


    @Test
    public void testLoadOntologyAndData() {
        loadOntology();

        ClassLoader classLoader = getClass().getClassLoader();
        File fileData = new File(classLoader.getResource("small_nametags.gql").getFile());
        long startTime = System.currentTimeMillis();
        loader.setExecutorSize(2);
        loader.setBatchSize(10);
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        long endTime = System.currentTimeMillis();
        long firstLoadingTime = endTime - startTime;

        cleanGraph();
        loadOntology();

        loader.setExecutorSize(16);
        loader.setBatchSize(10);
        startTime = System.currentTimeMillis();
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(fileData)).forEach(pattern -> loader.addToQueue(pattern.admin().asVar()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        loader.waitToFinish();
        endTime = System.currentTimeMillis();
        long secondLoadingTime = endTime - startTime;
        LOG.info("First load time " + firstLoadingTime + ". Second load time " + secondLoadingTime);

        // TODO: Make this assertion consistently pass
        // Assert.assertTrue(secondLoadingTime < firstLoadingTime);

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getTransaction().getConcept("X506965727265204162656c").getId());
    }

    private void loadOntology() {
        MindmapsTransaction transaction = GraphFactory.getInstance().getGraphBatchLoading(graphName).getTransaction();
        List<Var> ontologyBatch = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();

        LOG.info("Loading new ontology .. ");
        try {
            QueryParser.create().parsePatternsStream(new FileInputStream(classLoader.getResource("dblp-ontology.gql").getFile())).map(x -> x.admin().asVar()).forEach(ontologyBatch::add);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        insert(ontologyBatch).withTransaction(transaction).execute();
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
        }

        LOG.info("Ontology loaded. ");
    }

    @After
    public void cleanGraph() {
            GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

}