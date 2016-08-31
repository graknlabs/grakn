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

package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.response.Response;
import io.mindmaps.MindmapsComputerImpl;
import io.mindmaps.Util;
import io.mindmaps.constants.RESTUtil.GraphConfig;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static io.mindmaps.constants.RESTUtil.Request.GRAPH_CONFIG_PARAM;
import static io.mindmaps.constants.RESTUtil.WebPath.GRAPH_FACTORY_URI;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class GraphFactoryControllerTest {
    @Before
    public void setUp() throws Exception {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);

        new GraphFactoryController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());

    }

    @Test
    public void testConfigWorking(){
        Response response = get(GRAPH_FACTORY_URI).then().statusCode(200).extract().response().andReturn();
        String config = response.getBody().prettyPrint();
        assertTrue(config.contains("factory"));
    }

    @Test
    public void testSpecificConfigWorking(){
        String endPoint = GRAPH_FACTORY_URI + "?" + GRAPH_CONFIG_PARAM + "=";

        Response responseDefault = get(endPoint + GraphConfig.DEFAULT).
                then().statusCode(200).extract().response().andReturn();

        Response responseBatch = get(endPoint + GraphConfig.BATCH).
                then().statusCode(200).extract().response().andReturn();

        Response responseComputer = get(endPoint + GraphConfig.COMPUTER).
                then().statusCode(200).extract().response().andReturn();

        assertNotEquals(responseDefault, responseBatch);
        assertNotEquals(responseComputer, responseBatch);
    }

    @Test
    public void testMindmapsClientBatch(){
        MindmapsGraph batch = MindmapsClient.getGraphBatchLoading("mindmapstest");
        assertTrue(batch.isBatchLoadingEnabled());
    }

    @Test
    public void testMindmapsClient(){
        MindmapsGraph graph = MindmapsClient.getGraph("mindmapstest");
        MindmapsGraph graph2 = MindmapsClient.getGraph("mindmapstest2");
        MindmapsGraph graphCopy = MindmapsClient.getGraph("mindmapstest");
        assertNotEquals(0, ((AbstractMindmapsGraph) graph).getGraph().traversal().V().toList().size());
        assertFalse(graph.isBatchLoadingEnabled());
        assertNotEquals(graph, graph2);
        assertEquals(graph, graphCopy);
        graph.close();

        assertThat(MindmapsClient.getGraphComputer("Keyspace"), instanceOf(MindmapsComputerImpl.class));

        MindmapsGraph batch = MindmapsClient.getGraphBatchLoading("mindmapstest");
        assertTrue(batch.isBatchLoadingEnabled());
        assertNotEquals(graph, batch);

    }

}