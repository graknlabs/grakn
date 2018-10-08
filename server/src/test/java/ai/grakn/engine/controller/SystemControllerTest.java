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

package ai.grakn.engine.controller;

import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStoreFake;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.controller.response.Keyspace;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Felix Chapman
 */
public class SystemControllerTest {
    private static final ServerStatus status = mock(ServerStatus.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final KeyspaceStoreFake keyspaceStore = KeyspaceStoreFake.of();

    private static final String INDEX_CONTENTS = "<html>hello world!</html>";

    private static GraknConfig config;

    @ClassRule
    public static final SparkContext sparkContext = SparkContext.withControllers((spark, config) -> {
        SystemControllerTest.config = config;
        new SystemController(config, keyspaceStore, status, metricRegistry).start(spark);
    });

    @BeforeClass
    public static void createIndex() throws IOException {
        File index = sparkContext.staticFiles().newFile("dashboard.html");
        Files.write(index.toPath(), ImmutableList.of(INDEX_CONTENTS), StandardCharsets.UTF_8);
    }

    @Before
    public void setUp() {
        keyspaceStore.clear();
    }

    @Test
    public void whenCallingRootEndpoint_Return200() {
        when().get("/").then().statusCode(SC_OK);
    }

    @Test
    public void whenCallingRootEndpointWithoutContentType_ReturnIndexFile() {
        when().get("/").then().contentType(ContentType.HTML).and().body(containsString(INDEX_CONTENTS));
    }

    @Test
    public void whenCallingRootEndpointAndRequestingJson_ReturnJson() {
        given().accept(ContentType.JSON).get("/").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingRootEndpoint_ReturnIdLinkToSelf() {
        given().accept(ContentType.JSON).when().get("/").then().body("@id", is("/"));
    }

    @Test
    public void whenCallingRootEndpoint_ReturnLinkToKeyspacesEndpoint() {
        given().accept(ContentType.JSON).when().get("/").then().body("keyspaces", is("/kb"));
    }

    @Test
    public void whenCallingKBEndpoint_Return200() {
        when().get("/kb").then().statusCode(SC_OK);
    }

    @Test
    public void whenCallingKBEndpoint_ReturnJson() {
        when().get("/kb").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingKbEndpoint_ReturnIdLinkToSelf() {
        when().get("/kb").then().body("@id", is("/kb"));
    }

    @Test
    public void whenCallingKbEndpoint_ReturnLinkToPutANewKeyspace() {
        when().get("/kb").then().body("keyspace", is("/kb/{keyspace}"));
    }

    @Test
    public void whenInitiallyCallingKBEndpoint_GetEmptyList() throws IOException {
        when().get("/kb").then().body("keyspaces", empty());
    }

    @Test
    public void whenCallingGetKBEndpointOnNonExistentKeyspace_Return404() {
        when().get("/kb/myks").then().statusCode(SC_NOT_FOUND).body(isEmptyString());
    }

    @Test
    public void whenCallingGETKBKeyspaceEndpoint_EnsureSpecificKeyspaceReturned(){
        Keyspace expected = Keyspace.of(ai.grakn.Keyspace.of("myks"));
        keyspaceStore.addKeyspace(ai.grakn.Keyspace.of("myks"));

        Response response = when().get(expected.id());
        assertEquals(SC_OK, response.statusCode());

        Keyspace foundKeyspace = response.as(Keyspace.class);
        assertEquals(expected, foundKeyspace);
    }



    @Test
    public void whenCallingDeleteKBEndpointOnExistingKeyspace_Return204() {
        keyspaceStore.addKeyspace(ai.grakn.Keyspace.of("myks"));

        when().delete("/kb/myks").then().statusCode(SC_NO_CONTENT).body(isEmptyString());
    }

    @Test
    public void whenCallingDeleteKBEndpointOnExistingKeyspace_KeyspaceDisappearsFromList() {
        keyspaceStore.addKeyspace(ai.grakn.Keyspace.of("myks"));

        RestAssured.delete("/kb/myks");

        when().get("/kb").then().body("", not(hasItem("myks")));
    }

    @Test
    public void whenCallingStatusEndpoint_AndStatusIsReady_ReturnReady() {
        Mockito.when(status.isReady()).thenReturn(true);
        when().get("/status").then().body(is("READY"));
    }

    @Test
    public void whenCallingStatusEndpoint_AndStatusIsNotReady_ReturnInitializing() {
        Mockito.when(status.isReady()).thenReturn(false);
        when().get("/status").then().body(is("INITIALIZING"));
    }

    @Test
    public void whenCallingMetricsEndpoint_ReturnJson() {
        when().get("/metrics").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingMetricsEndpoint_ReturnOK() {
        when().get("/metrics").then().statusCode(SC_OK);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingJson_ReturnJson() {
        given().param("format", "json").when().get("/metrics").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingText_ReturnText() {
        given().param("format", "prometheus").when().get("/metrics").then().contentType(ContentType.TEXT);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingInvalidType_Return400() {
        given().param("format", "rainbows").when().get("/metrics").then().statusCode(SC_BAD_REQUEST);
    }
}

