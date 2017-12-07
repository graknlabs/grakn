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

package ai.grakn.engine.controller;

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Optional;

import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.exception;
import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.jsonResponse;
import static ai.grakn.engine.controller.GraqlControllerReadOnlyTest.stringResponse;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraqlControllerInsertTest {

    private final GraknTx tx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

    private static final Keyspace keyspace = Keyspace.of("akeyspace");
    private static TaskManager taskManager = mock(TaskManager.class);
    private static PostProcessor postProcessor = mock(PostProcessor.class);
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new GraqlController(mockFactory, spark, taskManager, postProcessor, new MetricRegistry());
    });

    @Before
    public void setupMock(){
        when(mockFactory.tx(eq(keyspace), any())).thenReturn(tx);
        when(tx.keyspace()).thenReturn(keyspace);

        // Describe expected response to a typical query
        Query<Object> query = tx.graql().parser().parseQuery("insert $x isa person;");
        Concept person = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(person.getId()).thenReturn(ConceptId.of("V123"));
        when(person.isThing()).thenReturn(true);
        when(person.asThing().type().getLabel()).thenReturn(Label.of("person"));

        when(query.execute()).thenReturn(ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(var("x"), person))
        ));
    }

    @After
    public void clearExceptions(){
        reset(taskManager);
        reset(postProcessor);
    }

    @Test
    public void POSTGraqlInsert_InsertWasExecutedOnGraph(){
        String queryString = "insert $x isa movie;";

        sendRequest(queryString);

        Query<?> query = tx.graql().parser().parseQuery(queryString);

        InOrder inOrder = inOrder(query, tx.admin());

        inOrder.verify(query).execute();
        inOrder.verify(tx.admin(), times(1)).commitSubmitNoLogs();
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseStatusIs400(){
        GraqlSyntaxException syntaxError = GraqlSyntaxException.parsingError("syntax error");
        when(tx.graql().parser().parseQuery("insert $x isa ;")).thenThrow(syntaxError);

        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void POSTMalformedGraqlQuery_ResponseExceptionContainsSyntaxError(){
        GraqlSyntaxException syntaxError = GraqlSyntaxException.parsingError("syntax error");
        when(tx.graql().parser().parseQuery("insert $x isa ;")).thenThrow(syntaxError);

        String query = "insert $x isa ;";
        Response response = sendRequest(query);

        assertThat(exception(response), containsString("syntax error"));
    }

    @Test
    public void POSTWithNoQueryInBody_ResponseIs400(){
        Response response = RestAssured.with()
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, "some-kb"));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_REQUEST_BODY.getMessage()));
    }

    @Test
    public void POSTGraqlInsert_ResponseStatusIs200(){
        String query = "insert $x isa person;";
        Response response = sendRequest(query);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseStatusCodeIs422(){
        GraknTxOperationException exception = GraknTxOperationException.invalidCasting(Object.class, Object.class);
        when(tx.graql().parser().parseQuery("define person plays movie;").execute()).thenThrow(exception);

        Response response = sendRequest("define person plays movie;");

        assertThat(response.statusCode(), equalTo(422));
    }

    @Test
    public void POSTGraqlDefineNotValid_ResponseExceptionContainsValidationErrorMessage(){
        GraknTxOperationException exception = GraknTxOperationException.invalidCasting(Object.class, Object.class);
        when(tx.graql().parser().parseQuery("define person plays movie;").execute()).thenThrow(exception);

        Response response = sendRequest("define person plays movie;");

        assertThat(exception(response), containsString("is not of type"));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseContentTypeIsJson(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(response.contentType(), equalTo(APPLICATION_JSON_GRAQL));
    }

    @Test
    public void POSTGraqlInsertWithJsonType_ResponseIsCorrectJson(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_JSON_GRAQL);

        assertThat(jsonResponse(response).asJsonList().size(), equalTo(1));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsTextType(){
        Response response = sendRequest("insert $x isa person;", APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void POSTGraqlInsertWithTextType_ResponseIsCorrectText(){

        Response response = sendRequest("insert $x isa person;", APPLICATION_TEXT);

        assertThat(stringResponse(response), containsString("isa person"));
    }

    @Test
    public void POSTGraqlDefine_GraphCommitNeverCalled(){
        String query = "define thingy sub entity;";

        sendRequest(query);

        verify(tx, times(0)).commit();
    }

    @Test
    public void POSTGraqlDefine_GraphCommitSubmitNoLogsIsCalled(){
        String query = "define thingy sub entity;";

        verify(tx.admin(), times(0)).commitSubmitNoLogs();

        sendRequest(query);

        verify(tx.admin(), times(1)).commitSubmitNoLogs();
    }

    @Test
    public void POSTGraqlInsertAndNoLogs_PPTaskIsNotSubmitted(){
        String query = "insert $x isa person has name 'Alice';";

        when(tx.admin().commitSubmitNoLogs()).thenReturn(Optional.empty());

        sendRequest(query);

        verify(taskManager, times(0)).addTask(any(), any());
    }

    @Test
    public void POSTGraqlInsertAndNoLogs_CountsAreNotUpdated(){
        String query = "insert $x isa person has name 'Alice';";

        when(tx.admin().commitSubmitNoLogs()).thenReturn(Optional.empty());

        sendRequest(query);

        verify(postProcessor, times(0)).updateCounts(any(), any());
    }

    @Test
    public void POSTGraqlInsert_PPTaskIsSubmitted(){
        String query = "insert $x isa person has name 'Alice';";

        CommitLog commitLog = CommitLog.create(tx.keyspace(), Collections.emptyMap(), Collections.emptyMap());
        when(tx.admin().commitSubmitNoLogs()).thenReturn(Optional.of(commitLog));

        sendRequest(query);

        verify(taskManager, times(1)).addTask(any(), any());
    }

    @Test
    public void POSTGraqlInsert_CountsAreUpdated(){
        String query = "insert $x isa person has name 'Alice';";

        CommitLog commitLog = CommitLog.create(tx.keyspace(), Collections.emptyMap(), Collections.emptyMap());
        when(tx.admin().commitSubmitNoLogs()).thenReturn(Optional.of(commitLog));

        sendRequest(query);

        verify(postProcessor, times(1)).updateCounts(tx.keyspace(), commitLog);
    }

    private Response sendRequest(String query){
        return sendRequest(query, APPLICATION_TEXT);
    }

    private Response sendRequest(String query, String acceptType){
        return RestAssured.with()
                .accept(acceptType)
                .queryParam(INFER, false)
                .body(query)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace.getValue()));
    }
}
