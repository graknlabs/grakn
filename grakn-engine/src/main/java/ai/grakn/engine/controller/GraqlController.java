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
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.controller.response.ExplanationBuilder;
import ai.grakn.engine.controller.util.Requests;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.DEFINE_ALL_VARS;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.LOADING_DATA;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.Graql.TX_TYPE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.Boolean.parseBoolean;
import static org.apache.http.HttpStatus.SC_OK;


/**
 * <p>
 * Endpoints used to query the graph using Graql and build a HAL, Graql or Json response.
 * </p>
 *
 * @author Marco Scoppetta, alexandraorth
 */
public class GraqlController {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(GraqlController.class);
    private static final RetryLogger retryLogger = new RetryLogger();
    private static final int MAX_RETRY = 10;
    private final Printer printer;
    private final EngineGraknTxFactory factory;
    private final TaskManager taskManager;
    private final PostProcessor postProcessor;
    private final Timer executeGraql;
    private final Timer executeExplanation;

    public GraqlController(
            EngineGraknTxFactory factory, Service spark, TaskManager taskManager,
            PostProcessor postProcessor, Printer printer, MetricRegistry metricRegistry
    ) {
        this.factory = factory;
        this.taskManager = taskManager;
        this.postProcessor = postProcessor;
        this.printer = printer;
        this.executeGraql = metricRegistry.timer(name(GraqlController.class, "execute-graql"));
        this.executeExplanation = metricRegistry.timer(name(GraqlController.class, "execute-explanation"));

        spark.post(REST.WebPath.KEYSPACE_GRAQL, this::executeGraql);
        spark.get(REST.WebPath.KEYSPACE_EXPLAIN, this::explainGraql);

        spark.exception(GraqlQueryException.class, (e, req, res) -> handleError(400, e, res));
        spark.exception(GraqlSyntaxException.class, (e, req, res) -> handleError(400, e, res));

        // Handle invalid type castings and invalid insertions
        spark.exception(GraknTxOperationException.class, (e, req, res) -> handleError(422, e, res));
        spark.exception(InvalidKBException.class, (e, req, res) -> handleError(422, e, res));
    }

    @GET
    @Path("/kb/{keyspace}/explain")
    @ApiOperation(value = "Execute an arbitrary Graql query and get the explanation from the results")
    @ApiImplicitParams({
            @ApiImplicitParam(value = "Query to execute", dataType = "string", required = true, paramType = "query"),
    })
    private String explainGraql(Request request, Response response) throws RetryException, ExecutionException {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        String queryString = mandatoryQueryParameter(request, QUERY);

        response.status(SC_OK);

        return executeFunctionWithRetrying(() -> {
            try (GraknTx tx = factory.tx(keyspace, GraknTxType.WRITE); Timer.Context context = executeExplanation.time()) {
                Answer answer = tx.graql().infer(true).parser().<GetQuery>parseQuery(queryString).execute().stream().findFirst().orElse(new QueryAnswer());
                return mapper.writeValueAsString(ExplanationBuilder.buildExplanation(answer, printer));
            }
        });
    }

    @POST
    @Path("/kb/{keyspace}/graql")
    @ApiOperation(value = "Execute an arbitrary Graql query")
    @ApiImplicitParams({
            @ApiImplicitParam(value = "Query to execute", dataType = "string", required = true, paramType = "body"),
            @ApiImplicitParam(name = EXECUTE_WITH_INFERENCE, value = "Enable inference", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(
                    name = DEFINE_ALL_VARS,
                    value = "Define all variables in response", dataType = "boolean", paramType = "query"
            ),
            @ApiImplicitParam(name = ALLOW_MULTIPLE_QUERIES, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = TX_TYPE, dataType = "string", paramType = "query")
    })
    private String executeGraql(Request request, Response response) throws RetryException, ExecutionException {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        String queryString = mandatoryBody(request);

        //Run the query with reasoning on or off
        Optional<Boolean> infer = queryParameter(request, EXECUTE_WITH_INFERENCE).map(Boolean::parseBoolean);

        //Allow multiple queries to be executed
        boolean multiQuery = parseBoolean(queryParameter(request, ALLOW_MULTIPLE_QUERIES).orElse("false"));

        //Define all anonymous variables in the query
        Optional<Boolean> defineAllVars = queryParameter(request, DEFINE_ALL_VARS).map(Boolean::parseBoolean);

        //Used to check if serialisation of results is needed. When loading we skip this for the sake of speed
        boolean skipSerialisation = parseBoolean(queryParameter(request, LOADING_DATA).orElse("false"));

        //Check the transaction type to use
        GraknTxType txType = queryParameter(request, TX_TYPE)
                .map(String::toUpperCase).map(GraknTxType::valueOf).orElse(GraknTxType.WRITE);

        //This is used to determine the response format
        //TODO: Maybe we should really try to stick with one representation? This would require dashboard console interpreting the json representation
        final String acceptType;
        if (APPLICATION_TEXT.equals(Requests.getAcceptType(request))) {
            acceptType = APPLICATION_TEXT;
        } else {
            acceptType = APPLICATION_JSON;
        }
        response.type(APPLICATION_JSON);

        //Execute the query and get the results
        LOG.trace(String.format("Executing graql statements: {%s}", queryString));

        return executeFunctionWithRetrying(() -> {
            try (GraknTx tx = factory.tx(keyspace, txType); Timer.Context context = executeGraql.time()) {

                System.out.println("RUNNING: " + queryString);
                QueryBuilder builder = tx.graql();

                infer.ifPresent(builder::infer);

                QueryParser parser = builder.parser();
                defineAllVars.ifPresent(parser::defineAllVars);

                response.status(SC_OK);

                return executeQuery(tx, queryString, acceptType, multiQuery, skipSerialisation, parser);
            }
        });
    }

    private String executeFunctionWithRetrying(Callable<String> callable) throws RetryException, ExecutionException {
        try {
            Retryer<String> retryer = RetryerBuilder.<String>newBuilder()
                    .retryIfExceptionOfType(TemporaryWriteException.class)
                    .withRetryListener(retryLogger)
                    .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_RETRY))
                    .build();

            return retryer.call(callable);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw e;
            }
        }
    }

    private static class RetryLogger implements RetryListener {
        @Override
        public <V> void onRetry(Attempt<V> attempt) {
            if (attempt.hasException()) {
                LOG.warn("Retrying transaction after {" + attempt.getAttemptNumber() + "} attempts due to exception {" + attempt.getExceptionCause().getMessage() + "}");
            }
        }
    }


    /**
     * Handle any {@link Exception} that are thrown by the server. Configures and returns
     * the correct JSON response with the given status.
     *
     * @param exception exception thrown by the server
     * @param response  response to the client
     */
    private static void handleError(int status, Exception exception, Response response) {
        LOG.error("REST error", exception);
        response.status(status);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * Execute a query and return a response in the format specified by the request.
     *
     * @param tx          open transaction to current graph
     * @param queryString read query to be executed
     * @param acceptType  response format that the client will accept
     * @param multi       execute multiple statements
     * @param parser
     */
    private String executeQuery(GraknTx tx, String queryString, String acceptType, boolean multi, boolean skipSerialisation, QueryParser parser) throws JsonProcessingException {
        Printer printer = this.printer;

        if (APPLICATION_TEXT.equals(acceptType)) printer = Printers.graql(false);

        String formatted;
        boolean commitQuery = true;
        if (multi) {
            Stream<Query<?>> query = parser.parseList(queryString);
            List<?> collectedResults = query.map(this::executeAndMonitor).collect(Collectors.toList());
            if (skipSerialisation) {
                formatted = mapper.writeValueAsString(new Object[collectedResults.size()]);
            } else {
                formatted = printer.graqlString(collectedResults);
            }
        } else {
            Query<?> query = parser.parseQuery(queryString);
            if (skipSerialisation) {
                formatted = "";
            } else {
                formatted = printer.graqlString(executeAndMonitor(query));
            }
            commitQuery = !query.isReadOnly();
        }

        if (commitQuery) commitAndSubmitPPTask(tx, postProcessor, taskManager);

        return formatted;
    }

    private static void commitAndSubmitPPTask(
            GraknTx graph, PostProcessor postProcessor, TaskManager taskSubmitter
    ) {
        Optional<CommitLog> result = graph.admin().commitSubmitNoLogs();
        if (result.isPresent()) { // Submit more tasks if commit resulted in created commit logs
            CommitLog logs = result.get();

            //Update the attributes which need to be merged
            System.out.println("Adding task to manager . . . ");
            taskSubmitter.addTask(
                    PostProcessingTask.createTask(GraqlController.class),
                    PostProcessingTask.createConfig(logs)
            );

            //Update the counts which need to be updated
            System.out.println("Updating counts . . . ");
            postProcessor.updateCounts(graph.keyspace(), logs);
        }
    }

    private Object executeAndMonitor(Query<?> query) {
        return query.execute();
    }

}
