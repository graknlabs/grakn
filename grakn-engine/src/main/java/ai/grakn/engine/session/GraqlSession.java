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

package ai.grakn.engine.session;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_CLEAN;
import static ai.grakn.util.REST.RemoteShell.ACTION_COMMIT;
import static ai.grakn.util.REST.RemoteShell.ACTION_DISPLAY;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static ai.grakn.util.REST.RemoteShell.ACTION_ERROR;
import static ai.grakn.util.REST.RemoteShell.ACTION_PING;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY_ABORT;
import static ai.grakn.util.REST.RemoteShell.ACTION_ROLLBACK;
import static ai.grakn.util.REST.RemoteShell.ACTION_TYPES;
import static ai.grakn.util.REST.RemoteShell.DISPLAY;
import static ai.grakn.util.REST.RemoteShell.ERROR;
import static ai.grakn.util.REST.RemoteShell.QUERY;
import static ai.grakn.util.REST.RemoteShell.QUERY_RESULT;
import static ai.grakn.util.REST.RemoteShell.TYPES;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * A Graql shell session for a single client, running on one graph in one thread
 */
class GraqlSession {
    private final Session session;
    private final boolean showImplicitTypes;
    private final boolean infer;
    private final boolean materialise;
    private GraknGraph graph;
    private final GraknGraphFactory factory;
    private final String outputFormat;
    private Printer printer;
    private StringBuilder queryStringBuilder = new StringBuilder();
    private final Logger LOG = LoggerFactory.getLogger(GraqlSession.class);

    private static final int QUERY_CHUNK_SIZE = 1000;
    private static final int PING_INTERVAL = 60_000;

    private boolean queryCancelled = false;

    // All requests are run within a single thread, so they always happen in a single thread-bound transaction
    private final ExecutorService queryExecutor =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("graql-session-%s").build());

    GraqlSession(
            Session session, GraknGraphFactory factory, String outputFormat,
            boolean showImplicitTypes, boolean infer, boolean materialise
    ) {
        this.showImplicitTypes = showImplicitTypes;
        this.infer = infer;
        this.materialise = materialise;
        this.session = session;
        this.factory = factory;
        this.outputFormat = outputFormat;
        this.printer = getPrinter();

        queryExecutor.submit(() -> {
            try {
                refreshGraph();
                sendTypes();
                sendEnd();
            } catch (Throwable e) {
                LOG.error(getFullStackTrace(e));
                throw e;
            }
        });

        // Begin sending pings
        Thread thread = new Thread(this::ping, "graql-session-ping");
        thread.setDaemon(true);
        thread.start();
    }

    private void refreshGraph() {
        graph = factory.getGraph();
        graph.showImplicitConcepts(showImplicitTypes);
    }

    void handleMessage(Json json) {
        switch (json.at(ACTION).asString()) {
            case ACTION_QUERY:
                receiveQuery(json);
                break;
            case ACTION_END:
                executeQuery();
                break;
            case ACTION_QUERY_ABORT:
                abortQuery();
                break;
            case ACTION_COMMIT:
                commit();
                break;
            case ACTION_ROLLBACK:
                rollback();
                break;
            case ACTION_CLEAN:
                clean();
                break;
            case ACTION_DISPLAY:
                setDisplayOptions(json);
                break;
            case ACTION_PING:
                // Ignore
                break;
            default:
                throw new RuntimeException("Unrecognized message: " + json);
        }
    }

    private void ping() {
        // This runs on a daemon thread, so it will be terminated when the JVM stops
        while (session.isOpen()) {
            try {
                sendJson(Json.object(ACTION, ACTION_PING));
            } catch (WebSocketException e) {
                // Report an error if the session is still open
                if (session.isOpen()) {
                    LOG.error(e.getMessage());
                }
            } finally {
                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Close the session, which will close the transaction.
     */
    void close() {
        queryExecutor.submit(() -> {
            try {
                graph.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Receive and remember part of a query
     */
    void receiveQuery(Json json) {
        queryExecutor.submit(() -> {
            String queryString = json.at(QUERY).asString();
            queryStringBuilder.append(queryString);
        });
    }

    /**
     * Execute the Graql query described in the given JSON request
     */
    void executeQuery() {
        queryExecutor.submit(() -> {

            String errorMessage = null;
            List<Query<?>> queries = null;

            try {
                String queryString = queryStringBuilder.toString();
                queryStringBuilder = new StringBuilder();

                queries = graph.graql().infer(infer).materialise(materialise).parseList(queryString);

                // Return results unless query is cancelled
                queries.stream().flatMap(query -> query.resultsString(printer)).forEach(result -> {
                    if (queryCancelled) return;
                    sendQueryResult(result);
                });
                queryCancelled = false;
            } catch (IllegalArgumentException | IllegalStateException | ConceptException e) {
                errorMessage = e.getMessage();
                LOG.error(errorMessage,e);
            } catch (Throwable e) {
                errorMessage = "An unexpected error occurred";
                LOG.error(errorMessage,e);
            } finally {
                // Refresh the graph, in case it has been closed by analytics
                // TODO: Handle this elsewhere (analytics or graph factory?)
                attemptRefresh();

                if (errorMessage != null) {
                    if (queries != null && !queries.stream().allMatch(Query::isReadOnly)) {
                        graph.close();
                    }
                    sendQueryError(errorMessage);
                }

                sendEnd();
            }
        });
    }

    void abortQuery() {
        queryCancelled = true;
    }

    /**
     * Commit and report any errors to the client
     */
    void commit() {
        queryExecutor.submit(() -> {
            try {
                graph.commitOnClose();
                graph.close();
            } catch (GraknValidationException e) {
                sendCommitError(e.getMessage());
            } finally {
                sendEnd();
                attemptRefresh();
            }
        });
    }

    /**
     * Rollback the transaction, removing uncommitted changes
     */
    void rollback() {
        queryExecutor.submit(() -> {
            graph.close();
            attemptRefresh();
        });
    }

    /**
     * Clean the transaction, removing everything in the graph (but not committing)
     */
    void clean() {
        queryExecutor.submit(() -> {
            graph.clear();
            attemptRefresh();
        });
    }

    private void attemptRefresh() {
        try {
            refreshGraph();
        } catch (Throwable e) {
            LOG.error("Error during refresh", e);
        }
    }

    void setDisplayOptions(Json json) {
        queryExecutor.submit(() -> {
            ResourceType[] displayOptions = json.at(DISPLAY).asJsonList().stream()
                    .map(Json::asString)
                    .map(graph::getResourceType)
                    .filter(Objects::nonNull)
                    .toArray(ResourceType[]::new);
            printer = getPrinter(displayOptions);
        });
    }

    /**
     * Send a single query result back to the client
     */
    private void sendQueryResult(String result) {
        // Split result into chunks
        Iterable<String> splitResult = Splitter.fixedLength(QUERY_CHUNK_SIZE).split(result + "\n");

        for (String resultChunk : splitResult) {
            sendJson(Json.object(
                    ACTION, ACTION_QUERY,
                    QUERY_RESULT, resultChunk
            ));
        }
    }

    /**
     * Tell the client that there are no more query results
     */
    private void sendEnd() {
        sendJson(Json.object(ACTION, ACTION_END));
    }

    /**
     * Tell the client about an error in their query
     */
    private void sendQueryError(String errorMessage) {
        // Split error into chunks
        Iterable<String> splitError = Splitter.fixedLength(QUERY_CHUNK_SIZE).split(errorMessage + "\n");

        for (String errorChunk : splitError) {
            sendJson(Json.object(
                    ACTION, ACTION_ERROR,
                    ERROR, errorChunk
            ));
        }
    }

    /**
     * Tell the client about an error during commit
     */
    private void sendCommitError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_ERROR,
                ERROR, errorMessage
        ));
    }

    /**
     * Send a list of all types in the ontology
     */
    private void sendTypes() {
        sendJson(Json.object(
                ACTION, ACTION_TYPES,
                TYPES, getTypes(graph).map(TypeName::getValue).collect(toList())
        ));
    }

    /**
     * Send the given JSON to the client
     */
    private void sendJson(Json json) {
        queryExecutor.submit(() -> {
            LOG.debug("Sending message: " + json);
            try {
                session.getRemote().sendString(json.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param graph the graph to find types in
     * @return all type IDs in the ontology
     */
    private static Stream<TypeName> getTypes(GraknGraph graph) {
        return graph.admin().getMetaConcept().subTypes().stream().map(Type::getName);
    }

    private Printer getPrinter(ResourceType... resources) {
        switch (outputFormat) {
            case "graql":
            default:
                return Printers.graql(resources);
            case "json":
                return Printers.json();
            case "hal":
                return Printers.hal();
        }
    }
}
