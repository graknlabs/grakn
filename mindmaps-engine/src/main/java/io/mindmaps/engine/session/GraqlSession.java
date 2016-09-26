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

package io.mindmaps.engine.session;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.controller.TransactionController;
import io.mindmaps.exception.ConceptException;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.Autocomplete;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.Query;
import io.mindmaps.graql.Reasoner;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.REST.RemoteShell.*;

/**
 * A Graql shell session for a single client, running on one graph in one thread
 */
class GraqlSession {
    private final Session session;
    private final MindmapsGraph graph;
    private final Reasoner reasoner;
    private final Logger LOG = LoggerFactory.getLogger(GraqlSession.class);


    private boolean queryCancelled = false;

    // All requests are run within a single thread, so they always happen in a single thread-bound transaction
    private final ExecutorService queryExecutor = Executors.newSingleThreadExecutor();

    GraqlSession(Session session, MindmapsGraph graph) {
        this.session = session;
        this.graph = graph;
        reasoner = new Reasoner(graph);
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
     * Execute the Graql query described in the given JSON request
     */
    void executeQuery(Json json) {
        queryExecutor.submit(() -> {

            String errorMessage = null;

            try {
                String queryString = json.at(QUERY).asString();

                Query<?> query = withGraph(graph).parse(queryString);

                if (query instanceof MatchQuery) {
                    query = reasonMatchQuery((MatchQuery) query);
                }

                // Return results unless query is cancelled
                query.resultsString().forEach(result -> {
                    if (queryCancelled) return;
                    sendQueryResult(result);
                });
                queryCancelled = false;

                if (!query.isReadOnly()) {
                    reasoner.linkConceptTypes();
                }

            } catch (IllegalArgumentException | IllegalStateException | ConceptException e) {
                errorMessage = e.getMessage();
            } catch (Throwable e) {
                errorMessage = "An unexpected error occurred";
                LOG.error(errorMessage,e);
            } finally {
                if (errorMessage != null) {
                    sendQueryError(errorMessage);
                } else {
                    sendQueryEnd();
                }
            }
        });
    }

    void stopQuery() {
        queryCancelled = true;
    }

    /**
     * Commit and report any errors to the client
     */
    void commit() {
        queryExecutor.submit(() -> {
            try {
                graph.commit();
            } catch (MindmapsValidationException e) {
                sendCommitError(e.getMessage());
            }
        });
    }

    /**
     * Rollback the transaction, removing uncommitted changes
     */
    void rollback() {
        queryExecutor.submit(graph::rollback);
    }

    /**
     * Find autocomplete results and send them to the client
     */
    void autocomplete(Json json) {
        queryExecutor.submit(() -> {
            String queryString = json.at(QUERY).asString();
            int cursor = json.at(AUTOCOMPLETE_CURSOR).asInteger();
            Autocomplete autocomplete = Autocomplete.create(graph, queryString, cursor);
            sendAutocomplete(autocomplete);
        });
    }

    /**
     * Apply reasoner to match query
     */
    private MatchQuery reasonMatchQuery(MatchQuery query) {
        // Expand match query with reasoner, if there are any rules in the graph
        // TODO: Make sure reasoner still applies things such as limit, even with rules in the graph
        if (!reasoner.getRules().isEmpty()) {
            return reasoner.resolveToQuery(query);
        } else {
            return query;
        }
    }

    /**
     * Send a single query result back to the client
     */
    private void sendQueryResult(String result) {
        sendJson(Json.object(
                ACTION, ACTION_QUERY,
                QUERY_LINES, Json.array(result)
        ));
    }

    /**
     * Tell the client that there are no more query results
     */
    private void sendQueryEnd() {
        sendJson(Json.object(ACTION, ACTION_QUERY_END));
    }

    /**
     * Tell the client about an error in their query
     */
    private void sendQueryError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_QUERY_END,
                ERROR, errorMessage
        ));
    }

    /**
     * Tell the client about an error during commit
     */
    private void sendCommitError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_COMMIT,
                ERROR, errorMessage
        ));
    }

    /**
     * Send the given autocomplete results to the client
     */
    private void sendAutocomplete(Autocomplete autocomplete) {
        sendJson(Json.object(
                ACTION, ACTION_AUTOCOMPLETE,
                AUTOCOMPLETE_CANDIDATES, autocomplete.getCandidates(),
                AUTOCOMPLETE_CURSOR, autocomplete.getCursorPosition()
        ));
    }

    /**
     * Send the given JSON to the client
     */
    private void sendJson(Json json) {
        try {
            session.getRemote().sendString(json.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
