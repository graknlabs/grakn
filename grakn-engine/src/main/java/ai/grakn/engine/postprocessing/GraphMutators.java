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

package ai.grakn.engine.postprocessing;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * <p>
 *     Abstract class containing utilities for graph mutations
 * </p>
 *
 * <p>
 *     This utility class is used to help with background tasks which need to mutate a graph
 * </p>
 *
 * @author alexandraorth, fppt
 */
public abstract class GraphMutators {

    private static final Logger LOG = LoggerFactory.getLogger(GraphMutators.class);

    /**
     *
     *
     * @param keyspace keyspace of the graph to mutate
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     */
    public static void runBatchMutationWithRetry(
            EngineGraknGraphFactory factory, String keyspace, int maxRetry, Consumer<GraknGraph> mutatingFunction){
        runGraphMutationWithRetry(factory, keyspace, GraknTxType.BATCH, maxRetry, mutatingFunction);
    }

    /**
     *
     * @param keyspace keyspace of the graph to mutate
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     */
    static void runGraphMutationWithRetry(
            EngineGraknGraphFactory factory, String keyspace, int maxRetry, Consumer<GraknGraph> mutatingFunction){
        runGraphMutationWithRetry(factory, keyspace, GraknTxType.WRITE, maxRetry, mutatingFunction);
    }

    /**
     *
     * @param keyspace keyspace of the graph to mutate
     * @param mutatingFunction Function that accepts a graph object and will mutate the given graph
     */
    private static void runGraphMutationWithRetry(
            EngineGraknGraphFactory factory, String keyspace, GraknTxType txType, int maxRetry,
            Consumer<GraknGraph> mutatingFunction
    ){
        //TODO Really hideous hack that will be removed when we move SystemKeyspace to engine
        SystemKeyspace.initialise(
                factory.properties().getProperty(GraknEngineConfig.SERVER_HOST_NAME) + ":" + factory.properties().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER),
                factory.properties());

        if(!SystemKeyspace.containsKeyspace(keyspace)){ //This may be slow.
            LOG.warn("Attempting to execute mutation on graph [" + keyspace + "] which no longer exists");
            return;
        }

        for(int retry = 0; retry < maxRetry; retry++) {
            try(GraknGraph graph = factory.getGraph(keyspace, txType))  {

                mutatingFunction.accept(graph);

                return;
            } catch (GraknBackendException e){
                // retry...
                LOG.debug(ErrorMessage.GRAPH_MUTATION_ERROR.getMessage(e.getMessage()), e);
            }

            performRetry(retry);
        }

        throw new RuntimeException(ErrorMessage.UNABLE_TO_MUTATE_GRAPH.getMessage(keyspace));
    }

    /**
     * Sleep the current thread for a random amount of time
     * @param retry Seed with which to calculate sleep time
     */
    private static void performRetry(int retry){
        double seed = 1.0 + (Math.random() * 5.0);
        double waitTime = (retry * 2.0)  + seed;
        LOG.debug(ErrorMessage.BACK_OFF_RETRY.getMessage(waitTime));

        try {
            Thread.sleep((long) Math.ceil(waitTime * 1000));
        } catch (InterruptedException e1) {
            LOG.error("Exception",e1);
        }
    }
}