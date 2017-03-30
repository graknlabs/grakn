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

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.CheckReturnValue;
import java.util.Properties;

import static javax.annotation.meta.When.NEVER;

/**
 * <p>
 *     Defines the construction of Grakn Graphs
 * <p/>
 *
 * <p>
 *     Defines the abstract construction of Grakn graphs on top of Tinkerpop Graphs.
 *     For this factory to function a vendor specific implementation of a graph extending
 *     {@link AbstractGraknGraph} must be provided. This must be provided with a matching TinkerPop {@link Graph}
 *     which is wrapped within the Grakn Graph
 * </p>
 *
 * @author fppt
 *
 * @param <M> A Graph Graph extending {@link AbstractGraknGraph} and wrapping a Tinkerpop Graph
 * @param <G> A vendor implementation of a Tinkerpop {@link Graph}
 */
abstract class AbstractInternalFactory<M extends AbstractGraknGraph<G>, G extends Graph> implements InternalFactory<M, G> {

    protected final String keyspace;
    protected final String engineUrl;
    protected final Properties properties;

    protected M graknGraph = null;
    private M batchLoadingGraknGraph = null;

    protected G graph = null;
    private G batchLoadingGraph = null;

    private SystemKeyspace<M, G> systemKeyspace;

    AbstractInternalFactory(String keyspace, String engineUrl, Properties properties){
        if(keyspace == null) {
            throw new GraphRuntimeException(ErrorMessage.NULL_VALUE.getMessage("keyspace"));
        }

        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.properties = properties;

        if(!keyspace.equals(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            systemKeyspace = new SystemKeyspace<>(getSystemFactory());
        }
    }

    InternalFactory<M, G> getSystemFactory(){
        //noinspection unchecked
        return FactoryBuilder.getGraknGraphFactory(this.getClass().getName(), SystemKeyspace.SYSTEM_GRAPH_NAME, engineUrl, properties);
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildGraknGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph(boolean batchLoading);

    @Override
    public synchronized M open(GraknTxType txType){
        if(GraknTxType.BATCH.equals(txType)){
            batchLoadingGraknGraph = getGraph(batchLoadingGraknGraph, true);
            batchLoadingGraknGraph.openTransaction(false);
            return batchLoadingGraknGraph;
        } else {
            graknGraph = getGraph(graknGraph, false);
            if(GraknTxType.WRITE.equals(txType)) {
                graknGraph.openTransaction(false);
            } else if(GraknTxType.READ.equals(txType)) {
                graknGraph.openTransaction(true);
            }
            return graknGraph;
        }
    }
    protected M getGraph(M graknGraph, boolean batchLoading){
        if(graknGraph == null){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
            if (!SystemKeyspace.SYSTEM_GRAPH_NAME.equalsIgnoreCase(this.keyspace)) {
                systemKeyspace.keyspaceOpened(this.keyspace);
            }
        } else {
            if(graknGraph.isClosed()){
                //TODO: Get rid of this redundant open. This is only here so we can do the inner check later
                graknGraph.openTransaction(false);
            } else {
                throw new GraphRuntimeException(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(graknGraph.getKeyspace()));
            }

            //This check exists because the innerGraph could be closed while the grakn graph is still flagged as open.
            G innerGraph = graknGraph.getTinkerPopGraph();
            synchronized (innerGraph){
                if(isClosed(innerGraph)){
                    graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
                } else {
                    getGraphWithNewTransaction(graknGraph.getTinkerPopGraph());
                }
            }
        }

        return graknGraph;
    }

    @Override
    public synchronized G getTinkerPopGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraph = getTinkerPopGraph(batchLoadingGraph, true);
            return batchLoadingGraph;
        } else {
            graph = getTinkerPopGraph(graph, false);
            return graph;
        }
    }
    protected G getTinkerPopGraph(G graph, boolean batchLoading){
        if(graph == null){
            return getGraphWithNewTransaction(buildTinkerPopGraph(batchLoading));
        }

        synchronized (graph){ //Block here because list of open transactions is not thread safe
            if(isClosed(graph)){
                return getGraphWithNewTransaction(buildTinkerPopGraph(batchLoading));
            } else {
                return getGraphWithNewTransaction(graph);
            }
        }
    }

    @CheckReturnValue(when=NEVER)
    protected abstract G getGraphWithNewTransaction(G graph);

}
