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

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;

abstract class AbstractInternalFactory<M extends AbstractGraknGraph<G>, G extends Graph> implements InternalFactory<M, G> {
	
    protected final String keyspace;
    protected final String engineUrl;
    protected final String config;

    protected M graknGraph = null;
    private M batchLoadingGraknGraph = null;

    protected G graph = null;
    private G batchLoadingGraph = null;

    private Boolean lastGraphBuiltBatchLoading = null;
    
    private SystemKeyspace<M, G> systemKeyspace;
    
    private SystemKeyspace<M, G> systemSpace() {
    	if (this.systemKeyspace == null)
    		this.systemKeyspace = new SystemKeyspace<M, G>(this.engineUrl, this.config);
    	return this.systemKeyspace;
    }
    
    AbstractInternalFactory(String keyspace, String engineUrl, String config){
        if(keyspace == null) {
            throw new GraphRuntimeException(ErrorMessage.NULL_VALUE.getMessage("keyspace"));
        }

        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.config = config;        
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildGraknGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph();

    @Override
    public synchronized M getGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraknGraph = getGraph(batchLoadingGraknGraph, batchLoading);
            lastGraphBuiltBatchLoading = true;
            System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] walking away with graph [" + batchLoadingGraknGraph.getTinkerPopGraph().hashCode() +"]");
            return batchLoadingGraknGraph;
        } else {
            graknGraph = getGraph(graknGraph, batchLoading);
            lastGraphBuiltBatchLoading = false;
            System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] walking away with graph [" + graknGraph.getTinkerPopGraph().hashCode() +"]");
            return graknGraph;
        }
    }
    protected M getGraph(M graknGraph, boolean batchLoading){
        //This checks if the previous graph built with this factory is the same as the one we trying to build now.
        if(lastGraphBuiltBatchLoading != null && lastGraphBuiltBatchLoading != batchLoading && graknGraph != null){
            //This then checks if the previous graph built has undergone a commit
            boolean hasCommitted = false;
            if(lastGraphBuiltBatchLoading) {
                hasCommitted = batchLoadingGraknGraph.hasCommitted();
            } else {
                hasCommitted = graknGraph.hasCommitted();
            }

            //Closes the graph to force a full refresh if the other graph has committed
            if(hasCommitted){
                try {
                    graknGraph.closeGraph(ErrorMessage.CLOSED_FACTORY.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if(graknGraph == null){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
            if (!SystemKeyspace.SYSTEM_GRAPH_NAME.equalsIgnoreCase(this.keyspace))
            	systemSpace().keyspaceOpened(this.keyspace);
        } else {
            //synchronized (graknGraph) {
                if (graknGraph.isClosed()) {
                    System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] building new graph");
                    graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
                } else {
                    //This check exists because the innerGraph could be closed while the grakn graph is still flagged as open.
                    G innerGraph = graknGraph.getTinkerPopGraph();
                    synchronized (innerGraph) {
                        if (isClosed(innerGraph)) {
                            System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] refreshing new graph on open grakn graph");
                            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
                        } else {
                            System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] refreshing fully open graph");
                            getGraphWithNewTransaction(graknGraph.getTinkerPopGraph());
                        }
                    }
                }
            //}
        }

        return graknGraph;
    }

    @Override
    public synchronized G getTinkerPopGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraph = getTinkerPopGraph(batchLoadingGraph);
            return batchLoadingGraph;
        } else {
            graph = getTinkerPopGraph(graph);
            return graph;
        }
    }
    protected G getTinkerPopGraph(G graph){
        if(graph == null){
            return getGraphWithNewTransaction(buildTinkerPopGraph());
        }

        synchronized (graph){ //Block here because list of open transactions is not thread safe
            if(isClosed(graph)){
                System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] getting new graph because [" + graph.hashCode() + "] is closed");
                return getGraphWithNewTransaction(buildTinkerPopGraph());
            } else {
                System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] refreshing on existing graph [" + graph.hashCode() + "]");
                return getGraphWithNewTransaction(graph);
            }
        }
    }
    protected G getGraphWithNewTransaction(G graph){
        return graph;
    }

}
