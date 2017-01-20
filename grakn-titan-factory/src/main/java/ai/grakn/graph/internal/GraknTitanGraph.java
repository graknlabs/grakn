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

package ai.grakn.graph.internal;

import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

/**
 * <p>
 *     A Grakn Graph using {@link TitanGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link TitanGraph} as a method of storing the Grakn Graph object Model.
 *     With this vendor some issues to be aware of:
 *     1. Whenever a transaction is closed if none remain open then the connection to the graph is closed permanently.
 *     2. Clearing the graph explicitly closes the connection as well.
 * </p>
 *
 * @author fppt
 */
public class GraknTitanGraph extends AbstractGraknGraph<TitanGraph> {
    public GraknTitanGraph(TitanGraph graph, String name, String engineUrl, boolean batchLoading){
        super(graph, name, engineUrl, batchLoading);
    }

    @Override
    protected void clearGraph() {
        TitanGraph titanGraph = getTinkerPopGraph();
        titanGraph.close();
        TitanCleanup.clear(titanGraph);
    }

    @Override
    public void closeGraph(String reason){
        finaliseClose(this::closeTitan, reason);
    }

    @Override
    public TitanGraph getTinkerPopGraph(){
        TitanGraph graph =  super.getTinkerPopGraph();
        if(graph.isClosed()){
            throw new GraphRuntimeException(ErrorMessage.GRAPH_PERMANENTLY_CLOSED.getMessage(getKeyspace()));
        } else {
            return graph;
        }
    }

    @Override
    public void commitTransaction(){
        try {
            super.commitTransaction();
        } catch (TitanException e){
            throw new GraknBackendException(e);
        }

        if(!getTinkerPopGraph().tx().isOpen()){
            getTinkerPopGraph().tx().open(); //Until we sort out the transaction handling properly commits have to result in transactions being auto opened
        }
    }

    private void closeTitan(){
        StandardTitanGraph graph = (StandardTitanGraph) getTinkerPopGraph();
        synchronized (graph) { //Have to block here because the list of open transactions in Titan is not thread safe.
            if(graph.tx().isOpen()) {
                graph.tx().close();
            }

            if (graph.getOpenTxs() == 0) {
                closePermanent();
            }
        }
    }
}
