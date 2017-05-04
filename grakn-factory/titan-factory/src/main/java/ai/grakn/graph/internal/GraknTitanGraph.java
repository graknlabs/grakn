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

import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknLockingException;
import ai.grakn.factory.FactoryBuilder;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.util.Schema;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Properties;
import java.util.function.Supplier;

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
    public GraknTitanGraph(TitanGraph graph, String name, String engineUrl, boolean batchLoading, Properties properties){
        super(graph, name, engineUrl, batchLoading, properties);
    }

    /**
     * Uses {@link TitanVertex#isModified()}
     *
     * @param concept A concept in the graph
     * @return true if the concept has been modified
     */
    @Override
    public boolean isConceptModified(ConceptImpl concept) {
        TitanVertex vertex = (TitanVertex) concept.getVertex();
        return vertex.isModified() || vertex.isNew();
    }

    @Override
    public void openTransaction(GraknTxType txType){
        super.openTransaction(txType);
        if(getTinkerPopGraph().isOpen() && !getTinkerPopGraph().tx().isOpen()) getTinkerPopGraph().tx().open();
    }

    @Override
    public void closeSession(){
        super.closeSession();

        //Close the system graph if possible
        if(!getKeyspace().equalsIgnoreCase(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            GraknTitanGraph system = (GraknTitanGraph) FactoryBuilder.getFactory(SystemKeyspace.SYSTEM_GRAPH_NAME, getEngineUrl(), getProperties()).open(GraknTxType.READ);
            system.close();
            if (!system.isSessionClosed() && system.numOpenTx() == 0) {
                system.closeSession();
            }
        }
    }

    @Override
    public boolean isSessionClosed() {
        return getTinkerPopGraph().isClosed();
    }

    @Override
    public int numOpenTx() {
        return ((StandardTitanGraph) getTinkerPopGraph()).getOpenTxs();
    }

    @Override
    protected void clearGraph() {
        getTinkerPopGraph().close();
        TitanCleanup.clear(getTinkerPopGraph());
    }

    @Override
    public void commitTransactionInternal(){
        executeLockingMethod(() -> {
            super.commitTransactionInternal();
            return null;
        });
    }

    @Override
    Vertex addVertex(Schema.BaseType baseType){
        return executeLockingMethod(() -> super.addVertex(baseType));
    }

    /**
     * Executes a method which has the potential to throw a {@link TemporaryLockingException} or a {@link PermanentLockingException}.
     * If the exception is thrown it is wrapped in a {@link GraknBackendException} so that the transaction can be retried.
     *
     * @param method The locking method to execute
     */
    private <X> X executeLockingMethod(Supplier<X> method){
        try {
            return method.get();
        } catch (TitanException e){
            if(e.isCausedBy(TemporaryLockingException.class) || e.isCausedBy(PermanentLockingException.class)){
                throw new GraknLockingException(e);
            } else {
                throw new GraknBackendException(e);
            }
        }
    }

    @Override
    public void validVertex(Vertex vertex) {
        super.validVertex(vertex);

        if(((TitanVertex) vertex).isRemoved()){
            throw new IllegalStateException("The vertex [" + vertex + "] has been removed and is no longer valid");
        }
    }
}
