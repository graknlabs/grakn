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

import ai.grakn.concept.Concept;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * <p>
 *     A Grakn Graph using {@link TinkerGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link TinkerGraph} as a method of storing the Grakn Graph object Model.
 *     With this vendor some exceptions are in place:
 *     1. Transactions do not exists and all threads work on the same graph at the same time.
 *     2. The {@link #rollback} operation is unsupported due to Tinkerpop Transactions not being supported.
 * </p>
 *
 * @author fppt
 */
public class GraknTinkerGraph extends AbstractGraknGraph<TinkerGraph> {
    public GraknTinkerGraph(TinkerGraph tinkerGraph, String name, String engineUrl, boolean batchLoading){
        super(tinkerGraph, name, engineUrl, batchLoading);
    }

    /**
     *
     * @param concept A concept in the graph
     * @return true all the time. There is no way to know if a
     * {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex} has been modified or not.
     */
    @Override
    public boolean isConceptModified(ConceptImpl concept) {
        return true;
    }

    @Override
    public <T extends Concept> T getConceptByBaseIdentifier(Object baseIdentifier) {
        try {
            return super.getConceptByBaseIdentifier(Long.valueOf(baseIdentifier.toString()));
        } catch (NumberFormatException e){
            return null;
        }
    }

    @Override
    public void rollback(){
        throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage(getTinkerPopGraph().getClass().getName(), "rollback"));
    }

}
