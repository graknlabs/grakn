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

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.constants.DataType;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static io.mindmaps.constants.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static io.mindmaps.constants.DataType.EdgeLabel.SHORTCUT;
import static io.mindmaps.constants.DataType.EdgeProperty.TO_TYPE;

/**
 * A class for handling ordering match queries.
 */
public class MatchOrder {

    private final String var;
    private final Optional<String> resourceType;
    private final boolean asc;

    public MatchOrder(String var, Optional<String> resourceType, boolean asc) {
        this.var = var;
        this.resourceType = resourceType;
        this.asc = asc;
    }

    public String getVar() {
        return var;
    }

    /**
     * Order the traversal
     * @param traversal the traversal to order
     */
    public void orderTraversal(MindmapsTransaction transaction, GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        if (resourceType.isPresent()) {
            // Order by resource type

            // Look up datatype of resource type
            String typeId = resourceType.get();
            DataType.ConceptProperty valueProp = transaction.getResourceType(typeId).getDataType().getConceptProperty();

            Comparator<Optional<Comparable>> comparator = new ResourceComparator();
            if (!asc) comparator = comparator.reversed();

            traversal.select(var).order().by(v -> getResourceValue(transaction, v, typeId, valueProp), comparator);
        } else {
            // Order by ITEM_IDENTIFIER by default
            Order order = asc ? Order.incr : Order.decr;
            traversal.select(var).order().by(ITEM_IDENTIFIER.name(), order);
        }
    }

    /**
     * Get the value of an attached resource, used for ordering by resource
     * @param elem the element in the graph (a gremlin object)
     * @param resourceTypeId the ID of a resource type
     * @param value the VALUE property to use on the vertex
     * @return the value of an attached resource, or nothing if there is no resource of this type
     */
    private Optional<Comparable> getResourceValue(MindmapsTransaction transaction, Object elem, String resourceTypeId, DataType.ConceptProperty value) {
        return ((MindmapsTransactionImpl) transaction).getTinkerTraversal().V(elem)
                .outE(SHORTCUT.getLabel()).has(TO_TYPE.name(), resourceTypeId).inV().values(value.name())
                .tryNext().map(o -> (Comparable) o);
    }

    @Override
    public String toString() {
        String resourceString = resourceType.map(r -> "(has " + r + ")").orElse("");
        return "order by $" + var + resourceString + " ";
    }

    /**
     * A comparator that parses (optionally present) resources into the correct datatype for comparison
     */
    private static class ResourceComparator implements Comparator<Optional<Comparable>> {

        @Override
        public int compare(Optional<Comparable> value1, Optional<Comparable> value2) {
            if (!value1.isPresent() && !value2.isPresent()) return 0;
            if (!value1.isPresent()) return -1;
            if (!value2.isPresent()) return 1;

            //noinspection unchecked
            return value1.get().compareTo(value2.get());
        }
    }
}
