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

package ai.grakn.graql.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute the connected components in the subgraph.
 */
public interface ClusterQuery<T> extends ComputeQuery<T> {

    /**
     * Return the instances in each cluster after executing the query. By default, the size of each cluster is
     * returned after executing the query.
     *
     * @return a ClusterQuery with members flag set
     */
    ClusterQuery<Map<String, Set<String>>> members();

    /**
     * Persist the result in the graph after executing the query. Be default, the cluster label is saved as a resource
     * of the vertex, with resource type name "cluster".
     *
     * @return a ClusterQuery with persist flag set
     */
    ClusterQuery<T> persist();

    /**
     * Persist the result in the graph after executing the query. The cluster label is saved as a resource of
     * the vertex.
     *
     * @param resourceTypeName the name of the resource type to save the cluster label
     * @return a ClusterQuery with persist flag and customised resource type name set
     */
    ClusterQuery<T> persist(String resourceTypeName);

    /**
     * @param clusterSize the size of the clusters returned and/or persisted
     * @return a ClusterQuery with cluster set
     */
    ClusterQuery<T> clusterSize(long clusterSize);

    /**
     * @param subTypeNames an array of types to include in the subgraph
     * @return a ClusterQuery with the subTypeNames set
     */
    @Override
    ClusterQuery<T> in(String... subTypeNames);

    /**
     * @param subTypeNames a collection of types to include in the subgraph
     * @return a ClusterQuery with the subTypeNames set
     */
    @Override
    ClusterQuery<T> in(Collection<String> subTypeNames);

    /**
     * @param graph the graph to execute the query on
     * @return a ClusterQuery with the graph set
     */
    @Override
    ClusterQuery<T> withGraph(GraknGraph graph);
}
