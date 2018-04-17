/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.CentralityQueryBuilder;
import ai.grakn.graql.analytics.ClusterQueryBuilder;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;

import javax.annotation.CheckReturnValue;
import java.util.Map;

/**
 * Starting point for creating compute queries.
 *
 * @author Jason Liu
 */
public interface ComputeQueryBuilder {

    /**
     * @param tx the {@link GraknTx} to execute the compute query on
     * @return a compute query builder with the graph set
     */
    @CheckReturnValue
    ComputeQueryBuilder withTx(GraknTx tx);

    /**
     * @return a count query that will count the number of instances
     */
    @CheckReturnValue
    CountQuery count();

    /**
     * @return a min query that will find the min value of the given resource types
     */
    @CheckReturnValue
    MinQuery min();

    /**
     * @return a max query that will find the max value of the given resource types
     */
    @CheckReturnValue
    MaxQuery max();

    /**
     * @return a sum query that will compute the sum of values of the given resource types
     */
    @CheckReturnValue
    SumQuery sum();

    /**
     * @return a mean query that will compute the mean of values of the given resource types
     */
    @CheckReturnValue
    MeanQuery mean();

    /**
     * @return a std query that will compute the standard deviation of values of the given resource types
     */
    @CheckReturnValue
    StdQuery std();

    /**
     * @return a median query that will compute the median of values of the given resource types
     */
    @CheckReturnValue
    MedianQuery median();

    /**
     * @return a path query that will find the shortest path between two instances
     */
    @CheckReturnValue
    PathQuery path();

    /**
     * @return a path query that will find all shortest path between two instances
     */
    @CheckReturnValue
    PathsQuery paths();

    /**
     * This method is deprecated. Please use centrality query instead.
     *
     * @return a cluster query that will find the connected components
     */
    @Deprecated
    @CheckReturnValue
    ConnectedComponentQuery<Map<String, Long>> connectedComponent();

    /**
     * This method is deprecated. Please use cluster query instead.
     *
     * @return a k-core query that will find interlinked core areas using k-core.
     */
    @Deprecated
    @CheckReturnValue
    KCoreQuery kCore();

    /**
     * This method is deprecated. Please use centrality query instead.
     *
     * @return a coreness query that computes centrality using k-core.
     */
    @Deprecated
    @CheckReturnValue
    CorenessQuery coreness();

    /**
     * This method is deprecated. Please use centrality query instead.
     *
     * @return a degree query that will compute the degree of instances
     */
    @Deprecated
    @CheckReturnValue
    DegreeQuery degree();

    /**
     * @return a centrality query builder for creating centrality query
     */
    @CheckReturnValue
    CentralityQueryBuilder centrality();

    /**
     * @return a cluster query builder for creating cluster query
     */
    @CheckReturnValue
    ClusterQueryBuilder cluster();
}
