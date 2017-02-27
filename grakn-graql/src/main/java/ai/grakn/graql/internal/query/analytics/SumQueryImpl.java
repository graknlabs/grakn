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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class SumQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements SumQuery {

    SumQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Optional<Number> execute() {
        LOGGER.info("SumMapReduce is called");
        long startTime = System.currentTimeMillis();

        initSubGraph();
        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();
        Set<TypeName> allSubTypes = getCombinedSubTypes();

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(allSubTypes, statisticsResourceTypeNames),
                new SumMapReduce(statisticsResourceTypeNames, dataType));
        Map<Serializable, Number> sum = result.memory().get(SumMapReduce.class.getName());

        LOGGER.info("SumMapReduce is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return Optional.of(sum.get(MapReduce.NullObject.instance()));
    }

    @Override
    public SumQuery of(String... resourceTypeNames) {
        return (SumQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public SumQuery of(Collection<TypeName> resourceTypeNames) {
        return (SumQuery) setStatisticsResourceType(resourceTypeNames);
    }

    @Override
    public SumQuery in(String... subTypeNames) {
        return (SumQuery) super.in(subTypeNames);
    }

    @Override
    public SumQuery in(Collection<TypeName> subTypeNames) {
        return (SumQuery) super.in(subTypeNames);
    }

    @Override
    public SumQuery withGraph(GraknGraph graph) {
        return (SumQuery) super.withGraph(graph);
    }

    @Override
    String getName() {
        return "sum";
    }
}
