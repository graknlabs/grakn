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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import ai.grakn.util.ErrorMessage;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the match query with.
 */
class MatchQueryGraph extends MatchQueryModifier {

    private final GraknGraph graph;

    MatchQueryGraph(GraknGraph graph, AbstractMatchQuery inner) {
        super(inner);
        this.graph = graph;
    }

    @Override
    public Stream<Map<VarName, Concept>> stream(Optional<GraknGraph> graph) {
        if (graph.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_GRAPH.getMessage());
        }

        return inner.stream(Optional.of(this.graph));
    }

    @Override
    public Optional<GraknGraph> getGraph() {
        return Optional.of(graph);
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes(graph);
    }

    @Override
    protected String modifierString() {
        return "";
    }
}
