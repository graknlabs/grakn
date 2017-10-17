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
 *
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknTx;
import ai.grakn.graql.Var;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

import static ai.grakn.graql.internal.pattern.Patterns.PLAYER_VAR;
import static ai.grakn.graql.internal.pattern.Patterns.RELATION_DIRECTION;
import static ai.grakn.graql.internal.pattern.Patterns.RELATION_EDGE;
import static ai.grakn.graql.internal.pattern.Patterns.ROLE_LABEL;
import static ai.grakn.graql.internal.pattern.Patterns.var;
import static ai.grakn.util.Schema.EdgeLabel.ROLE_PLAYER;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_LABEL_ID;

/**
 * A fragment representing traversing a {@link ai.grakn.util.Schema.EdgeLabel#ROLE_PLAYER} edge from the relation to the
 * role-player.
 * <p>
 * Part of a {@link ai.grakn.graql.internal.gremlin.EquivalentFragmentSet}, along with {@link InRolePlayerFragment}.
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class OutRolePlayerFragment extends AbstractRolePlayerFragment {

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, GraknTx graph, Collection<Var> vars) {

        return Fragments.union(traversal, ImmutableSet.of(
                reifiedRelationTraversal(graph, vars),
                edgeRelationTraversal(graph, Direction.OUT, RELATIONSHIP_ROLE_OWNER_LABEL_ID, vars),
                edgeRelationTraversal(graph, Direction.IN, RELATIONSHIP_ROLE_VALUE_LABEL_ID, vars)
        ));
    }

    private GraphTraversal<Element, Vertex> reifiedRelationTraversal(GraknTx graph, Collection<Var> vars) {
        Var edge = var();
        Var player = var();

        GraphTraversal<Element, Vertex> traversal = Fragments.isVertex(__.identity());

        GraphTraversal<Element, Edge> edgeTraversal = traversal.outE(ROLE_PLAYER.getLabel()).as(edge.name());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, ROLE_LABEL_ID, roleLabels(), graph);
        applyLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels(), graph);

        traverseToRole(edgeTraversal, role(), ROLE_LABEL_ID, vars);

        // Identify the role - role-player pair by combining the
        return edgeTraversal
                .values(ROLE_LABEL_ID.name()).as(ROLE_LABEL.name())
                .select(edge.name()).inV().as(PLAYER_VAR.name()).as(player.name())
                .select(Pop.last, ROLE_LABEL.name(), PLAYER_VAR.name()).as(roleAndPlayer().name())
                .select(player.name());
    }

    private GraphTraversal<Element, Vertex> edgeRelationTraversal(
            GraknTx graph, Direction direction, Schema.EdgeProperty roleProperty, Collection<Var> vars) {
        GraphTraversal<Element, Edge> edgeTraversal = Fragments.isEdge(__.identity());

        // Filter by any provided type labels
        applyLabelsToTraversal(edgeTraversal, roleProperty, roleLabels(), graph);
        applyLabelsToTraversal(edgeTraversal, RELATIONSHIP_TYPE_LABEL_ID, relationTypeLabels(), graph);

        traverseToRole(edgeTraversal, role(), roleProperty, vars);

        // Identify the relation - role-player pair by combining the relationship edge and direction into a map
        return edgeTraversal
                .as(RELATION_EDGE.name()).constant(direction).as(RELATION_DIRECTION.name())
                .select(Pop.last, RELATION_EDGE.name(), RELATION_DIRECTION.name()).as(roleAndPlayer().name())
                .select(RELATION_EDGE.name())
                .toV(direction);
    }

    @Override
    public String name() {
        return "-" + innerName() + "->";
    }

    @Override
    public double fragmentCost() {
        return roleLabels() != null ? COST_ROLE_PLAYERS_PER_ROLE : COST_ROLE_PLAYERS_PER_RELATION;
    }

    @Override
    public boolean canOperateOnEdges() {
        return true;
    }
}
