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

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
@FixMethodOrder(value= MethodSorters.NAME_ASCENDING)
public class TitanPreviousPropertyStepTest extends TitanTestBase {

    private static final GraphTraversalSource tinker = TinkerGraph.open().traversal();

    private static final GraphTraversalSource titan = titanGraphFactory.open(GraknTxType.WRITE).getTinkerPopGraph().traversal();

    private static final Vertex vertexWithFoo = titan.addV().property("v prop", "foo").next();
    private static final Vertex vertexWithBar = titan.addV().property("v prop", "bar").next();
    private static final Vertex vertexWithoutProperty = titan.addV().next();
    private static final Edge edge = titan.V(vertexWithoutProperty).as("x").addE("self").to("x").property("e prop", "foo").next();

    @Test
    public void whenFilteringAPropertyToBeEqualToAPreviousProperty_UseTitanGraphStep() {
        GraphTraversal traversal = optimisableTraversal(titan);

        GraphTraversal expected = optimisedTraversal(titan);

        traversal.asAdmin().applyStrategies();

        assertEquals(expected, traversal);
    }

    @Test
    public void whenExecutingOptimisedTraversal_ResultIsCorrect() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(titan);

        List<Vertex> vertices = traversal.toList();

        assertThat(vertices, contains(vertexWithFoo));
    }

    @Test
    public void whenExecutingManuallyOptimisedTraversal_ResultIsCorrect() {
        GraphTraversal<Vertex, Vertex> traversal = optimisedTraversal(titan);

        List<Vertex> vertices = traversal.toList();

        assertThat(vertices, contains(vertexWithFoo));
    }

    @Test
    public void whenUsingATitanGraph_ApplyStrategy() {
        GraphTraversal<?, ?> traversal = optimisableTraversal(titan);
        traversal.asAdmin().applyStrategies();

        List<Step> steps = traversal.asAdmin().getSteps();
        assertThat(steps, hasItem(instanceOf(TitanPreviousPropertyStep.class)));
    }

    @Test
    public void whenUsingANonTitanGraph_DontApplyStrategy() {
        GraphTraversal<?, ?> traversal = optimisableTraversal(tinker);
        traversal.asAdmin().applyStrategies();

        List<Step> steps = traversal.asAdmin().getSteps();
        assertThat(steps, not(hasItem(instanceOf(TitanPreviousPropertyStep.class))));
    }

    @Test
    public void whenUsingATitanGraph_TheTitanPreviousPropertyStepStrategyIsInList() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(titan);
        List<TraversalStrategy<?>> strategies = traversal.asAdmin().getStrategies().toList();

        assertThat(strategies, hasItem(instanceOf(TitanPreviousPropertyStepStrategy.class)));
    }

    @Test
    public void whenUsingANonTitanGraph_TheTitanPreviousPropertyStepStrategyIsNotInList() {
        GraphTraversal<Vertex, Vertex> traversal = optimisableTraversal(tinker);
        List<TraversalStrategy<?>> strategies = traversal.asAdmin().getStrategies().toList();

        assertThat(strategies, not(hasItem(instanceOf(TitanPreviousPropertyStepStrategy.class))));
    }

    private GraphTraversal<Vertex, Vertex> optimisableTraversal(GraphTraversalSource g) {
        return g.V().outE().values("e prop").as("x").V().has("v prop", __.where(P.eq("x")));
    }

    private GraphTraversal<Vertex, Vertex> optimisedTraversal(GraphTraversalSource g) {
        GraphTraversal expected = g.V().outE().values("e prop").as("x");

        GraphTraversal.Admin<Vertex, Object> admin = expected.asAdmin();
        TitanPreviousPropertyStep<?> graphStep = new TitanPreviousPropertyStep<>(admin, "v prop", "x");
        admin.addStep(graphStep);

        admin.applyStrategies();

        return expected;
    }

}
