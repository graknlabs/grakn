/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.planning.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import grakn.core.core.Schema;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static grakn.core.core.Schema.EdgeLabel.PLAYS;
import static grakn.core.core.Schema.EdgeLabel.SUB;
import static grakn.core.core.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static junit.framework.TestCase.assertEquals;

public class OutPlaysFragmentTest {

    private final Variable start = new Variable();
    private final Variable end = new Variable();
    private FragmentImpl fragment;

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsUpwards() {
        fragment = new OutPlaysFragment(null, start, end,false);
        GraphTraversal<Vertex, Vertex> traversal = __.V();
        fragment.applyTraversalInner(traversal, null, ImmutableSet.of());

        GraphTraversal<Object, Vertex> expected = __.V()
                .union(__.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())),
                        __.<Vertex>until(__.loops().is(Fragments.TRAVERSE_ALL_SUB_EDGES)).repeat(__.out(SUB.getLabel())).emit())
                .unfold()
                .out(PLAYS.getLabel());
        assertEquals(expected.toString(), traversal.toString());
    }
}