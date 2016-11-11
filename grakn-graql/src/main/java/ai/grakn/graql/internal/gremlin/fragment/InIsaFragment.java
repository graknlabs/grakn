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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.util.Schema;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inSubs;
import static ai.grakn.util.Schema.BaseType.ROLE_TYPE;
import static ai.grakn.util.Schema.EdgeLabel.ISA;

class InIsaFragment extends AbstractFragment {

    private final boolean allowCastings;

    InIsaFragment(String start, String end, boolean allowCastings) {
        super(start, end);
        this.allowCastings = allowCastings;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        if (!allowCastings) {
            // Make sure we never get instances of role types
            traversal.not(__.hasLabel(Schema.BaseType.ROLE_TYPE.name()));
        }
        Fragments.inSubs(Fragments.inSubs(traversal).in(Schema.EdgeLabel.ISA.getLabel()));
    }

    @Override
    public String getName() {
        return "<-[isa" + (allowCastings ? ":allow-castings" : "") + "]-";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNBOUNDED;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_INSTANCES_PER_TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InIsaFragment that = (InIsaFragment) o;

        return allowCastings == that.allowCastings;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (allowCastings ? 1 : 0);
        return result;
    }
}
