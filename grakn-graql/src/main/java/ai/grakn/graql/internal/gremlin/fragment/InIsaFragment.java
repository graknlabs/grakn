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

import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.Schema.EdgeLabel.ISA;
import static ai.grakn.util.Schema.EdgeLabel.SHARD;

class InIsaFragment extends AbstractFragment {

    InIsaFragment(VarProperty varProperty, Var start, Var end) {
        super(varProperty, start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal, GraknGraph graph) {
        Fragments.inSubs(traversal).in(SHARD.getLabel()).in(ISA.getLabel());
    }

    @Override
    public String getName() {
        return "<-[isa]-";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_INSTANCES_PER_TYPE;
    }
}
