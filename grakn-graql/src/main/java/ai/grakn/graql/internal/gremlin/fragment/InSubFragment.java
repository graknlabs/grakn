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

import ai.grakn.GraknTx;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Map;
import java.util.Set;

class InSubFragment extends AbstractFragment {

    InSubFragment(VarProperty varProperty, Var start, Var end) {
        super(varProperty, start, end);
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {
        return Fragments.inSubs(Fragments.isVertex(traversal));
    }

    @Override
    public String getName() {
        return "<-[sub]-";
    }

    @Override
    public double fragmentCost() {
        return COST_SUBTYPES_PER_TYPE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return getDirectedEdges(NodeId.NodeType.SUB, nodes, edges);
    }
}
