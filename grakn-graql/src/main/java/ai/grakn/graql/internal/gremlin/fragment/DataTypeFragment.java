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
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.Var;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;

import static ai.grakn.util.Schema.VertexProperty.DATA_TYPE;

@AutoValue
abstract class DataTypeFragment extends Fragment {

    abstract AttributeType.DataType dataType();

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversalInner(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx tx, Collection<Var> vars) {
        return traversal.has(DATA_TYPE.name(), dataType().getName());
    }

    @Override
    public String name() {
        return "[datatype:" + dataType().getName() + "]";
    }

    @Override
    public double fragmentCost() {
        return COST_DATA_TYPE;
    }
}
