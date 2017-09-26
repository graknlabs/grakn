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

package ai.grakn.graql.internal.parser;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Felix Chapman
 */
public class GremlinVisitorTest {

    @Test
    public void whenPrettifyingSimpleTraversal_ResultIsExpectedString() {
        GraphTraversal<?, ?> traversal = __.V().out("knows").as("x");

        String pretty = GremlinVisitor.prettify(traversal);

        assertEquals(
                "[\n" +
                "    GraphStep(vertex, []), \n" +
                "    VertexStep(OUT, [knows], vertex)@[x]\n" +
                "]",
                pretty
        );
    }
}