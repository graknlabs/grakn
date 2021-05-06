/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.graph.DataGraph;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;

public interface ThingVertex extends Vertex<VertexIID.Thing, Encoding.Vertex.Thing> {

    /**
     * Returns the {@code DataGraph} containing all {@code ThingVertex}.
     *
     * @return the {@code DataGraph} containing all {@code ThingVertex}
     */
    DataGraph graph();

    /**
     * Returns the {@code GraphManager} containing both {@code SchemaGraph} and {@code DataGraph}.
     *
     * @return the {@code GraphManager} containing both {@code SchemaGraph} and {@code DataGraph}
     */
    GraphManager graphs();

    /**
     * Returns the {@code ThingAdjacency} set of outgoing edges.
     *
     * @return the {@code ThingAdjacency} set of outgoing edges
     */
    ThingAdjacency outs();

    /**
     * Returns the {@code ThingAdjacency} set of incoming edges.
     *
     * @return the {@code ThingAdjacency} set of incoming edges
     */
    ThingAdjacency ins();

    /**
     * Returns the {@code TypeVertex} in which this {@code ThingVertex} is an instance of.
     *
     * @return the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     */
    TypeVertex type();

    /**
     * Returns true if this {@code ThingVertex} is a result of inference.
     *
     * @return true if this {@code ThingVertex} is a result of inference
     */
    boolean isInferred();

    /**
     * Sets a boolean flag to indicate whether this vertex was a result of inference.
     *
     * @param isInferred indicating whether this vertex was a result of inference
     */
    void isInferred(boolean isInferred);

    /**
     * Returns true if this {@code ThingVertex} is an instance of {@code AttributeVertex}.
     *
     * @return true if this {@code ThingVertex} is an instance of {@code AttributeVertex}
     */
    boolean isAttribute();

    /**
     * Casts this {@code ThingVertex} into an {@code AttributeVertex} if it is one.
     *
     * @return this object as an {@code AttributeVertex}
     */
    AttributeVertex<?> asAttribute();
}
