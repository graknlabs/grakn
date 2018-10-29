/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.kb.internal.structure;

import ai.grakn.concept.Entity;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.kb.internal.concept.EntityImpl;
import ai.grakn.kb.internal.concept.EntityTypeImpl;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EdgeTest extends TxTestBase {

    private EntityTypeImpl entityType;
    private EntityImpl entity;
    private EdgeElement edge;

    @Before
    public void createEdge(){
        entityType = (EntityTypeImpl) tx.putEntityType("My Entity Type");
        entity = (EntityImpl) entityType.create();
        Edge tinkerEdge = tx.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), entity.id().getValue()).outE().next();
        edge = new EdgeElement(tx, tinkerEdge);
    }

    @Test
    public void checkEqualityBetweenEdgesBasedOnID(){
        Entity entity2 = entityType.create();
        Edge tinkerEdge = tx.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), entity2.id().getValue()).outE().next();
        EdgeElement edge2 = new EdgeElement(tx, tinkerEdge);

        assertEquals(edge, edge);
        assertNotEquals(edge, edge2);
    }

    @Test
    public void whenGettingTheSourceOfAnEdge_ReturnTheConceptTheEdgeComesFrom() throws Exception {
        assertEquals(entity.vertex(), edge.source());
    }

    @Test
    public void whenGettingTheTargetOfAnEdge_ReturnTheConceptTheEdgePointsTowards() throws Exception {
        assertEquals(entityType.currentShard().vertex(), edge.target());
    }

    @Test
    public void whenGettingTheLabelOfAnEdge_ReturnExpectedType() throws Exception {
        assertEquals(Schema.EdgeLabel.ISA.getLabel(), edge.label());
    }
}