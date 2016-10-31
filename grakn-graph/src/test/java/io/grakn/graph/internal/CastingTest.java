/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.graph.internal;

import io.grakn.Grakn;
import io.grakn.concept.Concept;
import io.grakn.concept.EntityType;
import io.grakn.concept.Relation;
import io.grakn.exception.MoreThanOneEdgeException;
import io.grakn.exception.NoEdgeException;
import io.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CastingTest {

    private AbstractGraknGraph graknGraph;
    private CastingImpl casting;
    private RoleTypeImpl role;
    private RelationImpl relation;
    private InstanceImpl rolePlayer;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.initialiseMetaConcepts();
        role = (RoleTypeImpl) graknGraph.putRoleType("Role");
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("A thing");
        rolePlayer = (InstanceImpl) graknGraph.addEntity(conceptType);
        RelationTypeImpl relationType = (RelationTypeImpl) graknGraph.putRelationType("A type");
        relation = (RelationImpl) graknGraph.addRelation(relationType);
        casting = graknGraph.putCasting(role, rolePlayer, relation);
    }
    @After
    public void destroyGraphAccessManager() throws Exception {
        graknGraph.close();
    }

    @Test
    public void testEquals() throws Exception {
        Graph graph = graknGraph.getTinkerPopGraph();
        Vertex v = graph.traversal().V(relation.getBaseIdentifier()).out(Schema.EdgeLabel.CASTING.getLabel()).next();
        CastingImpl castingCopy = (CastingImpl) graknGraph.getConcept(v.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()));
        assertEquals(casting, castingCopy);

        EntityType type = graknGraph.putEntityType("Another entity type");
        RoleTypeImpl role = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        InstanceImpl rolePlayer = (InstanceImpl) graknGraph.addEntity(type);
        CastingImpl casting2 = graknGraph.putCasting(role, rolePlayer, relation);
        assertNotEquals(casting, casting2);
    }

    @Test
    public void hashCodeTest() throws Exception {
        Vertex castingVertex = graknGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).next();
        assertEquals(casting.hashCode(), castingVertex.hashCode());
    }

    @Test
    public void testGetRole() throws Exception {
        assertEquals(role, casting.getRole());

        String id = UUID.randomUUID().toString();
        Vertex vertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        vertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), id);

        CastingImpl casting2 = (CastingImpl) graknGraph.getConcept(id);
        boolean exceptionThrown = false;
        try{
            casting2.getRole();
        } catch(NoEdgeException e){
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);


        TypeImpl c1 = (TypeImpl) graknGraph.putEntityType("c1'");
        TypeImpl c2 = (TypeImpl) graknGraph.putEntityType("c2");
        Vertex casting2_Vertex = graknGraph.getTinkerPopGraph().traversal().V(casting2.getBaseIdentifier()).next();
        Vertex c1_Vertex = graknGraph.getTinkerPopGraph().traversal().V(c1.getBaseIdentifier()).next();
        Vertex c2_Vertex = graknGraph.getTinkerPopGraph().traversal().V(c2.getBaseIdentifier()).next();

        casting2_Vertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), c1_Vertex);
        casting2_Vertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), c2_Vertex);

        exceptionThrown = false;
        try{
            casting2.getRole();
        } catch(MoreThanOneEdgeException e){
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testGetRolePlayer() throws Exception {
        assertEquals(rolePlayer, casting.getRolePlayer());
    }

    @Test (expected = RuntimeException.class)
    public void testGetRolePlayerFail() throws Exception {
        Concept anotherConcept = graknGraph.putEntityType("ac'");
        casting.addEdge((ConceptImpl) anotherConcept, Schema.EdgeLabel.ROLE_PLAYER);
        casting.getRolePlayer();
    }

    @Test
    public void testGetAssertion(){
        RoleTypeImpl role2 = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        RelationTypeImpl genericRelation = (RelationTypeImpl) graknGraph.putRelationType("gr");
        RelationTypeImpl resourceType = (RelationTypeImpl) graknGraph.putRelationType("rt");
        RelationImpl relationValue = (RelationImpl) graknGraph.addRelation(resourceType);

        relation.addEdge(genericRelation, Schema.EdgeLabel.ISA);
        relationValue.addEdge(resourceType, Schema.EdgeLabel.ISA);

        CastingImpl casting2 = graknGraph.putCasting(role2, rolePlayer, relationValue);

        assertTrue(casting.getRelations().contains(relation));
        assertTrue(casting2.getRelations().contains(relationValue));
        assertThat(casting.getRelations().iterator().next(), instanceOf(Relation.class));
        assertThat(casting2.getRelations().iterator().next(), instanceOf(Relation.class));
    }

}