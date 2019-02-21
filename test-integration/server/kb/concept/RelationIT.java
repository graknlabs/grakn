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

package grakn.core.server.kb.concept;

import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.internal.Schema;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RelationIT {
    private RelationImpl relation;
    private RoleImpl role1;
    private ThingImpl rolePlayer1;
    private RoleImpl role2;
    private ThingImpl rolePlayer2;
    private RoleImpl role3;
    private EntityType type;
    private RelationType relationshipType;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
        role1 = (RoleImpl) tx.putRole("Role 1");
        role2 = (RoleImpl) tx.putRole("Role 2");
        role3 = (RoleImpl) tx.putRole("Role 3");

        type = tx.putEntityType("Main concept Type").plays(role1).plays(role2).plays(role3);
        relationshipType = tx.putRelationType("Main relation type").relates(role1).relates(role2).relates(role3);

        rolePlayer1 = (ThingImpl) type.create();
        rolePlayer2 = (ThingImpl) type.create();

        relation = (RelationImpl) relationshipType.create();

        relation.assign(role1, rolePlayer1);
        relation.assign(role2, rolePlayer2);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relation relationship = relationshipType.create();
        Role role = tx.putRole("A role");
        Entity entity1 = type.create();

        relationship.assign(role, entity1);
        assertThat(relationship.rolePlayersMap().keySet(), containsInAnyOrder(role1, role2, role3, role));
        assertThat(relationship.rolePlayersMap().get(role), containsInAnyOrder(entity1));
    }

    @Test
    public void whenCreatingAnInferredRelationship_EnsureMarkedAsInferred(){
        RelationTypeImpl rt = RelationTypeImpl.from(tx.putRelationType("rt"));
        Relation relationship = rt.create();
        Relation relationshipInferred = rt.addRelationshipInferred();
        assertFalse(relationship.isInferred());
        assertTrue(relationshipInferred.isInferred());
    }

    @Test
    public void checkRolePlayerEdgesAreCreatedBetweenAllRolePlayers(){
        //Create the Schema
        Role role1 = tx.putRole("Role 1");
        Role role2 = tx.putRole("Role 2");
        Role role3 = tx.putRole("Role 3");
        tx.putRelationType("Rel Type").relates(role1).relates(role2).relates(role3);
        EntityType entType = tx.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);

        //Data
        EntityImpl entity1r1 = (EntityImpl) entType.create();
        EntityImpl entity2r1 = (EntityImpl) entType.create();
        EntityImpl entity3r2r3 = (EntityImpl) entType.create();
        EntityImpl entity4r3 = (EntityImpl) entType.create();
        EntityImpl entity5r1 = (EntityImpl) entType.create();
        EntityImpl entity6r1r2r3 = (EntityImpl) entType.create();

        //Relationship
        Relation relationship = relationshipType.create();
        relationship.assign(role1, entity1r1);
        relationship.assign(role1, entity2r1);
        relationship.assign(role1, entity5r1);
        relationship.assign(role1, entity6r1r2r3);
        relationship.assign(role2, entity3r2r3);
        relationship.assign(role2, entity6r1r2r3);
        relationship.assign(role3, entity3r2r3);
        relationship.assign(role3, entity4r3);
        relationship.assign(role3, entity6r1r2r3);

        //Check the structure of the NEW role-player edges
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity1r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity2r1),
                containsInAnyOrder(entity2r1, entity1r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity3r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity4r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity5r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity6r1r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
    }
    private Set<Concept> followRolePlayerEdgesToNeighbours(TransactionOLTP tx, Thing thing) {
        List<Vertex> vertices = tx.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), thing.id().getValue()).
                in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                out(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).toList();

        return vertices.stream().map(vertex -> tx.buildConcept(vertex).asThing()).collect(Collectors.toSet());
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.rolePlayersMap().keySet(), Matchers.containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers(role1).collect(toSet()), containsInAnyOrder(rolePlayer1));
        assertThat(relation.rolePlayers(role2).collect(toSet()), containsInAnyOrder(rolePlayer2));
    }

    @Test
    public void ensureRelationToStringContainsRolePlayerInformation(){
        Role role1 = tx.putRole("role type 1");
        Role role2 = tx.putRole("role type 2");
        RelationType relationshipType = tx.putRelationType("A relationship Type").relates(role1).relates(role2);
        EntityType type = tx.putEntityType("concept type").plays(role1).plays(role2);
        Thing thing1 = type.create();
        Thing thing2 = type.create();

        Relation relationship = relationshipType.create().assign(role1, thing1).assign(role2, thing2);

        String mainDescription = "ID [" + relationship.id() +  "] Type [" + relationship.type().label() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + role1.label() + "] played by [" + thing1.id() + ",]";
        String rolerp2 = "    Role [" + role2.label() + "] played by [" + thing2.id() + ",]";

        assertTrue("Relationship toString missing main description", relationship.toString().contains(mainDescription));
        assertTrue("Relationship toString missing role and role player definition", relationship.toString().contains(rolerp1));
        assertTrue("Relationship toString missing role and role player definition", relationship.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingRelations_EnsureCastingsRemain(){
        Role entityRole = tx.putRole("Entity Role");
        Role degreeRole = tx.putRole("Degree Role");
        EntityType entityType = tx.putEntityType("Entity Type").plays(entityRole);
        AttributeType<Long> degreeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.LONG).plays(degreeRole);

        RelationType hasDegree = tx.putRelationType("Has Degree").relates(entityRole).relates(degreeRole);

        Entity entity = entityType.create();
        Attribute<Long> degree1 = degreeType.create(100L);
        Attribute<Long> degree2 = degreeType.create(101L);

        Relation relationship1 = hasDegree.create().assign(entityRole, entity).assign(degreeRole, degree1);
        hasDegree.create().assign(entityRole, entity).assign(degreeRole, degree2);

        assertEquals(2, entity.relations().count());

        relationship1.delete();

        assertEquals(1, entity.relations().count());
    }


    @Test
    public void whenDeletingFinalInstanceOfRelation_RelationIsDeleted(){
        Role roleA = tx.putRole("RoleA");
        Role roleB = tx.putRole("RoleB");
        Role roleC = tx.putRole("RoleC");

        RelationType relation = tx.putRelationType("relation type").relates(roleA).relates(roleB).relates(roleC);
        EntityType type = tx.putEntityType("concept type").plays(roleA).plays(roleB).plays(roleC);
        Entity a = type.create();
        Entity b = type.create();
        Entity c = type.create();

        ConceptId relationId = relation.create().assign(roleA, a).assign(roleB, b).assign(roleC, c).id();

        a.delete();
        assertNotNull(tx.getConcept(relationId));
        b.delete();
        assertNotNull(tx.getConcept(relationId));
        c.delete();
        assertNull(tx.getConcept(relationId));
    }

    @Test
    public void whenAddingNullRolePlayerToRelation_Throw(){
        expectedException.expect(NullPointerException.class);
        relationshipType.create().assign(null, rolePlayer1);
    }

    @Test
    public void whenAttemptingToLinkTheInstanceOfAResourceRelationToTheResourceWhichCreatedIt_ThrowIfTheRelationTypeDoesNotHavePermissionToPlayTheNecessaryRole(){
        AttributeType<String> attributeType = tx.putAttributeType("what a pain", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("a real pain");

        EntityType entityType = tx.putEntityType("yay").has(attributeType);
        Relation implicitRelationship = Iterables.getOnlyElement(entityType.create().has(attribute).relations().collect(Collectors.toSet()));

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.hasNotAllowed(implicitRelationship, attribute).getMessage());

        implicitRelationship.has(attribute);
    }


    @Test
    public void whenAddingDuplicateRelationsWithDifferentKeys_EnsureTheyCanBeCommitted(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        AttributeType<Long> attributeType = tx.putAttributeType("Death Number", AttributeType.DataType.LONG);
        RelationType relationshipType = tx.putRelationType("Dark Souls").relates(role1).relates(role2).key(attributeType);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.create();
        Entity e2 = entityType.create();

        Attribute<Long> r1 = attributeType.create(1000000L);
        Attribute<Long> r2 = attributeType.create(2000000L);

        Relation rel1 = relationshipType.create().assign(role1, e1).assign(role2, e2);
        Relation rel2 = relationshipType.create().assign(role1, e1).assign(role2, e2);

        //Set the keys and commit. Without this step it should fail
        rel1.has(r1);
        rel2.has(r2);

        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        assertThat(tx.getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel1));
        assertThat(tx.getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel2));
    }

    @Test
    public void whenRemovingRolePlayerFromRelationship_EnsureRolePlayerIsRemoved(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        RelationType relationshipType = tx.putRelationType("Dark Souls").relates(role1).relates(role2);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.create();
        Entity e2 = entityType.create();
        Entity e3 = entityType.create();
        Entity e4 = entityType.create();
        Entity e5 = entityType.create();
        Entity e6 = entityType.create();

        Relation relationship = relationshipType.create().
                assign(role1, e1).assign(role1, e2).assign(role1, e3).
                assign(role2, e4).assign(role2, e5).assign(role2, e6);

        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e2, e3, e4, e5, e6));
        relationship.unassign(role1, e2);
        relationship.unassign(role2, e1);
        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5, e6));
        relationship.unassign(role2, e6);
        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5));
    }

    @Test
    public void whenAttributeLinkedToRelationshipIsInferred_EnsureItIsMarkedAsInferred(){
        AttributeType attributeType = tx.putAttributeType("Another thing of sorts", AttributeType.DataType.STRING);
        RelationType relationshipType = tx.putRelationType("A thing of sorts").has(attributeType);

        Attribute attribute = attributeType.create("Things");
        Relation relationship = relationshipType.create();

        RelationImpl.from(relationship).attributeInferred(attribute);
        assertTrue(relationship.relations().findAny().get().isInferred());
    }

    @Test
    public void whenAddingRelationshipWithNoRolePlayers_Throw(){
        Role role1 = tx.putRole("r1");
        Role role2 = tx.putRole("r2");
        RelationType relationshipType = tx.putRelationType("A thing of sorts").relates(role1).relates(role2);
        Relation relationship = relationshipType.create();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATIONSHIP_WITH_NO_ROLE_PLAYERS.getMessage(relationship.id(), relationship.type().label())));

        tx.commit();
    }
}