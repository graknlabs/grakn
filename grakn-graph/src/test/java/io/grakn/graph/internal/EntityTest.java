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
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.Relation;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.concept.Rule;
import io.grakn.concept.RuleType;
import io.grakn.exception.ConceptException;
import io.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class EntityTest {
    private AbstractGraknGraph mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph(){
        mindmapsGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        mindmapsGraph.initialiseMetaConcepts();
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testDeleteScope() throws ConceptException {
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        Instance scope = mindmapsGraph.addEntity(entityType);
        Relation relation = mindmapsGraph.addRelation(relationType);
        relation.scope(scope);
        scope.delete();
        assertNull(mindmapsGraph.getConceptByBaseIdentifier(((ConceptImpl) scope).getBaseIdentifier()));
    }

    @Test
    public void testGetCastings(){
        RelationType relationType = mindmapsGraph.putRelationType("rel type");
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        InstanceImpl rolePlayer1 = (InstanceImpl) mindmapsGraph.addEntity(entityType);
        assertEquals(0, rolePlayer1.getIncomingNeighbours(Schema.EdgeLabel.CASTING).size());

        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        RoleTypeImpl role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 2");
        Relation relation = mindmapsGraph.addRelation(relationType);
        Relation relation2 = mindmapsGraph.addRelation(relationType);
        CastingImpl casting1 = mindmapsGraph.putCasting(role, rolePlayer1, (RelationImpl) relation);
        CastingImpl casting2 = mindmapsGraph.putCasting(role2, rolePlayer1, (RelationImpl) relation2);

        Set<ConceptImpl> castings = rolePlayer1.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER);

        assertEquals(2, castings.size());
        assertTrue(castings.contains(casting1));
        assertTrue(castings.contains(casting2));
        assertNotEquals(casting1, casting2);
    }

    @Test
    public void testDeleteConceptInstanceInRelationship() throws ConceptException{
        //Build
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RelationType relationType = mindmapsGraph.putRelationType("relationType");
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        RoleType role3 = mindmapsGraph.putRoleType("role3");
        Instance rolePlayer1 = mindmapsGraph.addEntity(type);
        Instance rolePlayer2 = mindmapsGraph.addEntity(type);
        Instance rolePlayer3 = mindmapsGraph.addEntity(type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        assertEquals(20, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(34, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());

        rolePlayer1.delete();

        assertNull(mindmapsGraph.getConcept("role-player1"));
        assertEquals(18, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(26, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testDeleteConceptInstanceInRelationshipLastRolePlayer() throws ConceptException {
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RelationType relationType = mindmapsGraph.putRelationType("relationType");
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        RoleType role3 = mindmapsGraph.putRoleType("role3");
        Instance rolePlayer1 = mindmapsGraph.addEntity(type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, null).
                putRolePlayer(role3, null);

        long value = mindmapsGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(16, value);
        value = mindmapsGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(20, value);

        rolePlayer1.delete();

        assertNull(mindmapsGraph.getConcept("role-player1"));
        assertEquals(13, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(15, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testRelationsAndPlayedRoleTypes(){
        EntityType entityType = mindmapsGraph.putEntityType("Concept Type");
        RelationType castSinging = mindmapsGraph.putRelationType("Acting Cast");
        RelationType castActing = mindmapsGraph.putRelationType("Singing Cast");
        RoleType feature = mindmapsGraph.putRoleType("Feature");
        RoleType musical = mindmapsGraph.putRoleType("Musical");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        RoleType singer = mindmapsGraph.putRoleType("Singer");
        Instance pacino = mindmapsGraph.addEntity(entityType);
        Instance godfather = mindmapsGraph.addEntity(entityType);
        Instance godfather2 = mindmapsGraph.addEntity(entityType);
        Instance godfather3 = mindmapsGraph.addEntity(entityType);
        Instance godfather4 = mindmapsGraph.addEntity(entityType);

        castActing.hasRole(actor).hasRole(feature);
        castSinging.hasRole(singer).hasRole(musical);

        Relation relation1 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        Relation relation2 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        Relation relation3 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);
        Relation relation4 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather4).putRolePlayer(singer, pacino);

        assertEquals(4, pacino.relations().size());
        assertEquals(1, godfather.relations().size());
        assertEquals(1, godfather2.relations().size());
        assertEquals(1, godfather3.relations().size());
        assertEquals(1, godfather4.relations().size());
        assertEquals(3, pacino.relations(actor).size());
        assertEquals(1, pacino.relations(singer).size());
        assertEquals(4, pacino.relations(actor, singer).size());

        assertTrue(pacino.relations(actor).contains(relation1));
        assertTrue(pacino.relations(actor).contains(relation2));
        assertTrue(pacino.relations(actor).contains(relation3));
        assertFalse(pacino.relations(actor).contains(relation4));
        assertTrue(pacino.relations(singer).contains(relation4));

        assertEquals(2, pacino.playsRoles().size());
        assertEquals(1, godfather.playsRoles().size());
        assertEquals(1, godfather2.playsRoles().size());
        assertEquals(1, godfather3.playsRoles().size());
        assertEquals(1, godfather4.playsRoles().size());

        assertTrue(pacino.playsRoles().contains(actor));
        assertTrue(pacino.playsRoles().contains(singer));
    }

    @Test
    public void testResources(){
        EntityType randomThing = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        ResourceType resourceType2 = mindmapsGraph.putResourceType("A Resource Thing 2", ResourceType.DataType.STRING);

        RelationType hasResource = mindmapsGraph.putRelationType("Has Resource");

        RoleType resourceRole = mindmapsGraph.putRoleType("Resource Role");
        RoleType actorRole = mindmapsGraph.putRoleType("Actor");

        Entity pacino = mindmapsGraph.addEntity(randomThing);
        Resource birthplace = mindmapsGraph.putResource("a place", resourceType);
        Resource age = mindmapsGraph.putResource("100", resourceType);
        Resource family = mindmapsGraph.putResource("people", resourceType);
        Resource birthDate = mindmapsGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        Resource randomResource = mindmapsGraph.putResource("Random 1", resourceType2);
        Resource randomResource2 = mindmapsGraph.putResource("Random 2", resourceType2);

        assertEquals(0, birthDate.ownerInstances().size());
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthDate);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthplace);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, age);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, family);

        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource2);

        assertEquals(1, birthDate.ownerInstances().size());
        assertEquals(6, pacino.resources().size());
        assertTrue(pacino.resources().contains(birthDate));
        assertTrue(pacino.resources().contains(birthplace));
        assertTrue(pacino.resources().contains(age));
        assertTrue(pacino.resources().contains(family));
        assertTrue(pacino.resources().contains(randomResource));
        assertTrue(pacino.resources().contains(randomResource2));

        assertEquals(2, pacino.resources(resourceType2).size());
        assertTrue(pacino.resources(resourceType2).contains(randomResource));
        assertTrue(pacino.resources(resourceType2).contains(randomResource2));
    }

    @Test
    public void testAutoGeneratedInstanceIds(){
        EntityType entityType = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType relationType = mindmapsGraph.putRelationType("Has Resource");
        RuleType ruleType = mindmapsGraph.putRuleType("Rule Type");

        Entity entity = mindmapsGraph.addEntity(entityType);
        Resource resource = mindmapsGraph.putResource("A resource thing", resourceType);
        Relation relation = mindmapsGraph.addRelation(relationType);
        Rule rule = mindmapsGraph.addRule("lhs", "rhs", ruleType);

        assertTrue(entity.getId().startsWith(Schema.BaseType.ENTITY.name() + "-" + entity.type().getId() + "-"));
        assertTrue(resource.getId().startsWith(Schema.BaseType.RESOURCE.name() + "-" + resource.type().getId() + "-"));
        assertTrue(relation.getId().startsWith(Schema.BaseType.RELATION.name() + "-" + relation.type().getId() + "-"));
        assertTrue(rule.getId().startsWith(Schema.BaseType.RULE.name() + "-" + rule.type().getId() + "-"));
    }
}