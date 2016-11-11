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

package ai.grakn.graph.internal;

import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RelationTest extends GraphTestBase{
    private RelationImpl relation;
    private RoleTypeImpl role1;
    private InstanceImpl rolePlayer1;
    private RoleTypeImpl role2;
    private InstanceImpl rolePlayer2;
    private RoleTypeImpl role3;
    private InstanceImpl rolePlayer3;
    private EntityType type;
    private RelationType relationType;

    private CastingImpl casting1;
    private CastingImpl casting2;

    @Before
    public void buildGraph(){
        type = graknGraph.putEntityType("Main concept Type");
        relationType = graknGraph.putRelationType("Main relation type");

        relation = (RelationImpl) graknGraph.addRelation(relationType);
        role1 = (RoleTypeImpl) graknGraph.putRoleType("Role 1");
        rolePlayer1 = (InstanceImpl) graknGraph.addEntity(type);
        role2 = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        rolePlayer2 = (InstanceImpl) graknGraph.addEntity(type);
        role3 = (RoleTypeImpl) graknGraph.putRoleType("Role 3");
        rolePlayer3 = null;

        casting1 = graknGraph.putCasting(role1, rolePlayer1, relation);
        casting2 = graknGraph.putCasting(role2, rolePlayer2, relation);
    }

    @Test
    public void testPutRolePlayer(){
        Relation relation = graknGraph.addRelation(relationType);
        RoleType roleType = graknGraph.putRoleType("A role");
        Instance instance = graknGraph.addEntity(type);

        relation.putRolePlayer(roleType, instance);
        assertEquals(1, relation.rolePlayers().size());
        assertTrue(relation.rolePlayers().keySet().contains(roleType));
        assertTrue(relation.rolePlayers().values().contains(instance));
    }

    @Test
    public void scopeTest(){
        RelationType relationType = graknGraph.putRelationType("rel type");
        RelationImpl relation = (RelationImpl) graknGraph.addRelation(relationType);
        RelationImpl relationValue = (RelationImpl) graknGraph.addRelation(relationType);
        InstanceImpl scope = (InstanceImpl) graknGraph.addEntity(type);
        relation.scope(scope);
        relationValue.scope(scope);

        Vertex vertex = graknGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).out(Schema.EdgeLabel.HAS_SCOPE.getLabel()).next();
        assertEquals(scope.getBaseIdentifier(), vertex.id());

        vertex = graknGraph.getTinkerPopGraph().traversal().V(relationValue.getBaseIdentifier()).out(Schema.EdgeLabel.HAS_SCOPE.getLabel()).next();
        assertEquals(scope.getBaseIdentifier(), vertex.id());
    }

    @Test
    public void testGetCasting() throws Exception {
        Set<CastingImpl> castings = relation.getMappingCasting();
        assertEquals(2, castings.size());
        assertTrue(castings.contains(casting1));
        assertTrue(castings.contains(casting2));
    }

    @Test
    public void testGetRoleAndRolePlayers() throws Exception {
        Map<RoleType, Instance> roleMap = relation.rolePlayers();
        assertEquals(2, roleMap.size());
        assertTrue(roleMap.keySet().contains(role1));
        assertTrue(roleMap.keySet().contains(role2));

        assertEquals(rolePlayer1, roleMap.get(role1));
        assertEquals(rolePlayer2, roleMap.get(role2));
        assertEquals(rolePlayer3, roleMap.get(role3));
    }

    @Test
    public void testGetScope(){
        assertEquals(0, relation.scopes().size());
        Instance scope1 = graknGraph.addEntity(type);
        Instance scope2 = graknGraph.addEntity(type);
        relation.scope(scope1);
        relation.scope(scope2);
        Collection<Instance> scopes = relation.scopes();
        assertEquals(2, scopes.size());
        assertTrue(scopes.contains(scope1));
        assertTrue(scopes.contains(scope2));
    }

    @Test
    public void testDeleteScope() throws ConceptException {
        RelationType relationType = graknGraph.putRelationType("Relation type");
        RelationImpl relation2 = (RelationImpl) graknGraph.addRelation(relationType);
        InstanceImpl scope1 = (InstanceImpl) graknGraph.addEntity(type);
        Instance scope2 = graknGraph.addEntity(type);
        InstanceImpl scope3 = (InstanceImpl) graknGraph.addEntity(type);
        relation2.scope(scope3);
        relation.scope(scope3);
        relation.scope(scope1);
        relation2.scope(scope1);
        relation2.scope(scope2);

        relation2.deleteScope(scope2);
        Vertex assertion2_vertex = graknGraph.getTinkerPopGraph().traversal().V(relation2.getBaseIdentifier()).next();

        int count = 0;
        Iterator<Edge> edges =  assertion2_vertex.edges(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE.getLabel());
        while(edges.hasNext()){
            edges.next();
            count ++;
        }
        assertEquals(2, count);

        relation2.deleteScope(scope3);
        assertTrue(graknGraph.getTinkerPopGraph().traversal().V(scope3.getBaseIdentifier()).hasNext());

        relation.deleteScope(scope1);
        assertTrue(graknGraph.getTinkerPopGraph().traversal().V(scope1.getBaseIdentifier()).hasNext());
        relation2.deleteScope(scope1);
        relation.deleteScope(scope3);
        assertFalse(graknGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next().edges(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE.getLabel()).hasNext());
    }

    @Test
    public void testDeleteAllScopes() throws ConceptException {
        Instance scope2 = graknGraph.addEntity(type);
        Instance scope1 = graknGraph.addEntity(type);
        relation.scope(scope1);
        relation.scope(scope2);

        relation.scopes().forEach(relation::deleteScope);
        assertFalse(graknGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next().edges(Direction.OUT, Schema.EdgeLabel.HAS_SCOPE.getLabel()).hasNext());
    }

    @Test
    public void testDelete() throws ConceptException{
        relation.delete();
        assertNull(graknGraph.getConceptByBaseIdentifier(relation.getBaseIdentifier()));
    }

    @Test
    public void testDeleteShortcuts() {
        EntityType type = graknGraph.putEntityType("A thing");
        ResourceType resourceType = graknGraph.putResourceType("A resource thing", ResourceType.DataType.STRING);
        EntityImpl instance1 = (EntityImpl) graknGraph.addEntity(type);
        EntityImpl instance2 = (EntityImpl) graknGraph.addEntity(type);
        EntityImpl instance3 = (EntityImpl) graknGraph.addEntity(type);
        ResourceImpl resource = (ResourceImpl) graknGraph.putResource("Resource 1", resourceType);
        RoleType roleType1 = graknGraph.putRoleType("Role 1");
        RoleType roleType2 = graknGraph.putRoleType("Role 2");
        RoleType roleType3 = graknGraph.putRoleType("Role 3");
        RelationType relationType = graknGraph.putRelationType("Relation Type");

        Relation rel = graknGraph.addRelation(relationType).
                putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2).putRolePlayer(roleType3, resource);

        Relation rel2 = graknGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance3);

        assertEquals(1, instance1.resources().size());
        assertEquals(2, resource.ownerInstances().size());

        assertEquals(3, instance1.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).size());
        assertEquals(3, instance1.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).size());

        rel.delete();

        assertEquals(0, instance1.resources().size());
        assertEquals(0, resource.ownerInstances().size());

        assertEquals(1, instance1.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).size());
        assertEquals(1, instance1.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).size());
    }

    @Test
    public void testRelationHash(){
        TreeMap<RoleType, Instance> roleMap = new TreeMap<>();
        EntityType type = graknGraph.putEntityType("concept type");
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        RelationType relationType = graknGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);
        Instance instance1 = graknGraph.addEntity(type);
        Instance instance2 = graknGraph.addEntity(type);

        RelationImpl relation = (RelationImpl) graknGraph.addRelation(relationType);
        assertTrue(relation.getIndex().startsWith("RelationBaseId_" + relation.getBaseIdentifier()));

        relation.putRolePlayer(roleType1, instance1);
        roleMap.put(roleType1, instance1);
        roleMap.put(roleType2, null);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());

        relation.putRolePlayer(roleType2, null);
        roleMap.put(roleType2, null);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());

        relation.putRolePlayer(roleType2, instance2);
        roleMap.put(roleType2, instance2);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());
    }
    private String getFakeId(RelationType relationType, TreeMap<RoleType, Instance> roleMap){
        String itemIdentifier = "RelationType_" + relationType.getId() + "_Relation";
        for(Map.Entry<RoleType, Instance> entry: roleMap.entrySet()){
            itemIdentifier = itemIdentifier + "_" + entry.getKey().getId();
            if(entry.getValue() != null)
                itemIdentifier = itemIdentifier + "_" + entry.getValue().getId();
        }
        return itemIdentifier;
    }

    @Test
    public void testCreatingRelationsWithSameRolePlayersButDifferentIds(){
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = graknGraph.addEntity(type);
        Instance instance2 = graknGraph.addEntity(type);
        RelationType relationType = graknGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);

        Relation relation1 = graknGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RELATION_EXISTS.getMessage(relation1))
        ));

        graknGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
    }

    @Test
    public void testCreatingRelationsWithSameRolePlayersButDifferentIds2(){
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = graknGraph.addEntity(type);
        Instance instance2 = graknGraph.addEntity(type);
        RelationType relationType = graknGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);

        Relation relation1 = graknGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        Relation relation2 = graknGraph.addRelation(relationType).putRolePlayer(roleType1, instance1);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RELATION_EXISTS.getMessage(relation1))
        ));

        relation2.putRolePlayer(roleType2, instance2);
    }

    @Test
    public void testGetRoleMapWithMissingInstance() {
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        RelationType relationType1 = graknGraph.putRelationType("Another relation type").hasRole(roleType1).hasRole(roleType2);
        Instance instance1 = graknGraph.addEntity(type);

        Relation relation1 = graknGraph.addRelation(relationType1).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, null);

        Map<RoleType, Instance> roleTypeInstanceMap = new HashMap<>();
        roleTypeInstanceMap.put(roleType1, instance1);
        roleTypeInstanceMap.put(roleType2, null);

        assertEquals(roleTypeInstanceMap, relation1.rolePlayers());
    }

    @Test
    public void makeSureCastingsNotRemoved(){
        RoleType entityRole = graknGraph.putRoleType("Entity Role");
        RoleType degreeRole = graknGraph.putRoleType("Degree Role");
        EntityType entityType = graknGraph.putEntityType("Entity Type").playsRole(entityRole);
        ResourceType<Long> degreeType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.LONG).playsRole(degreeRole);

        RelationType hasDegree = graknGraph.putRelationType("Has Degree").hasRole(entityRole).hasRole(degreeRole);

        Entity entity = graknGraph.addEntity(entityType);
        Resource<Long> degree1 = graknGraph.putResource(100L, degreeType);
        Resource<Long> degree2 = graknGraph.putResource(101L, degreeType);

        Relation relation1 = graknGraph.addRelation(hasDegree).putRolePlayer(entityRole, entity).putRolePlayer(degreeRole, degree1);
        graknGraph.addRelation(hasDegree).putRolePlayer(entityRole, entity).putRolePlayer(degreeRole, degree2);

        assertEquals(2, entity.relations().size());

        relation1.delete();

        assertEquals(1, entity.relations().size());
    }
}