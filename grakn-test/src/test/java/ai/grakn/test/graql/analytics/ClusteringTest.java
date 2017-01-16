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

package ai.grakn.test.graql.analytics;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.analytics.BulkResourceMutate;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.GraphContext;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.test.GraknTestEnv.usingOrientDB;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ClusteringTest {
    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;
    private List<ConceptId> instanceIds;

    @Rule
    public final GraphContext context = GraphContext.empty();

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(ComputeQuery.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(BulkResourceMutate.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // test on an empty rule.graph()
        Map<String, Long> sizeMap = Graql.compute().withGraph(context.graph()).cluster().execute();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap = context.graph().graql().compute().cluster().members().execute();
        assertTrue(memberMap.isEmpty());

        assertEquals(0L, context.graph().graql().compute().count().execute().longValue());
    }

    @Test
    public void testConnectedComponentSize() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap;
        Map<String, Set<String>> memberMap;
        Map<String, Long> sizeMapPersist;
        Map<String, Set<String>> memberMapPersist;

        addOntologyAndEntities();

        sizeMap = Graql.compute().withGraph(context.graph()).cluster().clusterSize(1L).execute();
        assertEquals(0, sizeMap.size());
        memberMap = context.graph().graql().compute().cluster().members().clusterSize(1L).execute();
        assertEquals(0, memberMap.size());

        addResourceRelations();

        sizeMap = context.graph().graql().compute().cluster().clusterSize(1L).execute();
        assertEquals(5, sizeMap.size());

        memberMap = context.graph().graql().compute().cluster().members().clusterSize(1L).execute();
        assertEquals(5, memberMap.size());

        sizeMapPersist = context.graph().graql().compute().cluster().clusterSize(1L).execute();
        assertEquals(5, sizeMapPersist.size());

        sizeMapPersist = context.graph().graql().compute().cluster().clusterSize(1L).execute();
        assertEquals(5, sizeMapPersist.size());

        memberMapPersist = context.graph().graql().compute().cluster().members().clusterSize(1L).execute();
        assertEquals(5, memberMapPersist.size());
    }

    @Test
    public void testConnectedComponentImplicitType() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        addOntologyAndEntities();
        addResourceRelations();
        String aResourceTypeName = "aResourceTypeName";
        ResourceType<String> resourceType =
                context.graph().putResourceType(aResourceTypeName, ResourceType.DataType.STRING);
        context.graph().getEntityType(thing).hasResource(resourceType);
        context.graph().getEntityType(anotherThing).hasResource(resourceType);
        Resource aResource = resourceType.putResource("blah");
        context.graph().getEntityType(thing).instances().forEach(instance -> instance.hasResource(aResource));
        context.graph().getEntityType(anotherThing).instances().forEach(instance -> instance.hasResource(aResource));
        context.graph().commit();

        Map<String, Set<String>> result = context.graph().graql().compute()
                .cluster().in(thing, anotherThing, aResourceTypeName).members().execute();
        assertEquals(1, result.size());
        assertEquals(5, result.values().iterator().next().size());

        assertEquals(1, context.graph().graql().compute()
                .cluster().in(thing, anotherThing, aResourceTypeName).members().execute().size());
    }

    @Test
    public void testConnectedComponent() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Map<String, Long> sizeMap;
        Map<String, Set<String>> memberMap;
        Map<String, Long> sizeMapPersist;
        Map<String, Set<String>> memberMapPersist;

        // add something, test again
        addOntologyAndEntities();

        sizeMap = Graql.compute().withGraph(context.graph()).cluster().execute();
        assertEquals(1, sizeMap.size());
        assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

        memberMap = Graql.compute().withGraph(context.graph()).cluster().in().members().execute();
        assertEquals(1, memberMap.size());
        assertEquals(7, memberMap.values().iterator().next().size());

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        sizeMap = context.graph().graql().compute().cluster().execute();
        Map<Long, Integer> populationCount00 = new HashMap<>();
        sizeMap.values().forEach(value -> populationCount00.put(value,
                populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
        assertEquals(5, populationCount00.get(1L).intValue()); // 5 resources are not connected to anything
        assertEquals(1, populationCount00.get(27L).intValue());

        memberMap = context.graph().graql().compute().cluster().members().execute();
        assertEquals(6, memberMap.size());
        Map<Integer, Integer> populationCount1 = new HashMap<>();
        memberMap.values().forEach(value -> populationCount1.put(value.size(),
                populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
        assertEquals(5, populationCount1.get(1).intValue());
        assertEquals(1, populationCount1.get(27).intValue());

        // test on subtypes. This will change existing cluster labels.
        Set<String> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                resourceType3, resourceType4, resourceType5, resourceType6);
        sizeMap = context.graph().graql().compute().cluster().in(subTypes).execute();
        assertEquals(7, sizeMap.size());
        memberMap = context.graph().graql().compute().cluster().members().in(subTypes).execute();
        assertEquals(7, memberMap.size());

        String id;
        id = context.graph().getResourceType(resourceType1).putResource(2.8).asInstance().getId().getValue();
        assertEquals(1L, sizeMap.get(id).longValue());
        id = context.graph().getResourceType(resourceType2).putResource(-5L).asInstance().getId().getValue();
        assertEquals(1L, sizeMap.get(id).longValue());
        id = context.graph().getResourceType(resourceType3).putResource(100L).asInstance().getId().getValue();
        assertEquals(1L, sizeMap.get(id).longValue());
        id = context.graph().getResourceType(resourceType5).putResource(10L).asInstance().getId().getValue();
        assertEquals(1L, sizeMap.get(id).longValue());
        id = context.graph().getResourceType(resourceType6).putResource(0.8).asInstance().getId().getValue();
        assertEquals(1L, sizeMap.get(id).longValue());
    }

    private void checkConnectedComponent(ConceptId id, String expectedClusterLabel) {
        Collection<Resource<?>> resources = context.graph().getConcept(id).asInstance()
                .resources(context.graph().getResourceType(Schema.Analytics.CLUSTER.getName()));
        assertEquals(1, resources.size());
        assertEquals(expectedClusterLabel, resources.iterator().next().getValue());
    }

    private void checkConnectedComponent(ConceptId id, String expectedClusterLabel, String resourceTypeName) {
        Collection<Resource<?>> resources = context.graph().getConcept(id).asInstance()
                .resources(context.graph().getResourceType(resourceTypeName));
        assertEquals(1, resources.size());
        assertEquals(expectedClusterLabel, resources.iterator().next().getValue());
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = context.graph().putEntityType(thing);
        EntityType entityType2 = context.graph().putEntityType(anotherThing);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType1.addEntity();
        Entity entity3 = entityType1.addEntity();
        Entity entity4 = entityType2.addEntity();
        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType role1 = context.graph().putRoleType("role1");
        RoleType role2 = context.graph().putRoleType("role2");
        entityType1.playsRole(role1).playsRole(role2);
        entityType2.playsRole(role1).playsRole(role2);
        RelationType relationType = context.graph().putRelationType(related).hasRole(role1).hasRole(role2);

        ConceptId relationId12 = relationType.addRelation()
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        ConceptId relationId23 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity3).getId();
        ConceptId relationId24 = relationType.addRelation()
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();
        instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                relationId12, relationId23, relationId24);

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(context.graph().putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(context.graph().putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(context.graph().putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(context.graph().putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(context.graph().putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(context.graph().putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(context.graph().putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));
        RoleType resourceOwner7 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType7));

        RoleType resourceValue1 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));
        RoleType resourceValue7 = context.graph().putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType7));

        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);
        context.graph().putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType7))
                .hasRole(resourceOwner7).hasRole(resourceValue7);

        entityType1.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);
        entityType2.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);

        resourceTypeList.forEach(resourceType -> resourceType
                .playsRole(resourceValue1)
                .playsRole(resourceValue2)
                .playsRole(resourceValue3)
                .playsRole(resourceValue4)
                .playsRole(resourceValue5)
                .playsRole(resourceValue6)
                .playsRole(resourceValue7));

        context.graph().commit();
    }

    private void addResourceRelations() throws GraknValidationException {
        Entity entity1 = context.graph().getConcept(entityId1);
        Entity entity2 = context.graph().getConcept(entityId2);
        Entity entity3 = context.graph().getConcept(entityId3);
        Entity entity4 = context.graph().getConcept(entityId4);

        RoleType resourceOwner1 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));

        RoleType resourceValue1 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = context.graph().getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));

        RelationType relationType1 = context.graph().getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, context.graph().getResourceType(resourceType1).putResource(1.2));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, context.graph().getResourceType(resourceType1).putResource(1.5));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity3)
                .putRolePlayer(resourceValue1, context.graph().getResourceType(resourceType1).putResource(1.8));

        RelationType relationType2 = context.graph().getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, context.graph().getResourceType(resourceType2).putResource(4L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, context.graph().getResourceType(resourceType2).putResource(-1L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, context.graph().getResourceType(resourceType2).putResource(0L));

        RelationType relationType5 = context.graph().getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, context.graph().getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, context.graph().getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, context.graph().getResourceType(resourceType5).putResource(-7L));

        RelationType relationType6 = context.graph().getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, context.graph().getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, context.graph().getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, context.graph().getResourceType(resourceType6).putResource(7.5));

        // some resources in, but not connect them to any instances
        context.graph().getResourceType(resourceType1).putResource(2.8);
        context.graph().getResourceType(resourceType2).putResource(-5L);
        context.graph().getResourceType(resourceType3).putResource(100L);
        context.graph().getResourceType(resourceType5).putResource(10L);
        context.graph().getResourceType(resourceType6).putResource(0.8);

        context.graph().commit();
    }
}
