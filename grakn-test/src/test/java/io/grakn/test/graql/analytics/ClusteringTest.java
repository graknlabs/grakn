package io.grakn.test.graql.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Sets;
import io.grakn.Grakn;
import io.grakn.concept.*;
import io.grakn.exception.GraknValidationException;
import io.grakn.graql.internal.analytics.Analytics;
import io.grakn.graql.internal.analytics.GraknVertexProgram;
import io.grakn.graql.internal.util.GraqlType;
import io.grakn.test.AbstractGraphTest;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ClusteringTest extends AbstractGraphTest {
    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;

    String keyspace;
    Analytics computer;
    double delta = 0.000001;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testConnectedComponent() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        Map<String, Long> sizeMap = computer.connectedComponentSize();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap = computer.connectedComponent();
        assertTrue(memberMap.isEmpty());

        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        sizeMap = computer.connectedComponentSize();
        assertEquals(1, sizeMap.size());
        assertEquals(7L, sizeMap.values().iterator().next().longValue());

        memberMap = computer.connectedComponent();
        assertEquals(1, memberMap.size());
        assertEquals(7, memberMap.values().iterator().next().size()); // 4 entities, 3 assertions

        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        sizeMap = computer.connectedComponentSize();
        Map<Long, Integer> populationCount0 = new HashMap<>();
        sizeMap.values().forEach(value -> populationCount0.put(value,
                populationCount0.containsKey(value) ? populationCount0.get(value) + 1 : 1));
        assertEquals(5, populationCount0.get(1L).intValue()); // 5 resources are not connected to anything
        assertEquals(1, populationCount0.get(27L).intValue());

        memberMap = computer.connectedComponent();
        assertEquals(6, memberMap.size());
        Map<Integer, Integer> populationCount1 = new HashMap<>();
        memberMap.values().forEach(value -> populationCount1.put(value.size(),
                populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
        assertEquals(5, populationCount1.get(1).intValue()); // 5 resources are not connected to anything
        assertEquals(1, populationCount1.get(27).intValue());

        computer = new Analytics(keyspace, Collections.singleton(resourceType1), new HashSet<>());

        sizeMap = computer.connectedComponentSize();
        assertEquals(4, sizeMap.size());
        memberMap = computer.connectedComponent();
        assertEquals(4, memberMap.size());

        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, "related", resourceType1,
                resourceType2, resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());

        sizeMap = computer.connectedComponentSize();
        assertEquals(14, sizeMap.size());
        Map<Long, Integer> populationCount2 = new HashMap<>();
        sizeMap.values().forEach(value -> populationCount2.put(value,
                populationCount2.containsKey(value) ? populationCount2.get(value) + 1 : 1));
        assertEquals(1, populationCount2.get(7L).intValue());
        assertEquals(13, populationCount2.get(1L).intValue());

        memberMap = computer.connectedComponent();
        assertEquals(14, memberMap.size());
        Map<Integer, Integer> populationCount3 = new HashMap<>();
        memberMap.values().forEach(value -> populationCount3.put(value.size(),
                populationCount3.containsKey(value.size()) ? populationCount3.get(value.size()) + 1 : 1));
        assertEquals(1, populationCount3.get(7).intValue());
        assertEquals(13, populationCount3.get(1).intValue());
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = graph.addEntity(entityType1);
        Entity entity2 = graph.addEntity(entityType1);
        Entity entity3 = graph.addEntity(entityType1);
        Entity entity4 = graph.addEntity(entityType2);
        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        entityType1.playsRole(relation1).playsRole(relation2);
        entityType2.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        graph.addRelation(related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);
        graph.addRelation(related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);
        graph.addRelation(related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType6));
        RoleType resourceOwner7 = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType7));

        RoleType resourceValue1 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType6));
        RoleType resourceValue7 = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType7));

        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType7))
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

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getEntity(entityId1);
        Entity entity2 = graph.getEntity(entityId2);
        Entity entity3 = graph.getEntity(entityId3);
        Entity entity4 = graph.getEntity(entityId4);

        RoleType resourceOwner1 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType2));
        RoleType resourceOwner3 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType3));
        RoleType resourceOwner4 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType2));
        RoleType resourceValue3 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType3));
        RoleType resourceValue4 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType4));
        RoleType resourceValue5 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType5));
        RoleType resourceValue6 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType6));

        RelationType relationType1 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType1));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.putResource(1.2, graph.getResourceType(resourceType1)));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.putResource(1.5, graph.getResourceType(resourceType1)));
        graph.addRelation(relationType1)
                .putRolePlayer(resourceOwner1, entity3)
                .putRolePlayer(resourceValue1, graph.putResource(1.8, graph.getResourceType(resourceType1)));

        RelationType relationType2 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType2));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.putResource(4L, graph.getResourceType(resourceType2)));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.putResource(-1L, graph.getResourceType(resourceType2)));
        graph.addRelation(relationType2)
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, graph.putResource(0L, graph.getResourceType(resourceType2)));

        RelationType relationType5 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType5));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, graph.putResource(-7L, graph.getResourceType(resourceType5)));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, graph.putResource(-7L, graph.getResourceType(resourceType5)));
        graph.addRelation(relationType5)
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, graph.putResource(-7L, graph.getResourceType(resourceType5)));

        RelationType relationType6 = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceType6));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));
        graph.addRelation(relationType6)
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, graph.putResource(7.5, graph.getResourceType(resourceType6)));

        // some resources in, but not connect them to any instances
        graph.putResource(2.8, graph.getResourceType(resourceType1));
        graph.putResource(-5L, graph.getResourceType(resourceType2));
        graph.putResource(100L, graph.getResourceType(resourceType3));
        graph.putResource(10L, graph.getResourceType(resourceType5));
        graph.putResource(0.8, graph.getResourceType(resourceType6));

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

}
