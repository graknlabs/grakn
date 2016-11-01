package io.mindmaps.test.graql.analytics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.mindmaps.Mindmaps;
import io.mindmaps.concept.*;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.graql.internal.analytics.MindmapsVertexProgram;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.test.AbstractGraphTest;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class ClusteringTest extends AbstractGraphTest {
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

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private List<String> instanceIds;

    String keyspace;
    Analytics computer;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(MindmapsVertexProgram.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testConnectedComponent() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        // test on an empty graph
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());

        Map<String, Long> sizeMap = computer.connectedComponentsSize();
        assertTrue(sizeMap.isEmpty());
        Map<String, Set<String>> memberMap = computer.connectedComponents();
        assertTrue(memberMap.isEmpty());
        Map<String, Long> sizeMapPersist = computer.connectedComponentsAndPersist();
        assertTrue(sizeMapPersist.isEmpty());
        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        assertEquals(0L, computer.count());

        // add something, test again
        addOntologyAndEntities();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        assertEquals(1, sizeMap.size());
        assertEquals(7L, sizeMap.values().iterator().next().longValue()); // 4 entities, 3 assertions

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        memberMap = computer.connectedComponents();
        assertEquals(1, memberMap.size());
        assertEquals(7, memberMap.values().iterator().next().size());
        String clusterLabel = memberMap.keySet().iterator().next();

        long count = computer.count();
        for (int i = 0; i < 3; i++) {
            computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            refreshGraph();
            assertEquals(1, sizeMapPersist.size());
            assertEquals(7L, sizeMapPersist.values().iterator().next().longValue());
            final String finalClusterLabel = clusterLabel;
            instanceIds.forEach(id -> {
                Instance instance = graph.getInstance(id);
                Collection<Resource<?>> resources = null;
                if (instance.isEntity()) {
                    resources = instance.asEntity().resources();
                } else if (instance.isRelation()) {
                    resources = instance.asRelation().resources();
                }
                assertNotNull(resources);
                assertEquals(1, resources.size());
                assertEquals(finalClusterLabel, resources.iterator().next().getValue());
            });
            assertEquals(count, computer.count());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        Map<Long, Integer> populationCount00 = new HashMap<>();
        sizeMap.values().forEach(value -> populationCount00.put(value,
                populationCount00.containsKey(value) ? populationCount00.get(value) + 1 : 1));
        assertEquals(5, populationCount00.get(1L).intValue()); // 5 resources are not connected to anything
        assertEquals(1, populationCount00.get(27L).intValue());

        computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        memberMap = computer.connectedComponents();
        assertEquals(6, memberMap.size());
        Map<Integer, Integer> populationCount1 = new HashMap<>();
        memberMap.values().forEach(value -> populationCount1.put(value.size(),
                populationCount1.containsKey(value.size()) ? populationCount1.get(value.size()) + 1 : 1));
        assertEquals(5, populationCount1.get(1).intValue());
        assertEquals(1, populationCount1.get(27).intValue());
        clusterLabel = memberMap.entrySet().stream()
                .filter(entry -> entry.getValue().size() != 1)
                .findFirst().get().getKey();

        count = computer.count();
        for (int i = 0; i < 2; i++) {
            computer = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            //TODO: Get rid of this refresh.
            refreshGraph();

            Map<Long, Integer> populationCount01 = new HashMap<>();
            sizeMapPersist.values().forEach(value -> populationCount01.put(value,
                    populationCount01.containsKey(value) ? populationCount01.get(value) + 1 : 1));
            assertEquals(6, sizeMapPersist.size());
            assertEquals(5, populationCount01.get(1L).intValue());
            assertEquals(1, populationCount01.get(27L).intValue());
            final String finalClusterLabel = clusterLabel;
            memberMap.values().stream()
                    .filter(set -> set.size() != 1)
                    .flatMap(Collection::stream)
                    .forEach(id -> {
                        List<Concept> resources = withGraph(graph)
                                .match(id(id).has(Analytics.connectedComponent, var("x")))
                                .get("x").collect(Collectors.toList());
                        assertEquals(1, resources.size());
                        assertEquals(finalClusterLabel, resources.get(0).asResource().getValue());
                    });
            assertEquals(count, computer.count());
        }


        // test on subtypes. This will change existing cluster labels.
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());
        sizeMap = computer.connectedComponentsSize();
        assertEquals(17, sizeMap.size());
        sizeMap.values().forEach(value -> assertEquals(1L, value.longValue()));
        memberMap = computer.connectedComponents();
        assertEquals(17, memberMap.size());
        for (int i = 0; i < 2; i++) {
            computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6), new HashSet<>());
            sizeMapPersist = computer.connectedComponentsAndPersist();
            //TODO: Get rid of this refresh.
            refreshGraph();

            assertEquals(17, sizeMapPersist.size());
            memberMap.values().stream()
                    .flatMap(Collection::stream)
                    .forEach(id -> {
                        List<Concept> resources = withGraph(graph)
                                .match(id(id).has(Analytics.connectedComponent, var("x")))
                                .get("x").collect(Collectors.toList());
                        assertEquals(1, resources.size());
                        assertEquals(id, resources.get(0).asResource().getValue());
                    });
        }
    }

    private void refreshGraph() throws Exception {
        //TODO: Get rid of this method.
        // We should be refreshing the graph in the factory when switching between normal and batch
        ((AbstractMindmapsGraph) graph).getTinkerPopGraph().close();
        graph = factory.getGraph();
    }

    private void addOntologyAndEntities() throws MindmapsValidationException {
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

        RoleType role1 = graph.putRoleType("role1");
        RoleType role2 = graph.putRoleType("role2");
        entityType1.playsRole(role1).playsRole(role2);
        entityType2.playsRole(role1).playsRole(role2);
        RelationType relationType = graph.putRelationType(related).hasRole(role1).hasRole(role2);

        String relationId12 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity1)
                .putRolePlayer(role2, entity2).getId();
        String relationId23 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity3).getId();
        String relationId24 = graph.addRelation(relationType)
                .putRolePlayer(role1, entity2)
                .putRolePlayer(role2, entity4).getId();
        instanceIds = Lists.newArrayList(entityId1, entityId2, entityId3, entityId4,
                relationId12, relationId23, relationId24);

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
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getEntity(entityId1);
        Entity entity2 = graph.getEntity(entityId2);
        Entity entity3 = graph.getEntity(entityId3);
        Entity entity4 = graph.getEntity(entityId4);

        RoleType resourceOwner1 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType2));
        graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType3));
        graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType2));
        graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType3));
        graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType4));
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
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }
}
