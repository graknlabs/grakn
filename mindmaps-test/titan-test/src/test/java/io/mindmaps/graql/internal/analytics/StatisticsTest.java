package io.mindmaps.graql.internal.analytics;

import io.mindmaps.IntegrationUtils;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.Mindmaps;
import io.mindmaps.graql.internal.util.GraqlType;
import org.elasticsearch.common.collect.Sets;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatisticsTest {

    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";


    String keyspace;
    MindmapsGraph graph;
    Analytics computer;
    double delta = 0.000001;

    @BeforeClass
    public static void startController() throws Exception {
        IntegrationUtils.startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException, MindmapsValidationException {
        Pair<MindmapsGraph, String> result = IntegrationUtils.graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
    }

    @After
    public void cleanGraph() {
        graph.clear();
        graph.close();
    }

    @AfterClass
    public static void stop() {
        Spark.stop();
        System.out.println();
        System.out.println("AfterClass Done!!!");
        System.out.println("Congratulations!!!");
    }

    @Test
    public void testStatisticsExceptions() throws Exception {
        addOntologyAndEntities();
        addResourceRelations();

        //TODO: add more detailed error messages
        // resources-types set is null
        computer = new Analytics(keyspace, Collections.singleton("thing"), new HashSet<>());
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);

        // resources-types set is empty
        computer = new Analytics(keyspace, Collections.singleton("thing"), new HashSet<>());
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);

        // if it's not a resource-type
        computer = new Analytics(keyspace, Collections.singleton("thing"),
                Collections.singleton("thing"));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);

        // resource-type has no instance
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType7));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());

        // resources are not connected to any entities
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType3));
        assertFalse(computer.mean().isPresent());
        assertFalse(computer.max().isPresent());
        assertFalse(computer.min().isPresent());
        assertFalse(computer.sum().isPresent());
        assertFalse(computer.std().isPresent());

        // resource-type has incorrect data type
        computer = new Analytics(keyspace, new HashSet<>(), Collections.singleton(resourceType4));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);

        // resource-types have different data types
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet(resourceType1, resourceType2));
        assertIllegalStateExceptionThrown(computer::max);
        assertIllegalStateExceptionThrown(computer::min);
        assertIllegalStateExceptionThrown(computer::mean);
        assertIllegalStateExceptionThrown(computer::sum);
        assertIllegalStateExceptionThrown(computer::std);
    }

    private void assertIllegalStateExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.max().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.max().isPresent());


        // add resources, but resources are not connected to any entities
        addResourcesInstances();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(thing, anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertFalse(computer.min().isPresent());
        computer = new Analytics(keyspace, Sets.newHashSet(anotherThing),
                Collections.singleton(resourceType2));
        assertFalse(computer.min().isPresent());

        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(1.2, computer.min().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton(thing),
                Collections.singleton(resourceType2));
        assertEquals(-1L, computer.min().get());
        computer = new Analytics(keyspace, Collections.singleton(thing),
                Sets.newHashSet(resourceType2, resourceType5));
        assertEquals(-7L, computer.min().get());
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(1.8, computer.max().get().doubleValue(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet(resourceType1, resourceType6));
        assertEquals(7.5, computer.max().get().doubleValue(), delta);
        // TODO: fix this test
        computer = new Analytics(keyspace, Collections.singleton(anotherThing),
                Collections.singleton(resourceType2));
        assertEquals(4L, computer.max().get());

    }

    @Test
    public void testSum() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();


        // add resources, but resources are not connected to any entities
        addResourcesInstances();


        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton(resourceType1));
        assertEquals(4.5, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(3L, computer.sum().get());
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(27.0, computer.sum().get().doubleValue(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(-18L, computer.sum().get());
    }

    @Test
    public void testMean() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();


        // add resources, but resources are not connected to any entities
        addResourcesInstances();


        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType1)));
        assertEquals(1.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(1D, computer.mean().get(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(4.5, computer.mean().get(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(-3D, computer.mean().get(), delta);
    }

    @Test
    public void testStd() throws Exception {
        // resource-type has no instance
        addOntologyAndEntities();


        // add resources, but resources are not connected to any entities
        addResourcesInstances();


        // connect entity and resources
        addResourceRelations();
        computer = new Analytics(keyspace, new HashSet<>(),
                Collections.singleton((resourceType1)));
        assertEquals(Math.sqrt(0.18 / 3), computer.std().get(), delta);
        computer = new Analytics(keyspace, Collections.singleton((thing)),
                Collections.singleton((resourceType2)));
        assertEquals(Math.sqrt(14.0 / 3), computer.std().get(), delta);
        computer = new Analytics(keyspace, new HashSet<>(),
                Sets.newHashSet((resourceType1), (resourceType6)));
        assertEquals(Math.sqrt(54.18 / 6), computer.std().get(), delta);
        computer = new Analytics(keyspace, Sets.newHashSet((thing), (anotherThing)),
                Sets.newHashSet((resourceType2), (resourceType5)));
        assertEquals(Math.sqrt(110.0 / 6), computer.std().get(), delta);
    }

    private void addOntologyAndEntities() throws MindmapsValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = graph.putEntity("1", entityType1);
        Entity entity2 = graph.putEntity("2", entityType1);
        Entity entity3 = graph.putEntity("3", entityType1);
        Entity entity4 = graph.putEntity("4", entityType2);

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
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourcesInstances() throws MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

        graph.putResource(1.2, graph.getResourceType(resourceType1));
        graph.putResource(1.5, graph.getResourceType(resourceType1));
        graph.putResource(1.8, graph.getResourceType(resourceType1));

        graph.putResource(4L, graph.getResourceType(resourceType2));
        graph.putResource(-1L, graph.getResourceType(resourceType2));
        graph.putResource(0L, graph.getResourceType(resourceType2));

        graph.putResource(6L, graph.getResourceType(resourceType5));
        graph.putResource(7L, graph.getResourceType(resourceType5));
        graph.putResource(8L, graph.getResourceType(resourceType5));

        graph.putResource(7.2, graph.getResourceType(resourceType6));
        graph.putResource(7.5, graph.getResourceType(resourceType6));
        graph.putResource(7.8, graph.getResourceType(resourceType6));

        graph.putResource("a", graph.getResourceType(resourceType4));
        graph.putResource("b", graph.getResourceType(resourceType4));
        graph.putResource("c", graph.getResourceType(resourceType4));

        graph.commit();
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws MindmapsValidationException {
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getEntity("1");
        Entity entity2 = graph.getEntity("2");
        Entity entity3 = graph.getEntity("3");
        Entity entity4 = graph.getEntity("4");

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

        graph.putResource(100L, graph.getResourceType(resourceType3));

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
        graph.putResource(10L, graph.getResourceType(resourceType5));
        graph.putResource(0.8, graph.getResourceType(resourceType6));

        graph.commit();
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }
}
