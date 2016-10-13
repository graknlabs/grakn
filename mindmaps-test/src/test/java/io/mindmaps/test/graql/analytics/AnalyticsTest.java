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

package io.mindmaps.test.graql.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class AnalyticsTest extends AbstractMindmapsEngineTest {
    private static long startTime;

    @Before
    public void setUp() {
        // TODO: Make orientdb support analytics
        assumeFalse(usingOrientDB());
    }

    @Test
    public void testAkoIsAccountedForInSubgraph() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);
        EntityType dog = graph.putEntityType("dog").superType(animal);
        Entity foofoo = graph.putEntity("foofoo", dog);
        graph.commit();

        // set subgraph
        HashSet<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");

        Analytics analytics = new Analytics(graph.getKeyspace(), ct, new HashSet<>());
        analytics.degreesAndPersist();

        // check that dog has a degree to confirm ako has been inferred
        graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, graph.getKeyspace()).getGraph();
        foofoo = graph.getEntity("foofoo");
        Collection<Resource<?>> degrees = foofoo.resources();
        assertTrue(degrees.iterator().next().getValue().equals(0L));
    }

    @Test
    public void testCount() throws Exception {
        // assert the graph is empty
        System.out.println();
        System.out.println("Counting");
        Analytics computer = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        startTime = System.currentTimeMillis();
        Assert.assertEquals(0, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        // create 3 instances
        System.out.println();
        System.out.println("Creating 3 instances");
        graph = factory.getGraph();
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", anotherThing);
        graph.commit();
        graph.close();

        // assert computer returns the correct count of instances
        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        computer = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        Assert.assertEquals(3, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");

        System.out.println();
        System.out.println("Counting");
        startTime = System.currentTimeMillis();
        graph = factory.getGraph();
        computer = new Analytics(graph.getKeyspace(), Collections.singleton("thing"),new HashSet<>());
        Assert.assertEquals(2, computer.count());
        System.out.println();
        System.out.println(System.currentTimeMillis() - startTime + " ms");
    }

    @Test
    public void testDegrees() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", anotherThing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        anotherThing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        // relate them
        String id1 = UUID.randomUUID().toString();
        graph.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        graph.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        graph.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        graph.commit();

        // assert degrees are correct
        // create instances
        thing = graph.putEntityType("thing");
        anotherThing = graph.putEntityType("another");

        entity1 = graph.putEntity("1", thing);
        entity2 = graph.putEntity("2", thing);
        entity3 = graph.putEntity("3", thing);
        entity4 = graph.putEntity("4", anotherThing);

        relation1 = graph.putRoleType("relation1");
        relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        anotherThing.playsRole(relation1).playsRole(relation2);
        related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

        graph.commit();
        graph.close();

        // compute degrees
        Analytics computer = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        Map<Instance, Long> degrees = computer.degrees();

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertEquals(correctDegrees.get(degree.getKey()), degree.getValue());
        });

        // compute degrees again after persisting degrees
        computer.degreesAndPersist();
        Map<Instance, Long> degrees2 = computer.degrees();

        assertEquals(degrees.size(), degrees2.size());

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });

        // compute degrees on subgraph
        computer = new Analytics(graph.getKeyspace(), Sets.newHashSet("thing","related"),new HashSet<>());
        degrees = computer.degrees();

        graph = factory.getGraph();
        correctDegrees.put(graph.getRelation(id3), 1l);

        assertTrue(!degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertEquals(correctDegrees.get(degree.getKey()), degree.getValue());
        });
    }

    private static void checkDegrees(MindmapsGraph graph, Map<Instance, Long> correctDegrees) {
        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            // TODO: when shortcut edges are removed properly during concurrent deletion revert code
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assert resources != null;
            assertEquals(1, resources.size());
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });
    }

    @Test
    public void testDegreesAndPersist() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");

        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", anotherThing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
        anotherThing.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        // relate them
        String id1 = UUID.randomUUID().toString();
        graph.putRelation(id1, related)
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);

        String id2 = UUID.randomUUID().toString();
        graph.putRelation(id2, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);

        String id3 = UUID.randomUUID().toString();
        graph.putRelation(id3, related)
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        graph.commit();

        Map<Instance, Long> correctDegrees = new HashMap<>();

        // compute degrees on subgraph
        Analytics computer = new Analytics(graph.getKeyspace(), Sets.newHashSet("thing", "related"),new HashSet<>());
        computer.degreesAndPersist();

        // fetch instances
        graph = factory.getGraph();
        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");

        correctDegrees.clear();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 1l);

        // assert persisted degrees are correct
        checkDegrees(graph, correctDegrees);

        long numVertices = 0;

        // compute again and again ...
        for (int i = 0; i < 2; i++) {
            graph.close();
            computer.degreesAndPersist();

            // refresh everything after commit
            graph = factory.getGraph();
            // fetch instances
            entity1 = graph.getEntity("1");
            entity2 = graph.getEntity("2");
            entity3 = graph.getEntity("3");

            correctDegrees.clear();
            correctDegrees.put(entity1, 1l);
            correctDegrees.put(entity2, 3l);
            correctDegrees.put(entity3, 1l);
            correctDegrees.put(graph.getRelation(id1), 2l);
            correctDegrees.put(graph.getRelation(id2), 2l);
            correctDegrees.put(graph.getRelation(id3), 1l);

            checkDegrees(graph, correctDegrees);

            // assert the number of vertices remain the same
            if (i == 0) {
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }


        // compute degrees on all types, again and again ...
        for (int i = 0; i < 2; i++) {

            graph.close();
            computer = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
            computer.degreesAndPersist();

            // after computation refresh concepts
            graph = factory.getGraph();

            // fetch instances
            entity1 = graph.getEntity("1");
            entity2 = graph.getEntity("2");
            entity3 = graph.getEntity("3");
            entity4 = graph.getEntity("4");

            correctDegrees.clear();
            correctDegrees.put(entity1, 1l);
            correctDegrees.put(entity2, 3l);
            correctDegrees.put(entity3, 1l);
            correctDegrees.put(graph.getRelation(id1), 2l);
            correctDegrees.put(graph.getRelation(id2), 2l);
            correctDegrees.put(entity4, 1l);
            correctDegrees.put(graph.getRelation(id3), 2l);

            checkDegrees(graph, correctDegrees);

            // assert the number of vertices remain the same
            if (i == 0) {
                assertEquals(1, computer.count() - numVertices);
                numVertices = computer.count();
            } else {
                assertEquals(numVertices, computer.count());
            }
        }
    }

    @Test
    public void testDegreeIsCorrect() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).playsRole(value);
        ResourceType<String> altName = graph.putResourceType("alternate-name", ResourceType.DataType.STRING).playsRole(value);

        // add data to the graph
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);
        Resource coconut = graph.putResource("coconut", name);
        Resource stinky = graph.putResource("stinky", altName);
        Relation daveOwnsCoco = graph.addRelation(mansBestFriend).putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        Relation cocoName = graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, coconut);
        Relation cocoAltName = graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, stinky);

        // manually compute the degree for small graph
        Map<String, Long> subGraphReferenceDegrees = new HashMap<>();
        subGraphReferenceDegrees.put(coco.getId(), 1L);
        subGraphReferenceDegrees.put(dave.getId(), 1L);
        subGraphReferenceDegrees.put(daveOwnsCoco.getId(), 2L);

        // manually compute degree for almost full graph
        Map<String, Long> almostFullReferenceDegrees = new HashMap<>();
        almostFullReferenceDegrees.put(coco.getId(), 3L);
        almostFullReferenceDegrees.put(dave.getId(), 1L);
        almostFullReferenceDegrees.put(daveOwnsCoco.getId(), 2L);
        almostFullReferenceDegrees.put(cocoName.getId(), 2L);
        almostFullReferenceDegrees.put(cocoAltName.getId(), 1L);
        almostFullReferenceDegrees.put(coconut.getId(), 1L);

        // manually compute degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 3L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(coconut.getId(), 1L);
        referenceDegrees.put(stinky.getId(), 1L);
        referenceDegrees.put(daveOwnsCoco.getId(), 2L);
        referenceDegrees.put(cocoName.getId(), 2L);
        referenceDegrees.put(cocoAltName.getId(), 2L);

        graph.commit();

        // create a subgraph excluding resources and the relationship
        HashSet<String> subGraphTypes = Sets.newHashSet("animal", "person", "mans-best-friend");

        Analytics analytics = new Analytics(graph.getKeyspace(), subGraphTypes,new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(subGraphReferenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });

        // create a subgraph excluding resource type only
        HashSet<String> almostFullTypes = Sets.newHashSet("animal", "person", "mans-best-friend", "has-name", "name");

        analytics = new Analytics(graph.getKeyspace(), almostFullTypes,new HashSet<>());
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(almostFullReferenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });

        // full graph
        analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testDegreeIsPersisted() throws Exception {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // validate
        graph.commit();

        // compute and persist degrees
        Analytics analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        analytics.degreesAndPersist();

        // check degrees are correct
        graph = factory.getGraph();
        MindmapsGraph finalGraph = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph.getInstance(entry.getKey());
            if (instance.isEntity()) {
                assertTrue(instance.asEntity().resources().iterator().next().getValue().equals(entry.getValue()));
            } else if (instance.isRelation()) {
                assertTrue(instance.asRelation().resources().iterator().next().getValue().equals(entry.getValue()));
            }
        });

        // check only expected resources exist
        Collection<String> allConcepts = new ArrayList<>();
        ResourceType<Long> rt = graph.getResourceType(Analytics.degree);
        Collection<Resource<Long>> degrees = rt.instances();
        Map<Instance, Long> currentDegrees = new HashMap<>();
        degrees.forEach(degree -> {
            Long degreeValue = degree.getValue();
            degree.ownerInstances().forEach(instance -> {
                currentDegrees.put(instance, degreeValue);
            });
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(), referenceDegrees.values()));

        // persist again and check again
        analytics.degreesAndPersist();

        // check only expected resources exist
        graph = factory.getGraph();
        rt = graph.getResourceType(Analytics.degree);
        degrees = rt.instances();
        degrees.forEach(i -> i.ownerInstances().iterator().forEachRemaining(r ->
                allConcepts.add(r.getId())));

        // check degrees are correct
        MindmapsGraph finalGraph1 = graph;
        referenceDegrees.entrySet().forEach(entry -> {
            Instance instance = finalGraph1.getInstance(entry.getKey());
            if (instance.isEntity()) {
                assertTrue(instance.asEntity().resources().iterator().next().getValue().equals(entry.getValue()));
            } else if (instance.isRelation()) {
                assertTrue(instance.asRelation().resources().iterator().next().getValue().equals(entry.getValue()));
            }
        });

        degrees = rt.instances();
        currentDegrees.clear();
        degrees.forEach(degree -> {
            Long degreeValue = degree.getValue();
            degree.ownerInstances().forEach(instance -> {
                currentDegrees.put(instance, degreeValue);
            });
        });

        // check all resources exist and no more
        assertTrue(CollectionUtils.isEqualCollection(currentDegrees.values(), referenceDegrees.values()));
    }

    @Test
    public void testDegreeIsPersistedInPresenceOfOtherResource() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // create a decoy resource using same relationship
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(Analytics.degree));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(Analytics.degree));
        RelationType hasResource = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(Analytics.degree))
                .hasRole(degreeOwner).hasRole(degreeValue);
        ResourceType<Long> decoyResourceType = graph.putResourceType("decoy-resource", ResourceType.DataType.LONG).playsRole(degreeValue);
        Resource<Long> decoyResource = graph.putResource(100L, decoyResourceType);
        graph.addRelation(hasResource).putRolePlayer(degreeOwner, coco).putRolePlayer(degreeValue, decoyResource);
        animal.playsRole(degreeOwner);

        // validate
        graph.commit();

        HashSet<String> ct = Sets.newHashSet("person", "animal", "mans-best-friend");

        // compute and persist degrees
        Analytics analytics = new Analytics(graph.getKeyspace(), ct,new HashSet<>());
        analytics.degreesAndPersist();

        graph = factory.getGraph();
        ResourceType<Long> degreeResource = graph.getResourceType(Analytics.degree);

        // check degrees are correct
        boolean isSeen = false;
        for (Map.Entry<String, Long> entry : referenceDegrees.entrySet()) {
            Instance instance = graph.getInstance(entry.getKey());
            if (instance.isEntity()) {
                for (Resource<?> resource : instance.asEntity().resources()) {
                    if (resource.type().equals(degreeResource)) {
                        // check value is correct
                        assertTrue(resource.getValue().equals(entry.getValue()));
                        // ensure a resource of this type is seen
                        isSeen = true;
                    }
                }
            } else if (instance.isRelation()) {
                for (Resource<?> resource : instance.asRelation().resources()) {
                    if (resource.type().equals(degreeResource)) {
                        // check value
                        assertTrue(resource.getValue().equals(entry.getValue()));
                        // ensure exists
                        isSeen = true;
                    }
                }
            }
            // fails if a resource is not found for everything in the referenceDegree map
            assertTrue(isSeen);
            isSeen = false;
        }
    }

    @Test
    public void testDegreeIsCorrectAssertionAboutAssertion() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        RoleType target = graph.putRoleType("target");
        RoleType value = graph.putRoleType("value");
        RelationType hasName = graph.putRelationType("has-name").hasRole(value).hasRole(target);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType animal = graph.putEntityType("animal").playsRole(pet).playsRole(target);
        ResourceType<String> name = graph.putResourceType("name", ResourceType.DataType.STRING).playsRole(value);
        ResourceType<String> altName = graph.putResourceType("alternate-name", ResourceType.DataType.STRING).playsRole(value);
        RoleType ownership = graph.putRoleType("ownership");
        RoleType ownershipResource = graph.putRoleType("ownership-resource");
        RelationType hasOwnershipResource = graph.putRelationType("has-ownership-resource").hasRole(ownership).hasRole(ownershipResource);
        ResourceType<String> startDate = graph.putResourceType("start-date", ResourceType.DataType.STRING).playsRole(ownershipResource);
        mansBestFriend.playsRole(ownership);

        // add data to the graph
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);
        Resource coconut = graph.putResource("coconut", name);
        Resource stinky = graph.putResource("stinky", altName);
        Relation daveOwnsCoco = graph.addRelation(mansBestFriend).putRolePlayer(owner, dave).putRolePlayer(pet, coco);
        graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, coconut);
        graph.addRelation(hasName).putRolePlayer(target, coco).putRolePlayer(value, stinky);
        Resource sd = graph.putResource("01/01/01", startDate);
        Relation ownsFrom = graph.addRelation(hasOwnershipResource).putRolePlayer(ownershipResource, sd).putRolePlayer(ownership, daveOwnsCoco);

        // manually compute the degree
        Map<String, Long> referenceDegrees1 = new HashMap<>();
        referenceDegrees1.put(coco.getId(), 1L);
        referenceDegrees1.put(dave.getId(), 1L);
        referenceDegrees1.put(daveOwnsCoco.getId(), 3L);
        referenceDegrees1.put(sd.getId(), 1L);
        referenceDegrees1.put(ownsFrom.getId(), 2L);

        // manually compute degrees
        Map<String, Long> referenceDegrees2 = new HashMap<>();
        referenceDegrees2.put(coco.getId(), 1L);
        referenceDegrees2.put(dave.getId(), 1L);
        referenceDegrees2.put(daveOwnsCoco.getId(), 2L);

        graph.commit();

        // create a subgraph with assertion on assertion
        HashSet<String> ct = Sets.newHashSet("animal", "person", "mans-best-friend", "start-date", "has-ownership-resource");
        Analytics analytics = new Analytics(graph.getKeyspace(), ct,new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees1.get(entry.getKey().getId()), entry.getValue());
        });

        // create subgraph without assertion on assertion
        ct.clear();
        ct.add("animal");
        ct.add("person");
        ct.add("mans-best-friend");
        analytics = new Analytics(graph.getKeyspace(), ct,new HashSet<>());
        degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees2.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectTernaryRelationships() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make relation
        RoleType productionWithCast = graph.putRoleType("production-with-cast");
        RoleType actor = graph.putRoleType("actor");
        RoleType characterBeingPlayed = graph.putRoleType("character-being-played");
        RelationType hasCast = graph.putRelationType("has-cast")
                .hasRole(productionWithCast)
                .hasRole(actor)
                .hasRole(characterBeingPlayed);

        EntityType movie = graph.putEntityType("movie").playsRole(productionWithCast);
        EntityType person = graph.putEntityType("person").playsRole(actor);
        EntityType character = graph.putEntityType("character").playsRole(characterBeingPlayed);

        Entity godfather = graph.putEntity("Godfather", movie);
        Entity marlonBrando = graph.putEntity("Marlon-Brando", person);
        String marlonId = marlonBrando.getId();
        Entity donVitoCorleone = graph.putEntity("Don-Vito-Corleone", character);

        Relation relation = graph.addRelation(hasCast)
                .putRolePlayer(productionWithCast, godfather)
                .putRolePlayer(actor, marlonBrando)
                .putRolePlayer(characterBeingPlayed, donVitoCorleone);
        String relationId = relation.getId();

        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        graph = factory.getGraph();
        assertTrue(degrees.get(graph.getRelation(relationId)).equals(3L));
        assertTrue(degrees.get(graph.getEntity(marlonId)).equals(1L));
    }

    @Test
    public void testDegreeIsCorrectOneRoleplayerMultipleRoles() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);

        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco)
                .putRolePlayer(owner, dave)
                .putRolePlayer(breeder, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 2L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);

        // validate
        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectMissingRoleplayer() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType animal = graph.putEntityType("animal").playsRole(pet);

        // make one person breeder and owner
        Entity coco = graph.putEntity("coco", animal);
        Entity dave = graph.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(pet, coco).putRolePlayer(owner, dave);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 1L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 2L);

        // validate
        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testDegreeIsCorrectRoleplayerWrongType() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RoleType breeder = graph.putRoleType("breeder");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner).hasRole(breeder);
        EntityType person = graph.putEntityType("person").playsRole(owner).playsRole(breeder);
        EntityType dog = graph.putEntityType("dog").playsRole(pet);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity beast = graph.putEntity("beast", dog);
        Entity coco = graph.putEntity("coco", cat);
        Entity dave = graph.putEntity("dave", person);
        Relation daveBreedsAndOwnsCoco = graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(breeder, dave).putRolePlayer(pet, coco);
        Relation daveBreedsAndOwnsBeast = graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(breeder, dave).putRolePlayer(pet, beast);

        // manual degrees
        Map<String, Long> referenceDegrees = new HashMap<>();
        referenceDegrees.put(coco.getId(), 1L);
        referenceDegrees.put(dave.getId(), 4L);
        referenceDegrees.put(daveBreedsAndOwnsCoco.getId(), 3L);
        referenceDegrees.put(daveBreedsAndOwnsBeast.getId(), 2L);

        // validate
        graph.commit();

        // check degree for dave owning cats
        //TODO: should we count the relationship even if there is no cat attached?
        HashSet<String> ct = Sets.newHashSet("mans-best-friend", "cat", "person");
        Analytics analytics = new Analytics(graph.getKeyspace(), ct,new HashSet<>());
        Map<Instance, Long> degrees = analytics.degrees();
        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(entry -> {
            assertEquals(referenceDegrees.get(entry.getKey().getId()), entry.getValue());
        });
    }

    @Test
    public void testMultipleExecutionOfDegreeAndPersistWhileAddingNodes() throws MindmapsValidationException, ExecutionException, InterruptedException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create a simple graph
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = graph.putEntity("coco", cat);
        Entity dave = graph.putEntity("dave", person);
        graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);

        // validate
        graph.commit();

        // count and persist
        Analytics analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();

        analytics = new Analytics(graph.getKeyspace(),new HashSet<>(),new HashSet<>());
        assertEquals(3L, analytics.count());
        analytics.degreesAndPersist();
    }

    @Test
    public void testComputingUsingDegreeResource() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // create something with degrees
        RoleType pet = graph.putRoleType("pet");
        RoleType owner = graph.putRoleType("owner");
        RelationType mansBestFriend = graph.putRelationType("mans-best-friend").hasRole(pet).hasRole(owner);
        EntityType person = graph.putEntityType("person").playsRole(owner);
        EntityType cat = graph.putEntityType("cat").playsRole(pet);

        // make one person breeder and owner of a dog and a cat
        Entity coco = graph.putEntity("coco", cat);
        Entity dave = graph.putEntity("dave", person);
        graph.addRelation(mansBestFriend)
                .putRolePlayer(owner, dave).putRolePlayer(pet, coco);

        // validate
        graph.commit();

        // create degrees
        new Analytics(graph.getKeyspace(), new HashSet<String>(), new HashSet<String>()).degreesAndPersist();

        // compute sum
        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<String>(Arrays.asList("degree")));
        assertEquals(4L,analytics.sum().get());

        // compute count
        analytics = new Analytics(graph.getKeyspace(), new HashSet<String>(Arrays.asList("degree")), new HashSet<>());
        assertEquals(graph.getResourceType("degree").instances().size(), analytics.count());
    }

    @Test
    public void testNullResourceDoesntBreakAnalytics() throws MindmapsValidationException {
        // TODO: Fix on TinkerGraphComputer
        assumeFalse(usingTinker());

        // make slightly odd graph
        String resourceTypeId = "degree";
        EntityType thing = graph.putEntityType("thing");
        ResourceType resource = graph.putResourceType(resourceTypeId, ResourceType.DataType.LONG);
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        RelationType relationType = graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);
        thing.playsRole(degreeOwner);

        Entity thisThing = graph.addEntity(thing);
        graph.addRelation(relationType).putRolePlayer(degreeOwner,thisThing);
        graph.commit();

        Analytics analytics = new Analytics(graph.getKeyspace(), new HashSet<>(), new HashSet<>());

        // the null roleplayer caused analytics to fail at some stage
        try {
            analytics.degreesAndPersist();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
    }
}