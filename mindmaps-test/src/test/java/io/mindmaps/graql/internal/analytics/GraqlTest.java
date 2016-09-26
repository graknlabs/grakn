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

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.Mindmaps;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.QueryBuilder;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;
import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GraqlTest {

    String keyspace;
    MindmapsGraph graph;
    private QueryBuilder qb;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
        qb = withGraph(graph);
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testGraqlCount() throws MindmapsValidationException, InterruptedException, ExecutionException {

        // assert the graph is empty
        Analytics computer = new Analytics(keyspace);
        assertEquals(0, computer.count());

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", thing);
        graph.commit();

        long graqlCount = qb.match(
                var("x").isa(var("y")),
                or(var("y").isa("entity-type"), var("y").isa("resource-type"), var("y").isa("relation-type"))
        ).stream().count();

        long computeCount = ((Long) ((ComputeQuery) qb.parse("compute count")).execute());

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);

        computeCount = ((Long) ((ComputeQuery) qb.parse("compute count")).execute());

        assertEquals(graqlCount, computeCount);
        assertEquals(3L, computeCount);
    }

    @Test
    public void testGraqlCountSubgraph() throws Exception {
        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", anotherThing);
        graph.commit();

        long computeCount = ((Long) ((ComputeQuery) qb.parse("compute count in thing, thing")).execute());
        assertEquals(2, computeCount);
    }


    @Test
    public void testDegrees() throws Exception {

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", thing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
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

        // compute degrees
        Map<Instance, Long> degrees = ((Map) ((ComputeQuery) qb.parse("compute degrees")).execute());

        // assert degrees are correct
        graph = Mindmaps.factory().getGraph(keyspace);

        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");
        entity4 = graph.getEntity("4");

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

        assertFalse(degrees.isEmpty());
        degrees.entrySet().forEach(degree -> {
            assertTrue(correctDegrees.containsKey(degree.getKey()));
            assertTrue(correctDegrees.get(degree.getKey()).equals(degree.getValue()));
        });
    }

    @Ignore
    @Test
    public void testDegreesAndPersist() throws Exception {

        // create 3 instances
        EntityType thing = graph.putEntityType("thing");
        Entity entity1 = graph.putEntity("1", thing);
        Entity entity2 = graph.putEntity("2", thing);
        Entity entity3 = graph.putEntity("3", thing);
        Entity entity4 = graph.putEntity("4", thing);

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        thing.playsRole(relation1).playsRole(relation2);
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

        // compute degrees
        ((ComputeQuery) qb.parse("compute degreesAndPersist")).execute();

        // assert persisted degrees are correct
//        MindmapsGraph graph = MindmapsGraphFactoryImpl.getGraph(keyspace);
        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");
        entity4 = graph.getEntity("4");

        Map<Instance, Long> correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });

        // compute degrees again
        ((ComputeQuery) qb.parse("compute degreesAndPersist")).execute();

        // assert persisted degrees are correct
        graph = Mindmaps.factory().getGraph(keyspace);
        entity1 = graph.getEntity("1");
        entity2 = graph.getEntity("2");
        entity3 = graph.getEntity("3");
        entity4 = graph.getEntity("4");

        correctDegrees = new HashMap<>();
        correctDegrees.put(entity1, 1l);
        correctDegrees.put(entity2, 3l);
        correctDegrees.put(entity3, 1l);
        correctDegrees.put(entity4, 1l);
        correctDegrees.put(graph.getRelation(id1), 2l);
        correctDegrees.put(graph.getRelation(id2), 2l);
        correctDegrees.put(graph.getRelation(id3), 2l);

        correctDegrees.entrySet().forEach(degree -> {
            Instance instance = degree.getKey();
            Collection<Resource<?>> resources = null;
            if (instance.isEntity()) {
                resources = instance.asEntity().resources();
            } else if (instance.isRelation()) {
                resources = instance.asRelation().resources();
            }
            assertTrue(resources.iterator().next().getValue().equals(degree.getValue()));
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIdWithAnalytics() {
        ((ComputeQuery) qb.parse("compute sum in thing")).execute();
    }

    @Test
    public void testStatisticsMethods() throws MindmapsValidationException {

        ResourceType<Long> thing = graph.putResourceType("thing", ResourceType.DataType.LONG);
        graph.putResource(1L,thing);
        graph.putResource(2L,thing);
        graph.putResource(3L,thing);
        graph.commit();

        // use graql to compute various statistics
        Optional<Number> result = (Optional<Number>) ((ComputeQuery) qb.parse("compute sum in thing")).execute();
        assertEquals(6L,(long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute min in thing")).execute();
        assertEquals(1L,(long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute max in thing")).execute();
        assertEquals(3L,(long) result.orElse(0L));
        result = (Optional<Number>) ((ComputeQuery) qb.parse("compute mean in thing")).execute();
        assertEquals(2.0, (double) result.orElse(0L), 0.1);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws MindmapsValidationException {
        EntityType thing = graph.putEntityType("thing");
        graph.commit();

        ((ComputeQuery) qb.parse("compute sum in thing")).execute();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testErrorWhenNoSubgrapForAnalytics() throws MindmapsValidationException {
        ((ComputeQuery) qb.parse("compute sum")).execute();
        ((ComputeQuery) qb.parse("compute min")).execute();
        ((ComputeQuery) qb.parse("compute max")).execute();
        ((ComputeQuery) qb.parse("compute mean")).execute();
    }
}
