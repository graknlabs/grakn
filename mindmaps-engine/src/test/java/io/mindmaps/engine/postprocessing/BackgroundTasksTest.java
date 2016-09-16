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

package io.mindmaps.engine.postprocessing;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackgroundTasksTest {
    private BackgroundTasks backgroundTasks;
    private MindmapsGraph mindmapsGraph;
    private Cache cache;
    private String keyspace;

    @BeforeClass
    public static void startController() {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        new GraphFactoryController();
        new CommitLogController();
        Thread.sleep(5000);

        cache = Cache.getInstance();
        keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        backgroundTasks = BackgroundTasks.getInstance();
        mindmapsGraph = MindmapsClient.getGraphBatchLoading(keyspace);
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs().clear();
        cache.getResourceJobs().clear();
        Spark.stop();
        Thread.sleep(5000);
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        RoleType roleType1 = mindmapsGraph.putRoleType("role 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role 2");
        RelationType relationType = mindmapsGraph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = mindmapsGraph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("1", thing);
        Instance instance2 = mindmapsGraph.putEntity("2", thing);
        Instance instance3 = mindmapsGraph.putEntity("3", thing);
        Instance instance4 = mindmapsGraph.putEntity("4", thing);

        mindmapsGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        //Record Needed Ids
        String relationTypeId = relationType.getId();
        String mainRoleTypeId = roleType1.getId();
        String mainInstanceId = instance1.getId();
        String otherRoleTypeId = roleType2.getId();
        String otherInstanceId3 = instance3.getId();
        String otherInstanceId4 = instance4.getId();

        mindmapsGraph.commit();

        //Check Number of castings is as expected
        assertEquals(2, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());

        waitForCache(true, keyspace, 4);
        //Now fix everything
        backgroundTasks.forcePostprocessing();

        //Check it's all fixed
        assertEquals(4, ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).toList().size());
    }

    private void buildDuplicateCasting(String relationTypeId, String mainRoleTypeId, String mainInstanceId, String otherRoleTypeId, String otherInstanceId) throws Exception {
        //Get Needed Mindmaps Objects
        RelationType relationType = mindmapsGraph.getRelationType(relationTypeId);
        Instance otherInstance = mindmapsGraph.getInstance(otherInstanceId);
        RoleType otherRoleType = mindmapsGraph.getRoleType(otherRoleTypeId);
        Relation relation = mindmapsGraph.addRelation(relationType).putRolePlayer(otherRoleType, otherInstance);
        String relationId = relation.getId();

        mindmapsGraph.commit();

        Graph rawGraph = ((AbstractMindmapsGraph) this.mindmapsGraph).getTinkerPopGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), mainRoleTypeId).next();

        Vertex relationVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), relationId).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), mainInstanceId).next();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(Schema.BaseType.CASTING.name());
        castingVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), mainRoleTypeVertex);

        Edge edge = castingVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(Schema.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(Schema.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        rawGraph.tx().commit();
    }

    @Test
    public void testMergeDuplicateResources() throws MindmapsValidationException, InterruptedException {
        String keyspace = "TestBatchGraph";
        String value = "1";
        String sample = "Sample";
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();

        //Create Graph With Duplicate Resources
        MindmapsGraph graph = MindmapsClient.getGraphBatchLoading(keyspace);
        graph.putResourceType(sample, ResourceType.DataType.STRING);
        graph.commit();

        for(int i = 0; i < 10; i ++) {
            futures.add(pool.submit(() -> {
                try {
                    MindmapsGraph innerGraph = MindmapsClient.getGraphBatchLoading(keyspace);
                    innerGraph.putResource(value, innerGraph.getResourceType(sample));
                    innerGraph.commit();
                } catch (MindmapsValidationException e) {
                    e.printStackTrace();
                } catch (IllegalArgumentException ignored) {
                }
            }));
        }

        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        //Check duplicates have been created
        graph = MindmapsClient.getGraphBatchLoading(keyspace);
        Collection<Resource<Object>> resources = graph.getResourceType(sample).instances();

        assertTrue(resources.size() > 1);

        waitForCache(false, keyspace, 2);
        //Now fix everything
        backgroundTasks.forcePostprocessing();

        //Check it's fixed
        graph.rollback();
        assertEquals(1, graph.getResourceType(sample).instances().size());
    }

    private void waitForCache(boolean isCasting, String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(isCasting){
                if(cache.getCastingJobs().get(keyspace).size() < value){
                    Thread.sleep(1000);
                } else{
                    flag = false;
                }
            } else {
                if(cache.getResourceJobs().get(keyspace).size() < value){
                    Thread.sleep(1000);
                } else {
                    flag = false;
                }
            }
        }
    }
}