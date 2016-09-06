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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.constants.DataType;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.engine.util.ConfigProperties;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BackgroundTasksTest {
    private BackgroundTasks backgroundTasks;
    private MindmapsGraph mindmapsGraph;

    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);

    }

    @Before
    public void setUp() throws Exception {
        new GraphFactoryController();
        new CommitLogController();

        Thread.sleep(2000);

        backgroundTasks = BackgroundTasks.getInstance();
        MindmapsClient.getGraph("mindmapstesting").clear();
        mindmapsGraph = MindmapsClient.getGraph("mindmapstesting");

    }
    @After
    public void cleanup(){
        mindmapsGraph.clear();
    }

    @Test
    public void testMergingCastings() throws Exception {
        //Create Scenario
        MindmapsTransaction mindmapsTransaction = mindmapsGraph.getTransaction();
        RoleType roleType1 = mindmapsTransaction.putRoleType("role 1");
        RoleType roleType2 = mindmapsTransaction.putRoleType("role 2");
        RelationType relationType = mindmapsTransaction.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = mindmapsTransaction.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsTransaction.putEntity("1", thing);
        Instance instance2 = mindmapsTransaction.putEntity("2", thing);
        Instance instance3 = mindmapsTransaction.putEntity("3", thing);
        Instance instance4 = mindmapsTransaction.putEntity("4", thing);

        mindmapsTransaction.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        //Record Needed Ids
        String relationTypeId = relationType.getId();
        String mainRoleTypeId = roleType1.getId();
        String mainInstanceId = instance1.getId();
        String otherRoleTypeId = roleType2.getId();
        String otherInstanceId3 = instance3.getId();
        String otherInstanceId4 = instance4.getId();

        mindmapsTransaction.commit();

        //Check Number of castings is as expected
        assertEquals(2, ((AbstractMindmapsGraph) mindmapsGraph).getGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).toList().size());

        //Break The Graph With Fake Castings
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId3);
        buildDuplicateCasting(relationTypeId, mainRoleTypeId, mainInstanceId, otherRoleTypeId, otherInstanceId4);

        //Check the graph is broken
        assertEquals(6, ((AbstractMindmapsGraph) mindmapsGraph).getGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).toList().size());

        //Now fix everything
        backgroundTasks.forcePostprocessing();

        //Check it's all fixed
        assertEquals(4, ((AbstractMindmapsGraph) mindmapsGraph).getGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).toList().size());
    }
    private void buildDuplicateCasting(String relationTypeId, String mainRoleTypeId, String mainInstanceId, String otherRoleTypeId, String otherInstanceId) throws Exception {
        MindmapsTransactionImpl mindmapsTransaction = (MindmapsTransactionImpl) mindmapsGraph.getTransaction();

        //Get Needed Mindmaps Objects
        RelationType relationType = mindmapsTransaction.getRelationType(relationTypeId);
        Instance otherInstance = mindmapsTransaction.getInstance(otherInstanceId);
        RoleType otherRoleType = mindmapsTransaction.getRoleType(otherRoleTypeId);
        Relation relation = mindmapsTransaction.addRelation(relationType).putRolePlayer(otherRoleType, otherInstance);
        String relationId = relation.getId();

        mindmapsTransaction.commit();

        Graph rawGraph = ((AbstractMindmapsGraph) mindmapsGraph).getGraph();

        //Get Needed Vertices
        Vertex mainRoleTypeVertex = rawGraph.traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), mainRoleTypeId).next();

        Vertex relationVertex = rawGraph.traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), relationId).next();

        Vertex mainInstanceVertex = rawGraph.traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), mainInstanceId).next();

        //Create Fake Casting
        Vertex castingVertex = rawGraph.addVertex(DataType.BaseType.CASTING.name());
        castingVertex.addEdge(DataType.EdgeLabel.ISA.getLabel(), mainRoleTypeVertex);

        Edge edge = castingVertex.addEdge(DataType.EdgeLabel.ROLE_PLAYER.getLabel(), mainInstanceVertex);
        edge.property(DataType.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        edge = relationVertex.addEdge(DataType.EdgeLabel.CASTING.getLabel(), castingVertex);
        edge.property(DataType.EdgeProperty.ROLE_TYPE.name(), mainRoleTypeId);

        rawGraph.tx().commit();
    }
}