/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import mjson.Json;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.Schema.VertexProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {

    private GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        session = engine.factoryWithNewKeyspace();
    }

    @After
    public void takeDown() throws InterruptedException {
        session.close();
    }

    @Test
    public void whenCreatingDuplicateResources_EnsureTheyAreMergedInPost() throws InvalidGraphException, InterruptedException {
        String value = "1";
        String sample = "Sample";

        //Create Graph With Duplicate Resources
        GraknGraph graph = session.open(GraknTxType.WRITE);
        ResourceType<String> resourceType = graph.putResourceType(sample, ResourceType.DataType.STRING);

        Resource<String> resource = resourceType.putResource(value);
        graph.admin().commitNoLogs();
        graph = session.open(GraknTxType.WRITE);

        assertEquals(1, resourceType.instances().size());

        //Check duplicates have been created
        Set<Vertex> resource1 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource2 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource3 = createDuplicateResource(graph, resourceType, resource);
        Set<Vertex> resource4 = createDuplicateResource(graph, resourceType, resource);
        assertEquals(5, resourceType.instances().size());

        // Resource vertex index
        String resourceIndex = resource1.iterator().next().value(INDEX.name()).toString();

        // Merge the resource sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(resource1);
        merged.addAll(resource2);
        merged.addAll(resource3);
        merged.addAll(resource4);

        graph.close();

        //Now fix everything

        // Casting sets as ConceptIds
        Set<String> resourceConcepts = merged.stream().map(c -> c.id().toString()).collect(toSet());

        //Now fix everything
        PostProcessingTask task = new PostProcessingTask();
        TaskConfiguration configuration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, graph.getKeyspace(),
                        REST.Request.COMMIT_LOG_FIXING, Json.object(
                                Schema.BaseType.RESOURCE.name(), Json.object(resourceIndex, resourceConcepts)
                        ))
        );
        task.initialize(null, configuration, (x, y) -> {}, engine.config(), null, engine.server().factory());

        task.start();

        graph = session.open(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, graph.getResourceType(sample).instances().size());

        graph.close();
    }
}