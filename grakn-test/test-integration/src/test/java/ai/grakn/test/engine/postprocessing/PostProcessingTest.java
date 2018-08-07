/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.task.BackgroundTask;
import ai.grakn.engine.task.postprocessing.CountPostProcessor;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.Schema.VertexProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {
    private PostProcessor postProcessor;
    private EmbeddedGraknSession session;

    private static GraknConfig config;
    static {
        //This override is needed so we can test in a reasonable time frame
        config = EngineContext.createTestConfig();
        config.setConfigProperty(GraknConfigKey.POST_PROCESSOR_DELAY, 1);
    }

    @ClassRule
    public static final EngineContext engine = EngineContext.create(config);

    @Before
    public void setupPostProcessor() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RedisIndexStorage indexStorage = RedisIndexStorage.create(engine.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(engine.server().lockProvider(), indexStorage);

        RedisCountStorage countStorage = RedisCountStorage.create(engine.getJedisPool(), metricRegistry);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(engine.config(), engine.factory(), engine.server().lockProvider(), metricRegistry, countStorage);

        session = engine.sessionWithNewKeyspace();
        postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
    }

    @After
    public void takeDown(){
        session.close();
    }

    // TODO: must set INDEX=false in indices-composite.properties, before enabling this test. otherwise the test will fail since Janus will reject duplicate resource with a SchemaViolationException
    @Ignore
    @Test
    public void whenCreatingDuplicateResources_EnsureTheyAreMergedInPost() throws InvalidKBException, InterruptedException, JsonProcessingException {
        String value = "1";
        String sample = "Sample";

        //Create GraknTx With Duplicate Resources
        EmbeddedGraknTx<?> tx = session.transaction(GraknTxType.WRITE);
        AttributeType<String> attributeType = tx.putAttributeType(sample, AttributeType.DataType.STRING);

        Attribute<String> attribute = attributeType.create(value);
        tx.commit();
        tx = session.transaction(GraknTxType.WRITE);

        assertEquals(1, attributeType.instances().count());
        //Check duplicates have been created
        Set<Vertex> resource1 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource2 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource3 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource4 = createDuplicateResource(tx, attributeType, attribute);
        assertEquals(5, attributeType.instances().count());

        // Attribute vertex index
        String resourceIndex = resource1.iterator().next().value(INDEX.name()).toString();

        // Merge the attribute sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(resource1);
        merged.addAll(resource2);
        merged.addAll(resource3);
        merged.addAll(resource4);

        tx.close();

        // Casting sets as ConceptIds
        Set<ConceptId> resourceConcepts = merged.stream().map(c -> ConceptId.of(Schema.PREFIX_VERTEX + c.id().toString())).collect(toSet());

        //Create Commit Log
        CommitLog commitLog = CommitLog.createDefault(tx.keyspace());
        commitLog.attributes().put(resourceIndex, resourceConcepts);

        //Submit it
        postProcessor.submit(commitLog);

        //Force running the PP job
        engine.server().backgroundTaskRunner().tasks().forEach(BackgroundTask::run);

        Thread.sleep(2000);

        tx = session.transaction(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, tx.getAttributeType(sample).instances().count());

        tx.close();
    }

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated() throws InterruptedException {
        RedisCountStorage redis = engine.redis();
        Keyspace keyspace = SampleKBLoader.randomKeyspace();
        String entityType1 = "e1";
        String entityType2 = "e2";

        //Create Artificial configuration
        createAndUploadCountCommitLog(keyspace, ConceptId.of(entityType1), 6L);
        createAndUploadCountCommitLog(keyspace, ConceptId.of(entityType2), 3L);
        // Check cache in redis has been updated
        assertEquals(6L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType1))));
        assertEquals(3L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType2))));

        //Create Artificial configuration
        createAndUploadCountCommitLog(keyspace, ConceptId.of(entityType1), 1L);
        createAndUploadCountCommitLog(keyspace, ConceptId.of(entityType2), -1L);
        // Check cache in redis has been updated
        assertEquals(7L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType1))));
        assertEquals(2L, redis.getCount(RedisCountStorage.getKeyNumInstances(keyspace, ConceptId.of(entityType2))));
    }

    private void createAndUploadCountCommitLog(Keyspace keyspace, ConceptId conceptId, long count) {
        //Create the fake commit log
        CommitLog commitLog = CommitLog.createDefault(keyspace);
        commitLog.instanceCount().put(conceptId, count);

        //Start up the Job
        postProcessor.submit(commitLog);
    }

    @Test
    public void whenShardingThresholdIsBreached_ShardTypes() {
        Keyspace keyspace = session.keyspace();
        EntityType et1;
        EntityType et2;

        //Create Simple GraknTx
        try (GraknTx graknTx = session.transaction(GraknTxType.WRITE)) {
            et1 = graknTx.putEntityType("et1");
            et2 = graknTx.putEntityType("et2");
            graknTx.commit();
        }

        checkShardCount(et1, 1);
        checkShardCount(et2, 1);

        //Add new counts
        createAndUploadCountCommitLog(keyspace, et1.id(), 99_999L);
        createAndUploadCountCommitLog(keyspace, et2.id(), 99_999L);

        checkShardCount(et1, 1);
        checkShardCount(et2, 1);

        //Add new counts
        createAndUploadCountCommitLog(keyspace, et1.id(), 2L);
        createAndUploadCountCommitLog(keyspace, et2.id(), 1L);

        checkShardCount(et1, 2);
        checkShardCount(et2, 1);
    }

    private void checkShardCount(Concept concept, int expectedValue) {
        try (EmbeddedGraknTx<?> graknTx = session.transaction(GraknTxType.WRITE)) {
            int shards = graknTx.getTinkerTraversal().V().
                    has(Schema.VertexProperty.ID.name(), concept.id().getValue()).
                    in(Schema.EdgeLabel.SHARD.getLabel()).toList().size();

            assertEquals(expectedValue, shards);

        }
    }
}