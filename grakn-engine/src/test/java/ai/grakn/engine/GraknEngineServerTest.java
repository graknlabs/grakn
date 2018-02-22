/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.task.postprocessing.CountStorage;
import ai.grakn.engine.task.postprocessing.IndexStorage;
import ai.grakn.engine.task.postprocessing.PostProcessingTask;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.redismock.RedisServer;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;
import spark.Service;

import java.io.IOException;
import java.util.Collections;

import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraknEngineServerTest {
    private static final String VERSION_KEY = "info:version";
    private static final String OLD_VERSION = "0.0.1-ontoit-alpha";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final SystemOutRule stdout = new SystemOutRule();
    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    GraknConfig config = GraknConfig.create();
    Service spark = Service.ignite();
    RedisWrapper mockRedisWrapper = mock(RedisWrapper.class);
    Jedis mockJedis = mock(Jedis.class);

    @Before
    public void setUp() {
        Pool<Jedis> jedisPool = mock(JedisPool.class);
        when(mockRedisWrapper.getJedisPool()).thenReturn(jedisPool);
        when(jedisPool.getResource()).thenReturn(mockJedis);
    }

    @After
    public void tearDown() {
        spark.stop();
    }


    @Test
    public void whenEngineServerIsStarted_SystemKeyspaceIsLoaded() throws IOException {
        SimpleURI uri = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
        RedisServer redisServer = RedisServer.newRedisServer(uri.getPort());

        redisServer.start();

        try {
            try (GraknEngineServer server = createGraknEngineServer(RedisWrapper.create(config))) {
                server.start();
                assertNotNull(server.factory().systemKeyspace());

                // init a random keyspace
                String keyspaceName = "thisisarandomwhalekeyspace";
                server.factory().systemKeyspace().openKeyspace(Keyspace.of(keyspaceName));

                assertTrue(server.factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspaceName)));
            }
        } finally {
            redisServer.stop();
        }
    }

    @Test
    public void whenStartingEngineServer_EnsureBackgroundTasksAreRegistered() throws IOException {
        try (GraknEngineServer server = createGraknEngineServer(mockRedisWrapper)) {
            assertThat(server.backgroundTaskRunner().tasks(), hasItem(isA(PostProcessingTask.class)));
        }
    }

    @Test
    public void whenEngineServerIsStartedTheFirstTime_TheVersionIsRecordedInRedis() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(null);

        try (GraknEngineServer server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis).set(VERSION_KEY, GraknVersion.VERSION);
    }

    @Test
    public void whenEngineServerIsStartedASecondTime_TheVersionIsNotChanged() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(GraknVersion.VERSION);

        try (GraknEngineServer server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis, never()).set(eq(VERSION_KEY), any());
    }

    @Test
    @Ignore("Printed but not detected")
    public void whenEngineServerIsStartedWithDifferentVersion_PrintWarning() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);
        stdout.enableLog();

        try (GraknEngineServer server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis).get(VERSION_KEY);
        assertThat(stdout.getLog(), containsString(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, OLD_VERSION)));
    }

    @Test
    public void whenEngineServerIsStartedWithDifferentVersion_TheVersionIsNotChanged() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);

        try (GraknEngineServer server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis, never()).set(eq(VERSION_KEY), any());
    }

    private GraknEngineServer createGraknEngineServer(RedisWrapper redisWrapper) throws IOException {
        EngineID id = EngineID.me();
        GraknEngineStatus status = new GraknEngineStatus();
        MetricRegistry metricRegistry = new MetricRegistry();
        IndexStorage indexStorage =  RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config);
        return GraknEngineServerFactory.createGraknEngineServer(id, spark, status, metricRegistry, config, redisWrapper,
                indexStorage, countStorage, lockProvider, Runtime.getRuntime(), Collections.emptyList(), engineGraknTxFactory);
    }
}
