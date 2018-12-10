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

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.engine.attribute.deduplicator.AttributeDeduplicatorDaemonImpl;
import ai.grakn.engine.attribute.deduplicator.AttributeDeduplicatorDaemon;
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.rpc.SessionService;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.rpc.OpenRequest;
import ai.grakn.test.rule.SessionContext;
import com.codahale.metrics.MetricRegistry;
import io.grpc.ServerBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ServerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final SystemOutRule stdout = new SystemOutRule();

    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    private Path dataDirTmp;

    private KeyspaceStore keyspaceStoreUnderTest;

    @Before
    public void setup() throws IOException {
        dataDirTmp = Files.createTempDirectory("db-for-test");
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(dataDirTmp.toFile());
    }

    @Test
    public void whenEngineServerIsStarted_SystemKeyspaceIsLoaded() throws IOException {
        assertNull(keyspaceStoreUnderTest);
        try (Server server = createGraknEngineServer()) {
            server.start();
            assertNotNull(keyspaceStoreUnderTest);

            // init a random keyspace
            String keyspaceName = "thisisarandomwhalekeyspace";
            keyspaceStoreUnderTest.addKeyspace(Keyspace.of(keyspaceName));

            assertTrue(keyspaceStoreUnderTest.containsKeyspace(Keyspace.of(keyspaceName)));
        }
    }

    private Server createGraknEngineServer() {
        GraknConfig config = GraknConfig.create();
        config.setConfigProperty(GraknConfigKey.DATA_DIR, dataDirTmp.toString());

        EngineID engineId = EngineID.me();
        ServerStatus status = new ServerStatus();
        MetricRegistry metricRegistry = new MetricRegistry();
        LockProvider lockProvider = new ProcessWideLockProvider();

        keyspaceStoreUnderTest = KeyspaceStoreFake.of();
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStoreUnderTest);

        AttributeDeduplicatorDaemon AttributeDeduplicatorDaemon = new AttributeDeduplicatorDaemonImpl(config, engineGraknTxFactory);

        // communication services: spark, http controller, and gRPC server
        spark.Service sparkHttp = spark.Service.ignite();
        Collection<HttpController> httpControllers = Collections.emptyList();
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);
        io.grpc.Server server = ServerBuilder.forPort(grpcPort).addService(new SessionService(requestOpener, AttributeDeduplicatorDaemon)).build();
        ServerRPC rpcServerRPC = ServerRPC.create(server);

        return ServerFactory.createServer(engineId, config, status,
                sparkHttp, httpControllers, rpcServerRPC,
                engineGraknTxFactory, metricRegistry,
                lockProvider, AttributeDeduplicatorDaemon, keyspaceStoreUnderTest);
    }
}
