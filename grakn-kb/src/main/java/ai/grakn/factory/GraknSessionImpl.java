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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.kb.internal.computer.GraknComputerImpl;
import ai.grakn.kb.internal.log.CommitLogHandler;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static ai.grakn.util.EngineCommunicator.contactEngine;
import static mjson.Json.read;

/**
 * <p>
 *     Builds a {@link TxFactory}
 * </p>
 *
 * <p>
 *     This class facilitates the construction of {@link GraknTx} by determining which factory should be built.
 *     It does this by either defaulting to an in memory tx {@link GraknTxTinker} or by
 *     retrieving the factory definition from engine.
 *
 *     The deployment of engine decides on the backend and this class will handle producing the correct graphs.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class GraknSessionImpl implements GraknSession {
    private static final Logger LOG = LoggerFactory.getLogger(GraknSessionImpl.class);
    private static final int LOG_SUBMISSION_PERIOD = 1;
    private final String engineUri;
    private final Keyspace keyspace;
    private final GraknConfig config;
    private final boolean remoteSubmissionNeeded;
    private final CommitLogHandler commitLogHandler;
    private ScheduledExecutorService commitLogSubmitter;


    //References so we don't have to open a tx just to check the count of the transactions
    private GraknTxAbstract<?> tx = null;
    private GraknTxAbstract<?> txBatch = null;

    GraknSessionImpl(Keyspace keyspace, String engineUri, GraknConfig config, boolean remoteSubmissionNeeded){
        Objects.requireNonNull(keyspace);
        Objects.requireNonNull(engineUri);

        this.remoteSubmissionNeeded = remoteSubmissionNeeded;
        this.engineUri = engineUri;
        this.keyspace = keyspace;

        //Create commit log submitter if needed
        if(remoteSubmissionNeeded) {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("commit-log-submit-%d").build();
            commitLogSubmitter = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
            commitLogSubmitter.scheduleAtFixedRate(this::submitLogs, 0, LOG_SUBMISSION_PERIOD, TimeUnit.SECONDS);
        }

        //Set properties directly or via a remote call
        if(config == null) {
            if (Grakn.IN_MEMORY.equals(engineUri)) {
                config = getTxInMemoryConfig();
            } else {
                config = getTxConfig();
            }
        }
        this.config = config;

        this.commitLogHandler = new CommitLogHandler(keyspace());
    }

    public CommitLogHandler commitLogHandler(){
        return commitLogHandler;
    }

    @SuppressWarnings("unused")//This must remain public because it is accessed via reflection
    public static GraknSessionImpl create(Keyspace keyspace, String engineUri){
        return new GraknSessionImpl(keyspace, engineUri, null, true);
    }

    public static GraknSessionImpl createEngineSession(Keyspace keyspace, String engineUri, GraknConfig config){
        return new GraknSessionImpl(keyspace, engineUri, config, false);
    }

    GraknConfig getTxConfig(){
        SimpleURI uri = new SimpleURI(engineUri);
        return getTxRemoteConfig(uri, keyspace);
    }

    /**
     * Gets the properties needed to create a {@link GraknTx} by pinging engine for the config file
     *
     * @return the properties needed to build a {@link GraknTx}
     */
    private static GraknConfig getTxRemoteConfig(SimpleURI uri, Keyspace keyspace){
        URI keyspaceUri = UriBuilder.fromUri(uri.toURI()).path(REST.resolveTemplate(REST.WebPath.KB_KEYSPACE, keyspace.getValue())).build();

        Properties properties = new Properties();
        //Get Specific Configs
        properties.putAll(read(contactEngine(Optional.of(keyspaceUri), REST.HttpConn.PUT_METHOD)).asMap());

        GraknConfig config = GraknConfig.of(properties);

        //Overwrite Engine IP with something which is remotely accessible
        config.setConfigProperty(GraknConfigKey.SERVER_HOST_NAME, uri.getHost());
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, uri.getPort());

        return config;
    }

    /**
     * Gets properties which let you build a toy in-mempoty {@link GraknTx}.
     * This does nto contact engine in anyway and can be run in an isolated manner
     *
     * @return the properties needed to build an in-memory {@link GraknTx}
     */
    static GraknConfig getTxInMemoryConfig(){
        GraknConfig config = GraknConfig.empty();
        config.setConfigProperty(GraknConfigKey.SHARDING_THRESHOLD, 100_000L);
        config.setConfigProperty(GraknConfigKey.SESSION_CACHE_TIMEOUT_MS, 30_000);
        config.setConfigProperty(GraknConfigKey.KB_MODE, FactoryBuilder.IN_MEMORY);
        config.setConfigProperty(GraknConfigKey.KB_ANALYTICS, FactoryBuilder.IN_MEMORY);
        return config;
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        final TxFactory<?> factory = configureTxFactory(REST.KBConfig.DEFAULT);
        switch (transactionType){
            case READ:
            case WRITE:
                tx = factory.open(transactionType);
                return tx;
            case BATCH:
                txBatch = factory.open(transactionType);
                return txBatch;
            default:
                throw GraknTxOperationException.transactionInvalid(transactionType);
        }
    }

    /**
     * @return A new or existing grakn tx compute with the defined name
     */
    @Override
    public GraknComputer getGraphComputer() {
        TxFactory<?> configuredFactory = configureTxFactory(REST.KBConfig.COMPUTER);
        Graph graph = configuredFactory.getTinkerPopGraph(false);
        return new GraknComputerImpl(graph);
    }

    @Override
    public void close() throws GraknTxOperationException {
        int openTransactions = openTransactions(tx) + openTransactions(txBatch);
        if(openTransactions > 0){
            LOG.warn(ErrorMessage.TXS_OPEN.getMessage(this.keyspace, openTransactions));
        }

        //Stop submitting commit logs automatically
        if(remoteSubmissionNeeded) commitLogSubmitter.shutdown();

        //Close the main tx connections
        submitLogs();
        if(tx != null) tx.closeSession();
        if(txBatch != null) txBatch.closeSession();
    }

    @Override
    public String uri() {
        return engineUri;
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    @Override
    public GraknConfig config() {
        return config;
    }

    protected void submitLogs(){
        commitLogHandler().submit(engineUri, keyspace).ifPresent(LOG::debug);
    }

    private int openTransactions(GraknTxAbstract<?> graph){
        if(graph == null) return 0;
        return graph.numOpenTx();
    }

    /**
     * Gets a factory capable of building {@link GraknTx}s based on the provided config type.
     * The will either build an analytics factory or a normal {@link TxFactory}
     *
     * @param configType the type of factory to build, a normal {@link TxFactory} or an analytics one
     * @return the factory
     */
    TxFactory<?> configureTxFactory(String configType){
        if(REST.KBConfig.COMPUTER.equals(configType)){
            return FactoryBuilder.getFactory(this, true);
        } else {
            return FactoryBuilder.getFactory(this, false);
        }
    }
}
