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

package ai.grakn.engine.factory;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.GraknTxFactoryBuilder;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * <p>
 * Engine's internal {@link GraknTx} Factory
 * </p>
 * <p>
 * <p>
 *     This internal factory is used to produce {@link GraknTx}s.
 * </p>
 *
 * @author fppt
 */
public class EngineGraknTxFactory {
    private final GraknConfig engineConfig;
    private final KeyspaceStore keyspaceStore;
    private final Map<Keyspace, EmbeddedGraknSession> openedSessions;
    private final LockProvider lockProvider;

    public static EngineGraknTxFactory create(LockProvider lockProvider, GraknConfig engineConfig, KeyspaceStore keyspaceStore) {
        return new EngineGraknTxFactory(engineConfig, lockProvider, keyspaceStore);
    }

    private EngineGraknTxFactory(GraknConfig engineConfig, LockProvider lockProvider, KeyspaceStore keyspaceStore) {
        this.openedSessions = new HashMap<>();
        this.engineConfig = engineConfig;
        this.lockProvider = lockProvider;
        this.keyspaceStore = keyspaceStore;
    }

    //Should only be used for testing
    @VisibleForTesting
    public synchronized void refreshConnections(){
        GraknTxFactoryBuilder.refresh();
    }


    public EmbeddedGraknTx<?> tx(Keyspace keyspace, GraknTxType type) {
        if (!keyspaceStore.containsKeyspace(keyspace)) {
            initialiseNewKeyspace(keyspace);
        }

        return session(keyspace).transaction(type);
    }

    /**
     * Retrieves the {@link GraknSession} needed to open the {@link GraknTx}.
     * This will open a new one {@link GraknSession} if it hasn't been opened before
     *
     * @param keyspace The {@link Keyspace} of the {@link GraknSession} to retrieve
     * @return a new or existing {@link GraknSession} connecting to the provided {@link Keyspace}
     */
    private EmbeddedGraknSession session(Keyspace keyspace){
        if(!openedSessions.containsKey(keyspace)){
            openedSessions.put(keyspace, EmbeddedGraknSession.createEngineSession(keyspace, engineConfig, GraknTxFactoryBuilder.getInstance()));
        }
        return openedSessions.get(keyspace);
    }

    /**
     * Initialise a new {@link Keyspace} by opening and closing a transaction on it.
     *
     * @param keyspace the new {@link Keyspace} we want to create
     */
    private void initialiseNewKeyspace(Keyspace keyspace) {
        //If the keyspace does not exist lock and create it
        Lock lock = lockProvider.getLock(getLockingKey(keyspace));
        lock.lock();
        try {
            // Create new empty keyspace in db
            session(keyspace).transaction(GraknTxType.WRITE).close();
            // Add current keyspace to list of available Grakn keyspaces
            keyspaceStore.addKeyspace(keyspace);
        } finally {
            lock.unlock();
        }
    }

    private static String getLockingKey(Keyspace keyspace) {
        return "/creating-new-keyspace-lock/" + keyspace.getValue();
    }

    public GraknConfig config() {
        return engineConfig;
    }

    public KeyspaceStore keyspaceStore() {
        return keyspaceStore;
    }

}