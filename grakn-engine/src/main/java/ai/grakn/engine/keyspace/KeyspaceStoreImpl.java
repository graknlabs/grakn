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

package ai.grakn.engine.keyspace;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.factory.KeyspaceSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link KeyspaceStore} that uses a {@link EngineGraknTxFactory} to access a knowledge
 * base and store keyspace information.
 *
 * @author Felix Chapman
 */
public class KeyspaceStoreImpl implements KeyspaceStore {
    private static final Label KEYSPACE_ENTITY = Label.of("keyspace");

    private static final Logger LOG = LoggerFactory.getLogger(KeyspaceStore.class);
    private final Set<Keyspace> existingKeyspaces;
    private final KeyspaceSession session;

    private KeyspaceStoreImpl(KeyspaceSession session){
        this.session = session;
        this.existingKeyspaces = ConcurrentHashMap.newKeySet();
    }

    public static KeyspaceStore create(KeyspaceSession session) {
        return new KeyspaceStoreImpl(session);
    }

    /**
     * Logs a new {@link Keyspace} to the {@link KeyspaceStore}.
     *
     * @param keyspace The new {@link Keyspace} we have just created
     */
    @Override
    public void addKeyspace(Keyspace keyspace){
        if(containsKeyspace(keyspace)) return;

        try (EmbeddedGraknTx<?> tx = session.tx(GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            if (keyspaceName == null) {
                throw GraknBackendException.initializationException(keyspace);
            }
            Attribute<String> attribute = keyspaceName.create(keyspace.getValue());
            if (attribute.owner() == null) {
                tx.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).create().has(attribute);
            }
            tx.commitSubmitNoLogs();

            // add to cache
            existingKeyspaces.add(keyspace);
        } catch (InvalidKBException e) {
            throw new RuntimeException("Could not add keyspace [" + keyspace + "] to system graph", e);
        }
    }

    @Override
    public boolean containsKeyspace(Keyspace keyspace){
        //Check the local cache to see which keyspaces we already have open
        if(existingKeyspaces.contains(keyspace)){
            return true;
        }

        try (GraknTx graph = session.tx(GraknTxType.READ)) {
            boolean keyspaceExists = (graph.getAttributeType(KEYSPACE_RESOURCE.getValue()).attribute(keyspace) != null);
            if(keyspaceExists) existingKeyspaces.add(keyspace);
            return keyspaceExists;
        }
    }

    @Override
    public boolean deleteKeyspace(Keyspace keyspace){
        if(keyspace.equals(SYSTEM_KB_KEYSPACE)){
           return false;
        }

        try (EmbeddedGraknTx<?> tx = session.tx(GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = tx.getSchemaConcept(KEYSPACE_RESOURCE);
            Attribute<String> attribute = keyspaceName.attribute(keyspace.getValue());

            if(attribute == null) return false;
            Thing thing = attribute.owner();
            if(thing != null) thing.delete();
            attribute.delete();

            existingKeyspaces.remove(keyspace);

            tx.commitSubmitNoLogs();
        }

        return true;
    }

    @Override
    public Set<Keyspace> keyspaces() {
        try (GraknTx graph = session.tx(GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = graph.getSchemaConcept(KEYSPACE_RESOURCE);

            return graph.<EntityType>getSchemaConcept(KEYSPACE_ENTITY).instances()
                    .flatMap(keyspace -> keyspace.attributes(keyspaceName))
                    .map(name -> (String) name.value())
                    .map(Keyspace::of)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public void loadSystemSchema() {
        Stopwatch timer = Stopwatch.createStarted();
        try (EmbeddedGraknTx<?> tx = session.tx(GraknTxType.WRITE)) {
            if (tx.getSchemaConcept(KEYSPACE_ENTITY) != null) {
                return;
            }
            LOG.info("Loading schema");
            loadSystemSchema(tx);
            tx.commitSubmitNoLogs();
            LOG.info("Loaded system schema to system keyspace. Took: {}", timer.stop());
        } catch (RuntimeException e) {
            LOG.error("Error while loading system schema in {}. The error was: {}", timer.stop(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Loads the system schema inside the provided {@link GraknTx}.
     *
     * @param tx The tx to contain the system schema
     */
    private void loadSystemSchema(GraknTx tx){
        //Keyspace data
        AttributeType<String> keyspaceName = tx.putAttributeType("keyspace-name", AttributeType.DataType.STRING);
        tx.putEntityType("keyspace").key(keyspaceName);
    }
}