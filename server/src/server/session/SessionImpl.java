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

package grakn.core.server.session;

import brave.ScopedSpan;
import com.google.common.annotations.VisibleForTesting;
import grakn.benchmark.lib.serverinstrumentation.ServerTracingInstrumentation;
import grakn.core.api.Session;
import grakn.core.api.Transaction;
import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.exception.SessionException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.cache.KeyspaceCache;
import org.janusgraph.core.JanusGraph;

import javax.annotation.CheckReturnValue;

/**
 * This class represents a Grakn Session.
 * A session is mapped to a single instance of a JanusGraph (they're both bound to a single Keyspace):
 * opening a session will open a new JanusGraph, closing a session will close the graph.
 *
 * The role of the Session is to provide multiple independent transactions that can be used by clients to
 * access a specific keyspace.
 *
 * NOTE:
 *  - Only 1 transaction per thread can exist.
 *  - A transaction cannot be shared between multiple threads, each thread will need to get a new transaction from a session.
 */
public class SessionImpl implements Session {

    private final HadoopGraphFactory hadoopGraphFactory;

    // Session can have at most 1 transaction per thread, so we keep a local reference here
    private final ThreadLocal<TransactionOLTP> localOLTPTransactionContainer = new ThreadLocal<>();

    private final KeyspaceImpl keyspace;
    private final Config config;
    private final JanusGraph graph;
    private final KeyspaceCache keyspaceCache;
    private final Runnable onClose;

    private boolean isClosed = false;

    /**
     * Instantiates SessionImpl specific for internal use (within Grakn Server),
     * using provided Grakn configuration
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used.
     */
    public SessionImpl(KeyspaceImpl keyspace, Config config, KeyspaceCache keyspaceCache, JanusGraph graph, Runnable onClose) {
        this.keyspace = keyspace;
        this.config = config;
        // Only save a reference to the factory rather than opening an Hadoop graph immediately because that can be
        // be an expensive operation TODO: refactor in the future
        this.hadoopGraphFactory = new HadoopGraphFactory(this);
        // Open Janus Graph
        this.graph = graph;

        this.keyspaceCache = keyspaceCache;
        this.onClose = onClose;

        TransactionOLTP tx = this.transaction().write();
        // copy schema to session cache if there are any schema concepts
        if (!keyspaceHasBeenInitialised(tx)) {
            initialiseMetaConcepts(tx);
        }
        copyMetaConceptsToKeyspaceCache(tx);
        tx.commit();

    }

    @Override
    public TransactionOLTP.Builder transaction() {
        return new TransactionOLTP.Builder(this);
    }

    TransactionOLTP transaction(Transaction.Type type) {

        ScopedSpan span = null;
        if (ServerTracingInstrumentation.tracingActive()) { span = ServerTracingInstrumentation.createScopedChildSpan("SessionImpl.transaction"); }

        // If graph is closed it means the session was already closed
        if (graph.isClosed()) { throw new SessionException(ErrorMessage.SESSION_CLOSED.getMessage(keyspace())); }

        if (span != null) { span.annotate("Getting local thread to see if need to throw exception"); }
        TransactionOLTP localTx = localOLTPTransactionContainer.get();
        // If transaction is already open in current thread throw exception
        if (localTx != null && !localTx.isClosed()) throw TransactionException.transactionOpen(localTx);

        if (span != null) { span.annotate("Getting new tx"); }
        // We are passing the graph to TransactionOLTP because there is the need to access graph tinkerpop traversal
        TransactionOLTP tx = new TransactionOLTP(this, graph, keyspaceCache);

        if (span != null) { span.annotate("Opening tx with type"); }
        tx.open(type);

        if (span != null) { span.annotate("Saving tx to local container"); }

        localOLTPTransactionContainer.set(tx);

        if (span != null) { span.finish(); }
        return tx;
    }

    private void initialiseMetaConcepts(TransactionOLTP tx) {
        VertexElement type = tx.addTypeVertex(Schema.MetaSchema.THING.getId(), Schema.MetaSchema.THING.getLabel(), Schema.BaseType.TYPE);
        VertexElement entityType = tx.addTypeVertex(Schema.MetaSchema.ENTITY.getId(), Schema.MetaSchema.ENTITY.getLabel(), Schema.BaseType.ENTITY_TYPE);
        VertexElement relationType = tx.addTypeVertex(Schema.MetaSchema.RELATION.getId(), Schema.MetaSchema.RELATION.getLabel(), Schema.BaseType.RELATION_TYPE);
        VertexElement resourceType = tx.addTypeVertex(Schema.MetaSchema.ATTRIBUTE.getId(), Schema.MetaSchema.ATTRIBUTE.getLabel(), Schema.BaseType.ATTRIBUTE_TYPE);
        tx.addTypeVertex(Schema.MetaSchema.ROLE.getId(), Schema.MetaSchema.ROLE.getLabel(), Schema.BaseType.ROLE);
        tx.addTypeVertex(Schema.MetaSchema.RULE.getId(), Schema.MetaSchema.RULE.getLabel(), Schema.BaseType.RULE);

        relationType.property(Schema.VertexProperty.IS_ABSTRACT, true);
        resourceType.property(Schema.VertexProperty.IS_ABSTRACT, true);
        entityType.property(Schema.VertexProperty.IS_ABSTRACT, true);

        relationType.addEdge(type, Schema.EdgeLabel.SUB);
        resourceType.addEdge(type, Schema.EdgeLabel.SUB);
        entityType.addEdge(type, Schema.EdgeLabel.SUB);
    }

    protected void copyMetaConceptsToKeyspaceCache(TransactionOLTP tx) {
        copyToCache(tx.getMetaConcept());
        copyToCache(tx.getMetaRole());
        copyToCache(tx.getMetaRule());
    }


    /**
     * @return The graph cache which contains all the data cached and accessible by all transactions.
     */
    @VisibleForTesting
    public KeyspaceCache getKeyspaceCache() {
        return keyspaceCache;
    }

    private void copyToCache(SchemaConcept schemaConcept) {
        schemaConcept.subs().forEach(concept -> {
            keyspaceCache.cacheLabel(concept.label(), concept.labelId());
//            keyspaceCache.cacheType(concept.label(), concept);
        });
    }

    private boolean keyspaceHasBeenInitialised(TransactionOLTP tx) {
        return tx.getMetaConcept() != null;
    }


    /**
     * Get a new or existing TransactionOLAP.
     *
     * @return A new or existing Grakn graph computer
     * @see TransactionOLAP
     */
    @CheckReturnValue
    public TransactionOLAP transactionOLAP() {
        return new TransactionOLAP(hadoopGraphFactory.getGraph());
    }

    /**
     * Close JanusGraph, it will not be possible to create new transactions using current instance of Session.
     * This closes local transaction before closing the graph.
     *
     * @throws TransactionException
     */
    @Override
    public void close() {
        if (isClosed) {
            return;
        }

        TransactionOLTP localTx = localOLTPTransactionContainer.get();
        if (localTx != null) {
            localTx.close(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
            localOLTPTransactionContainer.set(null);
        }


        this.onClose.run();
        isClosed = true;
    }

    @Override
    public KeyspaceImpl keyspace() {
        return keyspace;
    }

    /**
     * The config options of this Session which were passed in at the time of construction
     *
     * @return The config options of this Session
     */
    public Config config() {
        return config;
    }
}
