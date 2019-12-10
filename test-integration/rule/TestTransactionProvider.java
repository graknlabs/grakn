/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.rule;

import grakn.core.concept.impl.TypeImpl;
import grakn.core.concept.manager.ConceptListenerImpl;
import grakn.core.concept.manager.ConceptManagerImpl;
import grakn.core.concept.manager.ConceptNotificationChannelImpl;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graql.executor.ExecutorFactoryImpl;
import grakn.core.graql.gremlin.TraversalPlanFactoryImpl;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.cache.RuleCacheImpl;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.manager.ConceptListener;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.server.cache.TransactionCache;
import grakn.core.kb.keyspace.StatisticsDelta;
import grakn.core.keyspace.StatisticsDeltaImpl;
import grakn.core.server.session.TransactionImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Implementation of TransactionProvider that can be relied upon to return a `TestTransaction`,
 * which is an extension of `TransactionImpl` with further fields and getters
 */
public class TestTransactionProvider implements TransactionProvider {
    private final StandardJanusGraph graph;
    private final HadoopGraph hadoopGraph;
    private final KeyspaceSchemaCache keyspaceSchemaCache;
    private final KeyspaceStatistics keyspaceStatistics;
    private final AttributeManager attributeManager;
    private ReadWriteLock graphLock;
    private final long typeShardThreshold;

    public TestTransactionProvider(StandardJanusGraph graph, HadoopGraph hadoopGraph,
                                   KeyspaceSchemaCache keyspaceSchemaCache, KeyspaceStatistics keyspaceStatistics,
                                   AttributeManager attributeManager, ReadWriteLock graphLock, long typeShardThreshold) {
        this.graph = graph;
        this.hadoopGraph = hadoopGraph;
        this.keyspaceSchemaCache = keyspaceSchemaCache;
        this.keyspaceStatistics = keyspaceStatistics;
        this.attributeManager = attributeManager;
        this.graphLock = graphLock;
        this.typeShardThreshold = typeShardThreshold;
    }

    @Override
    public Transaction newTransaction(Session session) {
        // Data structures
        ConceptNotificationChannel conceptNotificationChannel = new ConceptNotificationChannelImpl();
        TransactionCache transactionCache = new TransactionCache(keyspaceSchemaCache);
        StatisticsDeltaImpl statisticsDelta = new StatisticsDeltaImpl();

        // Janus elements
        StandardJanusGraphTx janusGraphTransaction = graph.newThreadBoundTransaction();
        JanusTraversalSourceProvider janusTraversalSourceProvider = new JanusTraversalSourceProvider(janusGraphTransaction);
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction, janusTraversalSourceProvider);

        // Grakn elements
        ConceptManagerImpl conceptManager = new ConceptManagerImpl(elementFactory, transactionCache, conceptNotificationChannel, attributeManager);
        TraversalPlanFactory traversalPlanFactory = new TraversalPlanFactoryImpl(janusTraversalSourceProvider, conceptManager, typeShardThreshold, keyspaceStatistics);
        ExecutorFactoryImpl executorFactory = new ExecutorFactoryImpl(conceptManager, hadoopGraph, keyspaceStatistics, traversalPlanFactory);
        RuleCacheImpl ruleCache = new RuleCacheImpl(conceptManager);
        MultilevelSemanticCache queryCache = new MultilevelSemanticCache(executorFactory, traversalPlanFactory);

        PropertyAtomicFactory propertyAtomicFactory = new PropertyAtomicFactory(conceptManager, ruleCache, queryCache, keyspaceStatistics);
        ReasonerQueryFactory reasonerQueryFactory = new ReasonerQueryFactory(conceptManager, queryCache, ruleCache, executorFactory, propertyAtomicFactory, traversalPlanFactory);
        executorFactory.setReasonerQueryFactory(reasonerQueryFactory);
        propertyAtomicFactory.setReasonerQueryFactory(reasonerQueryFactory);
        ruleCache.setReasonerQueryFactory(reasonerQueryFactory);

        ConceptListener conceptListener = new ConceptListenerImpl(transactionCache, queryCache, ruleCache, statisticsDelta, attributeManager, janusGraphTransaction.toString());
        conceptNotificationChannel.subscribe(conceptListener);


        return new TestTransaction(
                session, janusGraphTransaction, conceptManager, janusTraversalSourceProvider, transactionCache,
                queryCache, ruleCache, statisticsDelta, executorFactory, traversalPlanFactory, reasonerQueryFactory,
                graphLock, typeShardThreshold,
                conceptNotificationChannel, elementFactory, propertyAtomicFactory, conceptListener
        );
    }


    public class TestTransaction extends TransactionImpl {
        private final ConceptNotificationChannel conceptNotificationChannel;
        private final ElementFactory elementFactory;
        private final PropertyAtomicFactory propertyAtomicFactory;
        private final ConceptListener conceptListener;

        // factories, etc.

        public TestTransaction(Session session, StandardJanusGraphTx janusGraphTransaction,
                               ConceptManagerImpl conceptManager, JanusTraversalSourceProvider janusTraversalSourceProvider,
                               TransactionCache transactionCache, MultilevelSemanticCache queryCache,
                               RuleCacheImpl ruleCache, StatisticsDeltaImpl statisticsDelta,
                               ExecutorFactoryImpl executorFactory, TraversalPlanFactory traversalPlanFactory,
                               ReasonerQueryFactory reasonerQueryFactory, ReadWriteLock graphLock, long typeShardThreshold,
                               ConceptNotificationChannel conceptNotificationChannel, ElementFactory elementFactory,
                               PropertyAtomicFactory propertyAtomicFactory, ConceptListener conceptListener) {

            super(session, janusGraphTransaction, conceptManager, janusTraversalSourceProvider, transactionCache,
                    queryCache, ruleCache, statisticsDelta, executorFactory, traversalPlanFactory,
                    reasonerQueryFactory, graphLock, typeShardThreshold);


            this.conceptNotificationChannel = conceptNotificationChannel;
            this.elementFactory = elementFactory;
            this.propertyAtomicFactory = propertyAtomicFactory;
            this.conceptListener = conceptListener;
        }

        public long getShardCount(grakn.core.kb.concept.api.Type concept) {
            return TypeImpl.from(concept).shardCount();
        }

        public long shardingThreshold() {
            return typeShardThreshold;
        }
        /*
            Getters for TransactionImpl state
         */

        public JanusTraversalSourceProvider janusTraversalSourceProvider() {
            return janusTraversalSourceProvider;
        }

        public ExecutorFactory executorFactory() {
            return executorFactory;
        }

        public TraversalPlanFactory traversalPlanFactory() {
            return traversalPlanFactory;
        }

        public ConceptManager conceptManager() {
            return conceptManager;
        }

        public ReasonerQueryFactory reasonerQueryFactory() {
            return reasonerQueryFactory;
        }

        public RuleCache ruleCache() {
            return ruleCache;
        }

        public StatisticsDelta uncomittedStatisticsDelta() {
            return uncomittedStatisticsDelta;
        }

        public TransactionCache cache() {
            return transactionCache;
        }

//        MultilevelSemanticCache queryCache() {
//            return queryCache;
//        }

        /*
            State only saved in TestTransactions, and not in TransationImpl
         */

        public ConceptNotificationChannel conceptNotificationChannel() {
            return conceptNotificationChannel;
        }

        public ElementFactory elementFactory() {
            return elementFactory;
        }

    }
}
