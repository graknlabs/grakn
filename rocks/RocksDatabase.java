/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.rocks;

import grakn.common.collection.Pair;
import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;
import grakn.core.reasoner.ReasonerCache;
import grakn.core.traversal.TraversalCache;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_CLOSED;
import static grakn.core.common.exception.ErrorMessage.Internal.DIRTY_INITIALISATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static java.util.Comparator.reverseOrder;

public class RocksDatabase implements Grakn.Database {

    private final String name;
    private final RocksGrakn rocksGrakn;
    private final OptimisticTransactionDB rocksData;
    private final OptimisticTransactionDB rocksSchema;
    private final KeyGenerator.Data.Persisted dataKeyGenerator;
    private final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    private final ConcurrentMap<UUID, Pair<RocksSession, Long>> sessions;
    private final StampedLock dataWriteSchemaLock;
    private final AtomicBoolean isOpen;
    private Cache cache;
    private final RocksSession.Data statisticsBackgroundCounterSession;
    final StatisticsBackgroundCounter statisticsBackgroundCounter;

    private RocksDatabase(RocksGrakn rocksGrakn, String name, boolean isNew) {
        this.name = name;
        this.rocksGrakn = rocksGrakn;
        schemaKeyGenerator = new KeyGenerator.Schema.Persisted();
        dataKeyGenerator = new KeyGenerator.Data.Persisted();
        sessions = new ConcurrentHashMap<>();
        dataWriteSchemaLock = new StampedLock();

        try {
            rocksSchema = OptimisticTransactionDB.open(this.rocksGrakn.rocksOptions(), directory().resolve(Encoding.ROCKS_SCHEMA).toString());
            rocksData = OptimisticTransactionDB.open(this.rocksGrakn.rocksOptions(), directory().resolve(Encoding.ROCKS_DATA).toString());
        } catch (RocksDBException e) {
            throw new GraknException(e);
        }

        isOpen = new AtomicBoolean(true);
        if (isNew) initialise();
        else load();
        statisticsBackgroundCounterSession = new RocksSession.Data(this, new Options.Session());
        statisticsBackgroundCounter = new StatisticsBackgroundCounter(statisticsBackgroundCounterSession);
    }

    static RocksDatabase createNewAndOpen(RocksGrakn rocksGrakn, String name) {
        try {
            Files.createDirectory(rocksGrakn.directory().resolve(name));
        } catch (IOException e) {
            throw new GraknException(e);
        }
        return new RocksDatabase(rocksGrakn, name, true);
    }

    static RocksDatabase loadExistingAndOpen(RocksGrakn rocksGrakn, String name) {
        return new RocksDatabase(rocksGrakn, name, false);
    }

    private void initialise() {
        try (RocksSession session = createAndOpenSession(SCHEMA, new Options.Session())) {
            try (RocksTransaction txn = session.transaction(WRITE)) {
                if (txn.asSchema().graph().isInitialised()) throw new GraknException(DIRTY_INITIALISATION);
                txn.asSchema().graph().initialise();
                txn.commit();
            }
        }
    }

    private void load() {
        try (RocksSession session = createAndOpenSession(SCHEMA, new Options.Session())) {
            try (RocksTransaction txn = session.transaction(READ)) {
                schemaKeyGenerator.sync(txn.asSchema().schemaStorage());
                dataKeyGenerator.sync(txn.asSchema().dataStorage());
            }
        }
    }

    RocksSession createAndOpenSession(Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED.message(name));

        long lock = 0;
        final RocksSession session;

        if (type.isSchema()) {
            lock = dataWriteSchemaLock().writeLock();
            session = new RocksSession.Schema(this, options);
        } else if (type.isData()) {
            session = new RocksSession.Data(this, options);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }

        sessions.put(session.uuid(), new Pair<>(session, lock));
        return session;
    }

    synchronized Cache borrowCache() {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED.message(name));

        if (cache == null) cache = new Cache(this);
        cache.borrow();
        return cache;
    }

    synchronized void unborrowCache(Cache cache) {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED.message(name));

        cache.unborrow();
    }

    synchronized void invalidateCache() {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED.message(name));

        if (cache != null) {
            cache.invalidate();
            cache = null;
        }
    }

    private synchronized void closeCache() {
        if (cache != null) cache.close();
    }

    private Path directory() {
        return rocksGrakn.directory().resolve(name);
    }

    public Options.Database options() {
        return rocksGrakn.options();
    }

    OptimisticTransactionDB rocksData() {
        return rocksData;
    }

    OptimisticTransactionDB rocksSchema() {
        return rocksSchema;
    }

    KeyGenerator.Schema schemaKeyGenerator() {
        return schemaKeyGenerator;
    }

    KeyGenerator.Data dataKeyGenerator() {
        return dataKeyGenerator;
    }

    /**
     * Get the lock that guarantees that the schema is not modified at the same
     * time as data being written to the database. When a schema session is
     * opened (to modify the schema), all write transaction need to wait until
     * the schema session is completed. If there is a write transaction opened,
     * a schema session needs to wait until those transactions are completed.
     *
     * @return a {@code StampedLock} to protect data writes from concurrent schema modification
     */
    StampedLock dataWriteSchemaLock() {
        return dataWriteSchemaLock;
    }

    void remove(RocksSession session) {
        if (statisticsBackgroundCounterSession != session) {
            final long lock = sessions.remove(session.uuid()).second();
            if (session.type().isSchema()) dataWriteSchemaLock().unlockWrite(lock);
        }
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            sessions.values().forEach(p -> p.first().close());
            statisticsBackgroundCounter.stop();
            statisticsBackgroundCounterSession.close();
            closeCache();
            rocksData.close();
            rocksSchema.close();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean contains(UUID sessionID) {
        return sessions.containsKey(sessionID);
    }

    @Override
    public Grakn.Session get(UUID sessionID) {
        if (sessions.containsKey(sessionID)) return sessions.get(sessionID).first();
        else return null;
    }

    @Override
    public Stream<Grakn.Session> sessions() {
        return sessions.values().stream().map(Pair::first);
    }

    @Override
    public void delete() {
        close();
        rocksGrakn.databases().remove(this);
        try {
            Files.walk(directory()).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new GraknException(e);
        }
    }

    static class Cache {

        private final TraversalCache traversalCache;
        private final ReasonerCache reasonerCache;
        private final SchemaGraph schemaGraph;
        private final RocksStorage schemaStorage;
        private long borrowerCount;
        private boolean invalidated;

        private Cache(RocksDatabase database) {
            schemaStorage = new RocksStorage(database.rocksSchema(), true);
            schemaGraph = new SchemaGraph(schemaStorage, true);
            traversalCache = new TraversalCache();
            reasonerCache = new ReasonerCache();
            borrowerCount = 0L;
            invalidated = false;
        }

        public TraversalCache traversal() {
            return traversalCache;
        }

        public ReasonerCache reasoner() {
            return reasonerCache;
        }

        public SchemaGraph schemaGraph() {
            return schemaGraph;
        }

        private void borrow() {
            borrowerCount++;
        }

        private void unborrow() {
            borrowerCount--;
            mayClose();
        }

        private void invalidate() {
            invalidated = true;
            mayClose();
        }

        private void mayClose() {
            if (borrowerCount == 0 && invalidated) {
                schemaStorage.close();
            }
        }

        private void close() {
            schemaStorage.close();
        }
    }

    class StatisticsBackgroundCounter {
        private final RocksSession.Data session;
        private final Thread thread;
        private final Semaphore countJobNotifications;
        private boolean isStopped;

        StatisticsBackgroundCounter(RocksSession.Data session) {
            this.session = session;
            countJobNotifications = new Semaphore(1);
            thread = NamedThreadFactory.create(session.database.name + "::statistics-background-counter")
                    .newThread(this::countFn);
            thread.start();
        }

        public void needsBackgroundCounting() {
            countJobNotifications.release();
        }

        private void countFn() {
            while (running()) {
                waitForCountJob();
                if (running()) break;

                try (RocksTransaction.Data tx = session.transaction(WRITE)) {
                    tx.graphMgr.data().stats().processCountJobs();
                    tx.commit();
                } catch (GraknException e) {
                    // TODO: Add specific code indicating rocksdb conflict to GraknException status code
                    boolean txConflicted = e.getCause() instanceof RocksDBException &&
                            ((RocksDBException)e.getCause()).getStatus().getCode() == Status.Code.Busy;
                    if (txConflicted) {
                        countJobNotifications.release();
                    } else {
                        throw e;
                    }
                }
            }
        }

        private void waitForCountJob() {
            try {
                countJobNotifications.acquire();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
            countJobNotifications.drainPermits();
        }

        private boolean running() {
            return !isStopped && isOpen.get();
        }

        private void stop() {
            try {
                isStopped = true;
                countJobNotifications.release();
                thread.join();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }
    }
}
