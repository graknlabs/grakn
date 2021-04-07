/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server;

import grakn.common.collection.ConcurrentSet;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static grakn.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;
import static grakn.core.concurrent.executor.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SessionService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);

    private final ConcurrentSet<TransactionService> transactionServices;
    private final GraknService graknSvc;
    private final Options.Session options;
    private final Grakn.Session session;
    private final ReadWriteLock accessLock;
    private final AtomicBoolean isOpen;
    private final long idleTimeoutMillis;
    private ScheduledFuture<?> idleTimeoutTask;

    public SessionService(GraknService graknSvc, Grakn.Session session, Options.Session options) {
        this.graknSvc = graknSvc;
        this.session = session;
        this.options = options;
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = new AtomicBoolean(true);
        this.transactionServices = new ConcurrentSet<>();
        this.idleTimeoutMillis = options.sessionIdleTimeoutMillis();
        setIdleTimeout();
    }

    void register(TransactionService transactionSvc) {
        try {
            accessLock.readLock().lock();
            if (isOpen.get()) transactionServices.add(transactionSvc);
            else throw GraknException.of(SESSION_CLOSED);
        } finally {
            accessLock.readLock().unlock();
        }
    }

    void remove(TransactionService transactionSvc) {
        transactionServices.remove(transactionSvc);
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public UUID UUID() {
        return session.uuid();
    }

    public Grakn.Session session() {
        return session;
    }

    public Options.Session options() {
        return options;
    }

    private void setIdleTimeout() {
        if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
        this.idleTimeoutTask = scheduled().schedule(this::triggerIdleTimeout, idleTimeoutMillis, MILLISECONDS);
    }

    private void triggerIdleTimeout() {
        if (!transactionServices.isEmpty()) {
            keepAlive();
            return;
        }
        close();
        LOG.warn("Session with ID " + session.uuid() + " timed out due to inactivity");
    }

    public synchronized void keepAlive() {
        setIdleTimeout();
    }

    @Override
    public void close() {
        try {
            accessLock.writeLock().lock();
            if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
            if (isOpen.compareAndSet(true, false)) {
                transactionServices.forEach(TransactionService::close);
                session.close();
                graknSvc.remove(this);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    public void close(Throwable error) {
        try {
            accessLock.writeLock().lock();
            if (isOpen.compareAndSet(true, false)) {
                transactionServices.forEach(tr -> tr.close(error));
                session.close();
                graknSvc.remove(this);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }
}
