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

package grakn.core.server.rpc;

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.core.common.parameters.Options;
import grakn.protocol.TransactionProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.protobuf.ByteString.copyFrom;
import static grakn.core.common.collection.Bytes.uuidToBytes;
import static grakn.core.concurrent.common.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class SessionRPC {

    private static final Logger LOG = LoggerFactory.getLogger(SessionRPC.class);

    private final Grakn.Session session;
    private final GraknRPCService graknRPCService;
    private final ConcurrentHashMap<Integer, TransactionRPC> transactionRPCs;
    private final AtomicBoolean isOpen;
    private final Options.Session options;
    private final long idleTimeoutMillis;
    private ScheduledFuture<?> idleTimeoutTask;

    SessionRPC(GraknRPCService graknRPCService, Grakn.Session session, Options.Session options) {
        this.graknRPCService = graknRPCService;
        this.session = session;
        transactionRPCs = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
        this.options = options;
        idleTimeoutMillis = options.sessionIdleTimeoutMillis();
        setIdleTimeout();
    }

    TransactionRPC transaction(TransactionStream transactionStream, TransactionProto.Transaction.Open.Req request) {
        TransactionRPC transactionRPC = new TransactionRPC(this, transactionStream, request);
        transactionRPCs.put(transactionRPC.hashCode(), transactionRPC);
        return transactionRPC;
    }

    Grakn.Session session() {
        return session;
    }

    Options.Session options() {
        return options;
    }

    ByteString uuidAsByteString() {
        return copyFrom(uuidToBytes(session.uuid()));
    }

    void remove(TransactionRPC transactionRPC) {
        transactionRPCs.remove(transactionRPC.hashCode());
    }

    void close() {
        if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
        if (isOpen.compareAndSet(true, false)) {
            ConcurrentHashMap<Integer, TransactionRPC> transactionRPCsCopy = new ConcurrentHashMap<>(transactionRPCs);
            transactionRPCsCopy.values().parallelStream().forEach(TransactionRPC::close);
            session.close();
            graknRPCService.removeSession(session.uuid());
        }
    }

    void closeWithError(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            ConcurrentHashMap<Integer, TransactionRPC> transactionRPCsCopy = new ConcurrentHashMap<>(transactionRPCs);
            transactionRPCsCopy.values().parallelStream().forEach(tr -> tr.closeWithError(error));
            session.close();
            graknRPCService.removeSession(session.uuid());
        }
    }

    private void setIdleTimeout() {
        if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
        this.idleTimeoutTask = scheduled().schedule(this::triggerIdleTimeout, idleTimeoutMillis, MILLISECONDS);
    }

    private void triggerIdleTimeout() {
        if (!transactionRPCs.isEmpty()) {
            keepAlive();
            return;
        }
        close();
        LOG.warn("Session with ID " + session.uuid() + " timed out due to inactivity");
    }

    synchronized void keepAlive() {
        setIdleTimeout();
    }

    boolean isOpen() {
        return isOpen.get();
    }
}
