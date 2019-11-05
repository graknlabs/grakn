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
 */

package grakn.core.graph.graphdb.log;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.log.Change;
import grakn.core.graph.core.log.ChangeProcessor;
import grakn.core.graph.core.log.LogProcessorBuilder;
import grakn.core.graph.core.log.LogProcessorFramework;
import grakn.core.graph.core.schema.JanusGraphSchemaElement;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.Message;
import grakn.core.graph.diskstorage.log.MessageReader;
import grakn.core.graph.diskstorage.log.ReadMarker;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.database.log.LogTxMeta;
import grakn.core.graph.graphdb.database.log.TransactionLogHeader;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.vertices.StandardVertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardLogProcessorFramework implements LogProcessorFramework {

    private static final Logger LOG = LoggerFactory.getLogger(StandardLogProcessorFramework.class);

    private final StandardJanusGraph graph;
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Map<String, Log> processorLogs;

    private boolean isOpen = true;

    public StandardLogProcessorFramework(StandardJanusGraph graph) {
        Preconditions.checkArgument(graph != null && graph.isOpen());
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        this.processorLogs = new HashMap<>();
    }

    private void checkOpen() {
        Preconditions.checkState(isOpen, "Transaction LOG framework has already been closed");
    }

    @Override
    public synchronized boolean removeLogProcessor(String logIdentifier) {
        checkOpen();
        if (processorLogs.containsKey(logIdentifier)) {
            try {
                processorLogs.get(logIdentifier).close();
            } catch (BackendException e) {
                throw new JanusGraphException("Could not close transaction LOG: " + logIdentifier, e);
            }
            processorLogs.remove(logIdentifier);
            return true;
        } else return false;
    }

    @Override
    public synchronized void shutdown() throws JanusGraphException {
        if (!isOpen) return;
        isOpen = false;
        try {
            for (Log log : processorLogs.values()) {
                log.close();
            }
            processorLogs.clear();
        } catch (BackendException e) {
            throw new JanusGraphException(e);
        }
    }

    @Override
    public LogProcessorBuilder addLogProcessor(String logIdentifier) {
        return new Builder(logIdentifier);
    }

    private class Builder implements LogProcessorBuilder {

        private final String userLogName;
        private final List<ChangeProcessor> processors;

        private String readMarkerName = null;
        private Instant startTime = null;
        private int retryAttempts = 1;


        private Builder(String userLogName) {
            Preconditions.checkArgument(StringUtils.isNotBlank(userLogName));
            this.userLogName = userLogName;
            this.processors = new ArrayList<>();
        }

        @Override
        public String getLogIdentifier() {
            return userLogName;
        }

        @Override
        public LogProcessorBuilder setProcessorIdentifier(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name));
            this.readMarkerName = name;
            return this;
        }

        @Override
        public LogProcessorBuilder setStartTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        @Override
        public LogProcessorBuilder setStartTimeNow() {
            this.startTime = null;
            return this;
        }

        @Override
        public LogProcessorBuilder addProcessor(ChangeProcessor processor) {
            Preconditions.checkArgument(processor != null);
            this.processors.add(processor);
            return this;
        }

        @Override
        public LogProcessorBuilder setRetryAttempts(int attempts) {
            Preconditions.checkArgument(attempts > 0, "Invalid number: %s", attempts);
            this.retryAttempts = attempts;
            return this;
        }

        @Override
        public void build() {
            Preconditions.checkArgument(!processors.isEmpty(), "Must add at least one processor");
            ReadMarker readMarker;
            if (startTime == null && readMarkerName == null) {
                readMarker = ReadMarker.fromNow();
            } else if (readMarkerName == null) {
                readMarker = ReadMarker.fromTime(startTime);
            } else if (startTime == null) {
                readMarker = ReadMarker.fromIdentifierOrNow(readMarkerName);
            } else {
                readMarker = ReadMarker.fromIdentifierOrTime(readMarkerName, startTime);
            }
            synchronized (StandardLogProcessorFramework.this) {
                Preconditions.checkArgument(!processorLogs.containsKey(userLogName), "Processors have already been registered for user LOG: %s", userLogName);
                try {
                    Log log = graph.getBackend().getUserLog(userLogName);
                    log.registerReaders(readMarker, Iterables.transform(processors, new Function<ChangeProcessor, MessageReader>() {
                        @Override
                        public MessageReader apply(@Nullable ChangeProcessor changeProcessor) {
                            return new MsgReaderConverter(userLogName, changeProcessor, retryAttempts);
                        }
                    }));
                } catch (BackendException e) {
                    throw new JanusGraphException("Could not open user transaction LOG for name: " + userLogName, e);
                }
            }
        }
    }

    private class MsgReaderConverter implements MessageReader {

        private final String userlogName;
        private final ChangeProcessor processor;
        private final int retryAttempts;

        private MsgReaderConverter(String userLogName, ChangeProcessor processor, int retryAttempts) {
            this.userlogName = userLogName;
            this.processor = processor;
            this.retryAttempts = retryAttempts;
        }

        private void readRelations(TransactionLogHeader.Entry transactionEntry,
                                   StandardJanusGraphTx tx, StandardChangeState changes) {
            for (TransactionLogHeader.Modification modification : transactionEntry.getContentAsModifications(serializer)) {
                InternalRelation rel = ModificationDeserializer.parseRelation(modification, tx);

                //Special case for vertex addition/removal
                Change state = modification.state;
                if (rel.getType().equals(BaseKey.VertexExists) && !(rel.getVertex(0) instanceof JanusGraphSchemaElement)) {
                    if (state == Change.REMOVED) { //Mark as removed
                        ((StandardVertex) rel.getVertex(0)).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
                    }
                    changes.addVertex(rel.getVertex(0), state);
                } else if (!rel.isInvisible()) {
                    changes.addRelation(rel, state);
                }
            }
        }

        @Override
        public void read(Message message) {
            for (int i = 1; i <= retryAttempts; i++) {
                StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
                StandardChangeState changes = new StandardChangeState();
                StandardTransactionId transactionId;
                try {
                    ReadBuffer content = message.getContent().asReadBuffer();
                    String senderId = message.getSenderId();
                    TransactionLogHeader.Entry transactionEntry = TransactionLogHeader.parse(content, serializer, times);
                    if (transactionEntry.getMetadata().containsKey(LogTxMeta.SOURCE_TRANSACTION)) {
                        transactionId = (StandardTransactionId) transactionEntry.getMetadata().get(LogTxMeta.SOURCE_TRANSACTION);
                    } else {
                        transactionId = new StandardTransactionId(senderId, transactionEntry.getHeader().getId(), transactionEntry.getHeader().getTimestamp());
                    }
                    readRelations(transactionEntry, tx, changes);
                } catch (Throwable e) {
                    tx.rollback();
                    LOG.error("Encountered exception [{}] when preparing processor [{}] for user LOG [{}] on attempt {} of {}",
                            e.getMessage(), processor, userlogName, i, retryAttempts);
                    LOG.error("Full exception: ", e);
                    continue;
                }
                try {
                    processor.process(tx, transactionId, changes);
                    return;
                } catch (Throwable e) {
                    tx.rollback();
                    tx = null;
                    LOG.error("Encountered exception [{}] when running processor [{}] for user LOG [{}] on attempt {} of {}",
                            e.getMessage(), processor, userlogName, i, retryAttempts);
                    LOG.error("Full exception: ", e);
                } finally {
                    if (tx != null) tx.commit();
                }
            }
        }

        @Override
        public void updateState() {
        }
    }

}
