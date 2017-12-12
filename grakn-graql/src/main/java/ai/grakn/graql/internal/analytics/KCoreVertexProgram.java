/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.analytics;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Set;

/**
 * The vertex program for computing k-core.
 *
 * @author Jason Liu
 */

public class KCoreVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 100;

    public static final String CLUSTER_LABEL = "kCoreVertexProgram.clusterLabel";
    private static final String QUALIFIED = "kCoreVertexProgram.qualified";
    private static final String IMPLICIT_MESSAGE_COUNT = "kCoreVertexProgram.implicitMessageCount";

    private static final String K_CORE_STABLE = "kCoreVertexProgram.stable";
    private static final String K_CORE_EXIST = "kCoreVertexProgram.exist";
    private static final String CONNECTED_COMPONENT_STARTED = "kCoreVertexProgram.ccStarted";
    private static final String VOTE_TO_HALT = "kCoreVertexProgram.voteToHalt";
    private static final String K = "kCoreVertexProgram.k";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = Sets.newHashSet(
            MemoryComputeKey.of(K_CORE_STABLE, Operator.and, false, true),
            MemoryComputeKey.of(K_CORE_EXIST, Operator.or, false, true),
            MemoryComputeKey.of(CONNECTED_COMPONENT_STARTED, Operator.assign, true, true),
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = Sets.newHashSet(
            VertexComputeKey.of(CLUSTER_LABEL, false),
            VertexComputeKey.of(QUALIFIED, true),
            VertexComputeKey.of(IMPLICIT_MESSAGE_COUNT, true));

    private int k;

    // Needed internally for OLAP tasks
    public KCoreVertexProgram() {
    }

    public KCoreVertexProgram(int kValue) {
        this.persistentProperties.put(K, kValue);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        k = (int) persistentProperties.get(K_CORE_STABLE);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("KCoreVertexProgram Started !!!!!!!!");
        memory.set(K_CORE_STABLE, true);
        memory.set(K_CORE_EXIST, false);
        memory.set(VOTE_TO_HALT, true);
        memory.set(CONNECTED_COMPONENT_STARTED, false);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        String id;
        switch (memory.getIteration()) {
            case 0:
                sendMessage(messenger, vertex.value(Schema.VertexProperty.ID.name()));
                break;

            case 1: // get degree first, as degree must >= k
                if ((vertex.label().equals(Schema.BaseType.ENTITY.name()) ||
                        vertex.label().equals(Schema.BaseType.ATTRIBUTE.name())) &&
                        Iterators.size(messenger.receiveMessages()) >= k) {
                    vertex.property(QUALIFIED, true);
                    memory.add(K_CORE_EXIST, true);
                    sendMessage(messenger, vertex.value(Schema.VertexProperty.ID.name()));
                }
                break;

            default:
                if (memory.<Boolean>get(CONNECTED_COMPONENT_STARTED)) {
                    //

                } else {
                    if (memory.getIteration() % 2 == 0) {
                        relayOrSaveMessages(vertex, messenger);
                    } else { // entity iteration
                        if (vertex.property(QUALIFIED).isPresent()) {
                            id = vertex.value(Schema.VertexProperty.ID.name());
                            if (vertex.label().equals(Schema.BaseType.ATTRIBUTE.name())) {
                                int messageCount = getMessageCount(messenger, id);
                                if (vertex.property(IMPLICIT_MESSAGE_COUNT).isPresent()) {
                                    messageCount += (int) vertex.value(IMPLICIT_MESSAGE_COUNT);
                                }
                                updateVertex(vertex, messenger, memory, id, messageCount);
                            } else if (vertex.label().equals(Schema.BaseType.ENTITY.name())) {
                                int messageCount = getMessageCount(messenger, id);
                                updateVertex(vertex, messenger, memory, id, messageCount);
                            }
                        }
                    }
                }
                break;
        }
    }

    private static void relayOrSaveMessages(Vertex vertex, Messenger<String> messenger) {
        if (vertex.label().equals(Schema.BaseType.RELATIONSHIP.name())) {
            // relay the messages
            messenger.receiveMessages().forEachRemaining(msg -> sendMessage(messenger, msg));
        } else if (vertex.label().equals(Schema.BaseType.ATTRIBUTE.name()) &&
                vertex.property(QUALIFIED).isPresent()) {
            if (messenger.receiveMessages().hasNext()) {
                // messages received via implicit edge, save the count for next iteration
                vertex.property(IMPLICIT_MESSAGE_COUNT, Iterators.size(messenger.receiveMessages()));
            }
        }
    }

    private void updateVertex(Vertex vertex, Messenger<String> messenger, Memory memory, String id, int messageCount) {
        if (messageCount >= k) {
            sendMessage(messenger, id);
            memory.add(K_CORE_EXIST, true);
        } else {
            vertex.property(QUALIFIED).remove();
            memory.add(K_CORE_STABLE, false);
        }
    }


    private static int getMessageCount(Messenger<String> messenger, String id) {
        int messageCount = 0;
        Iterator<String> messages = messenger.receiveMessages();
        while (messages.hasNext()) {
            if (!messages.next().equals(id)) messageCount++;
        }
        return messageCount;
    }

    private void update(Vertex vertex, Messenger<String> messenger, Memory memory) {
        String currentMax = vertex.value(CLUSTER_LABEL);
        String max = IteratorUtils.reduce(messenger.receiveMessages(), currentMax,
                (a, b) -> a.compareTo(b) > 0 ? a : b);
        if (max.compareTo(currentMax) > 0) {
            vertex.property(CLUSTER_LABEL, max);
            messenger.sendMessage(messageScopeIn, max);
            messenger.sendMessage(messageScopeOut, max);
            memory.add(VOTE_TO_HALT, false);
        }
    }

    private static void sendMessage(Messenger<String> messenger, String message) {
        messenger.sendMessage(messageScopeIn, message);
        messenger.sendMessage(messageScopeOut, message);
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());
        if (memory.isInitialIteration()) return false;

        if (memory.<Boolean>get(CONNECTED_COMPONENT_STARTED)) {
            if (memory.<Boolean>get(VOTE_TO_HALT)) {
                LOGGER.debug("KCoreVertexProgram Finished !!!!!!!!");
                return true; // connected component is done
            } else {
                memory.set(VOTE_TO_HALT, true);
                return false;
            }
        } else {
            if (memory.getIteration() % 2 == 1) { // entity iteration
                if (memory.<Boolean>get(K_CORE_EXIST)) {
                    if (memory.<Boolean>get(K_CORE_STABLE)) {
                        memory.set(CONNECTED_COMPONENT_STARTED, true);
                    } else {
                        memory.set(K_CORE_EXIST, false);
                        memory.set(K_CORE_STABLE, true);
                        return false;
                    }
                } else {
                    LOGGER.debug("KCoreVertexProgram Finished !!!!!!!!");
                    return true; // no such core areas exist
                }
            } else {
                return false; // can only end or start cc in entity iteration
            }
        }


        if (memory.getIteration() < 2) return false;
        if (memory.<Boolean>get(VOTE_TO_HALT)) {
            LOGGER.debug("KCoreVertexProgram Finished !!!!!!!!");
            return true;
        }
        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        memory.set(VOTE_TO_HALT, true);
        return false;
    }

}