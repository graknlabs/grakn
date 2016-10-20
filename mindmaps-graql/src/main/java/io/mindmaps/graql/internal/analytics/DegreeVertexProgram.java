/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class DegreeVertexProgram extends MindmapsVertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    // element key
    public static final String DEGREE = "medianVertexProgram.degree";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Collections.singleton(DEGREE);

    public DegreeVertexProgram() {
    }

    public DegreeVertexProgram(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.countMessageScopeOut);
        set.add(this.countMessageScopeIn);
        return set;
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {

            // iteration 0 starts from all the instances in the subgraph
            case 0:
                // check if vertex is in the subgraph
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        // each role-player sends 1 to castings following incoming edges
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        // the assertion can also be role-player, so sending 1 to castings following incoming edges
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        // send -1 to castings following outgoing edges
                        messenger.sendMessage(this.countMessageScopeOut, -1L);
                    }
                }
                break;

            // iteration 1 starts from all the castings
            case 1:
                String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                if (type.equals(Schema.BaseType.CASTING.name())) {
                    boolean hasRolePlayer = false;
                    long assertionCount = 0;
                    Iterator<Long> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        long message = iterator.next();
                        // count number of assertions connected
                        if (message < 0) assertionCount++;
                            // check if a message is received from the role-player
                        else hasRolePlayer = true;
                    }

                    // make sure this role-player is in the subgraph
                    if (hasRolePlayer) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, assertionCount);
                    }
                }
                break;

            // last iteration starts from all the instances in the subgraph
            case 2:
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    vertex.property(DEGREE, edgeCount);
                }
                break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 2;
    }

}
