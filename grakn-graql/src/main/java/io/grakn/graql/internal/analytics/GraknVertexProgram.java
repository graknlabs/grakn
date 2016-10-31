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

package io.grakn.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.grakn.util.ErrorMessage;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * A vertex program specific to Grakn with common method implementations.
 */
public abstract class GraknVertexProgram<T> extends CommonOLAP implements VertexProgram<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(GraknVertexProgram.class);

    final MessageScope.Local<Long> messageScopeIn = MessageScope.Local.of(__::inE);
    final MessageScope.Local<Long> messageScopeOut = MessageScope.Local.of(__::outE);
    final Set<MessageScope> messageScopeSet = Sets.newHashSet(messageScopeIn, messageScopeOut);

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return messageScopeSet;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);

        // store class name for reflection on spark executor
        configuration.setProperty(VERTEX_PROGRAM, this.getClass().getName());
    }

    @Override
    public void setup(final Memory memory) {
    }

    @Override
    public void execute(Vertex vertex, Messenger<T> messenger, Memory memory) {
        // try to deal with ghost vertex issues by ignoring them
        if (Utility.isAlive(vertex)) {
            safeExecute(vertex, messenger, memory);
        }
    }

    /**
     * An alternative to the execute method when ghost vertices are an issue. Our "Ghostbuster".
     *
     * @param vertex    a vertex that may be a ghost
     * @param messenger Tinker message passing object
     * @param memory    Tinker memory object
     */
    abstract void safeExecute(Vertex vertex, Messenger<T> messenger, Memory memory);

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public GraknVertexProgram clone() {
        try {
            return (GraknVertexProgram) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(), e.getMessage()), e);
        }
    }

}
