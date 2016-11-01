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

package io.mindmaps.shell;

import io.mindmaps.Mindmaps;
import io.mindmaps.engine.session.RemoteSession;
import io.mindmaps.graql.GraqlClient;
import io.mindmaps.graql.GraqlShell;
import org.eclipse.jetty.websocket.api.Session;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

class GraqlClientMock implements GraqlClient {

    private RemoteSession server = new RemoteSession(keyspace -> {
        this.keyspace = keyspace;
        return Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
    });

    private String keyspace = null;
    private URI uri;

    String getKeyspace() {
        return keyspace;
    }

    URI getURI() {
        return uri;
    }

    @Override
    public Future<Session> connect(Object websocket, URI uri) {
        this.uri = uri;

        GraqlShell client = (GraqlShell) websocket;

        SessionMock serverSession = new SessionMock(client::onMessage);
        server.onConnect(serverSession);

        SessionMock clientSession = new SessionMock(serverSession, server::onMessage);
        CompletableFuture<Session> future = new CompletableFuture<>();
        future.complete(clientSession);
        return future;
    }

    @Override
    public void close() {
    }
}
