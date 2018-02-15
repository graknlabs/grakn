/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.remote;

import ai.grakn.GraknComputer;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.util.SimpleURI;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Remote implementation of {@link GraknSession} that communicates with a Grakn server using gRPC.
 *
 * @see RemoteGraknTx
 * @see RemoteGrakn
 *
 * @author Felix Chapman
 */
class RemoteGraknSession implements GraknSession {

    private final Keyspace keyspace;
    private final SimpleURI uri;
    private final ManagedChannel channel;

    private RemoteGraknSession(Keyspace keyspace, SimpleURI uri, ManagedChannel channel) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.channel = channel;
    }

    @VisibleForTesting
    public static RemoteGraknSession create(Keyspace keyspace, SimpleURI uri, ManagedChannel channel) {
        return new RemoteGraknSession(keyspace, uri, channel);
    }

    public static RemoteGraknSession create(Keyspace keyspace, SimpleURI uri){
        // TODO: usePlainText is insecure
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext(true).build();

        return create(keyspace, uri, channel);
    }

    GraknStub stub() {
        return GraknGrpc.newStub(channel);
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        return RemoteGraknTx.create(this, transactionType);
    }

    @Override
    public GraknComputer getGraphComputer() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void close() throws GraknTxOperationException {
        channel.shutdown();
    }

    @Override
    public String uri() {
        return uri.toString();
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    @Override
    public GraknConfig config() {
        throw new UnsupportedOperationException(); // TODO
    }
}
