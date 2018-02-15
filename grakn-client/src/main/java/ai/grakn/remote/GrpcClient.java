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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.grpc.TxGrpcCommunicator;
import ai.grakn.grpc.TxGrpcCommunicator.Response;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.AbstractIterator;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Communicates with a Grakn gRPC server, translating requests and responses to and from their gRPC representations.
 *
 * <p>
 *     This class is a light abstraction layer over gRPC - it understands how the sequence of calls should execute and
 *     how to translate gRPC objects into Java objects and back. However, any logic is kept in {@link RemoteGraknTx}.
 * </p>
 *
 * @author Felix Chapman
 */
final class GrpcClient implements AutoCloseable {

    private final TxGrpcCommunicator communicator;

    private GrpcClient(TxGrpcCommunicator communicator) {
        this.communicator = communicator;
    }

    public static GrpcClient create(GraknGrpc.GraknStub stub) {
        TxGrpcCommunicator observer = TxGrpcCommunicator.create(stub);
        return new GrpcClient(observer);
    }

    public void open(Keyspace keyspace, GraknTxType txType) {
        communicator.send(GrpcUtil.openRequest(keyspace, txType));
        responseOrThrow();
    }

    public Iterator<Object> execQuery(Query<?> query, @Nullable Boolean infer) {
        communicator.send(GrpcUtil.execQueryRequest(query.toString(), infer));

        return new AbstractIterator<Object>() {
            private boolean firstElem = true;

            @Override
            protected Object computeNext() {
                if (firstElem) {
                    firstElem = false;
                } else {
                    communicator.send(GrpcUtil.nextRequest());
                }

                TxResponse response = responseOrThrow();

                switch (response.getResponseCase()) {
                    case QUERYRESULT:
                        return GrpcUtil.getQueryResult(response.getQueryResult());
                    case DONE:
                        return endOfData();
                    default:
                    case RESPONSE_NOT_SET:
                        throw CommonUtil.unreachableStatement("Unexpected " + response);
                }
            }
        };
    }

    public void commit() {
        communicator.send(GrpcUtil.commitRequest());
        responseOrThrow();
    }

    @Override
    public void close() {
        communicator.close();
    }

    private TxResponse responseOrThrow() {
        Response response;

        try {
            response = communicator.receive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // This is called from classes like RemoteGraknTx, that impl methods which do not throw InterruptedException
            // Therefore, we have to wrap it in a RuntimeException.
            throw new RuntimeException(e);
        }

        switch (response.type()) {
            case OK:
                return response.ok();
            case ERROR:
                throw convertStatusRuntimeException(response.error());
            case COMPLETED:
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    private static RuntimeException convertStatusRuntimeException(StatusRuntimeException error) {
        Status status = error.getStatus();
        Metadata trailers = error.getTrailers();

        ErrorType errorType = trailers.get(ErrorType.KEY);

        if (errorType != null) {
            String message = status.getDescription();
            return errorType.toException(message);
        } else {
            return error;
        }
    }
}
