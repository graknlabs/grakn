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

import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GraknGrpc.GraknImplBase;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.test.rule.CompositeTestRule;
import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.rules.TestRule;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Semi-mocked gRPC server that can handle transactions.
 *
 * <p>
 *     The gRPC server itself is "real" and can be connected to using the {@link #channel()}. However, the
 *     {@link #service()} and {@link #requests()} are both mock objects and should be used with
 *     {@link org.mockito.Mockito#verify(Object)}.
 * </p>
 * <p>
 *     By default, the server will return a {@link GrpcUtil#doneResponse()} to every message. And will respond
 *     with {@link StreamObserver#onCompleted()} when receiving a {@link StreamObserver#onCompleted()} from the client.
 * </p>
 * <p>
 *     In order to mock additional responses, use the method {@link #setResponse(TxRequest, TxResponse)}.
 * </p>
 *
 * @author Felix Chapman
 */
public final class GrpcServerMock extends CompositeTestRule {

    private final GrpcServerRule serverRule = new GrpcServerRule().directExecutor();
    private final GraknImplBase service = mock(GraknImplBase.class);

    private @Nullable StreamObserver<TxResponse> serverResponses = null;

    @SuppressWarnings("unchecked") // safe because mock
    private StreamObserver<TxRequest> serverRequests = mock(StreamObserver.class);

    private GrpcServerMock() {
    }

    public static GrpcServerMock create() {
        return new GrpcServerMock();
    }

    public ManagedChannel channel() {
        return serverRule.getChannel();
    }

    GraknImplBase service() {
        return service;
    }

    public StreamObserver<TxRequest> requests() {
        return serverRequests;
    }

    public void setResponse(TxRequest request, TxResponse response) {
        setResponse(request, responses -> responses.onNext(response));
    }

    void setResponse(TxRequest request, Consumer<StreamObserver<TxResponse>> response) {
        doAnswer(args -> {
            response.accept(serverResponses);
            return null;
        }).when(serverRequests).onNext(request);
    }

    @Override
    protected List<TestRule> testRules() {
        return ImmutableList.of(serverRule);
    }

    @Override
    protected void before() throws Throwable {
        when(service.tx(any())).thenAnswer(args -> {
            serverResponses = args.getArgument(0);
            return serverRequests;
        });

        // Return a default "done" response to every message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            serverResponses.onNext(GrpcUtil.doneResponse());
            return null;
        }).when(serverRequests).onNext(any());

        // Return a default "complete" response to every "complete" message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            serverResponses.onCompleted();
            return null;
        }).when(serverRequests).onCompleted();

        serverRule.getServiceRegistry().addService(service);
    }

    @Override
    protected void after() {
        if (serverResponses != null) {
            try {
                serverResponses.onCompleted();
            } catch (IllegalStateException e) {
                // this occurs if something has already ended the call
            }
        }
    }

}
