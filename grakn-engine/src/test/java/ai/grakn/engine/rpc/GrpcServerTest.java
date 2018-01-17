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

package ai.grakn.engine.rpc;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import ai.grakn.rpc.GraknOuterClass.TxType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import mjson.Json;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class GrpcServerTest {

    private static final int PORT = 5555;
    private static final String MYKS = "myks";
    private static final String QUERY = "match $x isa person; get;";

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final GraknTx tx = mock(GraknTx.class);
    private final GetQuery query = mock(GetQuery.class);

    private GrpcServer server;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Before
    public void setUp() throws IOException {
        server = GrpcServer.create(PORT, txFactory);

        QueryBuilder qb = mock(QueryBuilder.class);

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY)).thenReturn(query);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.close();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Read));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Batch));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.BATCH);
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(commitRequest());
        }

        verify(tx).commit();
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        try (
                BidirectionalObserver<TxRequest, TxResponse> tx1 = startTx();
                BidirectionalObserver<TxRequest, TxResponse> tx2 = startTx()
        ) {
            tx1.send(openRequest(MYKS, TxType.Write));
            tx2.send(openRequest(MYKS, TxType.Write));
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest("not!@akeyspace", TxType.Write));

            exception.expect(hasMessage(GraknTxOperationException.invalidKeyspace("not!@akeyspace").getMessage()));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenClosingATransactionRemotely_TheTransactionIsClosedWithinTheThreadItWasCreated() {
        final Thread[] threadOpenedWith = new Thread[1];
        final Thread[] threadClosedWith = new Thread[1];

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenAnswer(invocation -> {
            threadOpenedWith[0] = Thread.currentThread();
            return tx;
        });

        doAnswer(invocation -> {
            threadClosedWith[0] = Thread.currentThread();
            return null;
        }).when(tx).close();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));
        }

        ai.grakn.graql.Query<?> query = tx.graql().parse(QUERY);
        verify(query).execute();
    }

    @Test
    public void whenExecutingAQueryRemotely_AResultIsReturned() {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.getId()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isThing()).thenReturn(true);
        when(conceptX.asThing().type().getLabel()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.getId()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isThing()).thenReturn(true);
        when(conceptY.asThing().type().getLabel()).thenReturn(Label.of("L456"));

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        when(query.execute()).thenReturn(answers);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));

            tx.send(execQueryRequest(QUERY));

            Json response = Json.read(tx.receive().elem().getQueryResult().getValue());

            Json expected = Json.array(
                    Json.object("x", Json.object("isa", "L123", "id", "V123")),
                    Json.object("y", Json.object("isa", "L456", "id", "V456"))
            );

            assertEquals(expected, response);
        }
    }

    @Test
    public void whenExecutingQueryWithoutInferenceSet_InferenceIsNotSet() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));
        }

        verify(tx.graql(), times(0)).infer(anyBoolean());
    }

    @Test
    public void whenExecutingQueryWithInferenceOff_InferenceIsTurnedOff() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY, false));
        }

        verify(tx.graql()).infer(false);
    }

    @Test
    public void whenExecutingQueryWithInferenceOn_InferenceIsTurnedOn() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY, true));
        }

        verify(tx.graql()).infer(true);
    }

    @Test
    public void whenCommittingBeforeOpeningTx_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(commitRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenExecutingAQueryBeforeOpeningTx_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenOpeningTxTwice_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(openRequest(MYKS, TxType.Write));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenOpeningTxFails_Throw() throws Throwable {
        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(GraknExceptionFake.EXCEPTION).when(tx).commit();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(commitRequest());

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenParsingQueryFails_Throw() throws Throwable {
        when(tx.graql().parse(QUERY)).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        when(query.execute()).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    private BidirectionalObserver<TxRequest, TxResponse> startTx() {
        return BidirectionalObserver.create(stub::tx);
    }

    private TxRequest openRequest(String keyspaceString, TxType txType) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        TxRequest.Open.Builder open = TxRequest.Open.newBuilder().setKeyspace(keyspace).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    private TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(TxRequest.Commit.getDefaultInstance()).build();
    }

    private TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, null);
    }

    private TxRequest execQueryRequest(String queryString, @Nullable Boolean infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        TxRequest.ExecQuery.Builder execQueryRequest = TxRequest.ExecQuery.newBuilder().setQuery(query);

        if (infer != null) {
            execQueryRequest.setSetInfer(true).setInfer(infer);
        }

        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    private Matcher<StatusRuntimeException> hasStatus(Status status) {
        return allOf(
                isA(StatusRuntimeException.class),
                hasProperty("status", is(status))
        );
    }

    private Matcher<StatusRuntimeException> hasMessage(String message) {
        return allOf(
                hasStatus(Status.UNKNOWN),
                new TypeSafeMatcher<StatusRuntimeException>() {
                    @Override
                    public void describeTo(Description description) {
                        description.appendText("has message " + message);
                    }

                    @Override
                    protected boolean matchesSafely(StatusRuntimeException item) {
                        return message.equals(item.getTrailers().get(GrpcServer.MESSAGE));
                    }
                }
        );
    }

    static class GraknExceptionFake extends GraknException {

        public static final String MESSAGE = "OH DEAR";
        public static final GraknExceptionFake EXCEPTION = new GraknExceptionFake();

        protected GraknExceptionFake() {
            super(MESSAGE);
        }
    }
}

