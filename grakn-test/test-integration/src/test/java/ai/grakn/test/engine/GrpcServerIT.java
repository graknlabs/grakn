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

package ai.grakn.test.engine;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.rpc.SynchronousObserver;
import ai.grakn.engine.rpc.GrpcUtil;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Next;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.Stop;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
public class GrpcServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    private final GraknSession session = engine.sessionWithNewKeyspace();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel =
            ManagedChannelBuilder.forAddress(engine.host(), engine.grpcPort()).usePlaintext(true).build();

    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Test
    public void whenExecutingAndCommittingAQuery_TheQueryIsCommitted() {
        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Write));
            tx.receive();
            tx.send(execQueryRequest("define person sub entity;"));
            queryResults(tx);
            tx.send(commitRequest());
            tx.receive();
        }

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() {
        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Write));
            tx.receive();
            tx.send(execQueryRequest("define person sub entity;"));
            queryResults(tx);
        }

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            assertNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQuery_ResultsAreReturned() {
        List<QueryResult> results;

        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Read));
            tx.receive();
            tx.send(execQueryRequest("match $x sub thing; get;"));

            results = queryResults(tx);
        }

        int numMetaTypes = Schema.MetaSchema.METATYPES.size();
        assertThat(results.toString(), results, hasSize(numMetaTypes));
        assertThat(Sets.newHashSet(results), hasSize(numMetaTypes));

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            for (QueryResult result : results) {
                Map<String, GraknOuterClass.Concept> map = result.getAnswer().getAnswerMap();

                assertThat(map.keySet(), contains("x"));
                assertNotNull(tx.getConcept(ConceptId.of(map.get("x").getId())));
            }
        }
    }

    @Test
    public void whenExecutingTwoSequentialQueries_ResultsAreTheSame() {
        Set<QueryResult> results1;
        Set<QueryResult> results2;

        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Read));
            tx.receive();
            tx.send(execQueryRequest("match $x sub thing; get;"));
            results1 = Sets.newHashSet(queryResults(tx));
            tx.send(execQueryRequest("match $x sub thing; get;"));
            results2 = Sets.newHashSet(queryResults(tx));
        }

        assertEquals(results1, results2);
    }

    @Test // This behaviour is temporary - we should eventually support it correctly
    public void whenExecutingTwoParallelQueries_Throw() throws Throwable {
        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Read));
            tx.receive();
            tx.send(execQueryRequest("match $x sub thing; get;"));
            tx.receive();
            tx.send(execQueryRequest("match $x sub thing; get;"));

            exception.expect(GrpcUtil.hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenExecutingAnInvalidQuery_Throw() throws Throwable {
        try (SynchronousObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue(), TxType.Read));
            tx.receive();
            tx.send(execQueryRequest("match $x sub thing; get $y;"));

            exception.expect(GrpcUtil.hasStatus(Status.UNKNOWN.withDescription(GraqlQueryException.varNotInQuery(var("y")).getMessage())));

            throw tx.receive().throwable();
        }
    }

    private SynchronousObserver<TxRequest, TxResponse> startTx() {
        return SynchronousObserver.create(stub::tx);
    }

    private static TxRequest openRequest(String keyspaceString, TxType txType) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        Open.Builder open = Open.newBuilder().setKeyspace(keyspace).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    private static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    private static TxRequest execQueryRequest(String queryString) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        return TxRequest.newBuilder().setExecQuery(ExecQuery.newBuilder().setQuery(query)).build();
    }

    private static TxRequest nextRequest() {
        return TxRequest.newBuilder().setNext(Next.getDefaultInstance()).build();
    }

    private static TxRequest stopRequest() {
        return TxRequest.newBuilder().setStop(Stop.getDefaultInstance()).build();
    }

    private static List<QueryResult> queryResults(SynchronousObserver<TxRequest, TxResponse> tx) {
        ImmutableList.Builder<QueryResult> results = ImmutableList.builder();

        while (true) {
            TxResponse response = tx.receive().elem();
            assert response != null;

            switch (response.getResponseCase()) {
                case QUERYRESULT:
                    results.add(response.getQueryResult());
                    break;
                case DONE:
                    return results.build();
                default:
                case RESPONSE_NOT_SET:
                    throw CommonUtil.unreachableStatement("Unexpected response: " + response);
            }
            tx.send(nextRequest());
        }
    }
}
