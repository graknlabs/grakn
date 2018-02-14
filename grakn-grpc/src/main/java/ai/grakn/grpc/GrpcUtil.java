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

package ai.grakn.grpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Commit;
import ai.grakn.rpc.generated.GraknOuterClass.Done;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.Next;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.Stop;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import mjson.Json;

import javax.annotation.Nullable;

/**
 * @author Felix Chapman
 */
public class GrpcUtil {

    /**
     * Enumeration of all sub-classes of {@link GraknException} that can be thrown during gRPC calls.
     */
    public enum Error {
        // TODO: it's likely some of these will NEVER be thrown normally, so shouldn't be here
        GRAQL_QUERY_EXCEPTION,
        GRAQL_SYNTAX_EXCEPTION,
        GRAKN_TX_OPERATION_EXCEPTION,
        TEMPORARY_WRITE_EXCEPTION,
        GRAKN_SERVER_EXCEPTION,
        PROPERTY_NOT_UNIQUE_EXCEPTION,
        INVALID_KB_EXCEPTION,
        GRAKN_MODULE_EXCEPTION,
        GRAKN_BACKEND_EXCEPTION,
        UNKNOWN;

        public static Metadata.Key<Error> KEY = Metadata.Key.of("error", new Metadata.AsciiMarshaller<Error>() {
            @Override
            public String toAsciiString(Error value) {
                return value.name();
            }

            @Override
            public Error parseAsciiString(String serialized) {
                return Error.valueOf(serialized);
            }
        });
    }

    public static TxRequest openRequest(Keyspace keyspace, GraknTxType txType) {
        Open.Builder open = Open.newBuilder().setKeyspace(convert(keyspace)).setTxType(convert(txType));
        return TxRequest.newBuilder().setOpen(open).build();
    }

    public static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, null);
    }

    public static TxRequest execQueryRequest(String queryString, @Nullable Boolean infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        if (infer != null) {
            execQueryRequest.setInfer(Infer.newBuilder().setValue(infer));
        }
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    public static TxRequest nextRequest() {
        return TxRequest.newBuilder().setNext(Next.getDefaultInstance()).build();
    }

    public static TxRequest stopRequest() {
        return TxRequest.newBuilder().setStop(Stop.getDefaultInstance()).build();
    }

    public static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    public static Keyspace getKeyspace(Open open) {
        return convert(open.getKeyspace());
    }

    public static GraknTxType getTxType(Open open) {
        return convert(open.getTxType());
    }

    public static Object getQueryResult(QueryResult queryResult) {
        switch (queryResult.getQueryResultCase()) {
            case ANSWER:
                return convert(queryResult.getAnswer());
            case OTHERRESULT:
                return Json.read(queryResult.getOtherResult()).getValue();
            default:
            case QUERYRESULT_NOT_SET:
                throw new IllegalArgumentException(); // TODO
        }
    }

    private static GraknTxType convert(TxType txType) {
        switch (txType) {
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + txType);
        }
    }

    private static TxType convert(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TxType.Read;
            case WRITE:
                return TxType.Write;
            case BATCH:
                return TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }

    private static Keyspace convert(GraknOuterClass.Keyspace keyspace) {
        return Keyspace.of(keyspace.getValue());
    }

    private static GraknOuterClass.Keyspace convert(Keyspace keyspace) {
        return GraknOuterClass.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
    }

    private static Answer convert(GraknOuterClass.Answer answer) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        answer.getAnswerMap().forEach((grpcVar, grpcConcept) -> {
            Concept concept = new MyConcept(ConceptId.of(grpcConcept.getId()));
            map.put(Graql.var(grpcVar), concept);
        });

        return new QueryAnswer(map.build());
    }

    private static class MyConcept implements Concept {

        private final ConceptId id;

        private MyConcept(ConceptId id) {
            this.id = id;
        }

        @Override
        public ConceptId getId() {
            return id;
        }

        @Override
        public Keyspace keyspace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete() throws GraknTxOperationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDeleted() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyConcept myConcept = (MyConcept) o;

            return id.equals(myConcept.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public int compareTo(Concept o) {
            throw new UnsupportedOperationException();
        }
    }
}
