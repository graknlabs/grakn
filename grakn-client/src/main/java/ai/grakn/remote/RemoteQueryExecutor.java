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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.ComputeJob;
import ai.grakn.QueryExecutor;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.rpc.GrpcClient;
import com.google.common.collect.Iterators;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Remote implementation of {@link QueryExecutor} that communicates with a Grakn server using gRPC.
 * Like {@link RemoteGraknTx}, this class is an adapter that uses the {@link GrpcClient} for gRPC calls.
 *
 * @author Grakn Warriors
 */
final class RemoteQueryExecutor implements QueryExecutor {

    private final GrpcClient client;

    private RemoteQueryExecutor(GrpcClient client) {
        this.client = client;
    }

    public static RemoteQueryExecutor create(GrpcClient client) {
        return new RemoteQueryExecutor(client);
    }

    @Override
    public Stream<Answer> run(GetQuery query) {
        return runAnswerStream(query);
    }

    @Override
    public Stream<Answer> run(InsertQuery query) {
        return runAnswerStream(query);
    }

    @Override
    public void run(DeleteQuery query) {
        runVoid(query);
    }

    @Override
    public Answer run(DefineQuery query) {
        return (Answer) Iterators.getOnlyElement(client.execQuery(query));
    }

    @Override
    public void run(UndefineQuery query) {
        runVoid(query);
    }

    @Override
    public <T> T run(AggregateQuery<T> query) {
        return (T) Iterators.getOnlyElement(client.execQuery(query));
    }


    @Override
    public ComputeJob<ComputeQuery.Answer> run(ComputeQuery query) {
        ComputeQuery.Answer answer = (ComputeQuery.Answer) Iterators.getOnlyElement(client.execQuery(query));
        return RemoteComputeJob.of(answer);
    }

    private void runVoid(Query<?> query) {
        client.execQuery(query).forEachRemaining(empty -> {});
    }

    private Stream<Answer> runAnswerStream(Query<?> query) {
        Iterable<Object> iterable = () -> client.execQuery(query);
        Stream<Object> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.map(Answer.class::cast);
    }
}
