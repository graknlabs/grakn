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

package ai.grakn.test.client;

import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.client.GraknClientException;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class BatchExecutorClientTest {

    @Test
    public void whenBatchExecutorClientCloses_AllTasksHaveCompleted() throws GraknClientException {
        Keyspace keyspace = Keyspace.of("yes");

        GraknClientFake graknClient = new GraknClientFake();

        // Make sure there are more queries to execute than are allowed to run at once
        int maxQueries = 10;
        int numQueries = 100;

        BatchExecutorClient.Builder clientBuilder =
                BatchExecutorClient.newBuilder().taskClient(graknClient).maxQueries(maxQueries);

        Set<Query<?>> queriesToExecute =
                IntStream.range(0, numQueries).mapToObj(this::createInsertQuery).collect(toImmutableSet());

        try (BatchExecutorClient client = clientBuilder.build()) {
            for (Query<?> query : queriesToExecute) {
                client.add(query, keyspace);
            }
        }

        assertThat(graknClient.queriesExecuted(), containsInAnyOrder(queriesToExecute.toArray()));
    }

    @Test
    public void whenQueriesFail_TheBatchExecutorClientStillCompletes() throws GraknClientException {
        Keyspace keyspace = Keyspace.of("yes");

        GraknClientFake graknClient = new GraknClientFake();
        graknClient.shouldThrow(new GraknClientException("UH OH"));

        // Make sure there are more queries to execute than are allowed to run at once
        int maxQueries = 10;
        int numQueries = 100;

        BatchExecutorClient.Builder clientBuilder =
                BatchExecutorClient.newBuilder().taskClient(graknClient).maxQueries(maxQueries);

        Set<Query<?>> queriesToExecute =
                IntStream.range(0, numQueries).mapToObj(this::createInsertQuery).collect(toImmutableSet());

        try (BatchExecutorClient client = clientBuilder.build()) {
            for (Query<?> query : queriesToExecute) {
                client.add(query, keyspace);
            }
        }

        assertThat(graknClient.queriesExecuted(), containsInAnyOrder(queriesToExecute.toArray()));
    }

    private InsertQuery createInsertQuery(int i) {
        return insert(var("x").id(ConceptId.of("V" + i)));
    }
}


class GraknClientFake implements GraknClient {

    private final Set<Query<?>> queriesExecuted = ConcurrentHashMap.newKeySet();
    private @Nullable GraknClientException exceptionToThrow = null;

    Set<Query<?>> queriesExecuted() {
        return ImmutableSet.copyOf(queriesExecuted);
    }

    void shouldThrow(@Nullable GraknClientException exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public List graqlExecute(List<Query<?>> queryList, Keyspace keyspace) throws GraknClientException {
        queriesExecuted.addAll(queryList);

        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }

        queryList.stream().forEach(query -> {assert query != null;});
        return queryList;
    }

    @Override
    public Optional<Keyspace> keyspace(String keyspace) throws GraknClientException {
        return Optional.of(Keyspace.of(keyspace));
    }
}