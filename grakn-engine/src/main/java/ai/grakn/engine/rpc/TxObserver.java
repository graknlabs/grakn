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
import ai.grakn.concept.Concept;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.grpc.GrpcConceptConverter;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.ExecQuery;
import ai.grakn.rpc.generated.GrpcGrakn.IteratorId;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.QueryResult;
import ai.grakn.rpc.generated.GrpcGrakn.RunConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link GrpcServer}.
 * <p>
 * <p>
 * Receives a stream of {@link TxRequest}s and returning a stream of {@link TxResponse}s.
 * </p>
 *
 * @author Felix Chapman
 */
class TxObserver implements StreamObserver<TxRequest>, AutoCloseable {

    private final StreamObserver<TxResponse> responseObserver;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final ExecutorService threadExecutor;
    private final GrpcOpenRequestExecutor requestExecutor;

    private final AtomicInteger iteratorIdCounter = new AtomicInteger();
    private final Map<IteratorId, Iterator<QueryResult>> iterators = new ConcurrentHashMap<>();

    @Nullable
    private GraknTx tx = null;

    private TxObserver(StreamObserver<TxResponse> responseObserver, ExecutorService threadExecutor, GrpcOpenRequestExecutor requestExecutor) {
        this.responseObserver = responseObserver;
        this.threadExecutor = threadExecutor;
        this.requestExecutor = requestExecutor;
    }

    public static TxObserver create(StreamObserver<TxResponse> responseObserver, GrpcOpenRequestExecutor requestExecutor) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
        ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
        return new TxObserver(responseObserver, threadExecutor, requestExecutor);
    }

    @Override
    public void onNext(TxRequest request) {
        try {
            submit(() -> {
                GrpcGraknService.runAndConvertGraknExceptions(() -> handleRequest(request));
            });
        } catch (StatusRuntimeException e) {
            if (!terminated.getAndSet(true)) {
                responseObserver.onError(e);
            }
        }
    }

    private void handleRequest(TxRequest request) {
        switch (request.getRequestCase()) {
            case OPEN:
                open(request.getOpen());
                break;
            case COMMIT:
                commit();
                break;
            case EXECQUERY:
                execQuery(request.getExecQuery());
                break;
            case NEXT:
                next(request.getNext());
                break;
            case STOP:
                stop(request.getStop());
                break;
            case RUNCONCEPTMETHOD:
                runConceptMethod(request.getRunConceptMethod());
                break;
            default:
            case REQUEST_NOT_SET:
                throw GrpcGraknService.error(Status.INVALID_ARGUMENT);
        }
    }

    @Override
    public void onError(Throwable t) {
        close();
    }

    @Override
    public void onCompleted() {
        close();
    }

    @Override
    public void close() {
        submit(() -> {
            if (tx != null) {
                tx.close();
            }

            if (!terminated.getAndSet(true)) {
                responseObserver.onCompleted();
            }
        });

        threadExecutor.shutdown();
    }

    private void submit(Runnable runnable) {
        try {
            threadExecutor.submit(runnable).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assert cause instanceof RuntimeException : "No checked exceptions are thrown, because it's a `Runnable`";
            throw (RuntimeException) cause;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void open(Open request) {
        if (tx != null) {
            throw GrpcGraknService.error(Status.FAILED_PRECONDITION);
        }
        tx = requestExecutor.execute(request);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void commit() {
        tx().commit();
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void execQuery(ExecQuery request) {
        String queryString = request.getQuery().getValue();

        QueryBuilder graql = tx().graql();

        if (request.hasInfer()) {
            graql = graql.infer(request.getInfer().getValue());
        }

        Iterator<QueryResult> iterator = graql.parse(queryString).results(GrpcConverter.get()).iterator();
        IteratorId iteratorId =
                IteratorId.newBuilder().setId(iteratorIdCounter.getAndIncrement()).build();

        iterators.put(iteratorId, iterator);

        responseObserver.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
    }

    private void next(GrpcGrakn.Next next) {
        IteratorId iteratorId = next.getIteratorId();

        Iterator<QueryResult> iterator = nonNull(iterators.get(iteratorId));

        TxResponse response;

        if (iterator.hasNext()) {
            QueryResult queryResult = iterator.next();
            response = TxResponse.newBuilder().setQueryResult(queryResult).build();
        } else {
            response = GrpcUtil.doneResponse();
            iterators.remove(iteratorId);
        }

        responseObserver.onNext(response);
    }

    private void stop(GrpcGrakn.Stop stop) {
        nonNull(iterators.remove(stop.getIteratorId()));
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void runConceptMethod(RunConceptMethod runConceptMethod) {
        Concept concept = nonNull(tx().getConcept(GrpcUtil.getConceptId(runConceptMethod)));

        GrpcConceptConverter converter = grpcConcept -> tx().getConcept(GrpcUtil.convert(grpcConcept.getId()));

        ConceptMethod<?> conceptMethod = ConceptMethod.fromGrpc(converter, runConceptMethod.getConceptMethod());

        TxResponse response = conceptMethod.run(concept);

        responseObserver.onNext(response);
    }

    private GraknTx tx() {
        return nonNull(tx);
    }

    private static <T> T nonNull(@Nullable T item) {
        if (item == null) {
            throw GrpcGraknService.error(Status.FAILED_PRECONDITION);
        } else {
            return item;
        }
    }

}
