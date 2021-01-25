/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.concurrent.producer;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.concurrent.common.ExecutorService.forkJoinPool;
import static java.util.concurrent.CompletableFuture.completedFuture;

@ThreadSafe
public class AsyncProducer<T> implements Producer<T> {

    private final int parallelisation;
    private final ResourceIterator<ResourceIterator<T>> iterators;
    private final ConcurrentMap<ResourceIterator<T>, CompletableFuture<Void>> runningJobs;
    private final AtomicBoolean isDone;
    private boolean isInitialised;

    AsyncProducer(ResourceIterator<ResourceIterator<T>> iterators, int parallelisation) {
        assert parallelisation > 0;
        this.iterators = iterators;
        this.parallelisation = parallelisation;
        this.runningJobs = new ConcurrentHashMap<>();
        this.isDone = new AtomicBoolean(false);
        this.isInitialised = false;
    }

    @Override
    public synchronized void produce(Producer.Queue<T> queue, int request) {
        if (isDone.get()) return;
        else if (!isInitialised) initialise(queue);
        distribute(queue, request);
    }

    private synchronized void initialise(Queue<T> queue) {
        for (int i = 0; i < parallelisation && iterators.hasNext(); i++) {
            runningJobs.put(iterators.next(), completedFuture(null));
        }
        isInitialised = true;
        if (runningJobs.isEmpty()) done(queue);
    }

    private synchronized void distribute(Queue<T> queue, int request) {
        if (isDone.get()) return;
        int requestSplitMax = (int) Math.ceil((double) request / runningJobs.size());
        int requestSent = 0;
        for (ResourceIterator<T> iterator : runningJobs.keySet()) {
            int requestSplit = Math.min(requestSplitMax, request - requestSent);
            runningJobs.computeIfPresent(iterator, (iter, asyncJob) -> asyncJob.thenRunAsync(
                    () -> job(queue, iter, requestSplit), forkJoinPool()
            ));
            requestSent += requestSplit;
            if (requestSent == request) break;
        }
    }

    private synchronized void transition(Queue<T> queue, ResourceIterator<T> iterator, int unfulfilled) {
        if (!iterator.hasNext()) {
            if (runningJobs.remove(iterator) != null && iterators.hasNext()) compensate(queue, unfulfilled);
            else if (!runningJobs.isEmpty() && unfulfilled > 0) distribute(queue, unfulfilled);
            else if (runningJobs.isEmpty()) done(queue);
            else if (unfulfilled != 0) throw GraknException.of(ILLEGAL_STATE);
        } else {
            if (unfulfilled != 0) throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private synchronized void compensate(Queue<T> queue, int unfulfilled) {
        ResourceIterator<T> newIter = iterators.next();
        runningJobs.put(newIter, completedFuture(null));
        if (unfulfilled > 0) {
            runningJobs.computeIfPresent(newIter, (iter, asyncJob) ->
                    asyncJob.thenRunAsync(() -> job(queue, newIter, unfulfilled), forkJoinPool()));
        }
    }

    private void job(Queue<T> queue, ResourceIterator<T> iterator, int request) {
        try {
            int unfulfilled = request;
            if (runningJobs.containsKey(iterator)) {
                for (; unfulfilled > 0 && iterator.hasNext() && !isDone.get(); unfulfilled--) {
                    queue.put(iterator.next());
                }
            }
            if (!isDone.get()) transition(queue, iterator, unfulfilled);
        } catch (Throwable e) {
            done(queue, e);
        }
    }

    private void done(Queue<T> queue) {
        if (isDone.compareAndSet(false, true)) {
            queue.done();
        }
    }

    private void done(Queue<T> queue, Throwable e) {
        if (isDone.compareAndSet(false, true)) {
            queue.done(e);
        }
    }

    @Override
    public synchronized void recycle() {
        iterators.recycle();
        runningJobs.keySet().forEach(ResourceIterator::recycle);
    }
}
