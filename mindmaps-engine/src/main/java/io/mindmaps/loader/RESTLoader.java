/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.loader;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.implementation.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.postprocessing.Cache;
import io.mindmaps.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Singleton class that handles insert queries received via REST end point.
 * It also maintains the statistics about the loading jobs.
 */

public class RESTLoader {
    private final Logger LOG = LoggerFactory.getLogger(RESTLoader.class);

    private Cache cache;

    private static int repeatCommits;

    private static RESTLoader instance = null;

    private ExecutorService executor;
    private final Map<UUID, TransactionState> loaderState;

    private AtomicBoolean maintenanceInProcess;
    private AtomicInteger enqueuedJobs;
    private AtomicInteger loadingJobs;
    private AtomicInteger finishedJobs;
    private AtomicLong lastJobFinished;
    private AtomicInteger errorJobs;
    private AtomicBoolean maintenanceInProgress;

    public long getLastJobFinished() {
        return lastJobFinished.get();
    }
    public int getLoadingJobs() {
        return loadingJobs.get();
    }

    private RESTLoader() {
        cache = Cache.getInstance();
        executor = Executors.newFixedThreadPool(8);
        loaderState = new ConcurrentHashMap<>();
        enqueuedJobs = new AtomicInteger();
        loadingJobs = new AtomicInteger();
        errorJobs = new AtomicInteger();
        finishedJobs = new AtomicInteger();
        lastJobFinished= new AtomicLong();
        maintenanceInProcess= new AtomicBoolean(false);
        repeatCommits = ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.LOADER_REPEAT_COMMITS);
    }

    public static synchronized RESTLoader getInstance() {
        if (instance == null) instance = new RESTLoader();
        return instance;
    }

    private class TransactionState{
        private State currentState;
        private String exception;

        public TransactionState(State state){
            currentState=state;
        }
        public void setException(String exceptionParam){
            exception=exceptionParam;
        }

        public void setState(State stateParam){ currentState=stateParam;}

        public State getState(){
            return currentState;
        }

        public String getException(){return exception;}


        @Override
        public String toString() {
            return "{ \"state\":\"" + currentState + "\", " +
                    " \"exception\":\"" + exception + "\"}";
        }
    }

    public UUID addJob(String name, String queryString) {
        UUID newUUID = UUID.randomUUID();
        loaderState.put(newUUID, new TransactionState(State.QUEUED));
        executor.submit(() -> loadData(name, queryString, newUUID));
        enqueuedJobs.incrementAndGet();
        return newUUID;
    }

    public void loadData(String name, String batch, UUID uuid) {
        // Attempt committing the transaction a certain number of times
        // If a transaction fails, it must be repeated from scratch because Titan is forgetful
        loaderState.put(uuid, new TransactionState(State.LOADING));
        loadingJobs.incrementAndGet();
        enqueuedJobs.decrementAndGet();

        for (int i = 0; i < repeatCommits; i++) {
            MindmapsTransactionImpl transaction = (MindmapsTransactionImpl) GraphFactory.getInstance().getGraphBatchLoading(name).newTransaction();

            try {
                QueryParser.create(transaction).parseInsertQuery(batch).execute();
                transaction.commit();
                cache.addJobCasting(name, transaction.getModifiedCastingIds());
                loaderState.get(uuid).setState(State.FINISHED);
                finishedJobs.incrementAndGet();
                return;

            } catch (MindmapsValidationException e) {
                //If it's a validation exception there is no point in re-trying
                LOG.error(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                loaderState.get(uuid).setState(State.ERROR);
                loaderState.get(uuid).setException(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                errorJobs.incrementAndGet();
                return;
            } catch (IllegalArgumentException e) {
                //If it's a parsing exception there is no point in re-trying
                LOG.error(ErrorMessage.PARSING_EXCEPTION.getMessage(e.getMessage()));
                loaderState.get(uuid).setState(State.ERROR);
                loaderState.get(uuid).setException(ErrorMessage.PARSING_EXCEPTION.getMessage(e.getMessage()));
                errorJobs.incrementAndGet();
                return;
            } catch (Exception e) {
                //If it's not a validation exception we need to remain in the for loop
                handleError(e, 1);
            } finally {
                try {
                    transaction.close();
                    loadingJobs.decrementAndGet();
                    lastJobFinished.set(System.currentTimeMillis());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        LOG.error(ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));
        loaderState.get(uuid).setState(State.ERROR);
        loaderState.get(uuid).setException(ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));
        errorJobs.incrementAndGet();

        //TODO: log the errors to a log file.
    }

    public String getStatus(UUID uuid) {
        return loaderState.get(uuid).toString();
    }

    private void handleError(Exception e, int i) {
        LOG.error("Caught exception ", e);
        e.printStackTrace();

        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
    }

    public synchronized void unlock(){
        maintenanceInProcess.set(false);
        notifyAll();
        LOG.info("Unlocking QueueManager [" + this + "]");
    }
    public void lock(){
        maintenanceInProcess.set(true);
        LOG.info("Locking QueueManager [" + this + "] for external maintenance");
    }

}
