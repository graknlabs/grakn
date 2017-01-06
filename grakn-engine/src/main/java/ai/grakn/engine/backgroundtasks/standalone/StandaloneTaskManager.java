/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.engine.backgroundtasks.standalone;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.taskstorage.InMemoryStateStorage;
import ai.grakn.engine.util.ConfigProperties;
import javafx.util.Pair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

public class StandaloneTaskManager implements TaskManager {
    private static String RUN_ONCE_NAME = "One off task scheduler.";
    private static String RUN_RECURRING_NAME = "Recurring task scheduler.";
    private static String EXCEPTION_CATCHER_NAME = "Task Exception Catcher.";
    private static String SAVE_CHECKPOINT_NAME = "Save task checkpoint.";

    private static StandaloneTaskManager instance = null;

    private final Logger LOG = LoggerFactory.getLogger(StandaloneTaskManager.class);

    private Map<String, Pair<ScheduledFuture<?>, BackgroundTask>> instantiatedTasks;
    private StateStorage stateStorage;
    private ReentrantLock stateUpdateLock;

    private ExecutorService executorService;
    private ScheduledExecutorService schedulingService;

    private StandaloneTaskManager() {
        instantiatedTasks = new ConcurrentHashMap<>();
        stateStorage = InMemoryStateStorage.getInstance();
        stateUpdateLock = new ReentrantLock();

        ConfigProperties properties = ConfigProperties.getInstance();
        schedulingService = Executors.newScheduledThreadPool(1);
        executorService = Executors.newFixedThreadPool(properties.getAvailableThreads());
    }

    public static synchronized StandaloneTaskManager getInstance() {
        if (instance == null)
            instance = new StandaloneTaskManager();
        return instance;
    }

    public TaskManager open() {
        return this;
    }

    @Override
    public void close(){
        executorService.shutdown();
        schedulingService.shutdown();
        instance = null;
    }

    public String scheduleTask(BackgroundTask task, String createdBy, Date runAt, long period, JSONObject configuration) {
        Boolean recurring = (period != 0);
        String id = stateStorage.newState(task.getClass().getName(), createdBy, runAt, recurring, period, configuration);

        // Schedule task to run.
        Date now = new Date();
        long delay = now.getTime() - runAt.getTime();

        try {
            stateStorage.updateState(id, SCHEDULED, this.getClass().getName(), null, null, null, null);

            ScheduledFuture<?> future;
            if(recurring)
                future = schedulingService.scheduleAtFixedRate(runTask(id, task, true), delay, period, MILLISECONDS);
            else
                future = schedulingService.schedule(runTask(id, task, false), delay, MILLISECONDS);

            instantiatedTasks.put(id, new Pair<>(future, task));

        }
        catch (Throwable t) {
            LOG.error(getFullStackTrace(t));
            stateStorage.updateState(id, FAILED, this.getClass().getName(), null, t, null, null);
            instantiatedTasks.remove(id);
            return null;
        }

        return id;
    }

    public CompletableFuture completableFuture(String taskId) {
        if(!instantiatedTasks.containsKey(taskId)){
            return null;
        }

        try {
            return CompletableFuture.runAsync(() -> {
                try {
                    while(true){
                        TaskState state = storage().getState(taskId);
                        if(state.status().equals(COMPLETED) || state.status().equals(FAILED)){
                            break;
                        }
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Throwable t){
            LOG.error(getFullStackTrace(t));
            throw new RuntimeException(t);
        }
    }

    public TaskManager stopTask(String id, String requesterName) {
        stateUpdateLock.lock();

        TaskState state = stateStorage.getState(id);
        if(state == null)
            return this;

        Pair<ScheduledFuture<?>, BackgroundTask> pair = instantiatedTasks.get(id);
        String name = this.getClass().getName();

        synchronized (pair) {
            if(state.status() == SCHEDULED || (state.status() == COMPLETED && state.isRecurring())) {
                LOG.info("Stopping a currently scheduled task "+id);
                pair.getKey().cancel(true);
                stateStorage.updateState(id, STOPPED, name,null, null, null, null);
            }
            else if(state.status() == RUNNING) {
                LOG.info("Stopping running task "+id);

                BackgroundTask task = pair.getValue();
                if(task != null) {
                    task.stop();
                }

                stateStorage.updateState(id, STOPPED, name, null, null, null, null);
            }
            else {
                LOG.warn("Task not running - "+id);
            }
        }
        stateUpdateLock.unlock();

        return this;
    }

    public StateStorage storage() {
        return stateStorage;
    }

    private Runnable exceptionCatcher(String id, BackgroundTask task) {
        return () -> {
            try {
                task.start(saveCheckpoint(id), stateStorage.getState(id).configuration());

                stateUpdateLock.lock();
                if(stateStorage.getState(id).status() == RUNNING)
                    stateStorage.updateState(id, COMPLETED, EXCEPTION_CATCHER_NAME, null, null, null, null);
                stateUpdateLock.unlock();
            }
            catch (Throwable t) {
                LOG.error(getFullStackTrace(t));
                stateStorage.updateState(id, FAILED, EXCEPTION_CATCHER_NAME, null, t, null, null);
            }
        };
    }

    private Runnable runTask(String id, BackgroundTask task, Boolean recurring) {
        return () -> {
            stateUpdateLock.lock();

            TaskState state = stateStorage.getState(id);
            if(recurring && (state.status() == SCHEDULED || state.status() == COMPLETED)) {
                stateStorage.updateState(id, RUNNING, RUN_RECURRING_NAME, null, null, null, null);
                executorService.submit(exceptionCatcher(id, task));
            }
            else if (!recurring && state.status() == SCHEDULED) {
                stateStorage.updateState(id, RUNNING, RUN_ONCE_NAME, null, null, null, null);
                executorService.submit(exceptionCatcher(id, task));
            }

            stateUpdateLock.unlock();
        };
    }

    private Consumer<String> saveCheckpoint(String id) {
        return s -> {
            stateUpdateLock.lock();
            stateStorage.updateState(id, stateStorage.getState(id).status(), SAVE_CHECKPOINT_NAME, null, null, s, null);
            stateUpdateLock.unlock();
        };
    }
}
