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

package ai.grakn.engine.tasks;

import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateGraphStore;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import ai.grakn.engine.GraknEngineConfig;

import static ai.grakn.engine.GraknEngineConfig.USE_ZOOKEEPER_STORAGE;

import ai.grakn.engine.TaskId;

/**
 * <p>
 *     The base TaskManager interface.
 * </p>
 *
 * <p>
 *     Provides common methods for scheduling tasks for execution and stopping task execution.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public interface TaskManager extends AutoCloseable {
    /**
     * Schedule a {@link BackgroundTask} for execution, giving it priority to run after all other tasks
     * @param taskState Task to execute
     */
    void addLowPriorityTask(TaskState taskState, TaskConfiguration configuration);

    /**
     * Schedule a {@link BackgroundTask} for execution, giving it priority to run before all other tasks
     * @param taskState Task to execute
     */
    void addHighPriorityTask(TaskState taskState, TaskConfiguration configuration);

    /**
     * Stop a Scheduled, Paused or Running task. Task's .stop() method will be called to perform any cleanup and the
     * task is killed afterwards.
     * @param id ID of task to stop.
     */
    void stopTask(TaskId id);

    /**
     * Return the StateStorage instance that is used by this class.
     * @return A StateStorage instance.
     */
    TaskStateStorage storage();

    // TODO: Add 'pause' and 'restart' methods

    default TaskStateStorage chooseStorage(GraknEngineConfig properties, ZookeeperConnection zookeeper){
        if(properties.getPropertyAsBool(USE_ZOOKEEPER_STORAGE)){
            return new TaskStateZookeeperStore(zookeeper);
        } else {
            return new TaskStateGraphStore();
        }
    }
}
