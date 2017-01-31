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

package ai.grakn.engine.backgroundtasks;

import javafx.util.Pair;

import java.util.Set;

/**
 * <p>
 *     The base StateStorage interface.
 * </p>
 *
 * <p>
 *     Provides common methods for storing and accessing the state of tasks.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public interface TaskStateStorage {
    /**
     * Create a new task state and store it, returning an ID to later access this task state.
     * @param state State to insert.
     * @return String form of the task id, which can be use later to update or retrieve the task state. Null if task could
     * not be created of mandatory fields were omitted.
     */
    String newState(TaskState state);

    /**
     * Used to update task state. With the exception of @id any other fields may individually be null, however all parameters
     * cannot be null at the same time. Setting any of the parameters to null indicates that their values should not be
     * changed.
     * @param state State to update.
     * @return true if inserted successfully
     */
    Boolean updateState(TaskState state);

    /**
     * This is a copy of the internal TaskState object. It is guaranteed to be correct at the time of call, however the actual
     * internal state may change at any time after.
     * @param id String id of task.
     * @return TaskState object or null if no TaskState with this id could be found.
     */
    TaskState getState(String id);

    /**
     * Return a Set of Pairs of tasks that match any of the criteria. The first value of the Pair is the task id, whilst
     * the second is the TaskState. Parameters may be set to null to not match against then (rather than to match null
     * values), if *all* parameters are null then all known tasks in the system are returned.
     *
     * @param taskStatus See TaskStatus enum.
     * @param taskClassName String containing task class name. See TaskState.
     * @param createdBy String containing created by. See TaskState.
     * @param limit Limit the returned result set to @limit amount of entries.
     * @param offset Use in conjunction with @limit for pagination.
     * @return Set<Pair<String, TaskState>> of task IDs and corresponding TaskState *copies*.
     */
    Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus,
                                          String taskClassName,
                                          String createdBy,
                                          int limit,
                                          int offset);
}
