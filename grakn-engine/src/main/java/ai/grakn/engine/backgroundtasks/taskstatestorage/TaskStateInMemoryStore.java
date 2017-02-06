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

package ai.grakn.engine.backgroundtasks.taskstatestorage;

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.TaskStatus;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Implementation of StateStorage that stores task state in memory.
 *     If engine fails, task state recovery is not possible.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class TaskStateInMemoryStore implements TaskStateStorage {
    private final Map<String, SoftReference<TaskState>> storage;

    public TaskStateInMemoryStore() {
        storage = new ConcurrentHashMap<>();
    }

    @Override
    public String newState(TaskState state) {
        storage.put(state.getId(), new SoftReference<>(state));
        return state.getId();
    }

    @Override
    public Boolean updateState(TaskState state) {
        storage.put(state.getId(), new SoftReference<>(state));
        return true;
    }

    @Override
    public TaskState getState(String id) {
        if(id == null || !storage.containsKey(id)) {
            return null;
        }

        return storage.get(id).get();
    }

    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset) {
        Set<TaskState> res = new HashSet<>();

        int count = 0;
        for(Map.Entry<String, SoftReference<TaskState>> x: storage.entrySet()) {
            TaskState state = x.getValue().get();
            if(state == null) {
                continue;
            }

            // AND
            if(taskStatus != null && state.status() != taskStatus) {
                continue;
            }
            if(taskClassName != null && !Objects.equals(state.taskClassName(), taskClassName)) {
                continue;
            }
            if(createdBy != null && !Objects.equals(state.creator(), createdBy)) {
                continue;
            }

            if(count < offset) {
                count++;
                continue;
            }
            else if(limit > 0 && count >= (limit+offset)) {
                break;
            }
            count++;

            res.add(state);
        }

        return res;
    }
}
