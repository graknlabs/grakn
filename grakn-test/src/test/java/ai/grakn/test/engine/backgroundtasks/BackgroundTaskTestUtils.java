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

package ai.grakn.test.engine.backgroundtasks;

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import mjson.Json;

import java.util.Date;
import java.util.Set;
import java.util.stream.IntStream;

import static java.time.Instant.now;
import static java.util.stream.Collectors.toSet;

/**
 * Class holding useful methods for use throughout background task tests
 */
public class BackgroundTaskTestUtils {

    public static Set<TaskState> createTasks(TaskStateStorage storage, int n, TaskStatus status) {
        return IntStream.range(0, n)
                .mapToObj(i -> createTask(storage, i, status, false, 0))
                .collect(toSet());
    }

    public static TaskState createTask(TaskStateStorage storage,
                                int i, TaskStatus status, boolean recurring, int interval) {
        TaskState state = new TaskState(TestTask.class.getName())
                .status(status)
                .creator(BackgroundTaskTestUtils.class.getName())
                .statusChangedBy(BackgroundTaskTestUtils.class.getName())
                .runAt(now())
                .isRecurring(recurring)
                .interval(interval)
                .configuration(Json.object("name", "task" + i));

        storage.newState(state);
        return state;
    }
    
    public static void waitForStatus(TaskStateStorage storage, Set<TaskState> tasks, TaskStatus status) {
        tasks.forEach(t -> waitForStatus(storage, t, status));
    }

    public static void waitForStatus(TaskStateStorage storage, TaskState task, TaskStatus status) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 60000) {
            try {
                TaskStatus currentStatus = storage.getState(task.getId()).status();
                if (currentStatus == status) {
                    return;
                }
            } catch (Exception ignored){}
        }
    }
}
