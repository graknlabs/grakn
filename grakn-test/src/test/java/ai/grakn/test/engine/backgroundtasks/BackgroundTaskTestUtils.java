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
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.backgroundtasks.distributed.TaskFailover;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;
import java.util.stream.IntStream;

import static java.time.Instant.now;
import static java.util.stream.Collectors.toSet;

/**
 * Class holding useful methods for use throughout background task tests
 */
public class BackgroundTaskTestUtils {
    private final static Logger LOG = LoggerFactory.getLogger(TaskFailover.class);

    public static Set<TaskState> createTasks(int n, TaskStatus status) {
        return IntStream.range(0, n)
                .mapToObj(i -> createTask(i, status, false, 0))
                .collect(toSet());
    }

    public static TaskState createTask(int i, TaskStatus status, boolean recurring, int interval) {
        return new TaskState(TestTask.class.getName())
                .status(status)
                .creator(BackgroundTaskTestUtils.class.getName())
                .statusChangedBy(BackgroundTaskTestUtils.class.getName())
                .runAt(now())
                .isRecurring(recurring)
                .interval(interval)
                .configuration(Json.object("name", "task" + i));
    }
    
    public static void waitForStatus(TaskStateStorage storage, Set<TaskState> tasks, TaskStatus status) {
        tasks.forEach(t -> waitForStatus(storage, t, status));
    }

    public static void waitForStatus(TaskStateStorage storage, TaskState task, TaskStatus status) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 15000) {
            try {
                TaskStatus currentStatus = storage.getState(task.getId()).status();
                if (currentStatus == status) {
                    return;
                }
            } catch (Exception ignored){}
        }

        LOG.debug("Timed out waiting for status of {}", task.getId());
    }
}
