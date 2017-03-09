/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.tasks;

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskId;
import mjson.Json;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.addCompletedTask;

public class ShortExecutionTestTask implements BackgroundTask {
    public static final AtomicInteger startedCounter = new AtomicInteger(0);
    public static final AtomicInteger resumedCounter = new AtomicInteger(0);

    public void start(Consumer<String> saveCheckpoint, Json config) {
        startedCounter.incrementAndGet();
        addCompletedTask(TaskId.of(config.at("id").asString()));
    }

    public void stop() {}

    public void pause() {}

    public void resume(Consumer<String> c, String s) {
        resumedCounter.incrementAndGet();
    }
}
