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

package ai.grakn.test.engine.backgroundtasks;

import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.taskstorage.InMemoryStateStorage;
import javafx.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static java.time.Instant.now;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InMemoryStateStorageTest {
    private StateStorage stateStorage;

    @Before
    public void setUp() {
        stateStorage = new InMemoryStateStorage();
    }

    @Test
    public void testNewState() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        TaskState state = stateStorage.getState(id);
        assertEquals("name", TestTask.class.getName(), state.taskClassName());
        assertEquals("creator", this.getClass().getName(), state.creator());
        assertEquals("recurring", false, state.isRecurring());
        assertEquals("interval", 0, state.interval());
    }

    @Test
    public void testUpdateState() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        // Get current values
        TaskState state = stateStorage.getState(id);

        // Change
        state.status(SCHEDULED)
                .statusChangedBy("bla")
                .statusChangedBy("newEngine")
                .exception(getFullStackTrace(new UnsupportedOperationException("message")));

        TaskState newState = stateStorage.getState(id);
        assertNotEquals("status", state.status(), newState.status());
        assertNotEquals("status changed by", state.statusChangedBy(), newState.statusChangedBy());
        assertNotEquals("hostname", state.engineID(), newState.engineID());
        assertNotEquals("exception message", state.exception(), newState.exception());
        assertNotEquals("stack trace", state.stackTrace(), newState.stackTrace());
        assertNotEquals("checkpoint", state.checkpoint(), newState.checkpoint());
    }

    @Test
    public void testGetByStatus() {
        String id = stateStorage.newState(task());
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(CREATED, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetByCreator() {
        String id = stateStorage.newState(task());
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, this.getClass().getName(), 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetByClassName() {
        String id = stateStorage.newState(task());
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, TestTask.class.getName(), null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetAll() {
        String id = stateStorage.newState(task());
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testPagination() {
        for (int i = 0; i < 20; i++) {
            stateStorage.newState(task());
        }

        Set<Pair<String, TaskState>> setA = stateStorage.getTasks(null, null, null, 10, 0);
        Set<Pair<String, TaskState>> setB = stateStorage.getTasks(null, null, null, 10, 10);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }

    public TaskState task(){
        return new TaskState(TestTask.class.getName())
                .statusChangedBy(this.getClass().getName())
                .runAt(now())
                .isRecurring(false)
                .interval(0)
                .configuration(null);
    }
}
