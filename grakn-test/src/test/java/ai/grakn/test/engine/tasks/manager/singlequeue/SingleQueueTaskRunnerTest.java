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
 *
 */

package ai.grakn.test.engine.tasks.manager.singlequeue;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskRunner;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.engine.tasks.EndlessExecutionTestTask;
import ai.grakn.test.engine.tasks.LongExecutionTestTask;
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import mjson.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.cancelledTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.clearTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.failingTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.whenTaskFinishes;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.whenTaskStarts;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitQuickcheck.class)
public class SingleQueueTaskRunnerTest {

    private SingleQueueTaskRunner taskRunner;
    private TaskStateInMemoryStore storage;

    private MockGraknConsumer<TaskId, TaskState> consumer;
    private TopicPartition partition;

    @Before
    public void setUp() {
        clearTasks();

        storage = new TaskStateInMemoryStore();

        consumer = new MockGraknConsumer<>(OffsetResetStrategy.EARLIEST);

        partition = new TopicPartition("hi", 0);

        consumer.assign(ImmutableSet.of(partition));
        consumer.updateBeginningOffsets(ImmutableMap.of(partition, 0L));
        consumer.updateEndOffsets(ImmutableMap.of(partition, 0L));
    }

    @After
    public void cleanup() throws Exception{
        taskRunner.close();
    }

    public void setUpTasks(List<List<TaskState>> tasks) {
        taskRunner = new SingleQueueTaskRunner(storage, consumer);

        createValidQueue(tasks);

        for (List<TaskState> taskList : tasks) {
            consumer.schedulePollTask(() -> taskList.forEach(this::addTask));
        }

        consumer.scheduleEmptyPollTask(() -> {
            Thread closeTaskRunner = new Thread(() -> {
                try {
                    taskRunner.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            closeTaskRunner.start();
        });
    }

    public static List<TaskState> tasks(List<? extends List<TaskState>> tasks) {
        return tasks.stream().flatMap(Collection::stream).collect(toList());
    }

    private void createValidQueue(List<List<TaskState>> tasks) {
        Set<TaskId> appearedTasks = Sets.newHashSet();

        tasks(tasks).forEach(task -> {
            TaskId taskId = task.getId();

            if (!appearedTasks.contains(taskId)) {
                task.status(CREATED);
            } else {
                // The second time a task appears it must be in RUNNING state
                task.status(RUNNING);
                storage.updateState(task);
            }

            appearedTasks.add(taskId);
        });
    }

    private void addTask(TaskState task) {
        Long offset = consumer.endOffsets(ImmutableSet.of(partition)).get(partition);
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, task.getId(), task));
        consumer.updateEndOffsets(ImmutableMap.of(partition, offset + 1));
    }

    @Property(trials=10)
    public void afterRunning_AllTasksAreAddedToStorage(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        tasks(tasks).forEach(task ->
                assertNotNull(storage.getState(task.getId()))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        completableTasks(tasks(tasks)).forEach(task ->
                assertThat(storage.getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        failingTasks(tasks(tasks)).forEach(task ->
                assertThat(storage.getState(task).status(), is(FAILED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksHaveCompletedExactlyOnce(List<List<TaskState>> tasks) throws Exception {
        setUpTasks(tasks);

        taskRunner.run();

        Multiset<TaskId> expectedCompletedTasks = ImmutableMultiset.copyOf(completableTasks(tasks(tasks)));

        assertEquals(expectedCompletedTasks, completedTasks());
    }

    @Test
    public void whenRunIsCalled_DontReturnUntilCloseIsCalled() throws Exception {
        setUpTasks(ImmutableList.of());

        Thread thread = new Thread(taskRunner);
        thread.start();

        assertTrue(thread.isAlive());

        taskRunner.close();
        thread.join();

        assertFalse(thread.isAlive());
    }

    @Test
    public void whenATaskIsStoppedBeforeExecution_TheTaskIsNotCancelled() throws Exception {
        TaskState task = createTask(ShortExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        taskRunner.stopTask(task.getId());

        taskRunner.run();

        assertThat(cancelledTasks(), empty());
        assertThat(completedTasks(), contains(task.getId()));
    }

    @Test
    public void whenATaskIsStoppedBeforeExecution_TheTaskIsMarkedAsCompleted() throws Exception {
        TaskState task = createTask(ShortExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        taskRunner.stopTask(task.getId());

        taskRunner.run();

        assertThat(storage.getState(task.getId()).status(), is(COMPLETED));
    }

    @Test
    public void whenATaskIsStoppedBeforeExecution_ReturnFalse() throws Exception {
        TaskState task = createTask(ShortExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        boolean stopped = taskRunner.stopTask(task.getId());

        assertFalse(stopped);
    }

    @Test
    public void whenATaskIsStoppedDuringExecution_TheTaskIsCancelled() throws Exception {
        TaskState task = createTask(EndlessExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        whenTaskStarts(taskRunner::stopTask);

        taskRunner.run();

        assertThat(cancelledTasks(), contains(task.getId()));
    }

    @Test
    public void whenATaskIsStoppedDuringExecution_TheTaskIsMarkedAsStopped() throws Exception {
        TaskState task = createTask(EndlessExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        whenTaskStarts(taskRunner::stopTask);

        taskRunner.run();

        assertThat(storage.getState(task.getId()).status(), is(STOPPED));
    }

    @Test
    public void whenATaskIsStoppedDuringExecution_ReturnTrue() throws Exception {
        TaskState task = createTask(EndlessExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        final Boolean[] stopped = {null};

        whenTaskStarts(taskId ->
            stopped[0] = taskRunner.stopTask(taskId)
        );

        taskRunner.run();

        assertTrue(stopped[0]);
    }

    @Test
    public void whenATaskIsStoppedAfterExecution_TheTaskIsCompleted() throws Exception {
        TaskState task = createTask(LongExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        whenTaskFinishes(taskRunner::stopTask);

        taskRunner.run();

        assertThat(cancelledTasks(), empty());
        assertThat(completedTasks(), contains(task.getId()));
    }

    @Test
    public void whenATaskIsStoppedAfterExecution_TheTaskIsMarkedAsCompleted() throws Exception {
        TaskState task = createTask(LongExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        whenTaskFinishes(taskRunner::stopTask);

        taskRunner.run();

        assertThat(storage.getState(task.getId()).status(), is(COMPLETED));
    }

    @Test
    public void whenATaskIsStoppedAfterExecution_ReturnFalse() throws Exception {
        TaskState task = createTask(ShortExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        final Boolean[] stopped = {null};

        whenTaskFinishes(taskId ->
                stopped[0] = taskRunner.stopTask(taskId)
        );

        taskRunner.run();

        assertFalse(stopped[0]);
    }

    @Test
    public void whenATaskIsMarkedAsStoppedInStorage_ItIsNotExecuted() throws Exception {
        TaskState task = createTask(ShortExecutionTestTask.class, TaskStatus.CREATED, TaskSchedule.now(), Json.object());

        setUpTasks(ImmutableList.of(ImmutableList.of(task)));

        storage.newState(TaskState.of(null, null, null, null, task.getId()).status(STOPPED));

        taskRunner.run();

        assertThat(completedTasks(), empty());
    }
}