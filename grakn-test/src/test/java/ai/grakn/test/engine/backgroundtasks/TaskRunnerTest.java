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

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.distributed.ZookeeperConnection;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.waitForStatus;
import static junit.framework.Assert.assertEquals;

public class TaskRunnerTest {

    private static ZookeeperConnection connection;

    private TaskStateStorage storage;
    private TaskRunner taskRunner;
    private KafkaProducer<String, String> producer;

    private Thread taskRunnerThread;

    @Rule
    public final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void setup() throws Exception {
        connection = new ZookeeperConnection();

        producer = ConfigHelper.kafkaProducer();
        storage = new TaskStateInMemoryStore();

        taskRunner = new TaskRunner(storage, connection);
        taskRunnerThread = new Thread(taskRunner);
        taskRunnerThread.start();
    }

    @After
    public void tearDown() throws Exception {
        producer.close();

        taskRunner.close();
        taskRunnerThread.join();

        connection.close();
    }

    @Test
    public void testSendReceive() throws Exception {
        TestTask.startedCounter.set(0);

        Set<TaskState> tasks = createTasks(5, SCHEDULED);
        tasks.forEach(storage::newState);
        sendTasksToWorkQueue(tasks);
        waitForStatus(storage, tasks, COMPLETED);

        assertEquals(5, TestTask.startedCounter.get());
    }

    @Test
    public void testSendDuplicate() throws Exception {
        TestTask.startedCounter.set(0);

        Set<TaskState> tasks = createTasks(5, SCHEDULED);
        tasks.forEach(storage::newState);
        sendTasksToWorkQueue(tasks);
        sendTasksToWorkQueue(tasks);

        waitForStatus(storage, tasks, COMPLETED);
        assertEquals(5, TestTask.startedCounter.get());
    }

    private void sendTasksToWorkQueue(Set<TaskState> tasks) {
        tasks.forEach(t -> producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, t.getId(), TaskState.serialize(t))));
        producer.flush();
    }
}
