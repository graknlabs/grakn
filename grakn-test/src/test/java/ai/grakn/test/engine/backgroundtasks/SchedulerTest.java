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

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Predicate;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Each test needs to be run with a clean Kafka to pass
 */
public class SchedulerTest {
    private GraknStateStorage stateStorage = new GraknStateStorage();
    private static ClusterManager clusterManager;
    private final SynchronizedStateStorage zkStorage = clusterManager.getStorage();

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @BeforeClass
    public static void setupCluster(){
        clusterManager = engine.getClusterManager();
    }

    @Before
    public void setup() throws Exception {
        ((Logger) org.slf4j.LoggerFactory.getLogger(KafkaLogger.class)).setLevel(Level.DEBUG);
    }

    @Test
    public void testInstantaneousOneTimeTasks() throws Exception {
        Map<String, TaskState> tasks = createTasks(5);
        sendTasksToNewTasksQueue(tasks);
        waitUntilScheduled(tasks.keySet());
    }

    @Test
    public void testRecurringTasksStarted() throws Exception {
        // persist a recurring task in system graph
        Pair<String, TaskState> task = createTask(0, CREATED, true, 3000);
        String taskId = task.getKey();

        // force scheduler restart
        clusterManager.getScheduler().close();

        // sleep a bit and stop scheduler
        waitUntilScheduled(taskId);

        // check recurring task in work queue
        Map<String, String> recurringTasks = getMessagesInWorkQueue();
        assertTrue(recurringTasks.containsKey(taskId));
    }

    @Test
    public void testRecurringTasksThatAreStoppedNotStarted() throws Exception{
        // persist a recurring task in system graph
        Pair<String, TaskState> task1 = createTask(0, CREATED, true, 3000);
        String taskId1 = task1.getKey();
        TaskState state1 = task1.getValue();

        // persist a recurring task in system graph
        Pair<String, TaskState> task2 = createTask(1, STOPPED, true, 3000);
        String taskId2 = task2.getKey();
        TaskState state2 = task2.getValue();

        System.out.println(taskId1 + "   " + state1);
        System.out.println(taskId2 + "   " + state2);

        // force scheduler to stop
        clusterManager.getScheduler().close();

        waitUntilScheduled(taskId1);

        // check CREATED task in work queue and not stopped
        Map<String, String> recurringTasks = getMessagesInWorkQueue();
        assertTrue(recurringTasks.containsKey(taskId1));
        assertTrue(!recurringTasks.containsKey(taskId2));
    }

    private Map<String, TaskState> createTasks(int n) throws Exception {
        Map<String, TaskState> tasks = new HashMap<>();

        for(int i=0; i < n; i++) {
            Pair<String, TaskState> task = createTask(i, CREATED, false, 0);
            tasks.put(task.getKey(), task.getValue());

            System.out.println("task " + i + " created");
        }

        return tasks;
    }

    private Pair<String, TaskState> createTask(int i, TaskStatus status, boolean recurring, int interval) throws Exception {
        String taskId = stateStorage.newState(
                TestTask.class.getName(),
                SchedulerTest.class.getName(),
                Instant.now(), recurring, interval, new JSONObject(singletonMap("name", "task"+i)));

        stateStorage.updateState(taskId, status, null, null, null, null, null);

        TaskState state = stateStorage.getState(taskId);

        zkStorage.newState(taskId, status, null, null);

        assertNotNull(taskId);
        assertNotNull(state);

        return new Pair<>(taskId, state);
    }

    private void sendTasksToNewTasksQueue(Map<String, TaskState> tasks) {
        KafkaProducer<String, String> producer = ConfigHelper.kafkaProducer();

        for(String taskId:tasks.keySet()){
            producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskId, tasks.get(taskId).configuration().toString()));
        }

        producer.flush();
        producer.close();
    }

    private Map<String, String> getMessagesInWorkQueue() {
        // Create a consumer in a new group to read all messages
        Properties properties = Utilities.testConsumer();
        properties.put("group.id", "workQueue");
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(WORK_QUEUE_TOPIC));

        ConsumerRecords<String, String> records = consumer.poll(1000);
        Map<String, String> recordsInMap = new HashMap<>();
        for(ConsumerRecord<String, String> record:records) {
            recordsInMap.put(record.key(), record.value());
        }

        System.out.println("test received total count: " + records.count());

        consumer.close();
        return recordsInMap;
    }

    private void waitUntilScheduled(Collection<String> tasks) {
        tasks.forEach(this::waitUntilScheduled);
    }

    private void waitUntilScheduled(String taskId) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 60000) {
            TaskStatus status = zkStorage.getState(taskId).status();
            System.out.println(taskId + "  -->>  " + status);
            if(status == SCHEDULED || status == RUNNING || status == COMPLETED) {
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private void waitForScheduler(ClusterManager clusterManager, Predicate<Scheduler> fn) throws Exception {
        int runs = 0;

        while (!fn.test(clusterManager.getScheduler()) && runs < 50 ) {
            Thread.sleep(100);
            runs++;
        }

        System.out.println("wait done, runs " + Integer.toString(runs) + " scheduler " + clusterManager.getScheduler());
    }
}
