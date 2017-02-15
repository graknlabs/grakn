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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateZookeeperStore;
import mjson.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;

import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;

/**
 * Class to manage tasks distributed using Kafka.
 *
 * @author Denis Lobanov
 * This class begins the TaskRunner instance that will be running on this machine.
 */
public final class DistributedTaskManager implements TaskManager {
    private final KafkaProducer<String, String> producer;

    private final SchedulerElector elector;
    private final ZookeeperConnection connection;
    private final TaskRunner taskRunner;
    private final TaskStateStorage stateStorage;

    private static final String TASKRUNNER_THREAD_NAME = "taskrunner-";
    private Thread taskRunnerThread;

    public DistributedTaskManager() {
        connection = new ZookeeperConnection();
        stateStorage = new TaskStateZookeeperStore(connection);

        // run the TaskRunner in a thread
        taskRunner = new TaskRunner(stateStorage, connection);
        taskRunnerThread = new Thread(taskRunner, TASKRUNNER_THREAD_NAME + taskRunner.hashCode());
        taskRunnerThread.start();

        // Elect the scheduler or add yourself to the scheduler pool
        elector = new SchedulerElector(stateStorage, connection);

        this.producer = ConfigHelper.kafkaProducer();
    }

    @Override
    public void close() {
        noThrow(producer::close, "Error shutting down producer in TaskManager");

        noThrow(elector::stop, "Error stopping Scheduler elector from TaskManager");
        noThrow(taskRunner::close, "Error shutting down TaskRunner");
        noThrow(taskRunnerThread::join, "Error waiting for TaskRunner to close");

        // stop zookeeper connection
        noThrow(connection::close, "Error waiting for zookeeper connection to close");
    }

    @Override
    public String createTask(String taskClassName, String createdBy, Instant runAt, long period, Json configuration) {
        Boolean recurring = period > 0;

        TaskState taskState = new TaskState(taskClassName)
                .creator(createdBy)
                .runAt(runAt)
                .isRecurring(recurring)
                .interval(period)
                .configuration(configuration);

        producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskState.getId(), TaskState.serialize(taskState)));
        producer.flush();

        return taskState.getId();
    }

    @Override
    public TaskManager stopTask(String id, String requesterName) {
        throw new UnsupportedOperationException(this.getClass().getName() + " currently doesn't support stopping tasks");
    }

    @Override
    public TaskStateStorage storage() {
        return stateStorage;
    }
}
