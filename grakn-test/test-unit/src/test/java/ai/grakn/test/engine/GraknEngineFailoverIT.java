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

package ai.grakn.test.engine;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskStorage;
import ai.grakn.engine.tasks.mock.FailingMockTask;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.test.DistributionContext;
import ai.grakn.test.engine.tasks.BackgroundTaskTestUtils;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.configuration;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineFailoverIT {

    private final static Logger LOG = LoggerFactory.getLogger(GraknEngineFailoverIT.class);

    private static TaskStateStorage storage;
    private static JedisPool jedisPool;

    @ClassRule
    public static final DistributionContext engine1 = DistributionContext.startSingleQueueEngineProcess().port(4567);

    @ClassRule
    public static final DistributionContext engine2 = DistributionContext.startSingleQueueEngineProcess().port(5678);

    @ClassRule
    public static final DistributionContext engine3 = DistributionContext.startSingleQueueEngineProcess().port(6789);

    @BeforeClass
    public static void getStorage() {
        jedisPool = new JedisPool();
        storage = RedisTaskStorage.create(jedisPool, new MetricRegistry());
    }

    @AfterClass
    public static void closeStorage() {
        jedisPool.close();
    }

    @Property(trials=10)
    public void whenSubmittingTasksToOneEngine_TheyComplete(List<TaskState> tasks1) throws Exception {
        // Create & Send tasks to rest api
        Set<TaskId> tasks = sendTasks(engine1.port(), tasks1);

        // Wait for those tasks to complete
        waitForStatus(tasks, COMPLETED, FAILED);

        // Assert the tasks have finished with the correct status depending on type
        assertTasksCompletedWithCorrectStatus(tasks);
    }


    @Property(trials=10)
    public void whenSubmittingTasksToTwoEngines_TheyComplete(
            List<TaskState> tasks1, List<TaskState> tasks2) throws Exception {
        // Create & Send tasks to rest api
        Set<TaskId> taskIds1 = sendTasks(engine1.port(), tasks1);
        Set<TaskId> taskIds2 = sendTasks(engine2.port(), tasks2);

        Set<TaskId> allTasks = new HashSet<>();
        allTasks.addAll(taskIds1);
        allTasks.addAll(taskIds2);

        // Wait for those tasks to complete
        waitForStatus(allTasks, COMPLETED, FAILED);

        // Assert the tasks have finished with the correct status depending on type
        assertTasksCompletedWithCorrectStatus(allTasks);
    }

    @Property(trials=1)
    // This fails occasionally when there's a lingering engine from some
    // previous run that is not connected to redis
    // It also leaves a redis instance running
    public void whenSubmittingTasksToOneEngineAndRandomlyKillingTheOthers_TheyComplete(
            @Size(min=10000, max=20000) List<TaskState> tasks) throws Exception {

        Set<TaskId> taskIds = sendTasks(engine1.port(), tasks);

        // Giving some time, the subscriptions to Redis are started asynchronously
        Thread.sleep(3000);
        // Randomly restart one of the other engines until all of the tasks are done
        Random random = new Random();
        List<DistributionContext> enginesToKill = ImmutableList.of(engine2, engine3);
        do {
            DistributionContext engineToKill = enginesToKill.get(random.nextInt(2));
            engineToKill.restart();
            Thread.sleep(5000);
        } while (!taskIds.stream().allMatch(GraknEngineFailoverIT::isDone));

        waitForStatus(taskIds, COMPLETED, FAILED);

        assertTasksCompletedWithCorrectStatus(taskIds);
    }

    private void assertTasksCompletedWithCorrectStatus(Set<TaskId> tasks) {
        tasks.stream().map(storage::getState).forEach(t -> {
            if(t.taskClass().equals(FailingMockTask.class)){
                assertThat(t.status(), equalTo(FAILED));
            } else {
                assertThat(t.status(), equalTo(COMPLETED));
            }
        });
    }


    private Set<TaskId> sendTasks(int port, List<TaskState> tasks) {
        TaskClient engineClient = TaskClient.of("localhost", port);

        return tasks.stream().map(t -> engineClient.sendTask(
                t.taskClass(),
                t.creator(),
                t.schedule().runAt(),
                t.schedule().interval().orElse(null),
                configuration(t).json())).collect(toSet());
    }

    private static void waitForStatus(Set<TaskId> taskIds, TaskStatus... status) {
        Set<TaskStatus> statusSet = Sets.newHashSet(status);
        taskIds.forEach(t -> BackgroundTaskTestUtils.waitForStatus(storage, t, statusSet));
    }

    private static boolean isDone(TaskId taskId){
        try {
            TaskStatus status = storage.getState(taskId).status();
            if (status == FAILED || status == COMPLETED || status == STOPPED) {
                return true;
            } else {
                LOG.info("Task {} not ready", taskId);
                return false;
            }
        } catch (GraknBackendException e){
            return false;
        }
    }
}