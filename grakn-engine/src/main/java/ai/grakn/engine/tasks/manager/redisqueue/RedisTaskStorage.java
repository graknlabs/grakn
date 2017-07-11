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

package ai.grakn.engine.tasks.manager.redisqueue;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknBackendException;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.util.Base64;
import java.util.Set;
import static java.util.stream.Collectors.toSet;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.SerializationUtils;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;


/**
 * DAO for redis task states
 *
 * @author Domenico Corapi
 */
public class RedisTaskStorage implements TaskStateStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTaskStorage.class);
    private final Timer updateTimer;
    private final Timer getTimer;

    private Pool<Jedis> redis;

    private RedisTaskStorage(Pool<Jedis> redis, MetricRegistry metricRegistry) {
        this.redis = redis;
        this.updateTimer = metricRegistry.timer(name(RedisTaskStorage.class, "update"));
        this.getTimer = metricRegistry.timer(name(RedisTaskStorage.class, "get"));
    }

    public static RedisTaskStorage create(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        return new RedisTaskStorage(jedisPool, metricRegistry);
    }

    @Override
    public TaskId newState(TaskState state) throws GraknBackendException {
        updateState(state);
        return state.getId();
    }

    @Override
    public Boolean updateState(TaskState state) {
        try(Jedis jedis = redis.getResource(); Context ignore = updateTimer.time()){
            // TODO find a better way to represent the state
            String value = state.getId().getValue();
            LOG.debug("Updating state {}", value);
            String status = jedis.set(value, new String(Base64.getEncoder().encode(SerializationUtils.serialize(state)),
                    Charsets.UTF_8));
            return status.equalsIgnoreCase("OK");
        }
    }

    @Override
    @Nullable
    public TaskState getState(TaskId id) throws GraknBackendException {
        try(Jedis jedis = redis.getResource(); Context ignore = getTimer.time()){
            String value = jedis.get(id.getValue());
            if (value != null) {
                return (TaskState) deserialize(Base64.getDecoder().decode(value));
            } else {
                LOG.info("Requested state {} was not found", id.getValue());
                // TODO Don't use exceptions for an expected return like this
                throw GraknBackendException.stateStorageMissingId(id);
            }
        }
    }

    @Override
    public boolean containsTask(TaskId id) {
        try(Jedis jedis = redis.getResource()){
            String value = jedis.get(id.getValue());
            return value != null;
        }
    }

    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
            EngineID runningOnEngine, int limit, int offset) {
        try (Jedis jedis = redis.getResource()) {
            // TODO change structure of task keys with a prefix. Can we do better than filtering?
            Stream<TaskState> stream = jedis.keys("*-*-*-*").stream().map(value -> (TaskState) deserialize(Base64.getDecoder().decode(value)));
            if (taskStatus != null) {
                stream = stream.filter(t -> t.status().equals(taskStatus));
            }
            if (taskClassName != null) {
                stream = stream.filter(t -> t.taskClass().getName().equals(taskClassName));
            }
            if (createdBy != null) {
                stream = stream.filter(t -> t.creator().equals(createdBy));
            }
            if (runningOnEngine != null) {
                stream = stream
                        .filter(t -> t.engineID() != null && t.engineID().equals(runningOnEngine));
            }
            stream = stream.skip(offset);
            if (limit > 0) {
                stream = stream.limit(limit);
            }
            return stream.collect(toSet());
        } catch (Exception e) {
            throw GraknBackendException.stateStorageTaskRetrievalFailure(e);
        }
    }

    boolean isTaskMarkedStopped(TaskId id) {
        try {
            TaskState state = getState(id);
            return state != null && state.getStatus().equals(TaskStatus.STOPPED);
        } catch (GraknBackendException e) {
            return false;
        }
    }
}
