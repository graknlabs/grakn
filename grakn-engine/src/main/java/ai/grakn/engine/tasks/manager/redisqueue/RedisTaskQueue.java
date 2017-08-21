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


import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerPool;
import net.greghaines.jesque.worker.WorkerPoolImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;


/**
 * Queue implemented in redis
 *
 * @author Domenico Corapi
 */
class RedisTaskQueue {

    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueue.class);

    private static final String QUEUE_NAME = "grakn_engine_queue";
    private static final String SUBSCRIPTION_CLASS_NAME = Task.class.getName();

    // Jesque configuration class for how jobs are mapped
    private static final MapBasedJobFactory JOB_FACTORY = new MapBasedJobFactory(
            map(entry(
                    // Assign elements with this class
                    SUBSCRIPTION_CLASS_NAME,
                    // To be run by this class
                    RedisTaskQueueConsumer.class)));
    public static final int GAUGE_CACHING_INTERVAL = 15;

    private final Client redisClient;
    private final Config config;
    private final Meter failures;
    private final int processingDelay;
    private final Timer timer;
    private Pool<Jedis> jedisPool;
    private LockProvider lockProvider;
    private final MetricRegistry metricRegistry;
    private final Meter putJobMeter;
    private WorkerPool workerPool;

    @SuppressFBWarnings(value = "SIC_INNER_SHOULD_BE_STATIC_ANON", justification = "No harm in inner classes for gauges here.")
    RedisTaskQueue(
            Pool<Jedis> jedisPool,
            LockProvider lockProvider,
            MetricRegistry metricRegistry,
            int processingDelay) {
        this.jedisPool = jedisPool;
        this.lockProvider = lockProvider;
        this.metricRegistry = metricRegistry;
        this.config = new ConfigBuilder().build();
        this.redisClient = new ClientPoolImpl(config, jedisPool);
        this.processingDelay = processingDelay;
        metricRegistry.register(MetricRegistry.name(RedisTaskQueue.class, "job-queue", "size"),
                new CachedGauge<Long>(GAUGE_CACHING_INTERVAL, TimeUnit.SECONDS) {
                    @Override
                    public Long loadValue() {
                        try (Jedis resource = jedisPool.getResource()) {
                            return resource.llen(String.format("resque:queue:%s", QUEUE_NAME));
                        }
                    }
                });
        metricRegistry.register(MetricRegistry.name(RedisTaskQueue.class, "redis-keys", "size"),
                new CachedGauge<Long>(GAUGE_CACHING_INTERVAL, TimeUnit.SECONDS) {
                    @Override
                    public Long loadValue() {
                        try (Jedis resource = jedisPool.getResource()) {
                            return (long) resource.keys("*").size();
                        }
                    }
                });
        this.putJobMeter = metricRegistry.meter(name(RedisTaskQueue.class, "put-job"));
        this.failures = metricRegistry.meter(name(RedisTaskQueue.class, "failures"));
        this.timer = new Timer();
    }

    void close() throws InterruptedException {
        timer.cancel();
        synchronized(this) {
            if (workerPool != null) {
                workerPool.endAndJoin(true, 5000);
            }
        }
        redisClient.end();
    }

    void putJob(Task job) {
        putJobMeter.mark();
        LOG.debug("Enqueuing job {}", job.getTaskState().getId());
        final Job queueJob = new Job(SUBSCRIPTION_CLASS_NAME, job);
        redisClient.enqueue(QUEUE_NAME, queueJob);
    }

    void runInFlightProcessor() {
        timer.scheduleAtFixedRate(new RedisInflightTaskConsumer(jedisPool, Duration.ofSeconds(
                processingDelay), config, QUEUE_NAME, metricRegistry), new Date(), 2000);
    }

    void subscribe(
            RedisTaskManager redisTaskManager,
            EngineID engineId,
            GraknEngineConfig engineConfig,
            EngineGraknTxFactory factory,
            int poolSize) {
        LOG.info("Subscribing worker to jobs in queue {}", QUEUE_NAME);
        // sync to avoid close while starting
        synchronized(this) {
            this.workerPool = new WorkerPool(() -> getWorker(redisTaskManager, engineId, engineConfig, factory), poolSize);
            // This just starts poolSize threads
            workerPool.run();
        }
    }

    private Worker getWorker(RedisTaskManager redisTaskManager, EngineID engineId,
            GraknEngineConfig engineConfig, EngineGraknTxFactory factory) {
        Worker worker = new WorkerPoolImpl(config, Collections.singletonList(QUEUE_NAME), JOB_FACTORY, jedisPool);
        // We need this since the job can only be instantiated with the
        // task coming from the queue
        worker.getWorkerEventEmitter().addListener(
                (event, worker1, queue, job, runner, result, t) -> {
                    if (runner instanceof RedisTaskQueueConsumer) {
                        ((RedisTaskQueueConsumer) runner)
                                .setRunningState(redisTaskManager, engineId, engineConfig, jedisPool,
                                        factory, lockProvider, metricRegistry);
                    } else {
                        LOG.error("Found unexpected job in queue of type {}", runner.getClass().getName());
                    }
                }, WorkerEvent.JOB_EXECUTE);
        worker.setExceptionHandler((jobExecutor, exception, curQueue) -> {
            // TODO review this strategy
            failures.mark();
            LOG.error("Exception while trying to run task, terminating!", exception);
            return RecoveryStrategy.TERMINATE;
        });
        return worker;
    }
}