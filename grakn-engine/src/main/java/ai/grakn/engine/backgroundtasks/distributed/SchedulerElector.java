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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.SCHEDULER;

/**
 * There is one "Scheduler" that will be constantly running on the "Leader" machine.
 *
 * This class registers this instance of Engine with Zookeeper & the Leader election process.
 *
 * If this machine is required to take over the Scheduler, the "takeLeadership" function will be called.
 * If this machine controls the Scheduler and leadership is relinquished/lost, the "stateChanged" function is called
 * and the Scheduler is stopped before a error is thrown.
 *
 * The Scheduler thread is a daemon thread
 *
 * @author Denis Lobanov, alexandraorth
 */
public class SchedulerElector extends LeaderSelectorListenerAdapter {
    private static final String SCHEDULER_THREAD_NAME = "scheduler-";

    private final LeaderSelector leaderSelector;
    private final TaskStateStorage storage;

    private Scheduler scheduler;
    private TreeCache cache;
    private TaskFailover failover;

    public SchedulerElector(TaskStateStorage storage, ZookeeperConnection zookeeperConnection) {
        this.storage = storage;

        leaderSelector = new LeaderSelector(zookeeperConnection.connection(), SCHEDULER, this);
        leaderSelector.autoRequeue();

        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        try {
            leaderSelector.start();
            while (!leaderSelector.getLeader().isLeader()) {
                Thread.sleep(1000);
            }
        } catch (Exception e){
            throw new RuntimeException("There were errors electing a leader- Engine should stop");
        }
    }

    /**
     * When stopping the ClusterManager we must:
     *  1. Interrupt the Scheduler on whichever machine it is running on. We do this by interrupting the leadership.
     *      When leadership is interrupted it will shut down the Scheduler on that machine.
     *
     *  2. Shutdown the TaskRunner on this machine.
     *
     *  3. Shutdown Zookeeper storage connection on this machine
     */
    public void stop(){
        leaderSelector.interruptLeadership();
        leaderSelector.close();

        scheduler.close();
        failover.close();
        cache.close();
    }

    /**
     * On leadership takeover, start a new Scheduler instance and wait for it to complete.
     * The method should not return until leadership is relinquished.
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        registerFailover(client);

        // start the scheduler
        scheduler = new Scheduler(storage);

        Thread schedulerThread = new Thread(scheduler, SCHEDULER_THREAD_NAME + scheduler.hashCode());
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        // wait for scheduler to fail
        schedulerThread.join();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    private void registerFailover(CuratorFramework client) throws Exception {
        cache = new TreeCache(client, RUNNERS_WATCH);
        failover = new TaskFailover(client, cache, storage);
        cache.getListenable().addListener(failover);
        cache.start();
    }
}
