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

package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.Duration;
import java.util.Optional;

import static ai.grakn.engine.GraknEngineConfig.DEFAULT_KEYSPACE_PROPERTY;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 *
 * @author Filipe Teixeira
 */
//TODO Implement delete
public class CommitLogController {
    private final Logger LOG = LoggerFactory.getLogger(CommitLogController.class);

    private final TaskManager manager;

    public CommitLogController(Service spark, TaskManager manager){
        this.manager = manager;

        spark.post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
        spark.delete(REST.WebPath.COMMIT_LOG_URI, this::deleteConcepts);
    }


    @DELETE
    @Path("/commit_log")
    @ApiOperation(value = "Delete all the post processing jobs for a specific keyspace")
    @ApiImplicitParam(name = "keyspace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path")
    private String deleteConcepts(Request req, Response res){
        return "Delete not implemented";
    }


    @GET
    @Path("/commit_log")
    @ApiOperation(value = "Submits post processing jobs for a specific keyspace")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "keyspace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path"),
        @ApiImplicitParam(name = COMMIT_LOG_FIXING, value = "A Json Array of IDs representing concepts to be post processed", required = true, dataType = "string", paramType = "body"),
        @ApiImplicitParam(name = COMMIT_LOG_COUNTING, value = "A Json Array types with new and removed instances", required = true, dataType = "string", paramType = "body")
    })
    private String submitConcepts(Request req, Response res) {
        String keyspace = Optional.ofNullable(req.queryParams(KEYSPACE_PARAM))
                .orElse(GraknEngineConfig.getInstance().getProperty(DEFAULT_KEYSPACE_PROPERTY));

        LOG.info("Commit log received for graph [" + keyspace + "]");

        // Instances to post process
        Json postProcessingConfiguration = Json.object();
        postProcessingConfiguration.set(KEYSPACE, keyspace);
        postProcessingConfiguration.set(COMMIT_LOG_FIXING, Json.read(req.body()).at(COMMIT_LOG_FIXING));

        // TODO Make interval configurable
        TaskState postProcessingTask = TaskState.of(
                PostProcessingTask.class, this.getClass().getName(),
                TaskSchedule.recurring(Duration.ofMinutes(5)));

        //Instances to count
        Json countingConfiguration = Json.object();
        countingConfiguration.set(KEYSPACE, keyspace);
        countingConfiguration.set(COMMIT_LOG_COUNTING, Json.read(req.body()).at(COMMIT_LOG_COUNTING));

        TaskState countingTask = TaskState.of(
                UpdatingInstanceCountTask.class, this.getClass().getName(), TaskSchedule.now());


        //TODO: Get rid of this update
        PostProcessingTask.lastPPTaskCreated.set(System.currentTimeMillis());

        // Send two tasks to the pipeline
        manager.addLowPriorityTask(postProcessingTask, TaskConfiguration.of(postProcessingConfiguration));
        manager.addHighPriorityTask(countingTask, TaskConfiguration.of(countingConfiguration));

        return "PP Task [ " + postProcessingTask.getId().getValue() + " ] and Counting task [" + countingTask.getId().getValue() + "] created for graph [" + keyspace + "]";
    }
}
