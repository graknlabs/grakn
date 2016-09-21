/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.controller;

import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.REST;
import io.mindmaps.util.Schema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.Collections;
import java.util.Set;

import static spark.Spark.delete;
import static spark.Spark.post;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 */
public class CommitLogController {
    private final Cache cache;
    private final Logger LOG = LoggerFactory.getLogger(CommitLogController.class);

    public CommitLogController(){
        cache = Cache.getInstance();
        post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
        delete(REST.WebPath.COMMIT_LOG_URI, this::deleteConcepts);
    }

    /**
     *
     * @param req The request which contains the graph to be post processed
     * @param res The current response code
     * @return The result of clearing the post processing for a single graph
     */
    private String deleteConcepts(Request req, Response res){
        String graphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);

        if(graphName == null){
            res.status(400);
           return ErrorMessage.NO_PARAMETER_PROVIDED.getMessage(REST.Request.GRAPH_NAME_PARAM, "delete");
        }

        cache.getCastingJobs().computeIfPresent(graphName, (key, set) -> {set.clear(); return set;});
        cache.getResourceJobs().computeIfPresent(graphName, (key, set) -> {set.clear(); return set;});

        return "The cache of Graph [" + graphName + "] has been cleared";
    }

    /**
     *
     * @param req The request which contains the graph to be post processed
     * @param res The current response code
     * @return The result of adding something for post processing
     */
    private String submitConcepts(Request req, Response res) {
        try {
            String graphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);

            if (graphName == null) {
                graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
            }
            LOG.info("Commit log received for graph [" + graphName + "]");

            JSONArray jsonArray = (JSONArray) new JSONObject(req.body()).get("concepts");

            for (Object object : jsonArray) {
                JSONObject jsonObject = (JSONObject) object;
                String conceptId = jsonObject.getString("id");
                Schema.BaseType type = Schema.BaseType.valueOf(jsonObject.getString("type"));

                switch (type) {
                    case CASTING:
                        cache.addJobCasting(graphName, Collections.singleton(conceptId));
                        break;
                    case RESOURCE:
                        cache.addJobResource(graphName, Collections.singleton(conceptId));
                    default:
                        LOG.warn(ErrorMessage.CONCEPT_POSTPROCESSING.getMessage(conceptId, type.name()));
                }
            }

            long numJobs = getJobCount(cache.getCastingJobs().get(graphName));
            numJobs += getJobCount(cache.getResourceJobs().get(graphName));

            return "Graph [" + graphName + "] now has [" + numJobs + "] post processing jobs";
        } catch(Exception e){
            LOG.error("Exception when submitting commit log", e);
            res.status(500);
            return e.getMessage();
        }
    }
    private long getJobCount(Set jobs){
        if(jobs != null)
            return jobs.size();
        return 0L;
    }
}
