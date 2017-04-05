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

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 *
 * @author Filipe Teixeira
 */
public class CommitLogController {
    private final ConceptCache cache = EngineCacheProvider.getCache();
    private final Logger LOG = LoggerFactory.getLogger(CommitLogController.class);

    public CommitLogController(Service spark){
        spark.post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
        spark.delete(REST.WebPath.COMMIT_LOG_URI, this::deleteConcepts);
    }


    @GET
    @Path("/commit_log")
    @ApiOperation(value = "Delete all the post processing jobs for a specific keyspace")
    @ApiImplicitParam(name = "keysoace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path")
    private String deleteConcepts(Request req, Response res){
        String graphName = req.queryParams(REST.Request.KEYSPACE_PARAM);

        if(graphName == null){
            res.status(400);
           return ErrorMessage.NO_PARAMETER_PROVIDED.getMessage(REST.Request.KEYSPACE_PARAM, "delete");
        }

        cache.clearAllJobs(graphName);

        return "The cache of Graph [" + graphName + "] has been cleared";
    }


    @GET
    @Path("/commit_log")
    @ApiOperation(value = "Submits post processing jobs for a specific keyspace")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "keyspace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = "concepts", value = "A Json Array of IDs representing concepts to be post processed", required = true, dataType = "string", paramType = "body")
    })
    private String submitConcepts(Request req, Response res) {
        try {
            String graphName = req.queryParams(REST.Request.KEYSPACE_PARAM);

            if (graphName == null) {
                graphName = GraknEngineConfig.getInstance().getProperty(GraknEngineConfig.DEFAULT_KEYSPACE_PROPERTY);
            }
            LOG.info("Commit log received for graph [" + graphName + "]");

            JSONArray jsonArray = (JSONArray) new JSONObject(req.body()).get("concepts");

            for (Object object : jsonArray) {
                JSONObject jsonObject = (JSONObject) object;

                String conceptVertexId = jsonObject.getString(REST.Request.COMMIT_LOG_ID);
                String conceptIndex = jsonObject.getString(REST.Request.COMMIT_LOG_INDEX);
                Schema.BaseType type = Schema.BaseType.valueOf(jsonObject.getString(REST.Request.COMMIT_LOG_TYPE));

                switch (type) {
                    case CASTING:
                        cache.addJobCasting(graphName, conceptIndex, ConceptId.of(conceptVertexId));
                        break;
                    case RESOURCE:
                        cache.addJobResource(graphName, conceptIndex, ConceptId.of(conceptVertexId));
                        break;
                    default:
                        LOG.warn(ErrorMessage.CONCEPT_POSTPROCESSING.getMessage(conceptVertexId, type.name()));
                }
            }

            return "Graph [" + graphName + "] now has [" + cache.getNumJobs(graphName) + "] post processing jobs";
        } catch(Exception e){
            throw new GraknEngineServerException(500,e);
        }
    }
}
