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

import ai.grakn.engine.util.GraknEngineConfig;
import ai.grakn.util.REST;
import com.google.common.collect.Maps;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 * <p>
 *     Endpoints used to retrieve all the info about the current instance of Grakn.
 * </p>
 *
 * @author Marco Scoppetta
 */
@Path("/status")
@Api(value = "/status", description = "Endpoints used to retrieve all the info about the current instance of Grakn")
@Produces({"application/json"})
public class StatusController {

    public StatusController(Service spark) {
        spark.get(REST.WebPath.GET_STATUS_CONFIG_URI, this::getStatus);
    }

    @GET
    @Path("/config")
    @ApiOperation(
            value = "Return config file as a JSONObject.")
    private String getStatus(Request req, Response res) {

        // Turn the properties into a Json object
        Json config = Json.make(GraknEngineConfig.getInstance().getProperties());

        // Remove the JWT Secret
        config.delAt(GraknEngineConfig.JWT_SECRET_PROPERTY);

        return config.toString();
    }
}
