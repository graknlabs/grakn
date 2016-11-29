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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ai.grakn.engine.controller.Utilities.getContenttype;
import static ai.grakn.engine.controller.Utilities.getKeyspace;
import static java.util.stream.Collectors.toList;
import static spark.Spark.get;
import static java.lang.Boolean.parseBoolean;
import static ai.grakn.factory.GraphFactory.getInstance;
import static ai.grakn.engine.visualiser.HALConceptRepresentationBuilder.renderHALArrayData;
import static ai.grakn.engine.visualiser.HALConceptRepresentationBuilder.renderHALConceptData;
import static ai.grakn.engine.visualiser.HALConceptRepresentationBuilder.renderHALConceptOntology;

import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.QUERY_FIELD;
import static ai.grakn.util.REST.Request.HAL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.GRAQL_CONTENTTYPE;
import static ai.grakn.util.REST.Response.ENTITIES_JSON_FIELD;
import static ai.grakn.util.REST.Response.RELATIONS_JSON_FIELD;
import static ai.grakn.util.REST.Response.RESOURCES_JSON_FIELD;
import static ai.grakn.util.REST.Response.ROLES_JSON_FIELD;
import static ai.grakn.engine.util.ConfigProperties.HAL_DEGREE_PROPERTY;

@Path("/graph")
@Api(value = "/graph", description = "Endpoints used to query the graph by ID or Graql match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class VisualiserController {

    private final static ConfigProperties properties = ConfigProperties.getInstance();

    private final static int separationDegree = properties.getPropertyAsInt(HAL_DEGREE_PROPERTY);
    private final static String SHORTEST_PATH_QUERY = "path";
    private final static String COMPUTE_RESPONSE_TYPE = "type";
    private final static String COMPUTE_RESPONSE_FIELD = "response";

    //TODO: implement a pagination system.
    public VisualiserController() {
        get(REST.WebPath.CONCEPT_BY_ID_URI + ID_PARAMETER, this::conceptById);
        get(REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI + ID_PARAMETER, this::conceptByIdOntology);
        get(REST.WebPath.GRAPH_ONTOLOGY_URI, this::ontology);
        get(REST.WebPath.GRAPH_MATCH_QUERY_URI, this::match);
        get(REST.WebPath.GRAPH_ANALYTICS_QUERY_URI, this::compute);
        get(REST.WebPath.GRAPH_PRE_MATERIALISE_QUERY_URI, this::preMaterialiseAll);
    }

    @GET
    @Path("/concept/:uuid")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String conceptById(Request req, Response res) {
        String keyspace = getKeyspace(req);

        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
            Concept concept = graph.getConcept(req.params(ID_PARAMETER));

            return renderHALConceptData(concept, separationDegree);
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/concept/ontology/:uuid")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String conceptByIdOntology(Request req, Response res) {
        String keyspace = getKeyspace(req);

        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
            Concept concept = graph.getConcept(req.params(ID_PARAMETER));
            return renderHALConceptOntology(concept);
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/ontology")
    @ApiOperation(
            value = "Produces a JSONObject containing meta-ontology types instances.",
            notes = "The built JSONObject will contain ontology nodes divided in roles, entities, relations and resources.",
            response = JSONObject.class)
    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    private String ontology(Request req, Response res) {
        String keyspace = getKeyspace(req);

        try(GraknGraph graph = getInstance().getGraph(keyspace)){
            JSONObject responseObj = new JSONObject();
            responseObj.put(ROLES_JSON_FIELD, instances(graph.getMetaRoleType()));
            responseObj.put(ENTITIES_JSON_FIELD, instances(graph.getMetaEntityType()));
            responseObj.put(RELATIONS_JSON_FIELD, instances(graph.getMetaRelationType()));
            responseObj.put(RESOURCES_JSON_FIELD, instances(graph.getMetaResourceType()));
            return responseObj.toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/match")
    @ApiOperation(
            value = "Executes match query on the server and build a representation for each concept in the query result. " +
                    "Return type is determined by the content type. Either application/graql or application/json/hal")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reasoner", value = "Boolean used to decide whether run reasoner together with the current query.", required = true, dataType = "sting/boolean", paramType = "query")
})
    private String match(Request req, Response res) {
        String keyspace = getKeyspace(req);
        boolean useReasoner = parseBoolean(req.queryParams("reasoner"));

        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
            MatchQuery matchQuery = graph.graql().parse(req.queryParams(QUERY_FIELD));
            matchQuery = useReasoner ? new Reasoner(graph).resolveToQuery(matchQuery, true) : matchQuery;

            switch (getContenttype(req)){
                case HAL_CONTENTTYPE:
                    return formatAsHAL(matchQuery);
                case GRAQL_CONTENTTYPE:
                    return formatAsGraql(matchQuery);
                default: return formatAsGraql(matchQuery);
            }
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/analytics")
    @ApiOperation(
            value = "Executes compute query on the server and build HAL representation of result or returns string containing statistics.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Compute query to execute", required = true, dataType = "string", paramType = "query")
    })
    private String compute(Request req, Response res) {
        try (GraknGraph graph = getInstance().getGraph(getKeyspace(req))) {

            ComputeQuery computeQuery = graph.graql().parse(req.queryParams(QUERY_FIELD));
            JSONObject response = new JSONObject();

            if (req.queryParams(QUERY_FIELD).contains(SHORTEST_PATH_QUERY)) {
                response.put(COMPUTE_RESPONSE_TYPE, "HAL");
                JSONArray array = new JSONArray();
                ((List<Concept>)computeQuery.execute()).iterator().forEachRemaining(concept ->
                        array.put(renderHALConceptData(concept, 0)));
                response.put(COMPUTE_RESPONSE_FIELD,array);
            } else {
                response.put(COMPUTE_RESPONSE_TYPE, "string");
                response.put(COMPUTE_RESPONSE_FIELD, formatAsGraql(computeQuery));
            }
            return response.toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/preMaterialiseAll")
    @ApiOperation(value = "Pre materialise all the rules on the graph.")
    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    private String preMaterialiseAll(Request req, Response res) {
        try (GraknGraph graph = getInstance().getGraph(getKeyspace(req))) {
            new Reasoner(graph).precomputeInferences();
            return "Done.";
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    /**
     * Format a match query as HAL
     * @param query query to format
     * @return HAL representation
     */
    private String formatAsHAL(MatchQuery query){
        Collection<Map<String, Concept>> results = query.stream().collect(toList());
        return renderHALArrayData(query, results).toString();
    }

    /**
     * Format a match query results as Graql
     * @param query query to format
     * @return Graql representation
     */
    private String formatAsGraql(Query query){
        return ((Query<String>) query).resultsString(Printers.graql())
                .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                .collect(Collectors.joining("\n"));
    }

    /**
     * Return all of the instances of the given type
     * @param type type to find instances of
     * @return JSONArray with IDs of instances
     */
    private JSONArray instances(Type type){
        return new JSONArray(type.instances().stream().map(Concept::getId).toArray());
    }
}
