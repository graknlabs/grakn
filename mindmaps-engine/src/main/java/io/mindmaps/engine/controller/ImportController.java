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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.engine.postprocessing.BackgroundTasks;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.mindmaps.graql.Graql.parseInsert;
import static io.mindmaps.graql.Graql.parsePatterns;
import static spark.Spark.before;
import static spark.Spark.halt;
import static spark.Spark.post;


@Api(value = "/import", description = "Endpoints to import data and ontologies from Graqlfiles to a graph.")
@Path("/import")
@Produces("text/plain")

public class ImportController {

    private final Logger LOG = LoggerFactory.getLogger(ImportController.class);
    private ScheduledExecutorService checkLoadingExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture printingState;

    //TODO: Use redis for caching LRU
    private Map<String, String> entitiesMap;
    //TODO: add relations to relationsList when they are referring to relations that have not been inserted yet
    private ArrayList<Var> relationsList;

    private AtomicLong processedEntities = new AtomicLong();
    private AtomicLong processedRelations = new AtomicLong();
    private AtomicBoolean loadingInProgress = new AtomicBoolean(false);
    private long totalPatterns;
    private long independentPatterns;

    private String defaultGraphName;

    public ImportController() {

        before((req,res)->{
            if(loadingInProgress.get())
                halt(423, "Another loading process is still running.\n");
        });
        post(REST.WebPath.IMPORT_DATA_URI, this::importDataREST);
        post(REST.WebPath.IMPORT_ONTOLOGY_URI, this::importOntologyREST);
        post(REST.WebPath.IMPORT_DISTRIBUTED_URI, this::importDataRESTDistributed);

        entitiesMap = new ConcurrentHashMap<>();
        relationsList = new ArrayList<>();
        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    }

    @POST
    @Path("/distribute/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading and distributed the batches to remote hosts.",
            notes = "This is a separate import from ontology, since a batch loading is performed to optimise the loading speed. ")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body"),
            @ApiImplicitParam(name = "hosts", value = "Collection of hosts' addresses.", required = true, dataType = "string", paramType = "body")
    })

    private String importDataRESTDistributed(Request req, Response res) {
        loadingInProgress.set(true);
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            final String pathToFile = bodyObject.get(REST.Request.PATH_FIELD).toString();
            final String graphName = (bodyObject.has(REST.Request.GRAPH_NAME_PARAM)) ? bodyObject.get(REST.Request.GRAPH_NAME_PARAM).toString() : defaultGraphName;
            final Collection<String> hosts = new HashSet<>();
            bodyObject.getJSONArray("hosts").forEach(x -> hosts.add(((String) x)));

            if (!(new File(pathToFile)).exists())
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));

            Executors.newSingleThreadExecutor().submit(() -> importDataFromFile(pathToFile, new DistributedLoader(graphName, hosts)));

        } catch (JSONException j) {
            LOG.error("Malformed request.", j);
            res.status(400);
            loadingInProgress.set(false);
            return j.getMessage();
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
            loadingInProgress.set(false);
            res.status(400);
            return e.getMessage();
        } catch (Exception e) {
            LOG.error("Exception", e);
            res.status(500);
            return e.getMessage();
        }

        return "Distributed loading successfully STARTED. \n";
    }


    @POST
    @Path("/batch/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading.",
            notes = "This is a separate import from ontology, since a batch loading is performed to optimise the loading speed. ")
    @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body")

    private String importDataREST(Request req, Response res) {
        loadingInProgress.set(true);
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            final String pathToFile = bodyObject.get(REST.Request.PATH_FIELD).toString();
            final String graphName = (bodyObject.has(REST.Request.GRAPH_NAME_PARAM)) ? bodyObject.get(REST.Request.GRAPH_NAME_PARAM).toString() : defaultGraphName;

            if (!(new File(pathToFile)).exists())
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));

            initialiseLoading(pathToFile);

            Executors.newSingleThreadExecutor().submit(() -> importDataFromFile(pathToFile, new BlockingLoader(graphName)));

        } catch (JSONException j) {
            LOG.error("Malformed request.", j);
            loadingInProgress.set(false);
            res.status(400);
            return j.getMessage();
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
            loadingInProgress.set(false);
            res.status(400);
            return e.getMessage();
        } catch (Exception e) {
            LOG.error("Exception", e);
            res.status(500);
            return e.getMessage();
        }

        return "Total patterns found [" + totalPatterns + "]. \n" +
                " -[" + independentPatterns + "] entities \n" +
                " -[" + (totalPatterns - independentPatterns) + "] relations/resources \n" +
                "Loading successfully STARTED. \n";
    }


    @POST
    @Path("/ontology")
    @ApiOperation(
            value = "Import ontology from a Graql file. It does not perform any batching.",
            notes = "This is a separate import from data, since a batch loading is not performed in this case. The ontology must be loaded in one single transaction. ")
    @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body")

    private String importOntologyREST(Request req, Response res) {
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            String pathToFile = bodyObject.get(REST.Request.PATH_FIELD).toString();
            String graphName;
            if (bodyObject.has(REST.Request.GRAPH_NAME_PARAM))
                graphName = bodyObject.get(REST.Request.GRAPH_NAME_PARAM).toString();
            else
                graphName = defaultGraphName;
            importOntologyFromFile(pathToFile, graphName);
        } catch (JSONException j) {
            LOG.error("Malformed request.", j);
            res.status(400);
            return j.getMessage();
        } catch (Exception e) {
            LOG.error("Exception while loading ontology.", e);
            res.status(500);
            return e.getMessage();
        }
        return "Ontology successfully loaded. \n";
    }

    private void initialiseLoading(String pathToFile) throws FileNotFoundException {
        totalPatterns = parsePatterns(new FileInputStream(pathToFile))
                .count();

        independentPatterns = parsePatterns(new FileInputStream(pathToFile))
                .filter(pattern -> isIndependentEntity(pattern.admin().asVar()))
                .count();

        printingState=checkLoadingExecutor.scheduleAtFixedRate(this::checkLoadingStatus, 10, 10, TimeUnit.SECONDS);

        processedEntities.set(0);
        processedRelations.set(0);
    }

    private void checkLoadingStatus() {
        LOG.info("===== Import from file in progress ====");
        LOG.info("Processed Entities: " + processedEntities + "/" + independentPatterns);
        LOG.info("Processed Relations: " + processedRelations + "/" + (totalPatterns - independentPatterns));
        LOG.info("=======================================");
    }

    private boolean isIndependentEntity(Var var) {
        return (!var.admin().isRelation() && var.admin().getType().isPresent());
    }

    private void importDataFromFile(String dataFile, Loader loaderParam) {
        LOG.info("Data loading started.");
        try {
            parsePatterns(new FileInputStream(dataFile)).forEach(pattern -> consumeEntity(pattern.admin().asVar(), loaderParam));
            loaderParam.waitToFinish();
            parsePatterns(new FileInputStream(dataFile)).forEach(pattern -> consumeRelationAndResource(pattern.admin().asVar(), loaderParam));
            loaderParam.waitToFinish();
            printingState.cancel(true);
            processedEntities.set(0);
            processedRelations.set(0);
            loadingInProgress.set(false);
            BackgroundTasks.getInstance().forcePostprocessing();
        } catch (Exception e) {
            LOG.error("Exception while batch loading data.", e);
            loadingInProgress.set(false);
        }
    }

    private void consumeEntity(Var var, Loader loader) {
        if (!entitiesMap.containsKey(var.admin().getName()) && isIndependentEntity(var)) {
            if (var.admin().isUserDefinedName()) {
                // Some variable might not have an explicit ID defined, in that case we generate explicitly one and we save it into our cache
                // so that we can refer to it.
                String varId = (var.admin().getId().isPresent()) ? var.admin().getId().get() : UUID.randomUUID().toString();
                entitiesMap.put(var.admin().getName(), varId);
                // We force the ID of the current var to be the one computed by this controller.
                loader.addToQueue(var.admin().id(varId));
            } else {
                loader.addToQueue(var);
            }
            processedEntities.incrementAndGet();
        }
    }

    private void consumeRelationAndResource(Var var, Loader loader) {
        boolean ready = false;

        if (var.admin().isRelation()) {
            ready = true;
            //If one of the role players is defined using a variable name and the variable name is not in our cache we cannot insert the relation.
            for (VarAdmin.Casting x : var.admin().getCastings()) {
                //If one of the role players is referring to a variable we check to have that var in the entities map cache.
                if (x.getRolePlayer().admin().isUserDefinedName()) {
                    if (entitiesMap.containsKey(x.getRolePlayer().getName()))
                        x.getRolePlayer().id(entitiesMap.get(x.getRolePlayer().getName()));
                    else ready = false;
                }
            }
        } else {
            // if it is not a relation and the isa is not specified it is probably a resource referring to an existing entity.
            if (!var.admin().getType().isPresent()) {
                ready = true;
            }
        }


        if (ready) {
            processedRelations.incrementAndGet();
            loader.addToQueue(var);
        }

    }

    private void importOntologyFromFile(String ontologyFile, String graphName) throws IOException, MindmapsValidationException {

        MindmapsGraph graph = GraphFactory.getInstance().getGraphBatchLoading(graphName);

        LOG.info("Loading new ontology .. ");

        List<String> lines = Files.readAllLines(Paths.get(ontologyFile), StandardCharsets.UTF_8);
        String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
        parseInsert(query).withGraph(graph).execute();
        graph.commit();

        LOG.info("Ontology loaded. ");

    }
}
