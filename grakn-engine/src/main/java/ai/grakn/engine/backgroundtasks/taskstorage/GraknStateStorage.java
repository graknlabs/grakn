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

package ai.grakn.engine.backgroundtasks.taskstorage;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.util.Schema;
import javafx.util.Pair;
import org.json.JSONObject;

import java.util.*;
import java.util.function.Function;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.util.SystemOntologyElements.*;
import static ai.grakn.graql.Graql.var;
import static java.lang.Thread.sleep;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

public class GraknStateStorage implements StateStorage {
    private final static String TASK_VAR = "task";
    private final static int retries = 10;

    private final KafkaLogger LOG = KafkaLogger.getInstance();

    public GraknStateStorage() {}

    public String newState(String taskName, String createdBy, Date runAt, Boolean recurring, long interval, JSONObject configuration) {
        if(taskName == null || createdBy == null || runAt == null || recurring == null)
            return null;

        Var state = var(TASK_VAR).isa(SCHEDULED_TASK)
                                 .has(STATUS, CREATED.toString())
                                 .has(TASK_CLASS_NAME, taskName)
                                 .has(CREATED_BY, createdBy)
                                 .has(RUN_AT, runAt.getTime())
                                 .has(RECURRING, recurring)
                                 .has(RECUR_INTERVAL, interval);

        if(configuration != null)
            state.has(TASK_CONFIGURATION, configuration.toString());

        Optional<String> result = attemptCommitToSystemGraph((graph) -> {
            InsertQuery query = graph.graql().insert(state);
            ConceptId id = query.stream().findFirst().get().get(TASK_VAR).getId();

            LOG.debug("Created " + graph.getConcept(id));

            return id.getValue();
        }, true);

        return result.map(x -> x).orElse(null);
    }

    public Boolean updateState(String id, TaskStatus status, String statusChangeBy, String engineID,
                               Throwable failure, String checkpoint, JSONObject configuration) {
        if(id == null)
            return false;

        if(status == null && statusChangeBy == null && engineID == null && failure == null
                && checkpoint == null && configuration == null)
            return false;

        // Existing resource relations to remove
        final Set<String> resourcesToDettach = new HashSet<>();
        
        // New resources to add
        Var resources = var(TASK_VAR).id(ConceptId.of(id));

        if(status != null) {
        	resourcesToDettach.add(STATUS);
        	resourcesToDettach.add(STATUS_CHANGE_TIME);
            resources.has(STATUS, status.toString())
                     .has(STATUS_CHANGE_TIME, new Date().getTime());
        }
        if(statusChangeBy != null) {
            resourcesToDettach.add(STATUS_CHANGE_BY);            
            resources.has(STATUS_CHANGE_BY, statusChangeBy);
        }
        if(engineID != null) {
        	resourcesToDettach.add(ENGINE_ID);        	
            resources.has(ENGINE_ID, engineID);
        }
        if(failure != null) {
            resourcesToDettach.add(TASK_EXCEPTION);
            resourcesToDettach.add(STACK_TRACE);            
            resources.has(TASK_EXCEPTION, failure.toString());
            if(failure.getStackTrace().length > 0)
                 resources.has(STACK_TRACE, Arrays.toString(failure.getStackTrace()));
        }
        if(checkpoint != null) {
        	resourcesToDettach.add(TASK_CHECKPOINT);        	
            resources.has(TASK_CHECKPOINT, checkpoint);
        }
        if(configuration != null) {
            resourcesToDettach.add(TASK_CONFIGURATION);            
            resources.has(TASK_CONFIGURATION, configuration.toString());
        }

        Optional<Boolean> result = attemptCommitToSystemGraph((graph) -> {
            LOG.debug("dettaching: " + resourcesToDettach);
            LOG.debug("inserting " + resources);
            final Entity task = graph.getConcept(ConceptId.of(id));
            // Remove relations to any resources we want to currently update
            resourcesToDettach.forEach(typeName -> {
                RoleType roleType = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(typeName));
                if (roleType == null)
                	System.err.println("NO ROLE TYPE FOR RESOURCE " + typeName);
            	task.relations(roleType).forEach(Concept::delete);            
            });
            // Insert new resources with new values
            graph.graql().insert(resources).execute();
            return true;
        }, true);

        return result.isPresent();
    }

    public TaskState getState(String id) {
        if(id == null)
            return null;

        Optional<TaskState> result = attemptCommitToSystemGraph((graph) -> {
            Instance instance = graph.getConcept(ConceptId.of(id));
            return instanceToState(graph, instance);
        }, false);

        return result.get();
    }

    private TaskState instanceToState(GraknGraph graph, Instance instance){
        Resource<?> name = instance.resources(graph.getResourceType(TASK_CLASS_NAME)).stream().findFirst().orElse(null);
        if (name == null) {
            LOG.error("Could not get 'task-class-name' for " + instance.getId());
            return null;
        }

        TaskState state = new TaskState(name.getValue().toString());

        List<Map<String, Concept>> resources = graph.graql()
                .match(var().rel(var().id(instance.getId())).rel(var("r").isa(var().sub("resource"))))
                .select("r")
                .execute();

        resources.forEach(x -> x.values().forEach(y -> {
            Resource<?> r = y.asResource();
            buildState(state, r.type().getName(), r.getValue());
        }));

        return state;
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset) {
        return getTasks(taskStatus, taskClassName, createdBy, limit, offset, false);
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset, Boolean recurring) {
        Var matchVar = var(TASK_VAR).isa(SCHEDULED_TASK);

        if(taskStatus != null)
            matchVar.has(STATUS, taskStatus.toString());
        if(taskClassName != null)
            matchVar.has(TASK_CLASS_NAME, taskClassName);
        if(createdBy != null)
            matchVar.has(CREATED_BY, createdBy);
        if(recurring != null)
            matchVar.has(RECURRING, recurring);

        Optional<Set<Pair<String, TaskState>>> result = attemptCommitToSystemGraph((graph) -> {
            MatchQuery q = graph.graql().match(matchVar);

            if (limit > 0)
                q.limit(limit);
            if (offset > 0)
                q.offset(offset);

            List<Map<String, Concept>> res = q.execute();

            // Create Set of pairs with IDs &
            Set<Pair<String, TaskState>> out = new HashSet<>();
            for (Map<String, Concept> m : res) {
                Concept c = m.values().stream().findFirst().orElse(null);
                if (c != null) {
                    String id = c.getId().getValue();
                    out.add(new Pair<>(id, instanceToState(graph, c.asInstance())));
                }
            }

            return out;
        }, false);

        return result.isPresent() ? result.get() : new HashSet<>();
    }

    /*
    Internal
     */
    private TaskState buildState(TaskState state, String resourceName, Object resourceValue) {
        switch (resourceName) {
            case STATUS:
                state.status(TaskStatus.valueOf(resourceValue.toString()));
                break;
            case STATUS_CHANGE_TIME:
                state.statusChangeTime(new Date((Long)resourceValue));
                break;
            case STATUS_CHANGE_BY:
                state.statusChangedBy(resourceValue.toString());
                break;
            case TASK_CLASS_NAME:
                // Set when instantiating TaskState, ignore it now.
                break;
            case CREATED_BY:
                state.creator(resourceValue.toString());
                break;
            case ENGINE_ID:
                state.engineID(resourceValue.toString());
                break;
            case RUN_AT:
                state.runAt(new Date((Long)resourceValue));
                break;
            case RECURRING:
                state.isRecurring((Boolean)resourceValue);
                break;
            case RECUR_INTERVAL:
                state.interval((Long)resourceValue);
                break;
            case TASK_EXCEPTION:
                state.exception(resourceValue.toString());
                break;
            case STACK_TRACE:
                state.stackTrace(resourceValue.toString());
                break;
            case TASK_CHECKPOINT:
                state.checkpoint(resourceValue.toString());
                break;
            case TASK_CONFIGURATION:
                state.configuration(new JSONObject(resourceValue.toString()));
                break;
            default:
                LOG.error("Unknown resource type when deserialising TaskState: "+resourceName);
                break;
        }

        return state;
    }

    private synchronized <T> Optional<T> attemptCommitToSystemGraph(Function<GraknGraph, T> function, boolean commit){
        double sleepFor = 100;
        for (int i = 0; i < retries; i++) {

            LOG.debug("Attempting "  + (commit ? "commit" : "query") + " on system graph @ t"+Thread.currentThread().getId());
            long time = System.currentTimeMillis();

            try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
                T result = function.apply(graph);
                if (commit) {
                    graph.commit();
                }

                return Optional.of(result);
            } 
            catch (GraknBackendException e) {
            	// retry...
            }            
            catch (Throwable e) {
            	e.printStackTrace(System.err);
                LOG.error("Failed to validate the graph when updating the state " + getFullStackTrace(e));
                break;
            } 
            finally {
                LOG.debug("Took " + (System.currentTimeMillis() - time) + " to " + (commit ? "commit" : "query") + " to system graph @ t" + Thread.currentThread().getId());
            }

            // Sleep
            try {
                sleep((long)sleepFor);
            }
            catch (InterruptedException e) {
                LOG.error(getFullStackTrace(e));
            }
            finally {
                sleepFor = ((1d/2d) * (Math.pow(2d,i) - 1d));
            }
        }

        return Optional.empty();
    }
}
