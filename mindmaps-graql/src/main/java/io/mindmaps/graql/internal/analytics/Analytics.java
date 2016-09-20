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

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsComputer;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;

/**
 * OLAP computations that can be applied to a Mindmaps Graph. The current implementation uses the SparkGraphComputer
 * with a Hadoop graph that connects directly to cassandra and de-serialises vertices.
 */

public class Analytics {

    public final String keySpace;
    // TODO: allow user specified resources
    public static final String degree = "degree";

    /**
     * The concept type ids that define which instances appear in the subgraph.
     */
    private final Set<String> allTypes = new HashSet<>();
    private final Map<String, String> resourceTypes = new HashMap<>();

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on all instances in the graph.
     */
    public Analytics(String keySpace) {
        this.keySpace = keySpace;

        MindmapsGraph graph = MindmapsClient.getGraph(this.keySpace);

        // collect resource-types for statistics
        graph.getMetaResourceType().instances()
                .forEach(type ->
                        resourceTypes.put(type.getId(), type.asResourceType().getDataType().getName()));

        // collect meta-types to exclude them as they do not have instances
        Set<Concept> excludedTypes = new HashSet<>();
        excludedTypes.add(graph.getMetaType());
        excludedTypes.add(graph.getMetaEntityType());
        excludedTypes.add(graph.getMetaRelationType());
        excludedTypes.add(graph.getMetaResourceType());
        excludedTypes.add(graph.getMetaRoleType());
        excludedTypes.add(graph.getMetaRuleType());

        // collect role-types to exclude them because the user does not see castings
        excludedTypes.addAll(graph.getMetaRoleType().instances());
        excludedTypes.addAll(graph.getMetaRuleType().instances());

        // collect analytics resource types to exclude
        HashSet<String> analyticsElements = Sets.newHashSet(Analytics.degree, GraqlType.HAS_RESOURCE.getId(Analytics.degree));
        analyticsElements.stream()
                .filter(element -> graph.getType(element) != null)
                .map(graph::getType)
                .forEach(excludedTypes::add);

        // fetch all types
        graph.getMetaType().instances().stream()
                .filter(concept -> !excludedTypes.contains(concept))
                .map(Concept::asType)
//                .collect(Collectors.toList())
                .forEach(type -> allTypes.add(type.getId()));

        graph.rollback();

        // add analytics ontology - hard coded for now
        insertOntology(degree, ResourceType.DataType.LONG);
    }

    /**
     * Create a graph computer from a Mindmaps Graph. The computer operates on the instances of the types provided in
     * the <code>types</code> argument. All subtypes of the given types are included when deciding whether to include an
     * instance.
     *
     * @param types the set of types the computer will use to filter instances
     */
    public Analytics(String keySpace, Set<Type> types) {
        this.keySpace = keySpace;
        MindmapsGraph graph = MindmapsClient.getGraph(this.keySpace);

        // collect resource-types for statistics
        graph.getMetaResourceType().instances()
                .forEach(type ->
                        resourceTypes.put(type.getId(), type.asResourceType().getDataType().getName()));

        // use ako relations to add subtypes of the provided types
        for (Type t : types) {
            t.subTypes().forEach(subtype -> allTypes.add(subtype.getId()));
        }

        // add analytics ontology - hard coded for now
        insertOntology(degree, ResourceType.DataType.LONG);
    }

    /**
     * Count the number of instances in the graph.
     *
     * @return the number of instances
     */
    public long count() {
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new CountMapReduce(allTypes));
        Map<String, Long> count = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return count.getOrDefault(CountMapReduce.MEMORY_KEY, 0L);
    }

    /**
     * Minimum value of the selected resource-type.
     *
     * @return min
     */
    public Optional<Number> min() {
        checkNumberOfTypes();
        String type = allTypes.iterator().next();
        checkResourceType(type);

        if (!hasInstance(type)) return Optional.empty();

        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new MinMapReduce(allTypes, resourceTypes));
        Map<String, Number> min = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(min.get(MinMapReduce.MEMORY_KEY));
    }

    /**
     * Maximum value of the selected resource-type.
     *
     * @return max
     */
    public Optional<Number> max() {
        checkNumberOfTypes();
        String type = allTypes.iterator().next();
        checkResourceType(type);

        if (!hasInstance(type)) return Optional.empty();

        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new MaxMapReduce(allTypes, resourceTypes));
        Map<String, Number> max = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(max.get(MaxMapReduce.MEMORY_KEY));
    }

    /**
     * Sum of values of the selected resource-type.
     *
     * @return sum
     */
    public Optional<Number> sum() {
        checkNumberOfTypes();
        String type = allTypes.iterator().next();
        checkResourceType(type);

        if (!hasInstance(type)) return Optional.empty();

        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new SumMapReduce(allTypes, resourceTypes));
        Map<String, Number> max = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        return Optional.of(max.get(SumMapReduce.MEMORY_KEY));
    }

    /**
     * Compute the mean of instances of the selected resource-type.
     *
     * @return mean
     */
    public Optional<Double> mean() {
        checkNumberOfTypes();
        String type = allTypes.iterator().next();
        checkResourceType(type);

        if (!hasInstance(type)) return Optional.empty();

        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new MeanMapReduce(allTypes, resourceTypes));
        Map<String, Map<String, Number>> mean = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Number> meanPair = mean.get(MeanMapReduce.MEMORY_KEY);
        return Optional.of(meanPair.get(MeanMapReduce.SUM).doubleValue() /
                meanPair.get(MeanMapReduce.COUNT).longValue());
    }

    /**
     * Compute the standard deviation of instances of the selected resource-type.
     *
     * @return standard deviation
     */
    public Optional<Double> std() {
        checkNumberOfTypes();
        String type = allTypes.iterator().next();
        checkResourceType(type);

        if (!hasInstance(type)) return Optional.empty();

        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new StdMapReduce(allTypes, resourceTypes));
        Map<String, Map<String, Number>> std = result.memory().get(MindmapsMapReduce.MAP_REDUCE_MEMORY_KEY);
        Map<String, Number> stdTuple = std.get(StdMapReduce.MEMORY_KEY);
        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM).doubleValue();
        double sum = stdTuple.get(StdMapReduce.SUM).doubleValue();
        long count = stdTuple.get(StdMapReduce.COUNT).longValue();
        return Optional.of(Math.sqrt(squareSum / count - (sum / count) * (sum / count)));
    }

    /**
     * Compute the number of relations that each instance takes part in.
     *
     * @return a map from each instance to its degree
     */
    public Map<Instance, Long> degrees() {
        Map<Instance, Long> allDegrees = new HashMap<>();
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        ComputerResult result = computer.compute(new DegreeVertexProgram(allTypes));
        MindmapsGraph graph = MindmapsClient.getGraph(keySpace);
        result.graph().traversal().V().forEachRemaining(v -> {
            if (v.keys().contains(DegreeVertexProgram.MEMORY_KEY)) {
                Instance instance = graph.getInstance(v.value(ITEM_IDENTIFIER.name()));
                allDegrees.put(instance, v.value(DegreeVertexProgram.MEMORY_KEY));
            }
        });
        return allDegrees;
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource attached to the relevant instance.
     *
     * @param resourceType the type of the resource that will contain the degree
     */
    private void degreesAndPersist(String resourceType) {
        MindmapsComputer computer = MindmapsClient.getGraphComputer(keySpace);
        computer.compute(new DegreeAndPersistVertexProgram(keySpace, allTypes));
    }

    /**
     * Compute the number of relations that each instance takes part in and persist this information in the graph. The
     * degree is stored as a resource of type "degree" attached to the relevant instance.
     */
    public void degreesAndPersist() throws ExecutionException, InterruptedException {
        degreesAndPersist(degree);
    }

    /**
     * Add the analytics elements to the ontology of the graph specified in <code>keySpace</code>. The ontology elements
     * are related to the resource type <code>resourceTypeId</code> used to persist data computed by analytics.
     *
     * @param resourceTypeId    the ID of a resource type used to persist information
     * @param resourceDataType  the datatype of the resource type
     */
    private void insertOntology(String resourceTypeId, ResourceType.DataType resourceDataType) {
        MindmapsGraph graph = MindmapsClient.getGraph(keySpace);
        ResourceType resource = graph.putResourceType(resourceTypeId, resourceDataType);
        RoleType degreeOwner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        RoleType degreeValue = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId))
                .hasRole(degreeOwner)
                .hasRole(degreeValue);

        for (String type : allTypes) {
            graph.getType(type).playsRole(degreeOwner);
        }
        resource.playsRole(degreeValue);

        try {
            graph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.ONTOLOGY_MUTATION.getMessage(e.getMessage()), e);
        }

    }

    private void checkResourceType(String type) {
        if (!resourceTypes.containsKey(type))
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
        if (!resourceTypes.get(type).equals(ResourceType.DataType.LONG.getName()) &&
                !resourceTypes.get(type).equals(ResourceType.DataType.DOUBLE.getName()))
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
    }

    private boolean hasInstance(String type) {
        MindmapsGraph graph = MindmapsClient.getGraph(this.keySpace);
        return withGraph(graph).match(var("x").isa(type)).ask().execute();
    }

    private void checkNumberOfTypes() {
        if (allTypes.size() != 1)
            throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                    .getMessage(this.getClass().toString()));
    }
}
