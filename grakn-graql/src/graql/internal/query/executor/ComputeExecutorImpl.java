/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.query.executor;

import ai.grakn.ComputeExecutor;
import ai.grakn.GraknComputer;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.ConceptList;
import ai.grakn.graql.answer.ConceptSet;
import ai.grakn.graql.answer.ConceptSetMeasure;
import ai.grakn.graql.answer.Value;
import ai.grakn.graql.internal.analytics.ClusterMemberMapReduce;
import ai.grakn.graql.internal.analytics.ConnectedComponentVertexProgram;
import ai.grakn.graql.internal.analytics.ConnectedComponentsVertexProgram;
import ai.grakn.graql.internal.analytics.CorenessVertexProgram;
import ai.grakn.graql.internal.analytics.CountMapReduceWithAttribute;
import ai.grakn.graql.internal.analytics.CountVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.graql.internal.analytics.KCoreVertexProgram;
import ai.grakn.graql.internal.analytics.MaxMapReduce;
import ai.grakn.graql.internal.analytics.MeanMapReduce;
import ai.grakn.graql.internal.analytics.MedianVertexProgram;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.graql.internal.analytics.NoResultException;
import ai.grakn.graql.internal.analytics.ShortestPathVertexProgram;
import ai.grakn.graql.internal.analytics.StatisticsMapReduce;
import ai.grakn.graql.internal.analytics.StdMapReduce;
import ai.grakn.graql.internal.analytics.SumMapReduce;
import ai.grakn.graql.internal.analytics.Utility;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.GraqlSyntax;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.DEGREE;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MAX;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEDIAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MIN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.PATH;
import static ai.grakn.util.GraqlSyntax.Compute.Method.STD;
import static ai.grakn.util.GraqlSyntax.Compute.Method.SUM;

/**
 * A Graql Compute query job executed against a {@link GraknComputer}.
 *
 * @author Grakn Warriors
 */
class ComputeExecutorImpl<T extends Answer> implements ComputeExecutor<T> {

    private final ComputeQuery<T> query;

    private static final Logger LOG = LoggerFactory.getLogger(ComputeExecutorImpl.class);
    private final EmbeddedGraknTx<?> tx;

    public ComputeExecutorImpl(EmbeddedGraknTx<?> tx, ComputeQuery query) {
        this.tx = tx;
        this.query = query;
    }

    @Override
    public void kill() { //todo: to be removed;
        tx.session().getGraphComputer().killJobs();
    }

    @Override
    public Stream<T> stream() {
        GraqlSyntax.Compute.Method<?> method = query.method();
        if (method.equals(MIN) || method.equals(MAX) || method.equals(MEDIAN) || method.equals(SUM)) {
            return (Stream<T>) runComputeMinMaxMedianOrSum();
        } else if (method.equals(MEAN)) {
            return (Stream<T>) runComputeMean();
        } else if (method.equals(STD)) {
            return (Stream<T>) runComputeStd();
        } else if (method.equals(COUNT)) {
            return (Stream<T>) runComputeCount();
        } else if (method.equals(PATH)) {
            return (Stream<T>) runComputePath();
        } else if (method.equals(CENTRALITY)) {
            return (Stream<T>) runComputeCentrality();
        } else if (method.equals(CLUSTER)) {
            return (Stream<T>) runComputeCluster();
        }

        throw GraqlQueryException.invalidComputeQuery_invalidMethod();
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope,
                                        Boolean includesRolePlayerEdges) {

        return tx.session().getGraphComputer().compute(program, mapReduce, scope, includesRolePlayerEdges);
    }

    public final ComputerResult compute(@Nullable VertexProgram<?> program,
                                        @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                                        @Nullable Set<LabelId> scope) {

        return tx.session().getGraphComputer().compute(program, mapReduce, scope);
    }

    /**
     * The Graql compute min, max, median, or sum query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Value> runComputeMinMaxMedianOrSum() {
        Number number = runComputeStatistics();
        if (number == null) return Stream.empty();
        else return Stream.of(new Value(number));
    }

    /**
     * The Graql compute mean query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Value> runComputeMean() {
        Map<String, Double> meanPair = runComputeStatistics();
        if (meanPair == null) return Stream.empty();

        Double mean = meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT);
        return Stream.of(new Value(mean));
    }

    /**
     * The Graql compute std query run method
     *
     * @return a Answer object containing a Number that represents the answer
     */
    private Stream<Value> runComputeStd() {
        Map<String, Double> stdTuple = runComputeStatistics();
        if (stdTuple == null)  return Stream.empty();

        double squareSum = stdTuple.get(StdMapReduce.SQUARE_SUM);
        double sum = stdTuple.get(StdMapReduce.SUM);
        double count = stdTuple.get(StdMapReduce.COUNT);
        Double std = Math.sqrt(squareSum / count - (sum / count) * (sum / count));

        return Stream.of(new Value(std));
    }

    /**
     * The compute statistics base algorithm that is called in every compute statistics query
     *
     * @param <S> The return type of {@link StatisticsMapReduce}
     * @return result of compute statistics algorithm, which will be of type S
     */
    @Nullable
    private <S> S runComputeStatistics() {
        AttributeType.DataType<?> targetDataType = validateAndGetTargetDataType();
        if (!targetContainsInstance()) return null;

        Set<LabelId> extendedScopeTypes = convertLabelsToIds(extendedScopeTypeLabels());
        Set<LabelId> targetTypes = convertLabelsToIds(targetTypeLabels());

        VertexProgram program = initStatisticsVertexProgram(query, targetTypes, targetDataType);
        StatisticsMapReduce<?> mapReduce = initStatisticsMapReduce(targetTypes, targetDataType);
        ComputerResult computerResult = compute(program, mapReduce, extendedScopeTypes);

        if (query.method().equals(MEDIAN)) {
            Number result = computerResult.memory().get(MedianVertexProgram.MEDIAN);
            LOG.debug("Median = " + result);
            return (S) result;
        }

        Map<Serializable, S> resultMap = computerResult.memory().get(mapReduce.getClass().getName());
        LOG.debug("Result = " + resultMap.get(MapReduce.NullObject.instance()));
        return resultMap.get(MapReduce.NullObject.instance());
    }

    /**
     * Helper method to validate that the target types are of one data type, and get that data type
     *
     * @return the DataType of the target types
     */
    @Nullable
    private AttributeType.DataType<?> validateAndGetTargetDataType() {
        AttributeType.DataType<?> dataType = null;
        for (Type type : targetTypes()) {
            // check if the selected type is a attribute type
            if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.label());
            AttributeType<?> attributeType = type.asAttributeType();
            if (dataType == null) {
                // check if the attribute type has data-type LONG or DOUBLE
                dataType = attributeType.dataType();
                if (!dataType.equals(AttributeType.DataType.LONG) && !dataType.equals(AttributeType.DataType.DOUBLE)) {
                    throw GraqlQueryException.attributeMustBeANumber(dataType, attributeType.label());
                }
            } else {
                // check if all the attribute types have the same data-type
                if (!dataType.equals(attributeType.dataType())) {
                    throw GraqlQueryException.attributesWithDifferentDataTypes(query.of().get());
                }
            }
        }
        return dataType;
    }

    /**
     * Helper method to intialise the vertex program for compute statistics queries
     *
     * @param query          representing the compute query
     * @param targetTypes    representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of VertexProgram
     */
    private VertexProgram initStatisticsVertexProgram(ComputeQuery query, Set<LabelId> targetTypes, AttributeType.DataType<?> targetDataType) {
        if (query.method().equals(MEDIAN)) return new MedianVertexProgram(targetTypes, targetDataType);
        else return new DegreeStatisticsVertexProgram(targetTypes);
    }

    /**
     * Helper method to initialise the MapReduce algorithm for compute statistics queries
     *
     * @param targetTypes    representing the attribute types in which the statistics computation is targeted for
     * @param targetDataType representing the data type of the target attribute types
     * @return an object which is a subclass of StatisticsMapReduce
     */
    private StatisticsMapReduce<?> initStatisticsMapReduce(Set<LabelId> targetTypes, AttributeType.DataType<?> targetDataType) {
        GraqlSyntax.Compute.Method<?> method = query.method();
        if (method.equals(MIN)) {
            return new MinMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(MAX)) {
            return new MaxMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(MEAN)) {
            return new MeanMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        }else if (method.equals(STD)) {
            return new StdMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        } else if (method.equals(SUM)) {
            return new SumMapReduce(targetTypes, targetDataType, DegreeVertexProgram.DEGREE);
        }

        return null;
    }

    /**
     * Run Graql compute count query
     *
     * @return a Answer object containing the count value
     */
    private Stream<Value> runComputeCount() {
        if (!scopeContainsInstance()) {
            LOG.debug("Count = 0");
            return Stream.of(new Value(0));
        }

        Set<LabelId> typeLabelIds = convertLabelsToIds(scopeTypeLabels());
        Map<Integer, Long> count;

        Set<LabelId> rolePlayerLabelIds = getRolePlayerLabelIds();
        rolePlayerLabelIds.addAll(typeLabelIds);

        ComputerResult result = compute(
                new CountVertexProgram(),
                new CountMapReduceWithAttribute(),
                rolePlayerLabelIds, false);
        count = result.memory().get(CountMapReduceWithAttribute.class.getName());

        long finalCount = count.keySet().stream()
                .filter(id -> typeLabelIds.contains(LabelId.of(id)))
                .mapToLong(count::get).sum();
        if (count.containsKey(GraknMapReduce.RESERVED_TYPE_LABEL_KEY)) {
            finalCount += count.get(GraknMapReduce.RESERVED_TYPE_LABEL_KEY);
        }

        LOG.debug("Count = " + finalCount);
        return Stream.of(new Value(finalCount));
    }

    /**
     * The Graql compute path query run method
     *
     * @return a Answer containing the list of shortest paths
     */
    private Stream<ConceptList> runComputePath() {
        ConceptId fromID = query.from().get();
        ConceptId toID = query.to().get();

        if (!scopeContainsInstances(fromID, toID)) throw GraqlQueryException.instanceDoesNotExist();
        if (fromID.equals(toID)) return Stream.of(new ConceptList(ImmutableList.of(fromID)));

        Set<LabelId> scopedLabelIds = convertLabelsToIds(scopeTypeLabels());

        ComputerResult result = compute(new ShortestPathVertexProgram(fromID, toID), null, scopedLabelIds);

        Multimap<ConceptId, ConceptId> pathsAsEdgeList = HashMultimap.create();
        Map<String, Set<String>> resultFromMemory = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATH);
        resultFromMemory.forEach((id, idSet) -> idSet.forEach(id2 -> {
            pathsAsEdgeList.put(ConceptId.of(id), ConceptId.of(id2));
        }));

        List<List<ConceptId>> paths;
        if (!resultFromMemory.isEmpty()) {
            paths = getComputePathResultList(pathsAsEdgeList, fromID);
            if (scopeIncludesAttributes()) {
                paths = getComputePathResultListIncludingImplicitRelations(paths);
            }
        }
        else {
            paths = Collections.emptyList();
        }

        return paths.stream().map(ConceptList::new);
    }

    /**
     * The Graql compute centrality query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeCentrality() {
        if (query.using().get().equals(DEGREE)) return runComputeDegree();
        if (query.using().get().equals(K_CORE)) return runComputeCoreness();

        throw GraqlQueryException.invalidComputeQuery_invalidMethodAlgorithm(query.method());
    }

    /**
     * The Graql compute centrality using degree query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeDegree() {
        Set<Label> targetTypeLabels;

        // Check if ofType is valid before returning emptyMap
        if (!query.of().isPresent() || query.of().get().isEmpty()) {
            targetTypeLabels = scopeTypeLabels();
        } else {
            targetTypeLabels = query.of().get().stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::label)
                    .collect(Collectors.toSet());
        }

        Set<Label> scopeTypeLabels = Sets.union(scopeTypeLabels(), targetTypeLabels);

        if (!scopeContainsInstance()) {
            return Stream.empty();
        }

        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels);
        Set<LabelId> targetTypeLabelIDs = convertLabelsToIds(targetTypeLabels);

        ComputerResult computerResult = compute(new DegreeVertexProgram(targetTypeLabelIDs),
                                                new DegreeDistributionMapReduce(targetTypeLabelIDs, DegreeVertexProgram.DEGREE),
                                                scopeTypeLabelIDs);

        Map<Long, Set<ConceptId>> centralityMap = computerResult.memory().get(DegreeDistributionMapReduce.class.getName());

        return centralityMap.entrySet().stream()
                .map(centrality -> new ConceptSetMeasure(centrality.getValue(), centrality.getKey()));
    }

    /**
     * The Graql compute centrality using k-core query run method
     *
     * @return a Answer containing the centrality count map
     */
    private Stream<ConceptSetMeasure> runComputeCoreness() {
        long k = query.where().get().minK().get();

        if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

        Set<Label> targetTypeLabels;

        // Check if ofType is valid before returning emptyMap
        if (!query.of().isPresent() || query.of().get().isEmpty()) {
            targetTypeLabels = scopeTypeLabels();
        } else {
            targetTypeLabels = query.of().get().stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        if (type.isRelationshipType()) throw GraqlQueryException.kCoreOnRelationshipType(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::label)
                    .collect(Collectors.toSet());
        }

        Set<Label> scopeTypeLabels = Sets.union(scopeTypeLabels(), targetTypeLabels);

        if (!scopeContainsInstance()) {
            return Stream.empty();
        }

        ComputerResult result;
        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels);
        Set<LabelId> targetTypeLabelIDs = convertLabelsToIds(targetTypeLabels);

        try {
            result = compute(new CorenessVertexProgram(k),
                             new DegreeDistributionMapReduce(targetTypeLabelIDs, CorenessVertexProgram.CORENESS),
                             scopeTypeLabelIDs);
        } catch (NoResultException e) {
            return Stream.empty();
        }

        Map<Long, Set<ConceptId>> centralityMap = result.memory().get(DegreeDistributionMapReduce.class.getName());

        return centralityMap.entrySet().stream()
                .map(centrality -> new ConceptSetMeasure(centrality.getValue(), centrality.getKey()));
    }

    private Stream<ConceptSet> runComputeCluster() {
        if (query.using().get().equals(K_CORE)) return runComputeKCore();
        if (query.using().get().equals(CONNECTED_COMPONENT)) return runComputeConnectedComponent();

        throw GraqlQueryException.invalidComputeQuery_invalidMethodAlgorithm(query.method());
    }


    private Stream<ConceptSet> runComputeConnectedComponent() {
        boolean restrictSize = query.where().get().size().isPresent();

        if (!scopeContainsInstance()) {
            LOG.info("Selected types don't have instances");
            return Stream.empty();
        }

        Set<LabelId> scopeTypeLabelIDs = convertLabelsToIds(scopeTypeLabels());

        GraknVertexProgram<?> vertexProgram;
        if (query.where().get().contains().isPresent()) {
            ConceptId conceptId = query.where().get().contains().get();
            if (!scopeContainsInstances(conceptId)) {
                throw GraqlQueryException.instanceDoesNotExist();
            }
            vertexProgram = new ConnectedComponentVertexProgram(conceptId);
        } else {
            vertexProgram = new ConnectedComponentsVertexProgram();
        }

        GraknMapReduce<?> mapReduce;
        if (restrictSize) mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.where().get().size().get());
        else mapReduce = new ClusterMemberMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);

        Memory memory = compute(vertexProgram, mapReduce, scopeTypeLabelIDs).memory();
        Map<String, Set<ConceptId>> result = memory.get(mapReduce.getClass().getName());
        return result.values().stream().map(ConceptSet::new);

//        TODO: Enable the following compute cluster-size through a separate compute method
//        if (!query.where().get().members().get()) {
//            GraknMapReduce<?> mapReduce;
//            if (restrictSize) {
//                mapreduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL, query.where().get().size().get());
//            } else {
//                mapReduce = new ClusterSizeMapReduce(ConnectedComponentsVertexProgram.CLUSTER_LABEL);
//            }
//            Map<String, Long> result = memory.get(mapReduce.getClass().getName());
//            return result.values().stream().map(Value::new);
//        }
    }

    private Stream<ConceptSet> runComputeKCore() {
        long k = query.where().get().k().get();

        if (k < 2L) throw GraqlQueryException.kValueSmallerThanTwo();

        if (!scopeContainsInstance()) {
            return Stream.empty();
        }

        ComputerResult computerResult;
        Set<LabelId> subLabelIds = convertLabelsToIds(scopeTypeLabels());
        try {
            computerResult = compute(
                    new KCoreVertexProgram(k),
                    new ClusterMemberMapReduce(KCoreVertexProgram.K_CORE_LABEL),
                    subLabelIds);
        } catch (NoResultException e) {
            return Stream.empty();
        }

        Map<String, Set<ConceptId>> result = computerResult.memory().get(ClusterMemberMapReduce.class.getName());
        return result.values().stream().map(ConceptSet::new);
    }

    /**
     * Helper method to get list of all shortest paths
     *
     * @param resultGraph
     * @param fromID
     * @return
     */
    private List<List<ConceptId>> getComputePathResultList(Multimap<ConceptId, ConceptId> resultGraph, ConceptId fromID) {
        List<List<ConceptId>> allPaths = new ArrayList<>();
        List<ConceptId> firstPath = new ArrayList<>();
        firstPath.add(fromID);

        Deque<List<ConceptId>> queue = new ArrayDeque<>();
        queue.addLast(firstPath);
        while (!queue.isEmpty()) {
            List<ConceptId> currentPath = queue.pollFirst();
            if (resultGraph.containsKey(currentPath.get(currentPath.size() - 1))) {
                Collection<ConceptId> successors = resultGraph.get(currentPath.get(currentPath.size() - 1));
                Iterator<ConceptId> iterator = successors.iterator();
                for (int i = 0; i < successors.size() - 1; i++) {
                    List<ConceptId> extendedPath = new ArrayList<>(currentPath);
                    extendedPath.add(iterator.next());
                    queue.addLast(extendedPath);
                }
                currentPath.add(iterator.next());
                queue.addLast(currentPath);
            } else {
                allPaths.add(currentPath);
            }
        }
        return allPaths;
    }

    /**
     * Helper method t get the list of all shortest path, but also including the implicit relations that connect
     * entities and attributes
     *
     * @param allPaths
     * @return
     */
    private List<List<ConceptId>> getComputePathResultListIncludingImplicitRelations(List<List<ConceptId>> allPaths) {
        List<List<ConceptId>> extendedPaths = new ArrayList<>();
        for (List<ConceptId> currentPath : allPaths) {
            boolean hasAttribute = currentPath.stream().anyMatch(conceptID -> tx.getConcept(conceptID).isAttribute());
            if (!hasAttribute) {
                extendedPaths.add(currentPath);
            }
        }

        // If there exist a path without attributes, we don't need to expand any path
        // as paths contain attributes would be longer after implicit relations are added
        int numExtensionAllowed = extendedPaths.isEmpty() ? Integer.MAX_VALUE : 0;
        for (List<ConceptId> currentPath : allPaths) {
            List<ConceptId> extendedPath = new ArrayList<>();
            int numExtension = 0; // record the number of extensions needed for the current path
            for (int j = 0; j < currentPath.size() - 1; j++) {
                extendedPath.add(currentPath.get(j));
                ConceptId resourceRelationId = Utility.getResourceEdgeId(tx, currentPath.get(j), currentPath.get(j + 1));
                if (resourceRelationId != null) {
                    numExtension++;
                    if (numExtension > numExtensionAllowed) break;
                    extendedPath.add(resourceRelationId);
                }
            }
            if (numExtension == numExtensionAllowed) {
                extendedPath.add(currentPath.get(currentPath.size() - 1));
                extendedPaths.add(extendedPath);
            } else if (numExtension < numExtensionAllowed) {
                extendedPath.add(currentPath.get(currentPath.size() - 1));
                extendedPaths.clear(); // longer paths are discarded
                extendedPaths.add(extendedPath);
                // update the minimum number of extensions needed so all the paths have the same length
                numExtensionAllowed = numExtension;
            }
        }
        return extendedPaths;
    }

    /**
     * Helper method to get the label IDs of role players in a relationship
     *
     * @return a set of type label IDs
     */
    private Set<LabelId> getRolePlayerLabelIds() {
        return scopeTypes()
                .filter(Concept::isRelationshipType)
                .map(Concept::asRelationshipType)
                .filter(RelationshipType::isImplicit)
                .flatMap(RelationshipType::roles)
                .flatMap(Role::players)
                .map(type -> tx.convertToId(type.label()))
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

    /**
     * Helper method to get implicit relationship types of attributes
     *
     * @param types
     * @return a set of type Labels
     */
    private static Set<Label> getAttributeImplicitRelationTypeLabes(Set<Type> types) {
        // If the sub graph contains attributes, we may need to add implicit relations to the path
        return types.stream()
                .filter(Concept::isAttributeType)
                .map(attributeType -> Schema.ImplicitType.HAS.getLabel(attributeType.label()))
                .collect(Collectors.toSet());
    }

    /**
     * Helper method to get the types to be included in the query target
     *
     * @return a set of Types
     */
    private ImmutableSet<Type> targetTypes() {
        if (!query.of().isPresent() || query.of().get().isEmpty()) {
            throw GraqlQueryException.statisticsAttributeTypesNotSpecified();
        }

        return query.of().get().stream()
                .map((label) -> {
                    Type type = tx.getSchemaConcept(label);
                    if (type == null) throw GraqlQueryException.labelNotFound(label);
                    if (!type.isAttributeType()) throw GraqlQueryException.mustBeAttributeType(type.label());
                    return type;
                })
                .flatMap(Type::subs)
                .collect(CommonUtil.toImmutableSet());
    }

    /**
     * Helper method to get the labels of the type in the query target
     *
     * @return a set of type Labels
     */
    private Set<Label> targetTypeLabels() {
        return targetTypes().stream()
                .map(SchemaConcept::label)
                .collect(CommonUtil.toImmutableSet());
    }

    /**
     * Helper method to check whether the concept types in the target have any instances
     *
     * @return true if they exist, false if they don't
     */
    private boolean targetContainsInstance() {
        for (Label attributeType : targetTypeLabels()) {
            for (Label type : scopeTypeLabels()) {
                Boolean patternExist = tx.graql().infer(false).match(
                        Graql.var("x").has(attributeType, Graql.var()),
                        Graql.var("x").isa(Graql.label(type))
                ).iterator().hasNext();
                if (patternExist) return true;
            }
        }
        return false;
        //TODO: should use the following ask query when ask query is even lazier
//        List<Pattern> checkResourceTypes = statisticsResourceTypes.stream()
//                .map(type -> var("x").has(type, var())).collect(Collectors.toList());
//        List<Pattern> checkSubtypes = inTypes.stream()
//                .map(type -> var("x").isa(Graql.label(type))).collect(Collectors.toList());
//
//        return tx.get().graql().infer(false)
//                .match(or(checkResourceTypes), or(checkSubtypes)).aggregate(ask()).execute();
    }

    /**
     * Helper method to get all the concept types that should scope of compute query, which includes the implicit types
     * between attributes and entities, if target types were provided. This is used for compute statistics queries.
     *
     * @return a set of type labels
     */
    private Set<Label> extendedScopeTypeLabels() {
        Set<Label> extendedTypeLabels = getAttributeImplicitRelationTypeLabes(targetTypes());
        extendedTypeLabels.addAll(scopeTypeLabels());
        extendedTypeLabels.addAll(query.of().get());
        return extendedTypeLabels;
    }

    /**
     * Helper method to get the types to be included in the query scope
     *
     * @return stream of Concept Types
     */
    private Stream<Type> scopeTypes() {
        // Get all types if query.inTypes() is empty, else get all scoped types of each meta type.
        // Only include attributes and implicit "has-xxx" relationships when user specifically asked for them.
        if (!query.in().isPresent() || query.in().get().isEmpty()) {
            ImmutableSet.Builder<Type> typeBuilder = ImmutableSet.builder();

            if (scopeIncludesAttributes()) {
                tx.admin().getMetaConcept().subs().forEach(typeBuilder::add);
            } else {
                tx.admin().getMetaEntityType().subs().forEach(typeBuilder::add);
                tx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(typeBuilder::add);
            }

            return typeBuilder.build().stream();
        } else {
            Stream<Type> subTypes = query.in().get().stream().map(label -> {
                Type type = tx.getType(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!scopeIncludesAttributes()) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    /**
     * Helper method to get the labels of the type in the query scope
     *
     * @return a set of Concept Type Labels
     */
    private ImmutableSet<Label> scopeTypeLabels() {
        return scopeTypes().map(SchemaConcept::label).collect(CommonUtil.toImmutableSet());
    }


    /**
     * Helper method to check whether the concept types in the scope have any instances
     *
     * @return
     */
    private boolean scopeContainsInstance() {
        if (scopeTypeLabels().isEmpty()) return false;
        List<Pattern> checkSubtypes = scopeTypeLabels().stream()
                .map(type -> Graql.var("x").isa(Graql.label(type))).collect(Collectors.toList());

        return tx.graql().infer(false).match(Graql.or(checkSubtypes)).iterator().hasNext();
    }

    /**
     * Helper method to check if concept instances exist in the query scope
     *
     * @param ids
     * @return true if they exist, false if they don't
     */
    private boolean scopeContainsInstances(ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !scopeTypeLabels().contains(thing.type().label())) return false;
        }
        return true;
    }

    /**
     * Helper method to check whether attribute types should be included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private final boolean scopeIncludesAttributes() {
        return query.includesAttributes() || scopeIncludesImplicitOrAttributeTypes();
    }

    /**
     * Helper method to check whether implicit or attribute types are included in the query scope
     *
     * @return true if they exist, false if they don't
     */
    private boolean scopeIncludesImplicitOrAttributeTypes() {
        if (!query.in().isPresent()) return false;
        return query.in().get().stream().anyMatch(label -> {
            SchemaConcept type = tx.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    /**
     * Helper method to convert type labels to IDs
     *
     * @param labelSet
     * @return a set of LabelIds
     */
    private Set<LabelId> convertLabelsToIds(Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }

}
