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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeId;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graph.internal.computer.GraknSparkComputer;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.util.EngineCommunicator;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import mjson.Json;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

/**
 * <p>
 *    The Grakn Graph Base Implementation
 * </p>
 *
 * <p>
 *     This defines how a grakn graph sits on top of a Tinkerpop {@link Graph}.
 *     It mostly act as a construction object which ensure the resulting graph conforms to the Grakn Object model.
 * </p>
 *
 * @author fppt
 *
 * @param <G> A vendor specific implementation of a Tinkerpop {@link Graph}.
 */
public abstract class AbstractGraknGraph<G extends Graph> implements GraknGraph, GraknAdmin {
    protected final Logger LOG = LoggerFactory.getLogger(AbstractGraknGraph.class);

    //TODO: Is this the correct place for these config paths
    //----------------------------- Config Paths
    public static final String SHARDING_THRESHOLD = "graph.sharding-threshold";
    public static final String NORMAL_CACHE_TIMEOUT_MS = "graph.ontology-cache-timeout-ms";

    //----------------------------- Graph Shared Variable
    private final String keyspace;
    private final String engine;
    private final Properties properties;
    private final G graph;
    private final ElementFactory elementFactory;
    private final GraphCache graphCache;

    //----------------------------- Transaction Specific
    private final ThreadLocal<TxCache> localConceptLog = new ThreadLocal<>();

    public AbstractGraknGraph(G graph, String keyspace, String engine, Properties properties) {
        this.graph = graph;
        this.keyspace = keyspace;
        this.engine = engine;
        this.properties = properties;
        elementFactory = new ElementFactory(this);

        //Initialise Graph Caches
        graphCache = new GraphCache(properties);

        //Initialise Graph
        getTxCache().openTx(GraknTxType.WRITE);

        getTxCache().showImplicitTypes(true);
        if(initialiseMetaConcepts()) close(true, false);
        getTxCache().showImplicitTypes(false);

    }

    @Override
    public TypeId convertToId(TypeLabel label){
        if(getTxCache().isLabelCached(label)){
            return getTxCache().convertLabelToId(label);
        }
        return TypeId.invalid();
    }

    /**
     * Gets and increments the current available type id.
     *
     * @return the current available Grakn id which can be used for types
     */
    private TypeId getNextId(){
        TypeImpl<?, ?> metaConcept = (TypeImpl<?, ?>) getMetaConcept();
        Integer currentValue = metaConcept.getProperty(Schema.ConceptProperty.CURRENT_TYPE_ID);
        if(currentValue == null){
            currentValue = Schema.MetaSchema.values().length + 1;
        } else {
            currentValue = currentValue + 1;
        }
        //Vertex is used directly here to bypass meta type mutation check
        metaConcept.getVertex().property(Schema.ConceptProperty.CURRENT_TYPE_ID.name(), currentValue);
        return TypeId.of(currentValue);
    }

    /**
     *
     * @return The graph cache which contains all the data cached and accessible by all transactions.
     */
    GraphCache getGraphCache(){
        return graphCache;
    }

    /**
     * @param concept A concept in the graph
     * @return True if the concept has been modified in the transaction
     */
    public abstract boolean isConceptModified(ConceptImpl concept);

    /**
     *
     * @return The number of open transactions currently.
     */
    public abstract int numOpenTx();

    /**
     * Opens the thread bound transaction
     */
    public void openTransaction(GraknTxType txType){
        getTxCache().openTx(txType);
    }

    String getEngineUrl(){
        return engine;
    }

    Properties getProperties(){
        return properties;
    }

    @Override
    public String getKeyspace(){
        return keyspace;
    }

    TxCache getTxCache() {
        TxCache txCache = localConceptLog.get();
        if(txCache == null){
            localConceptLog.set(txCache = new TxCache(getGraphCache()));
        }

        if(txCache.isTxOpen() && txCache.ontologyNotCached()){
            txCache.refreshOntologyCache();
        }

        return txCache;
    }

    @Override
    public boolean isClosed(){
        return !getTxCache().isTxOpen();
    }
    public abstract boolean isSessionClosed();

    @Override
    public boolean implicitConceptsVisible(){
        return getTxCache().implicitTypesVisible();
    }

    @Override
    public boolean isReadOnly(){
        return GraknTxType.READ.equals(getTxCache().txType());
    }

    @Override
    public void showImplicitConcepts(boolean flag){
        getTxCache().showImplicitTypes(flag);
    }

    @Override
    public GraknAdmin admin(){
        return this;
    }

    @Override
    public <T extends Concept> T buildConcept(Vertex vertex) {
        return getElementFactory().buildConcept(vertex);
    }

    @Override
    public boolean isBatchGraph(){
        return GraknTxType.BATCH.equals(getTxCache().txType());
    }

    @SuppressWarnings("unchecked")
    private boolean initialiseMetaConcepts(){
        boolean ontologyInitialised = false;
        if(isMetaOntologyNotInitialised()){
            Vertex type = addTypeVertex(Schema.MetaSchema.CONCEPT.getId(), Schema.MetaSchema.CONCEPT.getLabel(), Schema.BaseType.TYPE);
            Vertex entityType = addTypeVertex(Schema.MetaSchema.ENTITY.getId(), Schema.MetaSchema.ENTITY.getLabel(), Schema.BaseType.ENTITY_TYPE);
            Vertex relationType = addTypeVertex(Schema.MetaSchema.RELATION.getId(), Schema.MetaSchema.RELATION.getLabel(), Schema.BaseType.RELATION_TYPE);
            Vertex resourceType = addTypeVertex(Schema.MetaSchema.RESOURCE.getId(), Schema.MetaSchema.RESOURCE.getLabel(), Schema.BaseType.RESOURCE_TYPE);
            Vertex roleType = addTypeVertex(Schema.MetaSchema.ROLE.getId(), Schema.MetaSchema.ROLE.getLabel(), Schema.BaseType.ROLE_TYPE);
            Vertex ruleType = addTypeVertex(Schema.MetaSchema.RULE.getId(), Schema.MetaSchema.RULE.getLabel(), Schema.BaseType.RULE_TYPE);
            Vertex inferenceRuleType = addTypeVertex(Schema.MetaSchema.INFERENCE_RULE.getId(), Schema.MetaSchema.INFERENCE_RULE.getLabel(), Schema.BaseType.RULE_TYPE);
            Vertex constraintRuleType = addTypeVertex(Schema.MetaSchema.CONSTRAINT_RULE.getId(), Schema.MetaSchema.CONSTRAINT_RULE.getLabel(), Schema.BaseType.RULE_TYPE);

            relationType.property(Schema.ConceptProperty.IS_ABSTRACT.name(), true);
            roleType.property(Schema.ConceptProperty.IS_ABSTRACT.name(), true);
            resourceType.property(Schema.ConceptProperty.IS_ABSTRACT.name(), true);
            ruleType.property(Schema.ConceptProperty.IS_ABSTRACT.name(), true);
            entityType.property(Schema.ConceptProperty.IS_ABSTRACT.name(), true);

            relationType.addEdge(Schema.EdgeLabel.SUB.getLabel(), type);
            roleType.addEdge(Schema.EdgeLabel.SUB.getLabel(), type);
            resourceType.addEdge(Schema.EdgeLabel.SUB.getLabel(), type);
            ruleType.addEdge(Schema.EdgeLabel.SUB.getLabel(), type);
            entityType.addEdge(Schema.EdgeLabel.SUB.getLabel(), type);
            inferenceRuleType.addEdge(Schema.EdgeLabel.SUB.getLabel(), ruleType);
            constraintRuleType.addEdge(Schema.EdgeLabel.SUB.getLabel(), ruleType);

            //Manual creation of shards on meta types which have instances
            createMetaShard(inferenceRuleType, Schema.BaseType.RULE_TYPE);
            createMetaShard(constraintRuleType, Schema.BaseType.RULE_TYPE);

            ontologyInitialised = true;
        }

        //Copy entire ontology to the graph cache. This may be a bad idea as it will slow down graph initialisation
        getMetaConcept().subTypes().forEach(type -> {
            getGraphCache().cacheLabel(type.getLabel(), type.getTypeId());
            getGraphCache().cacheType(type.getLabel(), type);
        });

        return ontologyInitialised;
    }
    private void createMetaShard(Vertex metaNode, Schema.BaseType baseType){
        Vertex metaShard = addVertex(baseType);
        metaShard.addEdge(Schema.EdgeLabel.SHARD.getLabel(), metaNode);
        metaShard.property(Schema.ConceptProperty.IS_SHARD.name(), true);
        metaNode.property(Schema.ConceptProperty.CURRENT_SHARD.name(), metaShard.id().toString());
    }

    private boolean isMetaOntologyNotInitialised(){
        return getMetaConcept() == null;
    }

    public G getTinkerPopGraph(){
        return graph;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> getTinkerTraversal(){
        operateOnOpenGraph(() -> null); //This is to check if the graph is open
        ReadOnlyStrategy readOnlyStrategy = ReadOnlyStrategy.instance();
        return getTinkerPopGraph().traversal().asBuilder().with(readOnlyStrategy).create(getTinkerPopGraph()).V();
    }

    @Override
    public QueryBuilder graql(){
        return new QueryBuilderImpl(this);
    }

    ElementFactory getElementFactory(){
        return elementFactory;
    }

    //----------------------------------------------General Functionality-----------------------------------------------
    private EdgeImpl addEdge(Concept from, Concept to, Schema.EdgeLabel type){
        return ((ConceptImpl)from).addEdge((ConceptImpl) to, type);
    }

    @Override
    public <T extends Concept> T  getConcept(Schema.ConceptProperty key, Object value) {
        Iterator<Vertex> vertices = getTinkerTraversal().has(key.name(), value);

        if(vertices.hasNext()){
            Vertex vertex = vertices.next();
            if(vertices.hasNext()) {
                LOG.warn(ErrorMessage.TOO_MANY_CONCEPTS.getMessage(key.name(), value));
            }
            return getElementFactory().buildConcept(vertex);
        } else {
            return null;
        }
    }

    private Set<ConceptImpl> getConcepts(Schema.ConceptProperty key, Object value){
        Set<ConceptImpl> concepts = new HashSet<>();
        getTinkerTraversal().has(key.name(), value).
            forEachRemaining(v -> concepts.add(getElementFactory().buildConcept(v)));
        return concepts;
    }

    void checkOntologyMutation(){
        checkMutation();
        if(isBatchGraph()){
            throw new GraphRuntimeException(ErrorMessage.SCHEMA_LOCKED.getMessage());
        }
    }

    void checkMutation(){
        if(isReadOnly()) throw new GraphRuntimeException(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(getKeyspace()));
    }


    //----------------------------------------------Concept Functionality-----------------------------------------------
    //------------------------------------ Construction
    Vertex addVertex(Schema.BaseType baseType){
        Vertex vertex = operateOnOpenGraph(() -> getTinkerPopGraph().addVertex(baseType.name()));
        vertex.property(Schema.ConceptProperty.ID.name(), vertex.id().toString());
        return vertex;
    }

    private Vertex putVertex(TypeLabel label, Schema.BaseType baseType){
        Vertex vertex;
        ConceptImpl concept = getType(convertToId(label));
        if(concept == null) {
            vertex = addTypeVertex(getNextId(), label, baseType);
        } else {
            if(!baseType.equals(concept.getBaseType())) {
                throw new ConceptNotUniqueException(concept, label.getValue());
            }
            vertex = concept.getVertex();
        }
        return vertex;
    }

    /**
     * Adds a new type vertex which occupies a grakn id. This result in the grakn id count on the meta concept to be
     * incremented.
     *
     * @param label The label of the new type vertex
     * @param baseType The base type of the new type
     * @return The new type vertex
     */
    private Vertex addTypeVertex(TypeId id, TypeLabel label, Schema.BaseType baseType){
        Vertex vertex = addVertex(baseType);
        vertex.property(Schema.ConceptProperty.TYPE_LABEL.name(), label.getValue());
        vertex.property(Schema.ConceptProperty.TYPE_ID.name(), id.getValue());
        return vertex;
    }

    /**
     * An operation on the graph which requires it to be open.
     *
     * @param supplier The operation to be performed on the graph
     * @throws GraphRuntimeException if the graph is closed.
     * @return The result of the operation on the graph.
     */
    private <X> X operateOnOpenGraph(Supplier<X> supplier){
        if(isClosed()){
            String reason = getTxCache().getClosedReason();
            if(reason == null){
                throw new GraphRuntimeException(ErrorMessage.GRAPH_CLOSED.getMessage(getKeyspace()));
            } else {
                throw new GraphRuntimeException(reason);
            }
        }

        return supplier.get();
    }

    @Override
    public EntityType putEntityType(String label) {
        return putEntityType(TypeLabel.of(label));
    }

    @Override
    public EntityType putEntityType(TypeLabel label) {
        return putType(label, Schema.BaseType.ENTITY_TYPE,
                v -> getElementFactory().buildEntityType(v, getMetaEntityType()));
    }

    private <T extends TypeImpl> T putType(TypeLabel label, Schema.BaseType baseType, Function<Vertex, T> factory){
        checkOntologyMutation();
        TypeImpl type = buildType(label, () -> factory.apply(putVertex(label, baseType)));

        T finalType = validateConceptType(type, baseType, () -> {
            throw new ConceptNotUniqueException(type, label.getValue());
        });

        //Automatic shard creation - If this type does not have a shard create one
        if(!Schema.MetaSchema.isMetaLabel(label) &&
                !type.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHARD).findAny().isPresent()){
            type.createShard();
        }

        return finalType;
    }

    private <T extends Concept> T validateConceptType(Concept concept, Schema.BaseType baseType, Supplier<T> invalidHandler){
        if(concept != null && baseType.getClassType().isInstance(concept)){
            //noinspection unchecked
            return (T) concept;
        } else {
            return invalidHandler.get();
        }
    }

    /**
     * A helper method which either retrieves the type from the cache or builds it using a provided supplier
     *
     * @param label The label of the type to retrieve or build
     * @param dbBuilder A method which builds the type via a DB read or write
     *
     * @return The type which was either cached or built via a DB read or write
     */
    private TypeImpl buildType(TypeLabel label, Supplier<TypeImpl> dbBuilder){
        if(getTxCache().isTypeCached(label)){
            return getTxCache().getCachedType(label);
        } else {
            return dbBuilder.get();
        }
    }

    @Override
    public RelationType putRelationType(String label) {
        return putRelationType(TypeLabel.of(label));
    }

    @Override
    public RelationType putRelationType(TypeLabel label) {
        return putType(label, Schema.BaseType.RELATION_TYPE,
                v -> getElementFactory().buildRelationType(v, getMetaRelationType(), Boolean.FALSE)).asRelationType();
    }

    RelationType putRelationTypeImplicit(TypeLabel label) {
        return putType(label, Schema.BaseType.RELATION_TYPE,
                v -> getElementFactory().buildRelationType(v, getMetaRelationType(), Boolean.TRUE)).asRelationType();
    }

    @Override
    public RoleType putRoleType(String label) {
        return putRoleType(TypeLabel.of(label));
    }

    @Override
    public RoleType putRoleType(TypeLabel label) {
        return putType(label, Schema.BaseType.ROLE_TYPE,
                v -> getElementFactory().buildRoleType(v, getMetaRoleType(), Boolean.FALSE)).asRoleType();
    }

    RoleType putRoleTypeImplicit(TypeLabel label) {
        return putType(label, Schema.BaseType.ROLE_TYPE,
                v -> getElementFactory().buildRoleType(v, getMetaRoleType(), Boolean.TRUE)).asRoleType();
    }

    @Override
    public <V> ResourceType<V> putResourceType(String label, ResourceType.DataType<V> dataType) {
        return putResourceType(TypeLabel.of(label), dataType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> ResourceType<V> putResourceType(TypeLabel label, ResourceType.DataType<V> dataType) {
        @SuppressWarnings("unchecked")
        ResourceType<V> resourceType = putType(label, Schema.BaseType.RESOURCE_TYPE,
                v -> getElementFactory().buildResourceType(v, getMetaResourceType(), dataType)).asResourceType();

        //These checks is needed here because caching will return a type by label without checking the datatype
        if(Schema.MetaSchema.isMetaLabel(label)) {
            throw new ConceptException(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(label));
        } else if(!dataType.equals(resourceType.getDataType())){
            throw new InvalidConceptValueException(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType(), resourceType, dataType, Schema.ConceptProperty.DATA_TYPE.name()));
        }

        return resourceType;
    }

    @Override
    public RuleType putRuleType(String label) {
        return putRuleType(TypeLabel.of(label));
    }

    @Override
    public RuleType putRuleType(TypeLabel label) {
        return putType(label, Schema.BaseType.RULE_TYPE,
                v ->  getElementFactory().buildRuleType(v, getMetaRuleType()));
    }

    //------------------------------------ Lookup
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        if(getTxCache().isConceptCached(id)){
            return getTxCache().getCachedConcept(id);
        } else {
            return getConcept(Schema.ConceptProperty.ID, id.getValue());
        }
    }
    private <T extends Type> T getType(TypeLabel label, Schema.BaseType baseType){
        operateOnOpenGraph(() -> null); //Makes sure the graph is open

        Type type = buildType(label, ()-> getType(convertToId(label)));
        return validateConceptType(type, baseType, () -> null);
    }
    private <T extends Type> T getType(TypeId id){
        if(!id.isValid()) return null;
        return getConcept(Schema.ConceptProperty.TYPE_ID, id.getValue());
    }

    @Override
    public <V> Collection<Resource<V>> getResourcesByValue(V value) {
        if(value == null) return Collections.emptySet();

        //Make sure you trying to retrieve supported data type
        if(!ResourceType.DataType.SUPPORTED_TYPES.containsKey(value.getClass().getName())){
            String supported = ResourceType.DataType.SUPPORTED_TYPES.keySet().stream().collect(Collectors.joining(","));
            throw new InvalidConceptValueException(ErrorMessage.INVALID_DATATYPE.getMessage(value.getClass().getName(), supported));
        }

        HashSet<Resource<V>> resources = new HashSet<>();
        ResourceType.DataType dataType = ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName());

        //noinspection unchecked
        getConcepts(dataType.getConceptProperty(), dataType.getPersistenceValue(value)).forEach(concept -> {
            if(concept != null && concept.isResource()) {
                //noinspection unchecked
                resources.add(concept.asResource());
            }
        });

        return resources;
    }

    @Override
    public <T extends Type> T getType(TypeLabel label) {
        return getType(label, Schema.BaseType.TYPE);
    }

    @Override
    public EntityType getEntityType(String label) {
        return getType(TypeLabel.of(label), Schema.BaseType.ENTITY_TYPE);
    }

    @Override
    public RelationType getRelationType(String label) {
        return getType(TypeLabel.of(label), Schema.BaseType.RELATION_TYPE);
    }

    @Override
    public <V> ResourceType<V> getResourceType(String label) {
        return getType(TypeLabel.of(label), Schema.BaseType.RESOURCE_TYPE);
    }

    @Override
    public RoleType getRoleType(String label) {
        return getType(TypeLabel.of(label), Schema.BaseType.ROLE_TYPE);
    }

    @Override
    public RuleType getRuleType(String label) {
        return getType(TypeLabel.of(label), Schema.BaseType.RULE_TYPE);
    }

    @Override
    public Type getMetaConcept() {
        return getType(Schema.MetaSchema.CONCEPT.getId());
    }

    @Override
    public RelationType getMetaRelationType() {
        return getType(Schema.MetaSchema.RELATION.getId());
    }

    @Override
    public RoleType getMetaRoleType() {
        return getType(Schema.MetaSchema.ROLE.getId());
    }

    @Override
    public ResourceType getMetaResourceType() {
        return getType(Schema.MetaSchema.RESOURCE.getId());
    }

    @Override
    public EntityType getMetaEntityType() {
        return getType(Schema.MetaSchema.ENTITY.getId());
    }

    @Override
    public RuleType getMetaRuleType(){
        return getType(Schema.MetaSchema.RULE.getId());
    }

    @Override
    public RuleType getMetaRuleInference() {
        return getType(Schema.MetaSchema.INFERENCE_RULE.getId());
    }

    @Override
    public RuleType getMetaRuleConstraint() {
        return getType(Schema.MetaSchema.CONSTRAINT_RULE.getId());
    }

    //-----------------------------------------------Casting Functionality----------------------------------------------
    //------------------------------------ Construction
    private CastingImpl addCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        CastingImpl casting = getElementFactory().buildCasting(addVertex(Schema.BaseType.CASTING), role.currentShard()).setHash(role, rolePlayer);
        if(rolePlayer != null) {
            EdgeImpl castingToRolePlayer = addEdge(casting, rolePlayer, Schema.EdgeLabel.ROLE_PLAYER); // Casting to RolePlayer
            castingToRolePlayer.setProperty(Schema.EdgeProperty.ROLE_TYPE_ID, role.getTypeId().getValue());
        }
        return casting;
    }
    CastingImpl addCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation){
        CastingImpl foundCasting  = null;
        if(rolePlayer != null) {
            foundCasting = getCasting(role, rolePlayer);
        }

        if(foundCasting == null){
            foundCasting = addCasting(role, rolePlayer);
        }

        // Relation To Casting
        EdgeImpl relationToCasting = relation.putEdge(foundCasting, Schema.EdgeLabel.CASTING);
        relationToCasting.setProperty(Schema.EdgeProperty.ROLE_TYPE_ID, role.getTypeId().getValue());
        getTxCache().trackConceptForValidation(relation); //The relation is explicitly tracked so we can look them up without committing

        //TODO: Only execute this if we need to. I.e if the above relation.putEdge() actually added a new edge.
        if(rolePlayer != null) putShortcutEdge(rolePlayer, relation, role);

        return foundCasting;
    }

    private CastingImpl getCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        return getConcept(Schema.ConceptProperty.INDEX, CastingImpl.generateNewHash(role, rolePlayer));
    }

    private void putShortcutEdge(Instance toInstance, Relation fromRelation, RoleType roleType){
        boolean exists  = getTinkerPopGraph().traversal().V(fromRelation.getId().getRawValue()).
                outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                has(Schema.EdgeProperty.RELATION_TYPE_ID.name(), fromRelation.type().getTypeId().getValue()).
                has(Schema.EdgeProperty.ROLE_TYPE_ID.name(), roleType.getTypeId().getValue()).inV().
                hasId(toInstance.getId().getRawValue()).hasNext();

        if(!exists){
            EdgeImpl edge = addEdge(fromRelation, toInstance, Schema.EdgeLabel.SHORTCUT);
            edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_ID, fromRelation.type().getTypeId().getValue());
            edge.setProperty(Schema.EdgeProperty.ROLE_TYPE_ID, roleType.getTypeId().getValue());
        }
    }

    @Override
    public void delete() {
        closeSession();
        clearGraph();
        getTxCache().closeTx(ErrorMessage.CLOSED_CLEAR.getMessage());

        //Remove the graph from the system keyspace
        SystemKeyspace.deleteKeyspace(getKeyspace());
    }

    //This is overridden by vendors for more efficient clearing approaches
    protected void clearGraph(){
        getTinkerPopGraph().traversal().V().drop().iterate();
    }

    @Override
    public void closeSession(){
        try {
            getTxCache().closeTx(ErrorMessage.SESSION_CLOSED.getMessage(getKeyspace()));
            getTinkerPopGraph().close();
        } catch (Exception e) {
            throw new GraphRuntimeException("Unable to close graph [" + getKeyspace() + "]", e);
        }
    }

    @Override
    public void close(){
        close(false, false);
    }

    @Override
    public void abort(){
        close();
    }

    @Override
    public void commit() throws GraknValidationException{
        close(true, true);
    }

    private Optional<String> close(boolean commitRequired, boolean submitLogs){
        Optional<String> logs = Optional.empty();
        if(isClosed()) return logs;
        String closeMessage = ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("closed", getKeyspace());

        try{
            if(commitRequired) {
                closeMessage = ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("committed", getKeyspace());
                logs = commitWithLogs();
                if(logs.isPresent() && submitLogs) {
                    String logsToUpload = logs.get();
                    new Thread(() -> LOG.debug("Response from engine [" + EngineCommunicator.contactEngine(getCommitLogEndPoint(), REST.HttpConn.POST_METHOD, logsToUpload) + "]")).start();
                }
                getTxCache().writeToGraphCache(true);
            } else {
                getTxCache().writeToGraphCache(isReadOnly());
            }
        } finally {
            closeTransaction(closeMessage);
        }
        return logs;
    }

    private void closeTransaction(String closedReason){
        try {
            // TODO: We check `isOpen` because of a Titan bug which decrements the transaction counter even if the transaction is closed
            if (graph.tx().isOpen()) {
                graph.tx().close();
            }
        } catch (UnsupportedOperationException e) {
            //Ignored for Tinker
        } finally {
            getTxCache().closeTx(closedReason);
        }
    }

    /**
     * Commits to the graph without submitting any commit logs.
     *
     * @throws GraknValidationException when the graph does not conform to the object concept
     */
    @Override
    public Optional<String> commitNoLogs() throws GraknValidationException {
        return close(true, false);
    }

    private Optional<String> commitWithLogs() throws GraknValidationException {
        validateGraph();

        boolean submissionNeeded = !getTxCache().getShardingCount().isEmpty() ||
                !getTxCache().getModifiedCastings().isEmpty() ||
                !getTxCache().getModifiedResources().isEmpty();
        Json conceptLog = getTxCache().getFormattedLog();

        LOG.trace("Graph is valid. Committing graph . . . ");
        commitTransactionInternal();

        //TODO: Kill when analytics no longer needs this
        GraknSparkComputer.refresh();

        LOG.trace("Graph committed.");

        //No post processing should ever be done for the system keyspace
        if(!keyspace.equalsIgnoreCase(SystemKeyspace.SYSTEM_GRAPH_NAME) && submissionNeeded) {
            return Optional.of(conceptLog.toString());
        }
        return Optional.empty();
    }

    void commitTransactionInternal(){
        try {
            getTinkerPopGraph().tx().commit();
        } catch (UnsupportedOperationException e){
            //IGNORED
        }
    }

    void validateGraph() throws GraknValidationException {
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            StringBuilder error = new StringBuilder();
            error.append(ErrorMessage.VALIDATION.getMessage(errors.size()));
            for (String s : errors) {
                error.append(s);
            }
            throw new GraknValidationException(error.toString());
        }
    }

    private String getCommitLogEndPoint(){
        if(Grakn.IN_MEMORY.equals(engine)) {
            return Grakn.IN_MEMORY;
        }
        return engine + REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + keyspace;
    }

    public void validVertex(Vertex vertex){
        if(vertex == null) {
            throw new IllegalStateException("The provided vertex is null");
        }
    }

    //------------------------------------------ Fixing Code for Postprocessing ----------------------------------------
    @Override
    public boolean duplicateCastingsExist(String index, Set<ConceptId> castingVertexIds){
        CastingImpl mainCasting = (CastingImpl) getMainConcept(index);
        return getDuplicates(mainCasting, castingVertexIds).size() > 0;
    }

    /**
     * Returns the duplicates of the given concept
     * @param mainConcept primary concept - this one is returned by the index and not considered a duplicate
     * @param conceptIds Set of Ids containing potential duplicates of the main concept
     * @return a set containing the duplicates of the given concept
     */
    private Set<? extends ConceptImpl> getDuplicates(ConceptImpl mainConcept, Set<ConceptId> conceptIds){
        Set<ConceptImpl> duplicated = conceptIds.stream()
                .map(this::<ConceptImpl>getConcept)
                //filter non-null, will be null if previously deleted/merged
                .filter(Objects::nonNull)
                .collect(toSet());

        duplicated.remove(mainConcept);

        return duplicated;
    }

    /**
     * Given an index, get the concept associated with it. The "main" concept is the one
     * returned by the index after all of the duplicates have been created.
     *
     * @param index retrieve the concept associated with this index
     * @return Concept representing the vertex at the given index
     */
    private ConceptImpl getMainConcept(String index){
        //This is done to ensure we merge into the indexed casting.
        return getConcept(Schema.ConceptProperty.INDEX, index);
    }

    /**
     * Merges the provided duplicate castings.
     *
     * @param castingVertexIds The vertex Ids of the duplicate castings
     * @return if castings were merged, a commit is required and the casting index exists
     */
    @Override
    public boolean fixDuplicateCastings(String index, Set<ConceptId> castingVertexIds){
        CastingImpl mainCasting = (CastingImpl) getMainConcept(index);
        Set<CastingImpl> duplicated = (Set<CastingImpl>) getDuplicates(mainCasting, castingVertexIds);

        if (duplicated.size() > 0) {
            //Fix the duplicates
            Set<Relation> duplicateRelations = mergeCastings(mainCasting, duplicated);

            //Remove Redundant Relations
            duplicateRelations.forEach(relation -> ((ConceptImpl) relation).deleteNode());

            //Restore the index
            String newIndex = mainCasting.getIndex();
            mainCasting.getVertex().property(Schema.ConceptProperty.INDEX.name(), newIndex);

            return true;
        }

        return false;
    }

    /**
     *
     * @param mainCasting The main casting to absorb all of the edges
     * @param castings The castings to whose edges will be transferred to the main casting and deleted.
     * @return A set of possible duplicate relations.
     */
    private Set<Relation> mergeCastings(CastingImpl mainCasting, Set<CastingImpl> castings){
        RoleType role = mainCasting.getRole();
        Set<Relation> relations = mainCasting.getRelations();
        Set<Relation> relationsToClean = new HashSet<>();
        Set<RelationImpl> relationsRequiringReIndexing = new HashSet<>();

        for (CastingImpl otherCasting : castings) {
            //Transfer assertion edges
            for(Relation rel : otherCasting.getRelations()){
                RelationImpl otherRelation = (RelationImpl) rel;
                boolean transferEdge = true;

                //Check if an equivalent Relation is already connected to this casting. This could be a slow process
                for(Relation originalRelation: relations){
                    if(relationsEqual(originalRelation, otherRelation)){
                        relationsToClean.add(otherRelation);
                        transferEdge = false;
                        break;
                    }
                }

                //Perform the transfer
                if(transferEdge) {
                    //Delete index so we can reset it when things are finalised.
                    otherRelation.setProperty(Schema.ConceptProperty.INDEX, null);
                    EdgeImpl assertionToCasting = addEdge(otherRelation, mainCasting, Schema.EdgeLabel.CASTING);
                    assertionToCasting.setProperty(Schema.EdgeProperty.ROLE_TYPE_ID, role.getTypeId().getValue());
                    relations = mainCasting.getRelations();
                    relationsRequiringReIndexing.add(otherRelation);
                }
            }

            getTxCache().removeConcept(otherCasting);
            getTinkerPopGraph().traversal().V(otherCasting.getId().getRawValue()).next().remove();
        }

        relationsRequiringReIndexing.forEach(RelationImpl::setHash);

        return relationsToClean;
    }

    /**
     *
     * @param mainRelation The main relation to compare
     * @param otherRelation The relation to compare it with
     * @return True if the roleplayers of the relations are the same.
     */
    private boolean relationsEqual(Relation mainRelation, Relation otherRelation){
        String mainIndex = getRelationIndex((RelationImpl) mainRelation);
        String otherIndex = getRelationIndex((RelationImpl) otherRelation);
        return mainIndex.equals(otherIndex);
    }
    private String getRelationIndex(RelationImpl relation){
        String index = relation.getIndex();
        if(index == null) index = RelationImpl.generateNewHash(relation.type(), relation.allRolePlayers());
        return index;
    }

    /**
     * Check if the given index has duplicates to merge
     * @param index Index of the potentially duplicated resource
     * @param resourceVertexIds Set of vertex ids containing potential duplicates
     * @return true if there are duplicate resources amongst the given set and PostProcessing should proceed
     */
    @Override
    public boolean duplicateResourcesExist(String index, Set<ConceptId> resourceVertexIds){
        ResourceImpl<?> mainResource = (ResourceImpl<?>) getMainConcept(index);
        return getDuplicates(mainResource, resourceVertexIds).size() > 0;
    }

    /**
     *
     * @param resourceVertexIds The resource vertex ids which need to be merged.
     * @return True if a commit is required.
     */
    @Override
    public boolean fixDuplicateResources(String index, Set<ConceptId> resourceVertexIds){
        ResourceImpl<?> mainResource = (ResourceImpl<?>) getMainConcept(index);
        Set<ResourceImpl> duplicates = (Set<ResourceImpl>) getDuplicates(mainResource, resourceVertexIds);

        if(duplicates.size() > 0) {
            //Remove any resources associated with this index that are not the main resource
            for (ResourceImpl<?> otherResource : duplicates) {
                Collection<Relation> otherRelations = otherResource.relations();

                //Copy the actual relation
                for (Relation otherRelation : otherRelations) {
                    copyRelation(mainResource, otherResource, (RelationImpl) otherRelation);
                }

                //Delete the node and it's castings directly so we don't accidentally delete copied relations
                otherResource.castings().forEach(ConceptImpl::deleteNode);
                otherResource.deleteNode();
            }

            //Restore the index
            String newIndex = mainResource.getIndex();
            mainResource.getVertex().property(Schema.ConceptProperty.INDEX.name(), newIndex);

            return true;
        }

        return false;
    }

    /**
     *
     * @param main The main instance to possibly acquire a new relation
     * @param other The other instance which already posses the relation
     * @param otherRelation The other relation to potentially be absorbed
     */
    private void copyRelation(ResourceImpl main, ResourceImpl<?> other, RelationImpl otherRelation){
        //Gets the other resource index and replaces all occurrences of the other resource id with the main resource id
        //This allows us to find relations far more quickly.
        String newIndex = otherRelation.getIndex().replaceAll(other.getId().getValue(), main.getId().getValue());
        RelationImpl foundRelation = getTxCache().getCachedRelation(newIndex);
        if(foundRelation == null) foundRelation = getConcept(Schema.ConceptProperty.INDEX, newIndex);

        if (foundRelation != null) {//If it exists delete the other one
            otherRelation.deleteNode(); //Raw deletion because the castings should remain
        } else { //If it doesn't exist transfer the edge to the relevant casting node
            foundRelation = otherRelation;
            //Now that we know the relation needs to be copied we need to find the roles the other casting is playing
            otherRelation.allRolePlayers().forEach((roleType, instances) -> {
                if(instances.contains(other)) addCasting((RoleTypeImpl) roleType, main, otherRelation);
            });
        }

        //Explicitly track this new relation so we don't create duplicates
        getTxCache().getModifiedRelations().put(newIndex, foundRelation);
    }

    @Override
    public void updateConceptCounts(Map<ConceptId, Long> typeCounts){
       typeCounts.entrySet().forEach(entry -> {
           if(entry.getValue() != 0) {
               ConceptImpl concept = getConcept(entry.getKey());
               concept.setShardCount(concept.getShardCount() + entry.getValue());
           }
       });
    }

    @Override
    public void shard(ConceptId conceptId){
        ConceptImpl type = getConcept(conceptId);
        if(type == null) {
            LOG.warn("Cannot shard concept [" + conceptId + "] due to it not existing in the graph");
        } else {
            type.createShard();
        }
    }
}
