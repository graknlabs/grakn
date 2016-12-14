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
import ai.grakn.GraknAdmin;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.exception.MoreThanOneConceptException;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryBuilderImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public abstract class AbstractGraknGraph<G extends Graph> implements GraknGraph, GraknAdmin {
    protected final Logger LOG = LoggerFactory.getLogger(AbstractGraknGraph.class);
    private final ElementFactory elementFactory;
    private final String keyspace;
    private final String engine;
    private final boolean batchLoadingEnabled;
    private final G graph;

    private final ThreadLocal<ConceptLog> localConceptLog = new ThreadLocal<>();
    private final ThreadLocal<Boolean> localIsClosed = new ThreadLocal<>();
    private final ThreadLocal<String> localClosedReason = new ThreadLocal<>();
    private final ThreadLocal<Boolean> localShowImplicitStructures = new ThreadLocal<>();

    private boolean committed; //Shared between multiple threads so we know if a refresh must be performed

    public AbstractGraknGraph(G graph, String keyspace, String engine, boolean batchLoadingEnabled) {
        this.graph = graph;
        this.keyspace = keyspace;
        this.engine = engine;
        elementFactory = new ElementFactory(this);

        if(initialiseMetaConcepts()) {
            try {
                commit();
            } catch (GraknValidationException e) {
                throw new RuntimeException(ErrorMessage.CREATING_ONTOLOGY_ERROR.getMessage(e.getMessage()));
            }
        }

        this.batchLoadingEnabled = batchLoadingEnabled;
        this.committed = false;
        localIsClosed.set(false);
        localShowImplicitStructures.set(false);
    }

    @Override
    public String getKeyspace(){
        return keyspace;
    }

    @Override
    public boolean isClosed(){
        return getBooleanFromLocalThread(localIsClosed);
    }

    @Override
    public boolean implicitConceptsVisible(){
        return getBooleanFromLocalThread(localShowImplicitStructures);
    }

    private boolean getBooleanFromLocalThread(ThreadLocal<Boolean> local){
        Boolean value = local.get();
        if(value == null)
            return false;
        else
            return value;
    }

    @Override
    public void showImplicitConcepts(boolean flag){
        localShowImplicitStructures.set(flag);
    }

    @Override
    public GraknAdmin admin(){
        return this;
    }

    @Override
    public <T extends Concept> T buildConcept(Vertex vertex) {
        return elementFactory.buildUnknownConcept(vertex);
    }

    public boolean hasCommitted(){
        return committed;
    }

    public boolean isBatchLoadingEnabled(){
        return batchLoadingEnabled;
    }

    @SuppressWarnings("unchecked")
    public boolean initialiseMetaConcepts(){
        if(isMetaOntologyNotInitialised()){
            TypeImpl type = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            type.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.TYPE.getName());

            TypeImpl entityType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            entityType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.ENTITY_TYPE.getName());

            TypeImpl relationType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            relationType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.RELATION_TYPE.getName());

            TypeImpl resourceType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            resourceType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.RESOURCE_TYPE.getName());

            TypeImpl roleType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            roleType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.ROLE_TYPE.getName());

            TypeImpl ruleType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            ruleType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.RULE_TYPE.getName());

            RuleTypeImpl inferenceRuleType = elementFactory.buildRuleType(addVertex(Schema.BaseType.RULE_TYPE), ruleType);
            inferenceRuleType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.INFERENCE_RULE.getName());

            RuleTypeImpl constraintRuleType = elementFactory.buildRuleType(addVertex(Schema.BaseType.RULE_TYPE), ruleType);
            constraintRuleType.setProperty(Schema.ConceptProperty.NAME, Schema.MetaSchema.CONSTRAINT_RULE.getName());

            type.setType(type.getName());
            relationType.setType(type.getName());
            roleType.setType(type.getName());
            resourceType.setType(type.getName());
            ruleType.setType(type.getName());
            entityType.setType(type.getName());

            relationType.putEdge(type, Schema.EdgeLabel.SUB);
            roleType.putEdge(type, Schema.EdgeLabel.SUB);
            resourceType.putEdge(type, Schema.EdgeLabel.SUB);
            ruleType.putEdge(type, Schema.EdgeLabel.SUB);
            entityType.putEdge(type, Schema.EdgeLabel.SUB);

            return true;
        }

        return false;
    }

    private boolean isMetaOntologyNotInitialised(){
        return getMetaType() == null;
    }

    public G getTinkerPopGraph(){
        if(isClosed()){
            throw new GraphRuntimeException(localClosedReason.get());
        }
        return graph;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> getTinkerTraversal(){
        ReadOnlyStrategy readOnlyStrategy = ReadOnlyStrategy.instance();
        return getTinkerPopGraph().traversal().asBuilder().with(readOnlyStrategy).create(getTinkerPopGraph()).V();
    }

    @Override
    public QueryBuilder graql(){
        return new QueryBuilderImpl(this);
    }

    public ElementFactory getElementFactory(){
        return elementFactory;
    }

    //----------------------------------------------General Functionality-----------------------------------------------
    private EdgeImpl addEdge(Concept from, Concept to, Schema.EdgeLabel type){
        return ((ConceptImpl)from).addEdge((ConceptImpl) to, type);
    }

    public <T extends Concept> T  getConcept(Schema.ConceptProperty key, String value) {
        Iterator<Vertex> vertices = getTinkerTraversal().has(key.name(), value);

        if(vertices.hasNext()){
            Vertex vertex = vertices.next();
            if(!isBatchLoadingEnabled() && vertices.hasNext())
                throw new MoreThanOneConceptException(ErrorMessage.TOO_MANY_CONCEPTS.getMessage(key.name(), value));
            return (T) elementFactory.buildUnknownConcept(vertex);
        } else {
            return null;
        }
    }

    public Set<ConceptImpl> getConcepts(Schema.ConceptProperty key, Object value){
        Set<ConceptImpl> concepts = new HashSet<>();
        getTinkerTraversal().has(key.name(), value).
            forEachRemaining(v -> {
                concepts.add(elementFactory.buildUnknownConcept(v));
            });
        return concepts;
    }


    public ConceptLog getConceptLog() {
        ConceptLog conceptLog = localConceptLog.get();
        if(conceptLog == null){
            localConceptLog.set(conceptLog = new ConceptLog());
        }
        return conceptLog;
    }

    void checkOntologyMutation(){
        if(isBatchLoadingEnabled()){
            throw new GraphRuntimeException(ErrorMessage.SCHEMA_LOCKED.getMessage());
        }
    }

    //----------------------------------------------Concept Functionality-----------------------------------------------
    //------------------------------------ Construction
    Vertex addVertex(Schema.BaseType baseType){
        Vertex vertex = getTinkerPopGraph().addVertex(baseType.name());
        vertex.property(Schema.ConceptProperty.ID.name(), vertex.id().toString());
        return vertex;
    }

    private Vertex putVertex(String name, Schema.BaseType baseType){
        if(Schema.MetaSchema.isMetaName(name)){
            throw new ConceptException(ErrorMessage.ID_RESERVED.getMessage(name));
        }

        Vertex vertex;
        ConceptImpl concept = getConcept(Schema.ConceptProperty.NAME, name);
        if(concept == null) {
            vertex = addVertex(baseType);
            vertex.property(Schema.ConceptProperty.NAME.name(), name);
        } else {
            if(!baseType.name().equals(concept.getBaseType()))
                throw new ConceptNotUniqueException(concept, name);
            vertex = concept.getVertex();
        }
        return vertex;
    }

    @Override
    public EntityType putEntityType(String name) {
        return putType(name, Schema.BaseType.ENTITY_TYPE, getMetaEntityType()).asEntityType();
    }

    private TypeImpl putType(String name, Schema.BaseType baseType, Type metaType) {
        checkOntologyMutation();
        return elementFactory.buildSpecificType(putVertex(name, baseType), metaType);
    }
    @Override
    public RelationType putRelationType(String name) {
        return putType(name, Schema.BaseType.RELATION_TYPE, getMetaRelationType()).asRelationType();
    }

    RelationType putRelationTypeImplicit(String itemIdentifier) {
        Vertex v = putVertex(itemIdentifier, Schema.BaseType.RELATION_TYPE);
        return elementFactory.buildRelationTypeImplicit(v, getMetaRelationType());
    }
    @Override
    public RoleType putRoleType(String name) {
        return putType(name, Schema.BaseType.ROLE_TYPE, getMetaRoleType()).asRoleType();
    }

    RoleType putRoleTypeImplicit(String itemIdentifier) {
        Vertex v = putVertex(itemIdentifier, Schema.BaseType.ROLE_TYPE);
        return elementFactory.buildRoleTypeImplicit(v, getMetaRoleType());
    }
    @Override
    public <V> ResourceType<V> putResourceType(String name, ResourceType.DataType<V> dataType) {
        return elementFactory.buildResourceType(
                putType(name, Schema.BaseType.RESOURCE_TYPE, getMetaResourceType()).getVertex(),
                getMetaResourceType(),
                dataType,
                false);
    }

    @Override
    public <V> ResourceType <V> putResourceTypeUnique(String name, ResourceType.DataType<V> dataType){
        return elementFactory.buildResourceType(
                putType(name, Schema.BaseType.RESOURCE_TYPE, getMetaResourceType()).getVertex(),
                getMetaResourceType(),
                dataType,
                true);
    }

    @Override
    public RuleType putRuleType(String name) {
        return putType(name, Schema.BaseType.RULE_TYPE, getMetaRuleType()).asRuleType();
    }

    //------------------------------------ Lookup
    @SuppressWarnings("unchecked")
    private <T extends Concept> T validConceptOfType(Concept concept, Class type){
        if(concept != null &&  type.isInstance(concept)){
            return (T) concept;
        }
        return null;
    }
    public <T extends Concept> T getConceptByBaseIdentifier(Object baseIdentifier) {
        GraphTraversal<Vertex, Vertex> traversal = getTinkerPopGraph().traversal().V(baseIdentifier);
        if (traversal.hasNext()) {
            return (T) elementFactory.buildUnknownConcept(traversal.next());
        } else {
            return null;
        }
    }

    @Override
    public <T extends Concept> T getConcept(String id) {
        return getConcept(Schema.ConceptProperty.ID, id);
    }
    private <T extends Type> T getTypeByName(String name){
        return getConcept(Schema.ConceptProperty.NAME, name);
    }

    @Override
    public <V> Collection<Resource<V>> getResourcesByValue(V value) {
        HashSet<Resource<V>> resources = new HashSet<>();
        ResourceType.DataType dataType = ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName());

        getConcepts(dataType.getConceptProperty(), value).forEach(concept -> {
            if(concept != null && concept.isResource()) {
                Concept resource = validConceptOfType(concept, ResourceImpl.class);
                resources.add(resource.asResource());
            }
        });

        return resources;
    }

    @Override
    public Type getType(String name) {
        return validConceptOfType(getTypeByName(name), TypeImpl.class);
    }

    @Override
    public EntityType getEntityType(String name) {
        return validConceptOfType(getTypeByName(name), EntityTypeImpl.class);
    }

    @Override
    public RelationType getRelationType(String name) {
        return validConceptOfType(getTypeByName(name), RelationTypeImpl.class);
    }

    @Override
    public <V> ResourceType<V> getResourceType(String name) {
        return validConceptOfType(getTypeByName(name), ResourceTypeImpl.class);
    }

    @Override
    public RoleType getRoleType(String name) {
        return validConceptOfType(getTypeByName(name), RoleTypeImpl.class);
    }

    @Override
    public RuleType getRuleType(String name) {
        return validConceptOfType(getTypeByName(name), RuleTypeImpl.class);
    }

    @Override
    public Type getMetaType() {
        return getTypeByName(Schema.MetaSchema.TYPE.getName());
    }

    @Override
    public Type getMetaRelationType() {
        return getTypeByName(Schema.MetaSchema.RELATION_TYPE.getName());
    }

    @Override
    public Type getMetaRoleType() {
        return getTypeByName(Schema.MetaSchema.ROLE_TYPE.getName());
    }

    @Override
    public Type getMetaResourceType() {
        return getTypeByName(Schema.MetaSchema.RESOURCE_TYPE.getName());
    }

    @Override
    public Type getMetaEntityType() {
        return getTypeByName(Schema.MetaSchema.ENTITY_TYPE.getName());
    }

    @Override
    public Type getMetaRuleType(){
        return getTypeByName(Schema.MetaSchema.RULE_TYPE.getName());
    }

    @Override
    public RuleType getMetaRuleInference() {
        return getTypeByName(Schema.MetaSchema.INFERENCE_RULE.getName()).asRuleType();
    }

    @Override
    public RuleType getMetaRuleConstraint() {
        return getTypeByName(Schema.MetaSchema.CONSTRAINT_RULE.getName()).asRuleType();
    }

    //-----------------------------------------------Casting Functionality----------------------------------------------
    //------------------------------------ Construction
    private CastingImpl addCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        CastingImpl casting = elementFactory.buildCasting(addVertex(Schema.BaseType.CASTING), role).setHash(role, rolePlayer);
        if(rolePlayer != null) {
            EdgeImpl castingToRolePlayer = addEdge(casting, rolePlayer, Schema.EdgeLabel.ROLE_PLAYER); // Casting to RolePlayer
            castingToRolePlayer.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId());
        }
        return casting;
    }
    CastingImpl putCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation){
        CastingImpl foundCasting  = null;
        if(rolePlayer != null)
            foundCasting = getCasting(role, rolePlayer);

        if(foundCasting == null){
            foundCasting = addCasting(role, rolePlayer);
        }

        EdgeImpl assertionToCasting = addEdge(relation, foundCasting, Schema.EdgeLabel.CASTING);// Relation To Casting
        assertionToCasting.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId());

        putShortcutEdges(relation, relation.type());

        return foundCasting;
    }

    //------------------------------------ Lookup
    private CastingImpl getCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        try {
            String hash = CastingImpl.generateNewHash(role, rolePlayer);
            ConceptImpl concept = getConcept(Schema.ConceptProperty.INDEX, hash);
            if (concept != null)
                return concept.asCasting();
            else
                return null;
        } catch(GraphRuntimeException e){
            throw new MoreThanOneConceptException(ErrorMessage.TOO_MANY_CASTINGS.getMessage(role, rolePlayer));
        }
    }

    private void putShortcutEdges(Relation relation, RelationType relationType){
        Map<RoleType, Instance> roleMap = relation.rolePlayers();
        if(roleMap.size() > 1) {
            for(Map.Entry<RoleType, Instance> from : roleMap.entrySet()){
                for(Map.Entry<RoleType, Instance> to :roleMap.entrySet()){
                    if(from.getValue() != null && to.getValue() != null){
                        if(from.getKey() != to.getKey())
                            putShortcutEdge(
                                    relation,
                                    relationType.asRelationType(),
                                    from.getKey().asRoleType(),
                                    from.getValue().asInstance(),
                                    to.getKey().asRoleType(),
                                    to.getValue().asInstance());
                    }
                }
            }
        }
    }

    private void putShortcutEdge(Relation  relation, RelationType  relationType, RoleType fromRole, Instance from, RoleType  toRole, Instance to){
        InstanceImpl fromRolePlayer = (InstanceImpl) from;
        InstanceImpl toRolePlayer = (InstanceImpl) to;

        String hash = calculateShortcutHash(relation, relationType, fromRole, fromRolePlayer, toRole, toRolePlayer);
        boolean exists = getTinkerPopGraph().traversal().V(fromRolePlayer.getBaseIdentifier()).
                    local(outE(Schema.EdgeLabel.SHORTCUT.getLabel()).has(Schema.EdgeProperty.SHORTCUT_HASH.name(), hash)).
                    hasNext();

        if (!exists) {
            EdgeImpl edge = addEdge(fromRolePlayer, toRolePlayer, Schema.EdgeLabel.SHORTCUT);
            edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_NAME, relationType.getName());
            edge.setProperty(Schema.EdgeProperty.RELATION_ID, relation.getId());

            if (fromRolePlayer.getId() != null)
                edge.setProperty(Schema.EdgeProperty.FROM_ID, fromRolePlayer.getId());
            edge.setProperty(Schema.EdgeProperty.FROM_ROLE_NAME, fromRole.getName());

            if (toRolePlayer.getId() != null)
                edge.setProperty(Schema.EdgeProperty.TO_ID, toRolePlayer.getId());
            edge.setProperty(Schema.EdgeProperty.TO_ROLE_NAME, toRole.getName());

            edge.setProperty(Schema.EdgeProperty.FROM_TYPE_NAME, fromRolePlayer.getParentIsa().getName());
            edge.setProperty(Schema.EdgeProperty.TO_TYPE_NAME, toRolePlayer.getParentIsa().getName());
            edge.setProperty(Schema.EdgeProperty.SHORTCUT_HASH, hash);
        }
    }

    private String calculateShortcutHash(Relation relation, RelationType relationType, RoleType fromRole, Instance fromRolePlayer, RoleType toRole, Instance toRolePlayer){
        String hash = "";
        String relationIdValue = relationType.getId();
        String fromIdValue = fromRolePlayer.getId();
        String fromRoleValue = fromRole.getId();
        String toIdValue = toRolePlayer.getId();
        String toRoleValue = toRole.getId();
        String assertionIdValue = relation.getId();

        if(relationIdValue != null)
            hash += relationIdValue;
        if(fromIdValue != null)
            hash += fromIdValue;
        if(fromRoleValue != null)
            hash += fromRoleValue;
        if(toIdValue != null)
            hash += toIdValue;
        if(toRoleValue != null)
            hash += toRoleValue;
        hash += String.valueOf(assertionIdValue);

        return hash;
    }

    @Override
    public Relation getRelation(RelationType relationType, Map<RoleType, Instance> roleMap){
        String hash = RelationImpl.generateNewHash(relationType, roleMap);
        Concept concept = getConceptLog().getCachedRelation(hash);

        if(concept == null)
            concept = getConcept(Schema.ConceptProperty.INDEX, hash);

        if(concept == null)
            return null;
        return concept.asRelation();
    }

    @Override
    public void rollback() {
        try {
            getTinkerPopGraph().tx().rollback();
        } catch (UnsupportedOperationException e){
            throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage(getTinkerPopGraph().getClass().getName(), "rollback"));
        }
        getConceptLog().clearTransaction();
    }

    /**
     * Clears the graph completely.
     */
    @Override
    public void clear() {
        EngineCommunicator.contactEngine(getCommitLogEndPoint(), REST.HttpConn.DELETE_METHOD);
        clearGraph();
        finaliseClose(this::closePermanent, ErrorMessage.CLOSED_CLEAR.getMessage());
    }

    protected void clearGraph(){
        getTinkerPopGraph().traversal().V().drop().iterate();
    }

    /**
     * Closes the current graph, rendering it unusable.
     */
    @Override
    public void close() {
        getConceptLog().clearTransaction();
        closeGraph(ErrorMessage.CLOSED_USER.getMessage());
    }

    //Standard Close Operation Overridden in Titan
    public void closeGraph(String closedReason){
        finaliseClose(this::closePermanent, closedReason);
    }

    public void finaliseClose(Runnable closer, String closedReason){
        if(!isClosed()) {
            closer.run();
            localClosedReason.set(closedReason);
            localIsClosed.set(true);
        }
    }

    public void closePermanent(){
        try {
            getTinkerPopGraph().close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Commits the graph
     * @throws GraknValidationException when the graph does not conform to the object concept
     */
    @Override
    public final void commit() throws GraknValidationException {
        validateGraph();

        Map<Schema.BaseType, Set<String>> modifiedConcepts = new HashMap<>();
        Set<String> castings = getConceptLog().getModifiedCastingIds();
        Set<String> resources = getConceptLog().getModifiedResourceIds();

        if(castings.size() > 0)
            modifiedConcepts.put(Schema.BaseType.CASTING, castings);
        if(resources.size() > 0)
            modifiedConcepts.put(Schema.BaseType.RESOURCE, resources);

        LOG.debug("Graph is valid. Committing graph . . . ");
        commitTx();
        LOG.debug("Graph committed.");
        getConceptLog().clearTransaction();

        if(modifiedConcepts.size() > 0)
            submitCommitLogs(modifiedConcepts);
    }
    protected void commitTx(){
        try {
            getTinkerPopGraph().tx().commit();
        } catch (UnsupportedOperationException e){
            LOG.warn(ErrorMessage.TRANSACTIONS_NOT_SUPPORTED.getMessage(graph.getClass().getName()));
        }
        committed = true;
    }


    void validateGraph() throws GraknValidationException {
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            String error = ErrorMessage.VALIDATION.getMessage(errors.size());
            for (String s : errors) {
                error += s;
            }
            throw new GraknValidationException(error);
        }
    }

    private void submitCommitLogs(Map<Schema.BaseType, Set<String>> concepts){
        JSONArray jsonArray = new JSONArray();
        for (Map.Entry<Schema.BaseType, Set<String>> entry : concepts.entrySet()) {
            Schema.BaseType type = entry.getKey();

            for (String vertexId : entry.getValue()) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", vertexId);
                jsonObject.put("type", type.name());
                jsonArray.put(jsonObject);
            }

        }

        JSONObject postObject = new JSONObject();
        postObject.put("concepts", jsonArray);

        String result = EngineCommunicator.contactEngine(getCommitLogEndPoint(), REST.HttpConn.POST_METHOD, postObject.toString());
        LOG.debug("Response from engine [" + result + "]");
    }
    String getCommitLogEndPoint(){
        if(engine == null || Grakn.IN_MEMORY.equals(engine))
            return null;
        return engine + REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + keyspace;
    }

    //------------------------------------------ Fixing Code for Postprocessing ----------------------------------------
    /**
     * Merges duplicate castings if one is found.
     * @param castingId The id of the casting to check for duplicates
     * @return true if some castings were merged
     */
    public boolean fixDuplicateCasting(Object castingId){
        //Get the Casting
        ConceptImpl concept = getConceptByBaseIdentifier(castingId);
        if(concept == null || !concept.isCasting())
            return false;

        //Check if the casting has duplicates
        CastingImpl casting = concept.asCasting();
        InstanceImpl rolePlayer = casting.getRolePlayer();
        RoleType role = casting.getRole();

        //Traversal here is used to take advantage of vertex centric index
        List<Vertex> castingVertices = getTinkerPopGraph().traversal().V(rolePlayer.getBaseIdentifier()).
                inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                has(Schema.EdgeProperty.ROLE_TYPE.name(), role.getId()).otherV().toList();

        Set<CastingImpl> castings = castingVertices.stream().map( vertex -> elementFactory.buildCasting(vertex, null)).collect(Collectors.toSet());

        if(castings.size() < 2){
            return false;
        }

        //Fix the duplicates
        castings.remove(casting);
        Set<RelationImpl> duplicateRelations = mergeCastings(casting, castings);

        //Remove Redundant Relations
        deleteRelations(duplicateRelations);

        return true;
    }

    /**
     *
     * @param relations The duplicate relations to be merged
     */
    private void deleteRelations(Set<RelationImpl> relations){
        for (RelationImpl relation : relations) {
            String relationID = relation.getId();

            //Kill Shortcut Edges
            relation.rolePlayers().values().forEach(instance -> {
                if(instance != null) {
                    List<Edge> edges = getTinkerTraversal().
                            hasId(instance.getId()).
                            bothE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                            has(Schema.EdgeProperty.RELATION_ID.name(), relationID).toList();

                    edges.forEach(Element::remove);
                }
            });

            relation.deleteNode();
        }
    }

    /**
     *
     * @param mainCasting The main casting to absorb all of the edges
     * @param castings The castings to whose edges will be transferred to the main casting and deleted.
     * @return A set of possible duplicate relations.
     */
    private Set<RelationImpl> mergeCastings(CastingImpl mainCasting, Set<CastingImpl> castings){
        RoleType role = mainCasting.getRole();
        Set<RelationImpl> relations = mainCasting.getRelations();
        Set<RelationImpl> relationsToClean = new HashSet<>();

        for (CastingImpl otherCasting : castings) {
            //Transfer assertion edges
            for(RelationImpl otherRelation : otherCasting.getRelations()){
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
                    EdgeImpl assertionToCasting = addEdge(otherRelation, mainCasting, Schema.EdgeLabel.CASTING);
                    assertionToCasting.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId());
                }
            }

            getTinkerPopGraph().traversal().V(otherCasting.getBaseIdentifier()).next().remove();
        }

        return relationsToClean;
    }

    /**
     *
     * @param mainRelation The main relation to compare
     * @param otherRelation The relation to compare it with
     * @return True if the roleplayers of the relations are the same.
     */
    private boolean relationsEqual(Relation mainRelation, Relation otherRelation){
        return mainRelation.rolePlayers().equals(otherRelation.rolePlayers()) &&
                mainRelation.type().equals(otherRelation.type());
    }

    /**
     *
     * @param resourceIds The resourceIDs which possible contain duplicates.
     * @return True if a commit is required.
     */
    public boolean fixDuplicateResources(Set<Object> resourceIds){
        boolean commitRequired = false;

        Set<ResourceImpl> resources = new HashSet<>();
        for (Object resourceId : resourceIds) {
            ConceptImpl concept = getConceptByBaseIdentifier(resourceId);
            if(concept != null && concept.isResource()){
                resources.add((ResourceImpl) concept);
            }
        }

        Map<String, Set<ResourceImpl>> resourceMap = formatResourcesByType(resources);

        for (Map.Entry<String, Set<ResourceImpl>> entry : resourceMap.entrySet()) {
            Set<ResourceImpl> dups = entry.getValue();
            if(dups.size() > 1){ //Found Duplicate
                mergeResources(dups);
                commitRequired = true;
            }
        }

        return commitRequired;
    }

    /**
     *
     * @param resources A list of resources containing possible duplicates.
     * @return A map of resource indices to resources. If there is more than one resource for a specific
     *         resource index then there are duplicate resources which need to be merged.
     */
    private Map<String, Set<ResourceImpl>> formatResourcesByType(Set<ResourceImpl> resources){
        Map<String, Set<ResourceImpl>> resourceMap = new HashMap<>();

        resources.forEach(resource -> {
            String resourceKey = resource.getProperty(Schema.ConceptProperty.INDEX).toString();
            resourceMap.computeIfAbsent(resourceKey, (key) -> new HashSet<>()).add(resource);
        });

        return resourceMap;
    }

    /**
     *
     * @param resources A set of resources which should all be merged.
     */
    private void mergeResources(Set<ResourceImpl> resources){
        Iterator<ResourceImpl> it = resources.iterator();
        ResourceImpl<?> mainResource = it.next();

        while(it.hasNext()){
            ResourceImpl<?> otherResource = it.next();
            Collection<Relation> otherRelations = otherResource.relations();

            for (Relation otherRelation : otherRelations) {
                copyRelation(mainResource, otherResource, otherRelation);
            }

            otherResource.delete();
        }
    }

    /**
     *
     * @param main The main instance to possibly acquire a new relation
     * @param other The other instance which already posses the relation
     * @param otherRelation The other relation to potentially be absorbed
     */
    private void copyRelation(Instance main, Instance other, Relation otherRelation){
        RelationType relationType = otherRelation.type();
        Map<RoleType, Instance> rolePlayers = otherRelation.rolePlayers();

        //Replace all occurrences of other with main. That we we can quickly find out if the relation on main exists
        for (RoleType roleType : rolePlayers.keySet()) {
            if(rolePlayers.get(roleType).equals(other)){
                rolePlayers.put(roleType, main);
            }
        }

        Relation foundRelation = getRelation(relationType, rolePlayers);

        //Delete old Relation
        deleteRelations(Collections.singleton((RelationImpl) otherRelation));

        if(foundRelation != null){
            return;
        }

        //Relation was not found so create a new one
        Relation relation = relationType.addRelation();
        rolePlayers.entrySet().forEach(entry -> relation.putRolePlayer(entry.getKey(), entry.getValue()));
    }

}
