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

package io.mindmaps.graph.internal;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.ConceptException;
import io.mindmaps.exception.ConceptNotUniqueException;
import io.mindmaps.exception.GraphRuntimeException;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.exception.MoreThanOneConceptException;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.QueryBuilderImpl;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.REST;
import io.mindmaps.util.Schema;
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

public abstract class AbstractMindmapsGraph<G extends Graph> implements MindmapsGraph {
    protected final Logger LOG = LoggerFactory.getLogger(AbstractMindmapsGraph.class);
    private final ThreadLocal<ConceptLog> context = new ThreadLocal<>();
    private final ElementFactory elementFactory;
    //private final ConceptLog conceptLog;
    private final String keyspace;
    private final String engine;
    private final boolean batchLoadingEnabled;
    private final G graph;

    public AbstractMindmapsGraph(G graph, String keyspace, String engine, boolean batchLoadingEnabled) {
        this.graph = graph;
        this.keyspace = keyspace;
        this.engine = engine;
        this.batchLoadingEnabled = batchLoadingEnabled;
        elementFactory = new ElementFactory(this);

        if(initialiseMetaConcepts()) {
            try {
                commit();
            } catch (MindmapsValidationException e) {
                throw new RuntimeException(ErrorMessage.CREATING_ONTOLOGY_ERROR.getMessage(e.getMessage()));
            }
        }
    }

    @Override
    public String getKeyspace(){
        return keyspace;
    }

    public boolean isBatchLoadingEnabled(){
        return batchLoadingEnabled;
    }

    @SuppressWarnings("unchecked")
    public boolean initialiseMetaConcepts(){
        if(isMetaOntologyNotInitialised()){
            TypeImpl type = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            type.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.TYPE.getId());

            TypeImpl entityType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            entityType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.ENTITY_TYPE.getId());

            TypeImpl relationType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            relationType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.RELATION_TYPE.getId());

            TypeImpl resourceType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            resourceType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.RESOURCE_TYPE.getId());

            TypeImpl roleType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            roleType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.ROLE_TYPE.getId());

            TypeImpl ruleType = elementFactory.buildConceptType(addVertex(Schema.BaseType.TYPE), null);
            ruleType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.RULE_TYPE.getId());

            RuleTypeImpl inferenceRuleType = elementFactory.buildRuleType(addVertex(Schema.BaseType.RULE_TYPE), ruleType);
            inferenceRuleType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.INFERENCE_RULE.getId());

            RuleTypeImpl constraintRuleType = elementFactory.buildRuleType(addVertex(Schema.BaseType.RULE_TYPE), ruleType);
            constraintRuleType.setProperty(Schema.ConceptProperty.ITEM_IDENTIFIER, Schema.MetaSchema.CONSTRAINT_RULE.getId());

            type.setType(type.getId());
            relationType.setType(type.getId());
            roleType.setType(type.getId());
            resourceType.setType(type.getId());
            ruleType.setType(type.getId());
            entityType.setType(type.getId());

            relationType.superType(type);
            roleType.superType(type);
            resourceType.superType(type);
            ruleType.superType(type);
            entityType.superType(type);

            return true;
        }

        return false;
    }

    public boolean isMetaOntologyNotInitialised(){
        return getMetaType() == null;
    }

    public G getTinkerPopGraph(){
        if(graph == null){
            throw new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this.getClass().getName()));
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

    public ConceptImpl getConcept(Schema.ConceptProperty key, String value) {
        Iterator<Vertex> vertices = getTinkerTraversal().has(key.name(), value);

        if(vertices.hasNext()){
            Vertex vertex = vertices.next();
            if(!isBatchLoadingEnabled() && vertices.hasNext())
                throw new MoreThanOneConceptException(ErrorMessage.TOO_MANY_CONCEPTS.getMessage(key.name(), value));
            return elementFactory.buildUnknownConcept(vertex);
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


    public Set<ConceptImpl> getModifiedConcepts(){
        return getConceptLog().getModifiedConcepts();
    }

    public Set<String> getModifiedCastingIds(){
        return getConceptLog().getModifiedCastingIds();
    }

    public Set<String> getModifiedResourceIds(){
        return getConceptLog().getModifiedResourceIds();
    }

    public ConceptLog getConceptLog() {
        ConceptLog conceptLog = context.get();
        if(conceptLog == null){
            context.set(conceptLog = new ConceptLog());
        }
        return conceptLog;
    }

    //----------------------------------------------Concept Functionality-----------------------------------------------
    //------------------------------------ Construction
    private Vertex addVertex(Schema.BaseType baseType){
        Vertex v = getTinkerPopGraph().addVertex(baseType.name());
        return v;
    }

    private Vertex putVertex(String itemIdentifier, Schema.BaseType baseType){
        if(Schema.MetaSchema.isMetaId(itemIdentifier)){
            throw new ConceptException(ErrorMessage.ID_RESERVED.getMessage(itemIdentifier));
        }

        Vertex vertex;
        ConceptImpl concept = getConcept(Schema.ConceptProperty.ITEM_IDENTIFIER, itemIdentifier);
        if(concept == null) {
            vertex = addVertex(baseType);
            vertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), itemIdentifier);
        } else {
            if(!baseType.name().equals(concept.getBaseType()))
                throw new ConceptNotUniqueException(concept, itemIdentifier);
            vertex = concept.getVertex();
        }
        return vertex;
    }

    @Deprecated
    @Override
    public Entity putEntity(String itemIdentifier, EntityType type) {
        return elementFactory.buildEntity(putVertex(itemIdentifier, Schema.BaseType.ENTITY), type);
    }

    @Override
    public Entity addEntity(EntityType type) {
        return elementFactory.buildEntity(addVertex(Schema.BaseType.ENTITY), type);
    }

    @Override
    public EntityType putEntityType(String itemIdentifier) {
        return putConceptType(itemIdentifier, Schema.BaseType.ENTITY_TYPE, getMetaEntityType()).asEntityType();
    }
    private TypeImpl putConceptType(String itemIdentifier, Schema.BaseType baseType, Type metaType) {
        return elementFactory.buildSpecificConceptType(putVertex(itemIdentifier, baseType), metaType);
    }

    @Override
    public RelationType putRelationType(String itemIdentifier) {
        return putConceptType(itemIdentifier, Schema.BaseType.RELATION_TYPE, getMetaRelationType()).asRelationType();
    }
    RelationType putRelationTypeImplicit(String itemIdentifier) {
        Vertex v = putVertex(itemIdentifier, Schema.BaseType.RELATION_TYPE);
        return elementFactory.buildRelationTypeImplicit(v, getMetaRelationType());
    }

    @Override
    public RoleType putRoleType(String itemIdentifier) {
        return putConceptType(itemIdentifier, Schema.BaseType.ROLE_TYPE, getMetaRoleType()).asRoleType();
    }
    RoleType putRoleTypeImplicit(String itemIdentifier) {
        Vertex v = putVertex(itemIdentifier, Schema.BaseType.ROLE_TYPE);
        return elementFactory.buildRoleTypeImplicit(v, getMetaRoleType());
    }

    @Override
    public <V> ResourceType <V> putResourceType(String id, ResourceType.DataType<V> dataType) {
        return elementFactory.buildResourceType(
                putConceptType(id, Schema.BaseType.RESOURCE_TYPE, getMetaResourceType()).getVertex(),
                getMetaResourceType(),
                dataType,
                false);
    }

    @Override
    public <V> ResourceType <V> putResourceTypeUnique(String id, ResourceType.DataType<V> dataType){
        return elementFactory.buildResourceType(
                putConceptType(id, Schema.BaseType.RESOURCE_TYPE, getMetaResourceType()).getVertex(),
                getMetaResourceType(),
                dataType,
                true);
    }

    @Override
    public <V> Resource<V> putResource(V value, ResourceType<V> type) {
        Resource<V> resource = getResource(value, type);
        if(resource == null){
            resource = elementFactory.buildResource(addVertex(Schema.BaseType.RESOURCE), type, value);
        }
        return resource;
    }

    @Override
    public RuleType putRuleType(String itemIdentifier) {
        return putConceptType(itemIdentifier, Schema.BaseType.RULE_TYPE, getMetaRuleType()).asRuleType();
    }

    @Deprecated
    @Override
    public Rule putRule(String itemIdentifier, String lhs, String rhs, RuleType type) {
        return elementFactory.buildRule(putVertex(itemIdentifier, Schema.BaseType.RULE), type, lhs, rhs);
    }

    @Override
    public Rule addRule(String lhs, String rhs, RuleType type) {
        return elementFactory.buildRule(addVertex(Schema.BaseType.RULE), type, lhs, rhs);
    }

    @Deprecated
    @Override
    public Relation putRelation(String itemIdentifier, RelationType type) {
        RelationImpl relation = elementFactory.buildRelation(putVertex(itemIdentifier, Schema.BaseType.RELATION), type);
        relation.setHash(null);
        return relation;
    }

    @Override
    public Relation addRelation(RelationType type) {
        RelationImpl relation = elementFactory.buildRelation(addVertex(Schema.BaseType.RELATION), type);
        relation.setHash(null);
        return relation;
    }

    //------------------------------------ Lookup
    @SuppressWarnings("unchecked")
    private <T extends Concept> T validConceptOfType(Concept concept, Class type){
        if(concept != null &&  type.isInstance(concept)){
            return (T) concept;
        }
        return null;
    }
    public ConceptImpl getConceptByBaseIdentifier(Object baseIdentifier) {
        GraphTraversal<Vertex, Vertex> traversal = getTinkerPopGraph().traversal().V(baseIdentifier);
        if (traversal.hasNext()) {
            return elementFactory.buildUnknownConcept(traversal.next());
        } else {
            return null;
        }
    }
    @Override
    public Concept getConcept(String id) {
        return getConcept(Schema.ConceptProperty.ITEM_IDENTIFIER, id);
    }

    @Override
    public Type getType(String id) {
        return validConceptOfType(getConcept(id), TypeImpl.class);
    }

    @Override
    public Instance getInstance(String id) {
        return validConceptOfType(getConcept(id), InstanceImpl.class);
    }

    @Override
    public Entity getEntity(String id) {
        return validConceptOfType(getConcept(id), EntityImpl.class);
    }

    @Override
    public <V> Resource<V> getResource(String id) {
        return validConceptOfType(getConcept(id), ResourceImpl.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Resource<V> getResource(V value, ResourceType<V> type){
        String index = ResourceImpl.generateResourceIndex(type.getId(), value.toString());
        ConceptImpl concept = getConcept(Schema.ConceptProperty.INDEX, index);
        if(concept != null){
            return concept.asResource();
        }
        return null;
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
    public Rule getRule(String id) {
        return validConceptOfType(getConcept(id), RuleImpl.class);
    }

    @Override
    public EntityType getEntityType(String id) {
        return validConceptOfType(getConcept(id), EntityTypeImpl.class);
    }

    @Override
    public RelationType getRelationType(String id) {
        return validConceptOfType(getConcept(id), RelationTypeImpl.class);
    }

    @Override
    public <V> ResourceType<V> getResourceType(String id) {
        return validConceptOfType(getConcept(id), ResourceTypeImpl.class);
    }

    @Override
    public RoleType getRoleType(String id) {
        return validConceptOfType(getConcept(id), RoleTypeImpl.class);
    }

    @Override
    public RuleType getRuleType(String id) {
        return validConceptOfType(getConcept(id), RuleTypeImpl.class);
    }

    private Type getConceptType(String id){
        return validConceptOfType(getConcept(id), TypeImpl.class);
    }

    @Override
    public Type getMetaType() {
        return getConceptType(Schema.MetaSchema.TYPE.getId());
    }

    @Override
    public Type getMetaRelationType() {
        return getConceptType(Schema.MetaSchema.RELATION_TYPE.getId());
    }

    @Override
    public Type getMetaRoleType() {
        return getConceptType(Schema.MetaSchema.ROLE_TYPE.getId());
    }

    @Override
    public Type getMetaResourceType() {
        return getConceptType(Schema.MetaSchema.RESOURCE_TYPE.getId());
    }

    @Override
    public Type getMetaEntityType() {
        return getConceptType(Schema.MetaSchema.ENTITY_TYPE.getId());
    }

    @Override
    public Type getMetaRuleType(){
        return getConceptType(Schema.MetaSchema.RULE_TYPE.getId());
    }

    @Override
    public RuleType getMetaRuleInference() {
        return getConceptType(Schema.MetaSchema.INFERENCE_RULE.getId()).asRuleType();
    }

    @Override
    public RuleType getMetaRuleConstraint() {
        return getConceptType(Schema.MetaSchema.CONSTRAINT_RULE.getId()).asRuleType();
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
    public CastingImpl putCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation){
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
        ((RelationImpl)relation).setHash(relation.rolePlayers());
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
            edge.setProperty(Schema.EdgeProperty.RELATION_TYPE_ID, relationType.getId());
            edge.setProperty(Schema.EdgeProperty.RELATION_ID, relation.getId());

            if (fromRolePlayer.getId() != null)
                edge.setProperty(Schema.EdgeProperty.FROM_ID, fromRolePlayer.getId());
            edge.setProperty(Schema.EdgeProperty.FROM_ROLE, fromRole.getId());

            if (toRolePlayer.getId() != null)
                edge.setProperty(Schema.EdgeProperty.TO_ID, toRolePlayer.getId());
            edge.setProperty(Schema.EdgeProperty.TO_ROLE, toRole.getId());

            edge.setProperty(Schema.EdgeProperty.FROM_TYPE, fromRolePlayer.getParentIsa().getId());
            edge.setProperty(Schema.EdgeProperty.TO_TYPE, toRolePlayer.getParentIsa().getId());
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
        Object assertionIdValue = ((ConceptImpl) relation).getBaseIdentifier();

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
        Concept concept = getConcept(Schema.ConceptProperty.INDEX, hash);
        if(concept == null)
            return null;
        return concept.asRelation();
    }

    @Override
    public Relation getRelation(String id) {
        ConceptImpl concept = getConcept(Schema.ConceptProperty.ITEM_IDENTIFIER, id);
        if(concept != null && Schema.BaseType.RELATION.name().equals(concept.getBaseType()))
            return concept.asRelation();
        else
            return null;
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
        getTinkerPopGraph().traversal().V().drop().iterate();
    }

    /**
     * Closes the current graph, rendering it unusable.
     */
    @Override
    public void close() {
        getConceptLog().clearTransaction();
        try {
            closeGraphTransaction();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    protected void closeGraphTransaction() throws Exception {
        graph.close();
    }

    /**
     * Commits the graph
     * @throws MindmapsValidationException when the graph does not conform to the object concept
     */
    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();

        Map<Schema.BaseType, Set<String>> modifiedConcepts = new HashMap<>();
        Set<String> castings = getModifiedCastingIds();
        Set<String> resources = getModifiedResourceIds();

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
    }


    void validateGraph() throws MindmapsValidationException {
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            String error = ErrorMessage.VALIDATION.getMessage(errors.size());
            for (String s : errors) {
                error += s;
            }
            throw new MindmapsValidationException(error);
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
        if(engine == null)
            return null;
        return engine + REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + keyspace;
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
                            has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), instance.getId()).
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
        Relation relation = addRelation(relationType);
        rolePlayers.entrySet().forEach(entry -> relation.putRolePlayer(entry.getKey(), entry.getValue()));
    }

}
