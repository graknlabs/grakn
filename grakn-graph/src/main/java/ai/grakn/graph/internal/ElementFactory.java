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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

import static ai.grakn.util.Schema.BaseType.RELATION_TYPE;
import static ai.grakn.util.Schema.BaseType.RULE_TYPE;

/**
 * <p>
 *     Constructs Concepts And Edges
 * </p>
 *
 * <p>
 *     This class turns Tinkerpop {@link Vertex} and {@link org.apache.tinkerpop.gremlin.structure.Edge}
 *     into Grakn {@link Concept} and {@link EdgeElement}.
 *
 *     Construction is only successful if the vertex and edge properties contain the needed information.
 *     A concept must include a label which is a {@link ai.grakn.util.Schema.BaseType}.
 *     An edge must include a label which is a {@link ai.grakn.util.Schema.EdgeLabel}.
 * </p>
 *
 * @author fppt
 */
final class ElementFactory {
    private final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final AbstractGraknGraph graknGraph;

    ElementFactory(AbstractGraknGraph graknGraph){
        this.graknGraph = graknGraph;
    }

    private <X extends Concept> X getOrBuildConcept(VertexElement v, Function<VertexElement, X> conceptBuilder){
        ConceptId conceptId = ConceptId.of(v.id().toString());

        if(!graknGraph.txCache().isConceptCached(conceptId)){
            X newConcept = conceptBuilder.apply(v);
            graknGraph.txCache().cacheConcept(newConcept);
        }

        X concept = graknGraph.txCache().getCachedConcept(conceptId);

        //Only track concepts which have been modified.
        if(graknGraph.isConceptModified(concept)) {
            graknGraph.txCache().trackForValidation(concept);
        }

        return concept;
    }

    // ---------------------------------------- Building Resource Types  -----------------------------------------------
    <V> ResourceTypeImpl<V> buildResourceType(VertexElement vertex, ResourceType<V> type, ResourceType.DataType<V> dataType){
        return getOrBuildConcept(vertex, (v) -> new ResourceTypeImpl<>(v, type, dataType));
    }

    // ------------------------------------------ Building Resources
    <V> ResourceImpl <V> buildResource(VertexElement vertex, ResourceType<V> type, V value){
        return getOrBuildConcept(vertex, (v) -> new ResourceImpl<>(v, type, value));
    }

    // ---------------------------------------- Building Relation Types  -----------------------------------------------
    RelationTypeImpl buildRelationType(VertexElement vertex, RelationType type, Boolean isImplicit){
        return getOrBuildConcept(vertex, (v) -> new RelationTypeImpl(v, type, isImplicit));
    }

    // -------------------------------------------- Building Relations
    RelationImpl buildRelation(VertexElement vertex, RelationType type){
        return getOrBuildConcept(vertex, (v) -> new RelationImpl(new RelationReified(v, type)));
    }

    // ----------------------------------------- Building Entity Types  ------------------------------------------------
    EntityTypeImpl buildEntityType(VertexElement vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> new EntityTypeImpl(v, type));
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(VertexElement vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> new EntityImpl(v, type));
    }

    // ----------------------------------------- Building Rule Types  --------------------------------------------------
    RuleTypeImpl buildRuleType(VertexElement vertex, RuleType type){
        return getOrBuildConcept(vertex, (v) -> new RuleTypeImpl(v, type));
    }

    // -------------------------------------------- Building Rules
    RuleImpl buildRule(VertexElement vertex, RuleType type, Pattern when, Pattern then){
        return getOrBuildConcept(vertex, (v) -> new RuleImpl(v, type, when, then));
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    RoleImpl buildRole(VertexElement vertex, Role type, Boolean isImplicit){
        return getOrBuildConcept(vertex, (v) -> new RoleImpl(v, type, isImplicit));
    }

    /**
     * Constructors are called directly because this is only called when reading a known vertex or concept.
     * Thus tracking the concept can be skipped.
     *
     * @param v A vertex of an unknown type
     * @return A concept built to the correct type
     */
    @Nullable
    <X extends Concept> X buildConcept(Vertex v){
        return buildConcept(buildVertexElement(v));
    }

    @Nullable
    <X extends Concept> X buildConcept(VertexElement vertexElement){
        Schema.BaseType type;

        try {
            type = getBaseType(vertexElement);
        } catch (IllegalStateException e){
            LOG.warn("Invalid vertex [" + vertexElement + "] due to " + e.getMessage(), e);
            return null;
        }

        ConceptId conceptId = ConceptId.of(vertexElement.id().getValue());
        if(!graknGraph.txCache().isConceptCached(conceptId)){
            Concept concept;
            switch (type) {
                case RELATION:
                    concept = new RelationImpl(new RelationReified(vertexElement));
                    break;
                case TYPE:
                    concept = new TypeImpl<>(vertexElement);
                    break;
                case ROLE:
                    concept = new RoleImpl(vertexElement);
                    break;
                case RELATION_TYPE:
                    concept = new RelationTypeImpl(vertexElement);
                    break;
                case ENTITY:
                    concept = new EntityImpl(vertexElement);
                    break;
                case ENTITY_TYPE:
                    concept = new EntityTypeImpl(vertexElement);
                    break;
                case RESOURCE_TYPE:
                    concept = new ResourceTypeImpl<>(vertexElement);
                    break;
                case RESOURCE:
                    concept = new ResourceImpl<>(vertexElement);
                    break;
                case RULE:
                    concept = new RuleImpl(vertexElement);
                    break;
                case RULE_TYPE:
                    concept = new RuleTypeImpl(vertexElement);
                    break;
                default:
                    throw new RuntimeException("Unknown base type [" + vertexElement.label() + "]");
            }
            graknGraph.txCache().cacheConcept(concept);
        }
        return graknGraph.txCache().getCachedConcept(conceptId);
    }

    /**
     * This is a helper method to get the base type of a vertex.
     * It first tried to get the base type via the label.
     * If this is not possible it then tries to get the base type via the Shard Edge.
     *
     * @param vertex The vertex to build a concept from
     * @return The base type of the vertex, if it is a valid concept.
     */
    private Schema.BaseType getBaseType(VertexElement vertex){
        try {
            return Schema.BaseType.valueOf(vertex.label());
        } catch (IllegalArgumentException e){
            //Base type appears to be invalid. Let's try getting the type via the shard edge
            Optional<EdgeElement> type = vertex.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD).findAny();

            if(type.isPresent()){
                String label = type.get().target().label();
                if(label.equals(Schema.BaseType.ENTITY_TYPE.name())) return Schema.BaseType.ENTITY;
                if(label.equals(RELATION_TYPE.name())) return Schema.BaseType.RELATION;
                if(label.equals(Schema.BaseType.RESOURCE_TYPE.name())) return Schema.BaseType.RESOURCE;
                if(label.equals(RULE_TYPE.name())) return Schema.BaseType.RULE;
            }
        }
        throw new IllegalStateException("Could not determine the base type of vertex [" + vertex + "]");
    }

    // ---------------------------------------- Non Concept Construction -----------------------------------------------
    EdgeElement buildEdge(Edge edge){
        return new EdgeElement(graknGraph, edge);
    }

    Casting buildRolePlayer(Edge edge){
        return buildRolePlayer(buildEdge(edge));
    }

    Casting buildRolePlayer(EdgeElement edge) {
        return new Casting(edge);
    }

    Shard buildShard(ConceptImpl shardOwner, VertexElement vertexElement){
        return new Shard(shardOwner, vertexElement);
    }

    Shard buildShard(VertexElement vertexElement){
        return new Shard(vertexElement);
    }

    Shard buildShard(Vertex vertex){
        return new Shard(buildVertexElement(vertex));
    }

    @Nullable
    VertexElement buildVertexElement(Vertex vertex){
        try {
            graknGraph.validVertex(vertex);
        } catch (IllegalStateException e){
            LOG.warn("Invalid vertex [" + vertex + "] due to " + e.getMessage(), e);
            return null;
        }

        return new VertexElement(graknGraph, vertex);
    }
}
