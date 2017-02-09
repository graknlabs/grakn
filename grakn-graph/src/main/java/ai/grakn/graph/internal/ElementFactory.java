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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * <p>
 *     Constructs Concepts And Edges
 * </p>
 *
 * <p>
 *     This class turns Tinkerpop {@link Vertex} and {@link org.apache.tinkerpop.gremlin.structure.Edge}
 *     into Grakn {@link Concept} and {@link EdgeImpl}.
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

    private <X extends ConceptImpl> X getOrBuildConcept(Vertex v, Function<Vertex, X> conceptBuilder){
        ConceptId conceptId = ConceptId.of(v.id().toString());

        if(!graknGraph.getConceptLog().isConceptCached(conceptId)){
            X newConcept = conceptBuilder.apply(v);
            graknGraph.getConceptLog().cacheConcept(newConcept);
        }

        X concept = graknGraph.getConceptLog().getCachedConcept(conceptId);

        //Only track concepts which have been modified.
        if(graknGraph.isConceptModified(concept)) {
            graknGraph.getConceptLog().trackConceptForValidation(concept);
        }

        return concept;
    }

    // ------------------------------------------- Building Castings  --------------------------------------------------
    CastingImpl buildCasting(Vertex vertex, RoleType type){
        return getOrBuildConcept(vertex, (v) -> new CastingImpl(graknGraph, v, type));
    }

    // ---------------------------------------- Building Resource Types  -----------------------------------------------
    <V> ResourceTypeImpl<V> buildResourceType(Vertex vertex, ResourceType<V> type, ResourceType.DataType<V> dataType, Boolean isUnique){
        return getOrBuildConcept(vertex, (v) -> new ResourceTypeImpl<>(graknGraph, v, type, dataType, isUnique));
    }

    // ------------------------------------------ Building Resources
    <V> ResourceImpl <V> buildResource(Vertex vertex, ResourceType<V> type, V value){
        return getOrBuildConcept(vertex, (v) -> new ResourceImpl<>(graknGraph, v, type, value));
    }

    // ---------------------------------------- Building Relation Types  -----------------------------------------------
    RelationTypeImpl buildRelationType(Vertex vertex, RelationType type, Boolean isImplicit){
        return getOrBuildConcept(vertex, (v) -> new RelationTypeImpl(graknGraph, v, type, isImplicit));
    }

    // -------------------------------------------- Building Relations
    RelationImpl buildRelation(Vertex vertex, RelationType type){
        return getOrBuildConcept(vertex, (v) -> new RelationImpl(graknGraph, v, type));
    }

    // ----------------------------------------- Building Entity Types  ------------------------------------------------
    EntityTypeImpl buildEntityType(Vertex vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> new EntityTypeImpl(graknGraph, v, type));
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(Vertex vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> new EntityImpl(graknGraph, v, type));
    }

    // ----------------------------------------- Building Rule Types  --------------------------------------------------
    RuleTypeImpl buildRuleType(Vertex vertex, RuleType type){
        return getOrBuildConcept(vertex, (v) -> new RuleTypeImpl(graknGraph, v, type));
    }

    // -------------------------------------------- Building Rules
    RuleImpl buildRule(Vertex vertex, RuleType type, Pattern lhs, Pattern rhs){
        return getOrBuildConcept(vertex, (v) -> new RuleImpl(graknGraph, v, type, lhs, rhs));
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    RoleTypeImpl buildRoleType(Vertex vertex, RoleType type, Boolean isImplicit){
        return getOrBuildConcept(vertex, (v) -> new RoleTypeImpl(graknGraph, v, type, isImplicit));
    }

    /**
     * Constructors are called directly because this is only called when reading a known vertex or concept.
     * Thus tracking the concept can be skipped.
     *
     * @param v A vertex of an unknown type
     * @return A concept built to the correct type
     */
    <X extends Concept> X buildConcept(Vertex v){
        Schema.BaseType type;
        try {
            type = Schema.BaseType.valueOf(v.label());
        } catch (IllegalArgumentException e){
            LOG.warn("Found vertex [" + v + "] which has an invalid base type [" + v.label() + "] ignoring . . . ");
            return null;
        }

        ConceptId conceptId = ConceptId.of(v.id());

        if(!graknGraph.getConceptLog().isConceptCached(conceptId)){
            ConceptImpl concept;
            switch (type) {
                case RELATION:
                    concept = new RelationImpl(graknGraph, v);
                    break;
                case CASTING:
                    concept = new CastingImpl(graknGraph, v);
                    break;
                case TYPE:
                    concept = new TypeImpl<>(graknGraph, v);
                    break;
                case ROLE_TYPE:
                    concept = new RoleTypeImpl(graknGraph, v);
                    break;
                case RELATION_TYPE:
                    concept = new RelationTypeImpl(graknGraph, v);
                    break;
                case ENTITY:
                    concept = new EntityImpl(graknGraph, v);
                    break;
                case ENTITY_TYPE:
                    concept = new EntityTypeImpl(graknGraph, v);
                    break;
                case RESOURCE_TYPE:
                    concept = new ResourceTypeImpl<>(graknGraph, v);
                    break;
                case RESOURCE:
                    concept = new ResourceImpl<>(graknGraph, v);
                    break;
                case RULE:
                    concept = new RuleImpl(graknGraph, v);
                    break;
                case RULE_TYPE:
                    concept = new RuleTypeImpl(graknGraph, v);
                    break;
                default:
                    throw new RuntimeException("Unknown base type");
            }
            graknGraph.getConceptLog().cacheConcept(concept);
        }

        return graknGraph.getConceptLog().getCachedConcept(conceptId);
    }

    EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractGraknGraph graknGraph){
        return new EdgeImpl(edge, graknGraph);
    }
}
