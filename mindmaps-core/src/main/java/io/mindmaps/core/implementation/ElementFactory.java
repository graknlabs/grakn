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

package io.mindmaps.core.implementation;

import io.mindmaps.core.concept.Concept;
import io.mindmaps.core.concept.ResourceType;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Internal factory to produce different types of concepts
 */
final class ElementFactory {
    private final AbstractMindmapsGraph mindmapsGraph;

    public ElementFactory(AbstractMindmapsGraph mindmapsGraph){
        this.mindmapsGraph = mindmapsGraph;
    }

    public RelationImpl buildRelation(Vertex v){
        return new RelationImpl(v, mindmapsGraph);
    }
    public RelationImpl buildRelation(Concept c){
        return buildRelation(((ConceptImpl) c).getVertex());
    }

    public CastingImpl buildCasting(Vertex v){
        return new CastingImpl(v, mindmapsGraph);
    }
    public CastingImpl buildCasting(Concept c){
        return buildCasting(((ConceptImpl) c).getVertex());
    }

    public TypeImpl buildConceptType(Vertex v){
        return  new TypeImpl(v, mindmapsGraph);
    }
    public TypeImpl buildConceptType(Concept c){
        return buildConceptType(((ConceptImpl) c).getVertex());
    }

    public RuleTypeImpl buildRuleType(Vertex v){
        return  new RuleTypeImpl(v, mindmapsGraph);
    }
    public RuleTypeImpl buildRuleType(Concept c){
        return buildRuleType(((ConceptImpl) c).getVertex());
    }

    public RoleTypeImpl buildRoleType(Vertex v){
        return new RoleTypeImpl(v, mindmapsGraph);
    }
    public RoleTypeImpl buildRoleType(Concept c){
        return buildRoleType(((ConceptImpl) c).getVertex());
    }

    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v){
        return new ResourceTypeImpl<>(v, mindmapsGraph);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, ResourceType.DataType<V> type){
        return new ResourceTypeImpl<>(v, mindmapsGraph, type);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Concept c, ResourceType.DataType<V> type){
        return buildResourceType(((ConceptImpl) c).getVertex(), type);
    }

    public RelationTypeImpl buildRelationType(Vertex v){
        return  new RelationTypeImpl(v, mindmapsGraph);
    }
    public RelationTypeImpl buildRelationType(Concept c){
        return buildRelationType(((ConceptImpl) c).getVertex());
    }

    public EntityTypeImpl buildEntityType(Vertex v){
        return  new EntityTypeImpl(v, mindmapsGraph);
    }
    public EntityTypeImpl buildEntityType(Concept c){
        return buildEntityType(((ConceptImpl) c).getVertex());
    }

    public EntityImpl buildEntity(Vertex v){
        return  new EntityImpl(v, mindmapsGraph);
    }
    public EntityImpl buildEntity(Concept c){
        return buildEntity(((ConceptImpl) c).getVertex());
    }

    public <V> ResourceImpl <V> buildResource(Vertex v){
        return  new ResourceImpl<>(v, mindmapsGraph);
    }
    public <V> ResourceImpl <V> buildResource(Concept c){
        return  buildResource(((ConceptImpl) c).getVertex());
    }

    public RuleImpl buildRule(Vertex v){
        return buildRule(v, v.value(Schema.ConceptProperty.RULE_LHS.name()), v.value(Schema.ConceptProperty.RULE_RHS.name()));
    }
    public RuleImpl buildRule(Vertex v, String lhs, String rhs){
        return  new RuleImpl(v, mindmapsGraph, lhs, rhs);
    }
    public RuleImpl buildRule(ConceptImpl c){
        return  buildRule(c.getVertex(),
                c.getProperty(Schema.ConceptProperty.RULE_LHS).toString(),
                c.getProperty(Schema.ConceptProperty.RULE_RHS).toString());
    }


    public ConceptImpl buildUnknownConcept(Concept concept){
        return  buildUnknownConcept(((ConceptImpl) concept).getVertex());
    }

    /**
     *
     * @param v A vertex of an unknown type
     * @return A concept built to the correct type
     */
    public ConceptImpl buildUnknownConcept(Vertex v){
        Schema.BaseType type = Schema.BaseType.valueOf(v.label());
        ConceptImpl concept = null;
        switch (type){
            case RELATION:
                concept = buildRelation(v);
                break;
            case CASTING:
                concept = buildCasting(v);
                break;
            case TYPE:
                concept = buildConceptType(v);
                break;
            case ROLE_TYPE:
                concept = buildRoleType(v);
                break;
            case RELATION_TYPE:
                concept = buildRelationType(v);
                break;
            case ENTITY:
                concept = buildEntity(v);
                break;
            case ENTITY_TYPE:
                concept = buildEntityType(v);
                break;
            case RESOURCE_TYPE:
                concept = buildResourceType(v);
                break;
            case RESOURCE:
                concept = buildResource(v);
                break;
            case RULE:
                concept = buildRule(v);
                break;
            case RULE_TYPE:
                concept = buildRuleType(v);
                break;
        }
        return concept;
    }

    public TypeImpl buildSpecificConceptType(Concept concept){
        return buildSpecificConceptType(((ConceptImpl) concept).getVertex());
    }

    public TypeImpl buildSpecificConceptType(Vertex vertex){
        Schema.BaseType type = Schema.BaseType.valueOf(vertex.label());
        TypeImpl conceptType;
        switch (type){
            case ROLE_TYPE:
                conceptType = buildRoleType(vertex);
                break;
            case RELATION_TYPE:
                conceptType = buildRelationType(vertex);
                break;
            case RESOURCE_TYPE:
                conceptType = buildResourceType(vertex);
                break;
            case RULE_TYPE:
                conceptType = buildRuleType(vertex);
                break;
            case ENTITY_TYPE:
                conceptType = buildEntityType(vertex);
                break;
            default:
                conceptType = buildConceptType(vertex);
        }
        return conceptType;
    }


    public InstanceImpl buildSpecificInstance(Concept concept){
        return  buildSpecificInstance(((ConceptImpl) concept).getVertex());
    }

    public InstanceImpl buildSpecificInstance(Vertex vertex){
        Schema.BaseType type = Schema.BaseType.valueOf(vertex.label());
        InstanceImpl conceptInstance;
        switch (type){
            case RELATION:
                conceptInstance = buildRelation(vertex);
                break;
            case RESOURCE:
                conceptInstance = buildResource(vertex);
                break;
            case RULE:
                conceptInstance = buildRule(vertex);
                break;
            default:
                conceptInstance = buildEntity(vertex);
        }
        return conceptInstance;
    }

    public EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractMindmapsGraph mindmapsGraph){
        return new EdgeImpl(edge, mindmapsGraph);
    }
}
