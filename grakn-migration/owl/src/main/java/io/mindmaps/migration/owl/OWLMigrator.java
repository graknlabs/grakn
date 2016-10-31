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
package io.mindmaps.migration.owl;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * 
 * <p>
 * The OWL migrator is the main driver an OWL migration process: configure with the ontology to migrate, the
 * target Mindmaps graph and instance and hit go with the {@link OWLMigrator#migrate()}
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class OWLMigrator {
    
    private Namer namer;
    private OWLOntology ontology;
    private MindmapsGraph graph;

    private <T> T eval(Supplier<T> f) {
        return f.get();
    }
    
    public OWLMigrator() {
        this.namer = new Namer(){};
    }

    public OWLMigrator namer(Namer namer) {
        this.namer = namer;
        return this;
    }
    
    public Namer namer() {
        return namer;
    }   
    
    public OWLMigrator ontology(OWLOntology ontology) {
        this.ontology = ontology;
        return this;
    }
    
    public OWLOntology ontology() {
        return this.ontology;
    }
    
    public OWLMigrator graph(MindmapsGraph graph) {
        this.graph = graph;
        return this;
    }
    
    public MindmapsGraph graph() {
        return graph;
    }
    
    public void migrate() throws MindmapsValidationException { 
        OwlMindmapsGraphStoringVisitor visitor = new OwlMindmapsGraphStoringVisitor(this);
        visitor.prepareOWL();
        ontology.axioms().forEach(ax -> {
            ax.accept(visitor); 
        });
        graph.commit();
    }

    public ResourceType.DataType<?> owlBuiltInToMindmapsDatatype(OWL2Datatype propertyType) {
        if (propertyType == OWL2Datatype.XSD_BOOLEAN)
            return ResourceType.DataType.BOOLEAN;
        else if (propertyType == OWL2Datatype.XSD_FLOAT || 
                 propertyType == OWL2Datatype.XSD_DOUBLE ||
                 propertyType == OWL2Datatype.OWL_REAL ||
                 propertyType == OWL2Datatype.OWL_RATIONAL ||
                 propertyType == OWL2Datatype.XSD_DECIMAL)
            return ResourceType.DataType.DOUBLE;
        else if (propertyType.isNumeric())
            return ResourceType.DataType.LONG;
        else
            return ResourceType.DataType.STRING;
    }
    
    public EntityType owlThingEntityType() {
        return graph.putEntityType(
                namer.classEntityTypeName(
                        ontology.getOWLOntologyManager().getOWLDataFactory().getOWLClass(
                                OwlModel.THING.owlname()).getIRI()));
    }
    
    public EntityType entityType(OWLClass owlclass) {
        EntityType type = graph.putEntityType(namer.classEntityTypeName(owlclass.getIRI()));
        EntityType thing = owlThingEntityType();
        if (type.superType() == null && !type.equals(thing))
            type.superType(thing);
        return type;
    }

    public Entity entity(OWLNamedIndividual individual) {
        String id = namer.individualEntityName(individual.getIRI());
        Entity entity = graph.getEntity(id);
        if (entity != null)
            return entity;
        OWLClass owlclass = eval(() -> {
            Optional<OWLClassAssertionAxiom> expr = ontology
                    .classAssertionAxioms(individual)
                    .filter(ax -> ax.getClassExpression().isOWLClass())
                    .findFirst();
            return expr.isPresent() ? expr.get().getClassExpression().asOWLClass() : null;
        });
        return graph.putEntity(id, owlclass == null ? owlThingEntityType() : entityType(owlclass));
    }   

    public RelationType relation(OWLObjectProperty property) {
        RelationType relType = graph.putRelationType(namer.objectPropertyName(property.getIRI()));
        RoleType subjectRole = subjectRole(relType);
        RoleType objectRole = objectRole(relType);
        relType.hasRole(subjectRole);
        relType.hasRole(objectRole);
        EntityType top = this.owlThingEntityType();
        top.playsRole(subjectRole);
        top.playsRole(objectRole);
        return relType;
    }

    public RelationType relation(OWLDataProperty property) {
        RelationType relType = graph.putRelationType(namer.resourceRelation(property.getIRI()));
        ResourceType<?> resourceType = resourceType(property);
        relType.hasRole(entityRole(owlThingEntityType(), resourceType));
        relType.hasRole(resourceRole(resourceType));
        return relType;     
    }

    public RelationType relation(OWLAnnotationProperty property) {
        RelationType relType = graph.putRelationType(namer.resourceRelation(property.getIRI()));
        ResourceType<?> resourceType = graph.putResourceType(namer.fromIri(property.getIRI()), ResourceType.DataType.STRING);
        relType.hasRole(entityRole(owlThingEntityType(), resourceType));
        relType.hasRole(resourceRole(resourceType));
        return relType;
    }
    
    public RoleType subjectRole(RelationType relType) {
        return graph.putRoleType(namer.subjectRole(relType.getId()));
    }

    public RoleType objectRole(RelationType relType) {
        return graph.putRoleType(namer.objectRole(relType.getId()));
    }

    public RoleType entityRole(EntityType entityType, ResourceType<?> resourceType) {
        RoleType roleType = graph.putRoleType(namer.entityRole(resourceType.getId()));
        entityType.playsRole(roleType);
        return roleType;
    }
    
    public RoleType resourceRole(ResourceType<?> resourceType) {
        RoleType roleType = graph.putRoleType(namer.resourceRole(resourceType.getId()));
        resourceType.playsRole(roleType);
        return roleType;
    }
    
    public ResourceType<?> resourceType(OWLDataProperty property) {
        OWL2Datatype propertyType= eval(() -> {         
            Optional<OWLDataPropertyRangeAxiom> ax = ontology.dataPropertyRangeAxioms(property)
                .filter(rangeAxiom -> rangeAxiom.getRange().isOWLDatatype() &&
                                      rangeAxiom.getRange().asOWLDatatype().isBuiltIn())
                .findFirst();
            return ax.isPresent() ? ax.get().getRange().asOWLDatatype().getBuiltInDatatype() : null;
        });
        ResourceType.DataType<?> mindmapsType = propertyType == null ? ResourceType.DataType.STRING : owlBuiltInToMindmapsDatatype(propertyType);
        ResourceType<?> resourceType = graph.putResourceType(namer.fromIri(property.getIRI()), mindmapsType);
        return resourceType;        
    }   
}