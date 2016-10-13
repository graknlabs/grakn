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

import io.mindmaps.concept.*;
import io.mindmaps.concept.EntityType;
import io.mindmaps.exception.ConceptException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.semanticweb.owlapi.model.AsOWLObjectProperty;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLAxiomVisitorEx;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLEntityVisitorEx;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubDataPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLReflexiveObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLInverseObjectPropertiesAxiom;
import org.semanticweb.owlapi.model.OWLEquivalentObjectPropertiesAxiom;

import java.util.Optional;

import static io.mindmaps.graql.internal.reasoner.Utility.createPropertyChainRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createReflexiveRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createSubPropertyRule;
import static io.mindmaps.graql.internal.reasoner.Utility.createTransitiveRule;



/**
 * <p>
 * This is the main class processing an OWL ontology file. It implements the OWLAPI visitor interfaces
 * to traverse all axioms and entities in the ontology and convert them into equivalent Mindmaps elements.
 * </p>
 * <p>
 * TODO - a lot of the logical more advanced axioms are skipped for now, waiting for the Mindmaps reasoning engine
 * to mature a bit. 
 * </p>
 * 
 * @author borislav
 *
 */
public class OwlMindmapsGraphStoringVisitor implements OWLAxiomVisitorEx<Concept>, OWLEntityVisitorEx<Concept> {
    private OWLMigrator migrator;   
            
    public OwlMindmapsGraphStoringVisitor(OWLMigrator migrator) {
        this.migrator = migrator;
    }
    
    public OwlMindmapsGraphStoringVisitor prepareOWL() {
        migrator.entityType(migrator.ontology().getOWLOntologyManager().getOWLDataFactory().getOWLClass(OwlModel.THING.owlname()));
        migrator.relation(migrator.ontology().getOWLOntologyManager().getOWLDataFactory().getOWLObjectProperty(OwlModel.OBJECT_PROPERTY.owlname()))
          .hasRole(migrator.graph().putRoleType(OwlModel.OBJECT.owlname()))
          .hasRole(migrator.graph().putRoleType(OwlModel.SUBJECT.owlname()));
        return this;
    }
    
    @Override
    public Concept visit(OWLClass ce) {
        return migrator.entityType(ce);
    }

    @Override
    public Concept visit(OWLObjectProperty property) {
        return migrator.relation(property);
    }


    @Override
    public Concept visit(OWLDataProperty property) {
        return migrator.resourceType(property);
    }


    @Override
    public Concept visit(OWLAnnotationProperty property) {
        return migrator.relation(property);
    }


    @Override
    public Concept visit(OWLNamedIndividual individual) {
        return migrator.entity(individual);
    }

    @Override
    public Concept visit(OWLDeclarationAxiom axiom) {
        return axiom.getEntity().accept(this);
    }

    @Override
    public Concept visit(OWLSubClassOfAxiom axiom) {
        OWLClassExpression subclass = axiom.getSubClass();
        EntityType subtype = null;
        if (subclass.isOWLClass())
            subtype = migrator.entityType(subclass.asOWLClass());
        else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        OWLClassExpression superclass = axiom.getSuperClass();
        EntityType supertype = null;
        if (superclass.isOWLClass())
            supertype = migrator.entityType(superclass.asOWLClass());
        else {
            // TODO - we need a strategy to support class expressions, e.g. as constraints 
            // on instances
            return null;
        }
        if (!supertype.equals(subtype.superType()))
            subtype.superType(supertype);
        return null;
    }

    @Override
    public Concept visit(OWLObjectPropertyDomainAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType objectRelation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        if (axiom.getDomain().isOWLClass()) {           
            EntityType entityType = migrator.entityType(axiom.getDomain().asOWLClass());
            RoleType domain = migrator.subjectRole(objectRelation);
            migrator.owlThingEntityType().deletePlaysRole(domain);
            entityType.playsRole(domain);
            objectRelation.hasRole(domain);
//          System.out.println("Replaced domain thing with " + entityType.getId());
        }
        return objectRelation;
    }

    @Override
    public Concept visit(OWLObjectPropertyRangeAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType objectRelation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        if (axiom.getRange().isOWLClass()) {
            EntityType entityType = migrator.entityType(axiom.getRange().asOWLClass());
            RoleType range = migrator.objectRole(objectRelation);
            objectRelation.hasRole(range);          
            migrator.owlThingEntityType().deletePlaysRole(range);
            entityType.playsRole(range);
        }       
        return objectRelation;
    }

    @Override
    public Concept visit(OWLSubObjectPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLObjectProperty() || !axiom.getSuperProperty().isOWLObjectProperty())
            return null;
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLObjectProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());

        Map<String, String> roleMap = new HashMap<>();
        roleMap.put(migrator.namer().subjectRole(superRelation.getId()), migrator.namer().subjectRole(subRelation.getId()));
        roleMap.put(migrator.namer().objectRole(superRelation.getId()), migrator.namer().objectRole(subRelation.getId()));
        createSubPropertyRule("sub-" + superRelation.getId() + UUID.randomUUID().toString(),
                                superRelation, subRelation, roleMap, migrator.graph());

        subRelation.superType(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLSubDataPropertyOfAxiom axiom) {
        if (!axiom.getSubProperty().isOWLDataProperty() || !axiom.getSuperProperty().isOWLDataProperty())
            return null;
        RelationType subRelation = migrator.relation(axiom.getSubProperty().asOWLDataProperty());
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLDataProperty());
        subRelation.superType(superRelation);
        return null;
    }

    @Override
    public Concept visit(OWLEquivalentObjectPropertiesAxiom axiom) {
        Set<OWLObjectPropertyExpression> properties = axiom.getAxiomWithoutAnnotations()
                            .properties().filter(AsOWLObjectProperty::isOWLObjectProperty).collect(Collectors.toSet());
        if (properties.size() != axiom.getAxiomWithoutAnnotations().properties().count())
            return null;

        Iterator<OWLObjectPropertyExpression> it = properties.iterator();
        while(it.hasNext()){
            RelationType relation = migrator.relation(it.next().asOWLObjectProperty());
            properties.forEach(prop -> {
                RelationType eqRelation = migrator.relation(prop.asOWLObjectProperty());
                if (!relation.equals(eqRelation)){
                    Map<String, String> roleMap = new HashMap<>();
                    roleMap.put(migrator.namer().subjectRole(relation.getId()),
                            migrator.namer().subjectRole(eqRelation.getId()));
                    roleMap.put(migrator.namer().objectRole(relation.getId()),
                            migrator.namer().objectRole(eqRelation.getId()));
                    createSubPropertyRule("eq-" + relation.getId() + "-" + eqRelation.getId()
                                        + "-" + UUID.randomUUID().toString(), relation, eqRelation, roleMap, migrator.graph());
                }
            });
        }
        return null;
    }

    @Override
    public Concept visit(OWLInverseObjectPropertiesAxiom axiom) {
        if (!axiom.getFirstProperty().isOWLObjectProperty() || !axiom.getSecondProperty().isOWLObjectProperty())
            return null;
        RelationType relation = migrator.relation(axiom.getFirstProperty().asOWLObjectProperty());
        RelationType inverseRelation = migrator.relation(axiom.getSecondProperty().asOWLObjectProperty());

        Map<String, String> roleMapFD = new HashMap<>();
        roleMapFD.put(migrator.namer().subjectRole(relation.getId()), migrator.namer().objectRole(inverseRelation.getId()));
        roleMapFD.put(migrator.namer().objectRole(relation.getId()), migrator.namer().subjectRole(inverseRelation.getId()));
        createSubPropertyRule("inv-" + relation.getId() + "-" + UUID.randomUUID().toString(), relation, inverseRelation, roleMapFD, migrator.graph());

        Map<String, String> roleMapBD = new HashMap<>();
        roleMapBD.put(migrator.namer().subjectRole(inverseRelation.getId()), migrator.namer().objectRole(relation.getId()));
        roleMapBD.put(migrator.namer().objectRole(inverseRelation.getId()), migrator.namer().subjectRole(relation.getId()));
        createSubPropertyRule("inv-" + inverseRelation.getId() + "-" + UUID.randomUUID().toString(), inverseRelation, relation, roleMapBD, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLTransitiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        createTransitiveRule("trst-" + relation.getId() + "-" + UUID.randomUUID().toString(), relation, migrator.namer().subjectRole(relation.getId()),
                migrator.namer().objectRole(relation.getId()), migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLReflexiveObjectPropertyAxiom axiom) {
        if (!axiom.getProperty().isOWLObjectProperty())
            return null;
        RelationType relation = migrator.relation(axiom.getProperty().asOWLObjectProperty());
        createReflexiveRule("rflx-" + relation.getId() + "-" + UUID.randomUUID().toString(), relation, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLSubPropertyChainOfAxiom axiom) {
        if (!axiom.getSuperProperty().isOWLObjectProperty())
            return null;
        RelationType superRelation = migrator.relation(axiom.getSuperProperty().asOWLObjectProperty());
        LinkedHashMap<RelationType, Pair<String, String>> chain = new LinkedHashMap<>();

        axiom.getPropertyChain().forEach(property -> {
            RelationType relation = migrator.relation(property.asOWLObjectProperty());
            chain.put(relation,  new Pair<>(migrator.namer().subjectRole(relation.getId()), migrator.namer().objectRole(relation.getId())));
        });

        createPropertyChainRule("pch-" + superRelation.getId() + "-" + UUID.randomUUID().toString(), superRelation,
                migrator.namer().subjectRole(superRelation.getId()), migrator.namer().objectRole(superRelation.getId()), chain, migrator.graph());
        return null;
    }

    @Override
    public Concept visit(OWLClassAssertionAxiom axiom) {
        if (!axiom.getIndividual().isNamed())
            return null;
        else
            return migrator.entity(axiom.getIndividual().asOWLNamedIndividual());
    }

    @Override
    public Concept visit(OWLObjectPropertyAssertionAxiom axiom) {
        if (!axiom.getSubject().isNamed() || 
            !axiom.getObject().isNamed() || 
            !axiom.getProperty().isOWLObjectProperty()) {
            return null;
        }
        Entity subject = migrator.entity(axiom.getSubject().asOWLNamedIndividual());
        Entity object = migrator.entity(axiom.getObject().asOWLNamedIndividual());
        RelationType relationType = migrator.relation(axiom.getProperty().asOWLObjectProperty());       
        return migrator.graph().addRelation(relationType)
                 .putRolePlayer(migrator.subjectRole(relationType), subject)
                 .putRolePlayer(migrator.objectRole(relationType), object);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Concept visit(OWLDataPropertyAssertionAxiom axiom) {
        if (!axiom.getProperty().isOWLDataProperty() || !axiom.getSubject().isNamed()) {
            return null;
        }
        ResourceType resourceType = migrator.resourceType(axiom.getProperty().asOWLDataProperty());
        Entity entity = migrator.entity(axiom.getSubject().asOWLNamedIndividual());
        String valueAsString =  axiom.getObject().getLiteral();
        Object value = valueAsString;
        if (resourceType.getDataType() == ResourceType.DataType.BOOLEAN)
            value = Boolean.parseBoolean(valueAsString);
        else if (resourceType.getDataType() == ResourceType.DataType.LONG)
            value = Long.parseLong(valueAsString);
        else if (resourceType.getDataType() == ResourceType.DataType.DOUBLE)
            value = Double.parseDouble(valueAsString);
        Resource resource = migrator.graph().putResource(value, resourceType);
        RelationType propertyRelation = migrator.relation(axiom.getProperty().asOWLDataProperty());
        RoleType entityRole = migrator.entityRole(entity.type(), resource.type());
        RoleType resourceRole = migrator.resourceRole(resource.type());
        try {       
            return migrator.graph().addRelation(propertyRelation)
                     .putRolePlayer(entityRole, entity)
                     .putRolePlayer(resourceRole, resource);
        }
        catch (ConceptException ex) {
            if (ex.getMessage().contains("The Relation with the provided role players already exists"))
                System.err.println("[WARN] Mindmaps does not support multiple values per data property/resource, ignoring axiom " + axiom);
            else
                ex.printStackTrace(System.err);
            return null;
        }
    }

    @Override 
    public Concept visit(OWLAnnotationAssertionAxiom axiom) {
        if (! (axiom.getSubject() instanceof OWLNamedIndividual) )
            return null;
        Optional<OWLLiteral> value = axiom.getValue().asLiteral();
        if (!value.isPresent())
            return null;
        @SuppressWarnings("unchecked")
        ResourceType<String> resourceType = (ResourceType<String>)visit(axiom.getProperty());
        Entity entity = migrator.entity((OWLNamedIndividual)axiom.getSubject());
        Resource<String> resource = migrator.graph().putResource(value.get().getLiteral(), resourceType);
        RelationType propertyRelation = migrator.relation(axiom.getProperty());
        return migrator.graph().addRelation(propertyRelation)
                 .putRolePlayer(migrator.entityRole(entity.type(), resource.type()), entity)
                 .putRolePlayer(migrator.resourceRole(resource.type()), resource);
    }   
    
    
}