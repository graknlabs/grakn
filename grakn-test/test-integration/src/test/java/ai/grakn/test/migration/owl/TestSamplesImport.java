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
package ai.grakn.test.migration.owl;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Role;
import ai.grakn.graql.internal.reasoner.rule.RuleGraph;
import ai.grakn.migration.owl.OwlModel;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.owlapi.model.OWLOntology;

import java.util.Optional;
import java.util.stream.Collectors;

import static ai.grakn.test.migration.MigratorTestUtils.assertRelationBetweenInstancesExists;
import static ai.grakn.test.migration.MigratorTestUtils.assertResourceEntityRelationExists;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Load and verify the ontology from the test sample resources. 
 * 
 * @author borislav
 *
 */
public class TestSamplesImport extends TestOwlGraknBase {

    @Ignore //TODO: Failing due to tighter temporary restrictions
    @Test
    public void testShoppingOntology()  {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "Shopping.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            graph = factory.open(GraknTxType.WRITE);
            EntityType type = graph.getEntityType("tMensWear");
            EntityType sub = graph.getEntityType("tTshirts");
            Assert.assertNotNull(type);
            Assert.assertNotNull(sub);
            assertThat(type.subs().collect(Collectors.toSet()), hasItem(sub));
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }       
    }

    @Ignore //TODO: Fix this test. Not sure why it is not working remotely
    @Test
    public void testShakespeareOntology()   {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "shakespeare.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            graph = factory.open(GraknTxType.WRITE);
            EntityType top = graph.getEntityType("tThing");
            EntityType type = graph.getEntityType("tAuthor");
            Assert.assertNotNull(type);
            Assert.assertNull(graph.getEntityType("http://www.workingontologist.org/Examples/Chapter3/shakespeare.owl#Author"));
            Assert.assertNotNull(type.sup());
            Assert.assertEquals("tPerson", type.sup().getLabel());
            Assert.assertEquals(top, type.sup().sup());
            assertTrue(top.subs().anyMatch(sub -> sub.equals(graph.getEntityType("tPlace"))));
            Assert.assertNotEquals(0, type.instances().count());

            assertTrue(
                type.instances()
                        .flatMap(inst -> inst
                                .attributes(graph.getAttributeType(OwlModel.IRI.owlname())))
                        .anyMatch(s -> s.getValue().equals("eShakespeare"))
            );
            final Entity author = getEntity("eShakespeare");
            Assert.assertNotNull(author);
            final Entity work = getEntity("eHamlet");
            Assert.assertNotNull(work);
            assertRelationBetweenInstancesExists(graph, work, author, Label.of("op-wrote"));
            Assert.assertTrue(RuleGraph.getRules(graph).findFirst().isPresent());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }       
    }
    
    @Test
    public void testProductOntology()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "Product.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            graph = factory.open(GraknTxType.WRITE);
            EntityType type = graph.getEntityType("tProduct");
            Assert.assertNotNull(type);
            Optional<Entity> e = findById(type.instances().collect(toSet()), "eProduct5");
            assertTrue(e.isPresent());
            e.get().attributes().map(Attribute::type).forEach(System.out::println);
            assertResourceEntityRelationExists(graph, "Product_Available", "14", e.get());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }   
    
    @Test
    public void test1Ontology() {       
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "test1.owl");
            O.axioms().forEach(System.out::println);            
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            graph = factory.open(GraknTxType.WRITE);
            migrator.graph(graph);
            EntityType type = migrator.entityType(owlManager().getOWLDataFactory().getOWLClass(OwlModel.THING.owlname()));          
            Assert.assertNotNull(type);         
            assertTrue(type.instances().flatMap(inst -> inst
                    .attributes(graph.getAttributeType(OwlModel.IRI.owlname())))
                    .anyMatch(s -> s.getValue().equals("eItem1")));

            Entity item1 = getEntity("eItem1");
            // Item1 name data property is "First Name"
            assertTrue(item1.attributes().anyMatch(r -> r.getValue().equals("First Item")));
            item1.attributes().forEach(System.out::println);
            Entity item2 = getEntity("eItem2");
            Role subjectRole = graph.getSchemaConcept(migrator.namer().subjectRole(Label.of("op-related")));
            Role objectRole = graph.getSchemaConcept(migrator.namer().objectRole(Label.of("op-related")));
            assertTrue(item2.relations(subjectRole).anyMatch(
                    relation -> item1.equals(relation.rolePlayers(objectRole).iterator().next())));
            Role catsubjectRole = graph.getSchemaConcept(migrator.namer().subjectRole(Label.of("op-hasCategory")));
            Role catobjectRole = graph.getSchemaConcept(migrator.namer().objectRole(Label.of("op-hasCategory")));
            assertTrue(catobjectRole.playedByTypes().collect(toSet()).contains(migrator.graph().getEntityType("tCategory")));
            assertTrue(catsubjectRole.playedByTypes().collect(toSet()).contains(migrator.graph().getEntityType("tThing")));
            //Assert.assertFalse(catobjectRole.playedByTypes().contains(migrator.graph().getEntityType("Thing")));

            Entity category2 = getEntity("eCategory2");
            assertTrue(category2.relations(catobjectRole).anyMatch(
                    relation -> item1.equals(relation.rolePlayers(catsubjectRole).iterator().next())));
            Entity category1 = getEntity("eCategory1");
            category1.attributes().forEach(System.out::println);
            // annotation assertion axioms don't seem to be visited for some reason...need to troubleshoot seems like 
            // OWLAPI issue
            //this.checkResource(category1, "comment", "category 1 comment");
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }

    @Ignore //TODO: Fix this test. Not sure why it is not working remotely
    @Test
    public void testFamilyOntology()   {
        // Load
        try {
            OWLOntology O = loadOntologyFromResource("owl", "family.owl");
            migrator.ontology(O).graph(graph).migrate();
            migrator.graph().commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
        // Verify
        try {
            EntityType type = migrator.graph().getEntityType("tPerson");
            Assert.assertNotNull(type);

            RelationshipType ancestor = migrator.graph().getRelationshipType("op-hasAncestor");
            RelationshipType isSiblingOf = migrator.graph().getRelationshipType("op-isSiblingOf");
            RelationshipType isAuntOf = migrator.graph().getRelationshipType("op-isAuntOf");
            RelationshipType isUncleOf = migrator.graph().getRelationshipType("op-isUncleOf");
            RelationshipType bloodRelation = migrator.graph().getRelationshipType("op-isBloodRelationOf");

            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(ancestor)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isSiblingOf)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isAuntOf)));
            assertTrue(bloodRelation.subs().anyMatch(sub -> sub.equals(isUncleOf)));

            assertTrue(RuleGraph.getRules(graph).findFirst().isPresent());
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.toString());
        }
    }
}