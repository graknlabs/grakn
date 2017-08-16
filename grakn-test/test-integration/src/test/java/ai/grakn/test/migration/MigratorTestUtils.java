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

package ai.grakn.test.migration;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.util.Schema;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MigratorTestUtils {

    public static String getFileAsString(String component, String fileName){
        try {
            return Files.readLines(getFile(component, fileName), StandardCharsets.UTF_8).stream().collect(joining());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File getFile(String component, String fileName){
        return new File(MigratorTestUtils.class.getResource(component + "/" + fileName).getPath());
    }

    public static void load(GraknSession factory, File ontology) {
        try(GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            graph.graql()
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException |InvalidGraphException e){
            throw new RuntimeException(e);
        }
    }

    public static void assertResourceEntityRelationExists(GraknGraph graph, String resourceName, Object resourceValue, Entity owner){
        AttributeType attributeType = graph.getResourceType(resourceName);
        assertNotNull(attributeType);
        assertEquals(resourceValue, owner.resources(attributeType)
                .map(Attribute::getValue)
                .findFirst().get());
    }

    public static void assertRelationBetweenInstancesExists(GraknGraph graph, Thing thing1, Thing thing2, Label relation){
        RelationType relationType = graph.getOntologyConcept(relation);

        Role role1 = thing1.plays().filter(r -> r.relationTypes().anyMatch(rel -> rel.equals(relationType))).findFirst().get();
        assertTrue(thing1.relations(role1).anyMatch(rel -> rel.rolePlayers().anyMatch(r -> r.equals(thing2))));
    }


    public static Thing getProperty(GraknGraph graph, Thing thing, String label) {
        assertEquals(getProperties(graph, thing, label).size(), 1);
        return getProperties(graph, thing, label).iterator().next();
    }

    public static Collection<Thing> getProperties(GraknGraph graph, Thing thing, String label) {
        RelationType relation = graph.getRelationType(label);

        Set<Thing> things = new HashSet<>();

        relation.instances()
                .filter(i -> i.rolePlayers().anyMatch(t -> t.equals(thing)))
                .forEach(i -> i.rolePlayers().forEach(things::add));

        things.remove(thing);
        return things;
    }

    public static Attribute getResource(GraknGraph graph, Thing thing, Label label) {
        assertEquals(getResources(graph, thing, label).count(), 1);
        return getResources(graph, thing, label).findAny().get();
    }

    public static Stream<Attribute> getResources(GraknGraph graph, Thing thing, Label label) {
        Role roleOwner = graph.getOntologyConcept(Schema.ImplicitType.HAS_OWNER.getLabel(label));
        Role roleOther = graph.getOntologyConcept(Schema.ImplicitType.HAS_VALUE.getLabel(label));

        Stream<Relation> relations = thing.relations(roleOwner);
        return relations.flatMap(r -> r.rolePlayers(roleOther)).map(Concept::asResource);
    }

    /**
     * Check that the pet graph has been loaded correctly
     */
    public static void assertPetGraphCorrect(GraknSession session){
        try(GraknGraph graph = session.open(GraknTxType.READ)) {
            Collection<Entity> pets = graph.getEntityType("pet").instances().collect(Collectors.toSet());
            assertEquals(9, pets.size());

            Collection<Entity> cats = graph.getEntityType("cat").instances().collect(Collectors.toSet());
            assertEquals(2, cats.size());

            Collection<Entity> hamsters = graph.getEntityType("hamster").instances().collect(Collectors.toSet());
            assertEquals(1, hamsters.size());

            AttributeType<String> name = graph.getResourceType("name");
            AttributeType<String> death = graph.getResourceType("death");

            Entity puffball = name.getResource("Puffball").ownerInstances().iterator().next().asEntity();
            assertEquals(0, puffball.resources(death).count());

            Entity bowser = name.getResource("Bowser").ownerInstances().iterator().next().asEntity();
            assertEquals(1, bowser.resources(death).count());
        }
    }

    /**
     * Check that the pokemon graph has been loaded correctly
     */
    public static void assertPokemonGraphCorrect(GraknSession session){
        try(GraknGraph graph = session.open(GraknTxType.READ)){
            Collection<Entity> pokemon = graph.getEntityType("pokemon").instances().collect(Collectors.toSet());
            assertEquals(9, pokemon.size());

            AttributeType<String> typeid = graph.getResourceType("type-id");
            AttributeType<String> pokedexno = graph.getResourceType("pokedex-no");

            Entity grass = typeid.getResource("12").ownerInstances().iterator().next().asEntity();
            Entity poison = typeid.getResource("4").ownerInstances().iterator().next().asEntity();
            Entity bulbasaur = pokedexno.getResource("1").ownerInstances().iterator().next().asEntity();
            RelationType relation = graph.getRelationType("has-type");

            assertNotNull(grass);
            assertNotNull(poison);
            assertNotNull(bulbasaur);

            assertRelationBetweenInstancesExists(graph, bulbasaur, grass, relation.getLabel());
            assertRelationBetweenInstancesExists(graph, bulbasaur, poison, relation.getLabel());
        }
    }
}
