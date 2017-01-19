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

package ai.grakn.test.migration.csv;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFileAsString;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.migrate;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVMigratorTest {

    private GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Before
    public void setup(){
        graph = engine.graphWithNewKeyspace();
    }

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(graph, getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTemplate = "" +
                "insert $x isa pokemon                      " +
                "    has description <identifier>  \n" +
                "    has pokedex-no <id>           \n" +
                "    has height @int(<height>)       \n" +
                "    has weight @int(<weight>);        ";

        String pokemonTypeTemplate = "               " +
                "insert $x isa pokemon-type                 " +
                "   has type-id <id>                 " +
                "   has description <identifier>;    ";

        String edgeTemplate = "" +
                "match                                            " +
                "   $pokemon has pokedex-no <pokemon_id>        ; " +
                "   $type has type-id <type_id>                 ; " +
                "insert (pokemon-with-type: $pokemon, type-of-pokemon: $type) isa has-type;";

        migrate(graph, new CSVMigrator(pokemonTemplate, getFile("csv", "multi-file/data/pokemon.csv")));
        migrate(graph, new CSVMigrator(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv")));
        migrate(graph, new CSVMigrator(edgeTemplate, getFile("csv", "multi-file/data/edges.csv")));

        assertPokemonGraphCorrect(graph);
    }

    @Test
    public void quotesWithoutContentTest() throws IOException {
        load(graph, getFile("csv", "pets/schema.gql"));
        String template = getFileAsString("csv", "pets/template.gql");
        migrate(graph, new CSVMigrator(template, getFile("csv", "pets/data/pets.quotes")));
        assertPetGraphCorrect(graph);
    }

    @Test
    public void testMissingDataDoesNotThrowError() {
        load(graph, getFile("csv", "pets/schema.gql"));
        String template = getFileAsString("csv", "pets/template.gql");
        migrate(graph, new CSVMigrator(template, getFile("csv", "pets/data/pets.empty")).setNullString(""));

//        graph = factory.getGraph();
        Collection<Entity> pets = graph.getEntityType("pet").instances();
        assertEquals(1, pets.size());

        Collection<Entity> cats = graph.getEntityType("cat").instances();
        assertEquals(1, cats.size());

        ResourceType<String> name = graph.getResourceType("name");
        ResourceType<String> death = graph.getResourceType("death");

        Entity fluffy = name.getResource("Fluffy").ownerInstances().iterator().next().asEntity();
        assertEquals(1, fluffy.resources(death).size());
    }

    @Ignore //Ignored because this feature is not yet supported
    @Test
    public void multipleEntitiesInOneFileTest() throws IOException {
        load(graph, getFile("csv", "single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = getFileAsString("csv", "single-file/template.gql");
        migrate(graph, new CSVMigrator(template, getFile("csv", "single-file/data/cars.csv")));

        // test
        Collection<Entity> makes = graph.getEntityType("make").instances();
        assertEquals(3, makes.size());

        Collection<Entity> models = graph.getEntityType("model").instances();
        assertEquals(4, models.size());

        // test empty value not created
        ResourceType description = graph.getResourceType("description");

        Entity venture = graph.getConcept(ConceptId.of("Venture"));
        assertEquals(1, venture.resources(description).size());

        Entity ventureLarge = graph.getConcept(ConceptId.of("Venture Large"));
        assertEquals(0, ventureLarge.resources(description).size());
    }

    @Test
    public void testMigrateAsStringMethod(){
        load(graph, getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTypeTemplate = "insert $x isa pokemon-type has type-id @concat(@noescp(<id>), \"-type\") has description <identifier>;";
        String templated = new CSVMigrator(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv")).migrate()
                .map(InsertQuery::toString)
                .collect(joining("\n"));

        System.out.println(templated);
        String expected = "id \"17-type\"";
        assertTrue(templated.contains(expected));
    }
}
