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

package io.mindmaps.dist;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.mindmaps.graql.Graql.var;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertTrue;

public class ExamplesTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        MindmapsGraph graph = Mindmaps.factory("in-memory", "my-graph").getGraph();
        qb = graph.graql();
    }

    @Test
    public void testModern() throws IOException {
        runInsertQuery("src/examples/modern.gql");
        assertTrue(qb.match(var().has("name", "marko").isa("person")).ask().execute());
    }

    @Test
    public void testPhilosophers() throws IOException {
        runInsertQuery("src/examples/philosophers.gql");
        assertTrue(qb.match(var().has("name", "Alexander").has("title", "Shah of Persia")).ask().execute());
    }

    @Test
    public void testPokemon() throws IOException {
        runInsertQuery("src/examples/pokemon.gql");
        assertTrue(qb.match(var().rel(var().has("name", "Pikachu")).rel(var().has("name", "electric")).isa("has-type")).ask().execute());
    }

    private void runInsertQuery(String path) throws IOException {
        String query = Files.readAllLines(Paths.get(path)).stream().collect(joining("\n"));
        qb.parse(query).execute();
    }
}
