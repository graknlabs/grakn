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
package io.mindmaps.test.migration.export;

import io.mindmaps.example.PokemonGraphFactory;
import org.junit.Before;
import org.junit.Test;

public class PokemonGraphWriterTest extends GraphWriterTestBase {

    @Before
    public void setup(){
        PokemonGraphFactory.loadGraph(graph);
    }

    @Test
    public void testWritingPokemonGraphOntology(){
        String ontology = writer.dumpOntology();
        insert(copy, ontology);

        assertOntologiesEqual(graph, copy);
    }

    @Test
    public void testWritingPokemonGraphData(){
        String ontology = writer.dumpOntology();
        insert(copy, ontology);

        String data = writer.dumpData();
        insert(copy, data);

        assertDataEqual(graph, copy);
    }
}
