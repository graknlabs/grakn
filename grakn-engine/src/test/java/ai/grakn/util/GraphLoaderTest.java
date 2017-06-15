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
package ai.grakn.util;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

//TODO: Move this test class to a lower dependency. Specifically Graql
public class GraphLoaderTest {

    //TODO: Put this somewhere common
    @Before
    public void setup(){
        GraknTestSetup.ensureCassandraRunning();
    }

    @Test
    public void whenCreatingEmptyGraph_EnsureGraphIsEmpty(){
        GraphLoader loader = GraphLoader.empty();

        try (GraknGraph graph = loader.graph()){
            assertThat(graph.admin().getMetaEntityType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRelationType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRuleType().instances(), is(empty()));
        }
    }

    @Test
    public void whenCreatingGraphWithPreLoader_EnsureGraphContainsPreloadedEntities(){
        Set<TypeLabel> labels = new HashSet<>(Arrays.asList(TypeLabel.of("1"), TypeLabel.of("2"), TypeLabel.of("3")));

        Consumer<GraknGraph> preLoader = graph -> labels.forEach(graph::putEntityType);

        GraphLoader loader = GraphLoader.preLoad(preLoader);

        try (GraknGraph graph = loader.graph()){
            Set<TypeLabel> foundLabels = graph.admin().getMetaEntityType().subTypes().stream().
                    map(Type::getLabel).collect(Collectors.toSet());

            assertTrue(foundLabels.containsAll(labels));
        }
    }

    @Test
    public void whenBuildingGraph_EnsureBackendMatchesTheTestProfile(){
        try(GraknGraph graph = GraphLoader.empty().graph()){
            //String comparison is used here because we do not have the class available at compile time
            if(GraknTestSetup.usingTinker()){
                assertEquals("ai.grakn.graph.internal.GraknTinkerGraph", graph.getClass().getName());
            } else if (GraknTestSetup.usingTitan()) {
                assertEquals("ai.grakn.graph.internal.GraknTitanGraph", graph.getClass().getName());
            } else if (GraknTestSetup.usingOrientDB()) {
                assertEquals("ai.grakn.graph.internal.GraknOrientDBGraph", graph.getClass().getName());
            } else {
                throw new RuntimeException("Test run with unsupported graph backend");
            }
        }
    }
}
