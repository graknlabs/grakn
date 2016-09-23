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

package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class WineInferenceTest {

    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph graph = GenericGraph.getGraph("wines-test.gql");
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testRecommendation() {
        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation;";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match $x isa person;$y isa wine;" +
                            "{$x id 'Bob';$y id 'White Champagne';} or" +
                        "{$x id 'Alice';$y id 'Cabernet Sauvignion';} or" +
                        "{$x id 'Charlie';$y id 'Pinot Grigio Rose';} or" +
                        "{$x id 'Denis';$y id 'Busuioaca Romaneasca';} or" +
                        "{$x id 'Eva';$y id 'Tamaioasa Romaneasca';} or" +
                        "{$x id 'Frank';$y id 'Riojo Blanco CVNE 2003';};";

        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
