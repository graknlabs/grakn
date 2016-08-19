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
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.WineGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class WineInferenceTest {

    private static Reasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        MindmapsTransaction graph = WineGraph.getTransaction();
        reasoner = new Reasoner(graph);
        qp = QueryParser.create(graph);
    }

    @Test
    public void testRecommendation() {

        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);

        String explicitQuery = "match $x isa person;$y isa wine;" +
                               "{$x value 'Alice';$y value 'Cabernet Sauvignion'} or" +
                "{$x value 'Bob';$y value 'White Champagne'} or" +
                "{$x value 'Charlie';$y value 'Pinot Grigio Rose'} or" +
                "{$x value 'Denis';$y value 'Busuioaca Romaneasca'} or" +
                "{$x value 'Eva';$y value 'Tamaioasa Romaneasca'} or" +
                "{$x value 'Frank';$y value 'Riojo Blanco CVNE 2003'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
