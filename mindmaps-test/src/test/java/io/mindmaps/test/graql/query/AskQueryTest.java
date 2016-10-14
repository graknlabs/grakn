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

package io.mindmaps.test.graql.query;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AskQueryTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private QueryBuilder qb;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        qb = withGraph(mindmapsGraph);
    }

    @Test
    public void testPositiveQuery() {
        assertTrue(qb.match(var().isa("movie").has("tmdb-vote-count", 1000)).ask().execute());
    }

    @Test
    public void testNegativeQuery() {
        assertFalse(qb.match(var("y").isa("award")).ask().execute());
    }

    @Test
    public void testNegativeQueryShortcutEdge() {
        assertFalse(qb.match(
                var().rel("x").rel("y").isa("directed-by"),
                var("x").value("Apocalypse Now"),
                var("y").value("Martin Sheen")
        ).ask().execute());
    }
}
