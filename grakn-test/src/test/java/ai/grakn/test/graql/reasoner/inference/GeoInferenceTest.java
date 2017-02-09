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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class GeoInferenceTest {

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Rule
    public final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @Test
    public void testQuery() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x isa city;$x has name $name;"+
                        "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                        "$y isa country;$y has name 'Poland'; select $x, $name;";

        String explicitQuery = "match " +
                "$x isa city;$x has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $x, $name;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQueryPrime() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $z1 isa city;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;$z2 isa country;$z2 has name 'Poland'; select $z1, $name;";
        String queryString2 = "match $z2 isa city;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;$z1 isa country;$z1 has name 'Poland'; select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa city;$z1 has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $z1, $name;";
        String explicitQuery2 = "match " +
                "$z2 isa city;$z2 has name $name;{$name value 'Warsaw';} or {$name value 'Wroclaw';};select $z2, $name;";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    @Test
    public void testQuery2() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $x isa university;$x has name $name;"+
                "(geo-entity: $x, entity-location: $y) isa is-located-in;"+
                "$y isa country;$y has name 'Poland'; select $x, $name;";
        String explicitQuery = "match " +
                "$x isa university;$x has name $name;" +
                "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';};";

        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
    }

    @Test
    public void testQuery2Prime() {
        QueryBuilder qb = geoGraph.graph().graql().infer(false);
        QueryBuilder iqb = geoGraph.graph().graql().infer(true);
        String queryString = "match $z1 isa university;$z1 has name $name;"+
                "($z1, $z2) isa is-located-in;$z2 isa country;$z2 has name 'Poland'; select $z1, $name;";
        String queryString2 = "match $z2 isa university;$z2 has name $name;"+
                "($z1, $z2) isa is-located-in;$z1 isa country;$z1 has name 'Poland'; select $z2, $name;";
        String explicitQuery = "match " +
                "$z1 isa university;$z1 has name $name;" +
                "{$z1 has name 'University-of-Warsaw';} or {$z1 has name'Warsaw-Polytechnics';};";
        String explicitQuery2 = "match " +
                "$z2 isa university;$z2 has name $name;" +
                "{$z2 has name 'University-of-Warsaw';} or {$z2 has name'Warsaw-Polytechnics';};";
        assertQueriesEqual(iqb.materialise(false).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(true).parse(queryString), qb.parse(explicitQuery));
        assertQueriesEqual(iqb.materialise(false).parse(queryString2), qb.parse(explicitQuery2));
        assertQueriesEqual(iqb.materialise(true).parse(queryString2), qb.parse(explicitQuery2));
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(queryAnswers(q1), queryAnswers(q2));
    }
}
