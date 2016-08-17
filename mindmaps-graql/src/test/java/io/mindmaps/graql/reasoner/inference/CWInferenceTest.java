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
import io.mindmaps.core.model.Type;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.MindmapsReasoner;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.reasoner.graphs.CWGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class CWInferenceTest {

    private static MindmapsTransaction graph;
    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = CWGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }

    @Test
    public void testWeapon() {
        String queryString = "match $x isa weapon";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match \n" +
                "{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';}\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testTransactionQuery() {
        String queryString = "match" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "($x, $y, $z) isa transaction";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match \n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{($x, $y, $z) isa transaction} or {\n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "}} or {{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';};};\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testTransactionQuery2() {
        String queryString = "match" +
                       "$x isa person;$z isa country;$y isa weapon;\n" +
                        "($x, $y, $z) isa transaction";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match \n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "};\n" +
                "{($x, $y, $z) isa transaction} or {\n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "}} or {{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';};};\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testQuery()
    {
        String queryString = "match $x isa criminal;";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match " +
                "{$x isa criminal} or {" +
                "$x has nationality 'American';\n" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person;\n" +
                    "$z isa country;\n" +
                    "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                    "($x, $z) isa is-paid-by;\n" +
                    "($z, $y) isa owns\n" +
                    "};\n" +
                "{$y isa weapon} or {$y isa missile} or {$y has propulsion 'gsp';$y isa rocket};\n" +
                "{$z has alignment 'hostile'} or {" +
                    "$yy value 'America';\n" +
                    "($z, $yy) isa is-enemy-of;\n" +
                    "$z isa country;" +
                    "$yy isa country" +
                    "};\n" +
                "$x isa person;\n" +
                "$z isa country\n" +
                "}; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testQueryWithOr()
    {
        String queryString = "match {$x isa criminal} or {$x has nationality 'American';$x isa person}";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);


        String explicitQuery = "match " +
            "{{$x isa criminal} or {$x has nationality 'American';\n" +
            "{$z has alignment 'hostile'} or {" +
                "$yy value 'America';\n" +
                "($z, $yy) isa is-enemy-of;\n" +
                "$z isa country;\n" +
                "$yy isa country" +
            "};\n" +
            "($x, $y, $z) isa transaction or {" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
            "};\n" +
            "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
            "$x isa person;\n" +
            "$z isa country}} or {$x has nationality 'American';$x isa person} select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }


    @Test
    public void testGraphCase()
    {
        RuleType inferenceRule = graph.getRuleType("inference-rule");
        Rule R6 = graph.putRule("R6", inferenceRule);

        Type region = graph.putEntityType("region").setValue("region");

        String R6_LHS = "match $x isa region";
        String R6_RHS = "match $x isa country";

        R6.setLHS(R6_LHS);
        R6.setRHS(R6_RHS);

        reasoner.linkConceptTypes();
        String queryString = "match $x isa criminal;";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match " +
                "{$x isa criminal} or {\n" +
                "$x has nationality 'American';\n" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person ;\n" +
                    "{$z isa country} or {$z isa region};\n" +
                    "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                    "($x, $z) isa is-paid-by;\n" +
                    "($z, $y) isa owns\n" +
                "};\n" +
                "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
                "{$z has alignment 'hostile'} or {" +
                    "$yy value 'America';\n" +
                    "($z, $yy) isa is-enemy-of;\n" +
                    "$z isa country;\n" +
                    "$yy isa country" +
                "}" +
                "} select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testVarSub() {
        String queryString = "match" +
                "$y isa person;$yy isa country;$yyy isa weapon;\n" +
                "($y, $yy, $yyy) isa transaction";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match \n" +
                "$y isa person;\n" +
                "$yy isa country;\n" +
                "{$yyy isa weapon} or {\n" +
                "{{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';}} or {$yyy isa rocket;$yyy has propulsion 'gsp';};\n" +
                "};\n" +
                "{($y, $yy, $yyy) isa transaction} or {\n" +
                "$y isa person;\n" +
                "$yy isa country;\n" +
                "{{$yyy isa weapon} or {\n" +
                "{{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';}} or {$yyy isa rocket;$yyy has propulsion 'gsp';};\n" +
                "}} or {{$yyy isa missile} or {$yyy isa rocket;$yyy has propulsion 'gsp';};};\n" +
                "($y, $yy) isa is-paid-by;\n" +
                "($yy, $yyy) isa owns\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testVarSub2() {
        String queryString = "match" +
                "$y isa person;$z isa country;$x isa weapon;\n" +
                "($y, $z, $x) isa transaction";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expandQuery(query);

        String explicitQuery = "match \n" +
                "$y isa person;\n" +
                "$z isa country;\n" +
                "{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';};\n" +
                "};\n" +
                "{($y, $z, $x) isa transaction} or {\n" +
                "$y isa person;\n" +
                "$z isa country;\n" +
                "{{$x isa weapon} or {\n" +
                "{{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';}} or {$x isa rocket;$x has propulsion 'gsp';};\n" +
                "}} or {{$x isa missile} or {$x isa rocket;$x has propulsion 'gsp';};};\n" +
                "($y, $z) isa is-paid-by;\n" +
                "($z, $x) isa owns\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }


    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
