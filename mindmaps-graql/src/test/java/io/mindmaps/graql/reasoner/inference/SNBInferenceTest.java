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
import io.mindmaps.core.model.Rule;
import io.mindmaps.graql.MatchQueryDefault;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.isRuleRecursive;
import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;

public class SNBInferenceTest {

    private static MindmapsTransaction graph;
    private static Reasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {
        graph = SNBGraph.getTransaction();
        qp = QueryParser.create(graph);
        reasoner = new Reasoner(graph);
    }

    @Test
    public void testRecursive() {
        Rule R1 = graph.getRule("R1");
        Rule R2 = graph.getRule("R2");
        assertEquals(true, isRuleRecursive(R1) && isRuleRecursive(R2));
    }

    /**
     * Tests transitivity
     */
    @Test
    public void testTransitivity() {
        String queryString = "match " +
                "$x isa university;$y isa country;($x, $y) isa resides";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa university;$x id 'University of Cambridge';$y isa country;$y id 'UK'";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qp.parseMatchQuery(explicitQuery).getMatchQuery()));
    }

    /**
     * Tests transitivity for non-Horn clause query
     */
    @Test
    public void testTransitivity2() {
        String queryString = "match " +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa country;\n" +
                "($x, $y) isa resides";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa university;$x id 'University of Cambridge'} or" +
                "{$x isa company;$x id 'Mindmaps'};" +
                "$y isa country;$y id 'UK'";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testTransitivity3() {
        String queryString = "match " +
                "{$y isa university} or {$y isa company};\n" +
                "$x isa country;\n" +
                "(subject-location $x, located-subject $y) isa resides";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$y isa university;$y id 'University of Cambridge'} or" +
                "{$y isa company;$y id 'Mindmaps'};" +
                "$x isa country;$x id 'UK'";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests transitivity and Bug #7343
     */
    @Test
    public void testTransitivity4() {
        String queryString = " match" +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa continent;\n" +
                "($x, $y) isa resides";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                            "{$x isa university} or {$x isa company};\n" +
                            "$y isa continent;\n" +
                            "{($x, $y) isa resides} or\n" +
                            "{($x, $yy) isa resides; {(container-location $y, member-location $yy) isa sublocate} or\n" +
                            "{(container-location $y, member-location $yyyy) isa sublocate; (container-location $yyyy, member-location $yy) isa sublocate}}" +
                            "select $x, $y";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testTag() {
        String queryString = "match " +
                "$x isa person;$y isa tag;($x, $y) isa recommendation";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testTagVarSub() {
        String queryString = "match " +
                "$y isa person;$t isa tag;($y, $t) isa recommendation";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$y isa person;$t isa tag;" +
                "{$y id 'Charlie';" +
                "{$t id 'yngwie-malmsteen'} or {$t id 'cacophony'} or {$t id 'steve-vai'} or {$t id 'black-sabbath'}} or " +
                "{$y id 'Gary';$t id 'pink-floyd'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testProduct() {
        String queryString = "match " +
                "$x isa person;$y isa product;($x, $y) isa recommendation";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testProductVarSub() {
        String queryString = "match " +
                "$y isa person;$yy isa product;($y, $yy) isa recommendation";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$y isa person;$yy isa product;" +
                "{$y id 'Alice';$yy id 'war-of-the-worlds'} or" +
                "{$y id 'Bob';{$yy id 'Ducatti-1299'} or {$yy id 'The-good-the-bad-the-ugly'}} or" +
                "{$y id 'Charlie';{$yy id 'blizzard-of-ozz'} or {$yy id 'stratocaster'}} or " +
                "{$y id 'Denis';{$yy id 'colour-of-magic'} or {$yy id 'dorian-gray'}} or"+
                "{$y id 'Frank';$yy id 'nocturnes'} or" +
                "{$y id 'Karl Fischer';{$yy id 'faust'} or {$yy id 'nocturnes'}} or " +
                "{$y id 'Gary';$yy id 'the-wall'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    @Ignore
    public void testCombinedProductTag() {
        String queryString = "match " +
                "{$x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation}";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}} or" +
                "{$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    @Ignore
    public void testCombinedProductTag2() {
        String queryString = "match " +
                "{$x isa person;$y isa product;($x, $y) isa recommendation} or" +
                "{$x isa person;$y isa tag;($x, $y) isa recommendation}";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}} or" +
                "{$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testBook() {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c value 'book';\n" +
                "($y, $c) isa typing; select $x, $y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Karl Fischer';$y id 'faust'} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}}";


        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testBand() {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c value 'Band';\n" +
                "($y, $c) isa grouping; select $x, $y";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'cacophony'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests global variable consistency (Bug #7344)
     */
    @Test
    public void testVarConsistency(){
        String queryString = "match $x isa person;$y isa product;\n" +
                    "($x, $y) isa recommendation;\n" +
                    "$z isa category;$z value 'motorbike';\n" +
                    "($y, $z) isa typing; select $x(value), $y(value)";

        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $x isa person;$y isa product;" +
                "{$x id 'Bob';$y id 'Ducatti-1299'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * tests whether rules are filtered correctly (rules recommending products other than Chopin should not be attached)
     */
    @Test
    public void testVarConsistency2(){
        //select people that have Chopin as a recommendation
        String queryString = "match $x isa person; $y isa tag; ($x, $y) isa tagging;\n" +
                        "$z isa product, value 'Chopin - Nocturnes'; ($x, $z) isa recommendation; select $x(value), $y(value)";

        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match " +
                "{$x id 'Frank';$y id 'Ludwig_van_Beethoven'} or" +
                "{$x id 'Karl Fischer';" +
                "{$y id 'Ludwig_van_Beethoven'} or {$y id 'Johann Wolfgang von Goethe'} or {$y id 'Wolfgang_Amadeus_Mozart'}}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testVarConsistency3(){

        String queryString = "match $x isa person;$pr isa product, value \"Chopin - Nocturnes\";($x, $pr) isa recommendation; select $x(value)";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match {$x id 'Frank'} or {$x id 'Karl Fischer'}";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests transitivity and Bug #7416
     */
    @Test
    public void testQueryConsistency() {

        String queryString = "match $x isa person; $y isa place; ($x, $y) isa resides;\n" +
                        "$z isa person, value \"Miguel Gonzalez\"; ($x, $z) isa knows; select $x(value), $y(value)";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
        printMatchQueryResults(expandedQuery);

        System.out.println();

        String queryString2 = "match $x isa person; $y isa person, value \"Miguel Gonzalez\";\n" +
                        "$z isa place; ($x, $y) isa knows; ($x, $z) isa resides; select $x(value), $z(value)";
        MatchQueryDefault query2 = qp.parseMatchQuery(queryString2).getMatchQuery();
        MatchQueryDefault expandedQuery2 = reasoner.expand(query2);

        printMatchQueryResults(expandedQuery2);
    }

    /**
     * Tests Bug #7416
     * the $t variable in the query matches with $t from rules so if the rule var is not changed an extra condition is created
     * which renders the query unsatisfiable
     */
    @Test
    public void testOrdering() {
        //select recommendationS of Karl Fischer and their types
        String queryString = "match $p isa product;$x isa person, value \"Karl Fischer\";" +
                        "($x, $p) isa recommendation; ($p, $t) isa typing; select $p(value), $t(value)";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String queryString2 = "match $p isa product; $x isa person, value \"Karl Fischer\";" +
                        "($p, $c) isa typing; ($x, $p) isa recommendation; select $p(value), $c(value)";
        MatchQueryDefault query2 = qp.parseMatchQuery(queryString2).getMatchQuery();

        String explicitQuery = "match $p isa product;\n" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$tt isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $tt) isa tagging;$p isa product, value 'Faust'} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $tt isa tag; ($tt, $x), isa tagging};" +
                "($p, $t) isa typing; select $p(value), $t(value)";

        String explicitQuery2 = "match $p isa product;\n" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$t isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $t) isa tagging;$p isa product, value 'Faust'} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $t isa tag; ($t, $x), isa tagging};" +
                "($p, $c) isa typing; select $p(value), $c(value)";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertQueriesEqual(reasoner.expand(query2), qp.parseMatchQuery(explicitQuery2).getMatchQuery());
    }

    /**
     * Tests Bug #7422
     * Currently the necessary replacement $t->$tt doesn't take place.
     */
    @Test
    public void testInverseVars() {
        //select recommendation of Karl Fischer and their types
        String queryString = "match $p isa product;\n" +
                "$x isa person, value \"Karl Fischer\"; ($p, $x) isa recommendation; ($p, $t) isa typing; select $p(value), $t(value)";
        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();

        String explicitQuery = "match $p isa product;" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $tt isa tag; ($tt, $x), isa tagging} or" +
                "{$x isa person;$tt isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $tt) isa tagging;$p isa product, value 'Faust'}" +
                ";($p, $t) isa typing; select $p(value), $t(value)";

        assertQueriesEqual(reasoner.expand(query), qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testDoubleVars() {

        String queryString = "match $x isa person;{($x, $y) isa recommendation} or " +
                "{" +
                "$x isa person;$t isa tag, value 'Enter_the_Chicken';" +
                "($x, $t) isa tagging;$y isa tag;{$y value 'Buckethead'} or {$y value 'Primus'}" +
                "} select $x, $y";

        MatchQueryDefault query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQueryDefault expandedQuery = reasoner.expand(query);
    }
    private void assertQueriesEqual(MatchQueryDefault q1, MatchQueryDefault q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
