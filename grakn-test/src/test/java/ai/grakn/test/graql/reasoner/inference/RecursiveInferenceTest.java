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

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.graphs.MatrixGraph;
import ai.grakn.graphs.MatrixGraphII;
import ai.grakn.graphs.NguyenGraph;
import ai.grakn.graphs.PathGraph;
import ai.grakn.graphs.PathGraphII;
import ai.grakn.graphs.PathGraphSymmetric;
import ai.grakn.graphs.TailRecursionGraph;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class RecursiveInferenceTest {

    // The recursivity graph is loaded to test if possible, but is unused elsewhere
    @ClassRule
    public static final GraphContext recursivityContext = GraphContext.preLoad("recursivity-test.gql");

    @ClassRule
    public static final GraphContext recursivitySGContext = GraphContext.preLoad("recursivity-sg-test.gql");

    @ClassRule
    public static final GraphContext recursivityTCContext = GraphContext.preLoad("recursivity-tc-test.gql");

    @ClassRule
    public static final GraphContext recursivityRSGContext = GraphContext.preLoad("recursivity-rsg-test.gql");

    @ClassRule
    public static final GraphContext ancestorFriendContext = GraphContext.preLoad("ancestor-friend-test.gql");

    @ClassRule
    public static final GraphContext transitivityContext = GraphContext.preLoad("transitivity-test.gql");

    @ClassRule
    public static final GraphContext ancestorContext = GraphContext.preLoad("ancestor-test.gql");

    @ClassRule
    public static final GraphContext reachabilityContext = GraphContext.preLoad("reachability-test.gql");

    @ClassRule
    public static final GraphContext sameGenerationContext = GraphContext.preLoad("same-generation-test.gql");

    @ClassRule
    public static final GraphContext reachabilitySymmetricContext = GraphContext.preLoad("reachability-test-symmetric.gql");

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    /**from Vieille - Recursive Axioms in Deductive Databases p. 192*/
    @Test
    public void testTransitivity() {
        QueryBuilder qb = transitivityContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa R;$x has index 'i'; select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has index $ind;" +
                            "{$ind value 'j';} or {$ind value 's';} or {$ind value 'v';}; select $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /*single-directional*/
    /**from Bancilhon - An Amateur's Introduction to Recursive Query Processing Strategies p. 25*/
    @Test
    public void testAncestor() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;$X has name 'aa';" +
                            "$Y has name $name;select $Y, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name value 'aaa';} or {$name value 'aab';} or {$name value 'aaaa';};select $Y, $name;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**as above but both directions*/
    @Test
    public void testAncestorPrime() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);

        String queryString = "match ($X, $Y) isa Ancestor;$X has name 'aa'; select $Y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $name;" +
                "{$name value 'a';} or {$name value 'aaa';} or {$name value 'aab';} or {$name value 'aaaa';};select $Y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testAncestor2() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);

        String queryString = "match (ancestor: $X, descendant: $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX value 'a';$nameY value 'aa';} or {$nameX value 'a';$nameY value 'ab';} or" +
                "{$nameX value 'a';$nameY value 'aaa';} or {$nameX value 'a';$nameY value 'aab';} or" +
                "{$nameX value 'a';$nameY value 'aaaa';} or {$nameX value 'aa';$nameY value 'aaa';} or" +
                "{$nameX value 'aa';$nameY value 'aab';} or {$nameX value 'aa';$nameY value 'aaaa';} or " +
                "{$nameX value 'aaa';$nameY value 'aaaa';} or {$nameX value 'c';$nameY value 'ca';}; select $X, $Y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testAncestor2Prime() {
        QueryBuilder qb = ancestorContext.graph().graql().infer(false);

        String queryString = "match ($X, $Y) isa Ancestor;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y isa Person, has name $nameY; $X isa Person, has name $nameX;" +
                "{$nameX value 'a';$nameY value 'aa';} or " +
                "{$nameX value 'a';$nameY value 'ab';} or {$nameX value 'a';$nameY value 'aaa';} or" +
                "{$nameX value 'a';$nameY value 'aab';} or {$nameX value 'a';$nameY value 'aaaa';} or " +
                "{$nameY value 'a';$nameX value 'aa';} or" +
                "{$nameY value 'a';$nameX value 'ab';} or {$nameY value 'a';$nameX value 'aaa';} or" +
                "{$nameY value 'a';$nameX value 'aab';} or {$nameY value 'a';$nameX value 'aaaa';} or "
                +
                "{$nameX value 'aa';$nameY value 'aaa';} or {$nameX value 'aa';$nameY value 'aab';} or" +
                "{$nameX value 'aa';$nameY value 'aaaa';} or " +
                "{$nameY value 'aa';$nameX value 'aaa';} or {$nameY value 'aa';$nameX value 'aab';} or" +
                "{$nameY value 'aa';$nameX value 'aaaa';} or "
                +
                "{$nameX value 'aaa';$nameY value 'aaaa';} or " +
                "{$nameY value 'aaa';$nameX value 'aaaa';} or "
                +
                "{$nameX value 'c';$nameY value 'ca';} or " +
                "{$nameY value 'c';$nameX value 'ca';}; select $X, $Y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$X has name 'a'; $Y has name $name; select $Y, $name;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $Y has name $name;{$name value 'd';} or {$name value 'g';};";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriendPrime() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$X has name 'a'; select $Y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $Y has name $name;{$name value 'd';} or {$name value 'g';}; select $Y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);

        String queryString = "match (person: $X, ancestor-friend: $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $X has name $name;" +
                "{$name value 'a';} or {$name value 'b';} or {$name value 'c';}; select $X;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186*/
    @Test
    public void testAncestorFriend2Prime() {
        QueryBuilder qb = ancestorFriendContext.graph().graql().infer(false);

        String queryString = "match ($X, $Y) isa Ancestor-friend;$Y has name 'd'; select $X;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $X has name $name;" +
                "{$name value 'a';} or {$name value 'b';} or {$name value 'c';}; select $X;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /*from Vieille - Recursive Query Processing: The power of logic p. 25*/
    /** SG(X, X) :- H(X) doesn't get applied*/
    @Ignore
    @Test
    public void testSameGeneration(){
        QueryBuilder qb = recursivitySGContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa SameGen; $x has name 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has name $name;{$name value 'f';} or {$name value 'h';};select $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Vieille - Recursive Query Processing: The power of logic p. 18*/
    @Test
    public void testTC() {
        QueryBuilder qb = recursivityTCContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa N-TC; $y has index 'a'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x has index 'a2';";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testReachability(){
        QueryBuilder qb = reachabilityContext.graph().graql().infer(false);

        String queryString = "match (reach-from: $x, reach-to: $y) isa reachable;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x has index $indX;$y has index $indY;" +
                "{$indX value 'a';$indY value 'b';} or" +
                "{$indX value 'b';$indY value 'c';} or" +
                "{$indX value 'c';$indY value 'c';} or" +
                "{$indX value 'c';$indY value 'd';} or" +
                "{$indX value 'a';$indY value 'c';} or" +
                "{$indX value 'b';$indY value 'd';} or" +
                "{$indX value 'a';$indY value 'd';};select $x, $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testReachabilitySymmetric(){
        QueryBuilder qb = reachabilitySymmetricContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa reachable;$x has index 'a';select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has index $indY;" +
                "{$indY value 'a';} or {$indY value 'b';} or {$indY value 'c';} or {$indY value 'd';};select $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /** test 6.1 from Cao p 71*/
    @Test
    public void testMatrix(){
        final int N = 5;

        MatrixGraph.get(N, N).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (Q1-from: $x, Q1-to: $y) isa Q1; $x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity or $y isa end;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /** test 6.3 from Cao p 75*/
    @Test
    public void testTailRecursion(){
        final int N = 10;
        final int M = 5;

        TailRecursionGraph.get(N, M).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (P-from: $x, P-to: $y) isa P; $x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa b-entity;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**test3 from Nguyen (similar to test 6.5 from Cao)*/
    @Test
    public void testNguyen(){
        final int N = 9;

        NguyenGraph.get(N).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (N-rA: $x, N-rB: $y) isa N; $x has index 'c'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    //TODO bug #10635
    @Ignore
    @Test
    public void testNguyen2(){
        final int N = 9;
        NguyenGraph.get(N).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match $y isa S;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**test 6.6 from Cao p.76*/
    @Test
    public void testSameGenerationCao(){
        QueryBuilder qb = sameGenerationContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa SameGen;$x has name 'ann';select $y;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $y has name $name;" +
                "{$name value 'ann';} or {$name value 'bill';} or {$name value 'peter';};select $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**test 6.9 from Cao p.82*/
    @Test
    public void testMatrixII(){
        final int N = 5;
        final int M = 5;
        MatrixGraphII.getGraph(N, M).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (P-from: $x, P-to: $y) isa P;$x has index 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa a-entity;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**test 6.10 from Cao p. 82*/
    @Test
    public void testPath(){
        final int N = 3;

        PathGraph.get(N, 3).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testPathPrime(){
        final int N = 3;

        PathGraph.get(N, 3).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Ignore
    @Test
    public void testPathSymmetric(){
        final int N = 3;

        PathGraphSymmetric.get(N, 3).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathII(){
        final int N = 3;

        PathGraphII.get(N, N).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match (path-from: $x, path-to: $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    /*modified test 6.10 from Cao p. 82*/
    public void testPathIIPrime(){
        final int N = 3;

        PathGraphII.get(N, N).accept(graphContext.graph());
        QueryBuilder qb = graphContext.graph().graql().infer(false);

        String queryString = "match ($x, $y) isa path;$x has index 'a0'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa vertex;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    /**from Abiteboul - Foundations of databases p. 312/Cao test 6.14 p. 89*/
    @Test
    public void testReverseSameGeneration(){
        QueryBuilder qb = recursivityRSGContext.graph().graql().infer(false);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;$x has name 'a'; select $y;";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $y isa person, has name $name;" +
                                "{$name value 'b';} or {$name value 'c';} or {$name value 'd';};select $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }
    @Test
    public void testReverseSameGeneration2() {
        QueryBuilder qb = recursivityRSGContext.graph().graql().infer(false);

        String queryString = "match (RSG-from: $x, RSG-to: $y) isa RevSG;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match $x has name $nameX;$y has name $nameY;" +
                "{$nameX value 'a';$nameY value 'b';} or {$nameX value 'a';$nameY value 'c';} or" +
                "{$nameX value 'a';$nameY value 'd';} or {$nameX value 'm';$nameY value 'n';} or" +
                "{$nameX value 'm';$nameY value 'o';} or {$nameX value 'p';$nameY value 'm';} or" +
                "{$nameX value 'g';$nameY value 'f';} or {$nameX value 'h';$nameY value 'f';} or" +
                "{$nameX value 'i';$nameY value 'f';} or {$nameX value 'j';$nameY value 'f';} or" +
                "{$nameX value 'f';$nameY value 'k';};select $x, $y;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(Reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.map(CommonUtil::resultVarNameToString).collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }
}
