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

package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.MatrixGraphII;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.query.QueryAnswerStream;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class LazyTest {

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext graphContext = GraphContext.empty();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testLazyCache(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";

        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Stream<Map<VarName, Concept>> dbStream = query.DBlookup();
        cache.record(query, dbStream);

        Set<Map<VarName, Concept>> collect = cache.getAnswerStream(query).collect(toSet());
        Set<Map<VarName, Concept>> collect2 = cache.getAnswerStream(query2).collect(toSet());
        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testLazyCache2(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $x, entity-location: $z) isa is-located-in;}";

        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(pattern3, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Stream<Map<VarName, Concept>> stream = query.lookup(cache);
        Stream<Map<VarName, Concept>> stream2 = query2.lookup(cache);
        Stream<Map<VarName, Concept>> joinedStream = QueryAnswerStream.join(stream, stream2);

        joinedStream = cache.record(query3, joinedStream.flatMap(a -> varFilterFunction.apply(a, query3.getVarNames())));

        Set<Map<VarName, Concept>> collect = joinedStream.collect(toSet());
        Set<Map<VarName, Concept>> collect2 = cache.getAnswerStream(query3).collect(toSet());

        assertEquals(collect.size(), 37);
        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testJoin(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $z, entity-location: $w) isa is-located-in;}";

        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(pattern3, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Stream<Map<VarName, Concept>> stream = query.lookup(cache);
        Stream<Map<VarName, Concept>> stream2 = query2.lookup(cache);
        Stream<Map<VarName, Concept>> stream3 = query3.lookup(cache);

        Stream<Map<VarName, Concept>> join = QueryAnswerStream.join(QueryAnswerStream.join(stream, stream2), stream3);
        assertEquals(join.collect(toSet()).size(), 10);
    }

    @Test
    public void testKnownFilter(){
        GraknGraph graph = geoGraph.graph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = graph.graql().parse(queryString);
        QueryAnswers answers = new QueryAnswers(query.admin().streamWithVarNames().collect(toSet()));
        long count = query.admin()
                .streamWithVarNames()
                .filter(a -> QueryAnswerStream.knownFilter(a, answers.stream()))
                .count();
        assertEquals(count, 0);
    }

    @Test 
    public void testLazy()  {
        final int N = 20;

        long startTime = System.currentTimeMillis();
        graphContext.graph().close();
        graphContext.load(MatrixGraphII.get(N, N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        String queryString = "match (P-from: $x, P-to: $y) isa P;";
        MatchQuery query = iqb.parse(queryString);

        final int limit = 20;
        final long maxTime = 1000;
        startTime = System.currentTimeMillis();
        List<Map<String, Concept>> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
        assertEquals(results.size(), limit);
        assertTrue(answerTime < maxTime);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
