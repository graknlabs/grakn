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
import ai.grakn.graphs.AdmissionsGraph;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.SNBGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


import java.util.Set;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryTest {

    @ClassRule
    public static final GraphContext snbGraph = GraphContext.preLoad(SNBGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext admissionsGraph = GraphContext.preLoad(AdmissionsGraph.get()).assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext ancestorGraph = GraphContext.preLoad("ancestor-friend-test.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext genealogyOntology = GraphContext.preLoad("genealogy/ontology.gql").assumeTrue(usingTinker());

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test //simple equality tests between original and a copy of a query
    public void testCopyConstructor(){
        String patternString = "{$x isa person;$y isa product;($x, $y) isa recommendation;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, snbGraph.graph()), snbGraph.graph());
        ReasonerQueryImpl copy = ReasonerQueries.create(query);
        assertQueriesEqual(query.getMatchQuery(), copy.getMatchQuery());
    }

    @Test //check two queries are alpha-equivalent - equal up to the choice of free variables
    public void testAlphaEquivalence() {
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x isa person;$t isa tag;$t val 'Michelangelo';" +
                "($x, $t) isa tagging;" +
                "$y isa product;$y val 'Michelangelo  The Last Judgement';}";

        String patternString2 = "{$x isa person;$y isa tag;$y val 'Michelangelo';" +
                "($x, $y) isa tagging;" +
                "$pr isa product;$pr val 'Michelangelo  The Last Judgement';}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        assertEquals(query, query2);
        assertEquals(query.hashCode(), query2.hashCode());
    }

    @Test //check that patterns defined using single and multiproperty vars are alpha-equivalent
    public void testAlphaEquivalence_multiPropertyPatterns() {
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x isa person;$x has name 'Bob';}";
        String patternString2 = "{$x isa person, has name 'Bob';}";
        String patternString3 = "{$x isa person, val 'Bob';}";
        String patternString4 = "{$x isa person;$x val 'Bob';}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(patternString4, graph), graph);
        assertEquals(query, query2);
        assertEquals(query3, query4);
    }
    
    @Test //should return false as the id-predicate is mapped to a different role
    public void testAlphaEquivalence_nonMatchingIdPredicates() {
        GraknGraph graph = ancestorGraph.graph();
        String aId = getConcept(graph, "name", "a").getId().getValue();
        String patternString = "{$X id '" + aId + "'; (ancestor-friend: $X, person: $Y), isa Ancestor-friend;}";
        String patternString2 = "{$X id '" + aId + "'; (person: $X, ancestor-friend: $Y), isa Ancestor-friend;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        assertNotEquals(query, query2);
        assertNotEquals(query.hashCode(), query2.hashCode());
    }
    
    @Test //tests various configurations of alpha-equivalence with extra type atoms present
    public void testAlphaEquivalence_nonMatchingTypes() {
        GraknGraph graph = geoGraph.graph();
        String polandId = getConcept(graph, "name", "Poland").getId().getValue();
        String patternString = "{$y id '" + polandId + "'; $y isa country; (geo-entity: $y1, entity-location: $y), isa is-located-in;}";
        String patternString2 = "{$y id '" + polandId + "'; $x isa city; (geo-entity: $x, entity-location: $y), isa is-located-in; $y isa country;}";
        String patternString3 = "{$x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in;}";
        String patternString4 = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, graph), graph);
        ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(patternString4, graph), graph);

        assertTrue(!query.isEquivalent(query2));
        assertTrue(!query.isEquivalent(query3));
        assertTrue(!query.isEquivalent(query4));

        assertTrue(!query2.isEquivalent(query3));
        assertTrue(!query2.isEquivalent(query4));
        assertTrue(!query3.isEquivalent(query4));

        String patternString5 = "{(entity-location: $y, geo-entity: $y1), isa is-located-in;}";
        String patternString6 = "{(geo-entity: $y1, entity-location: $y2), isa is-located-in;}";
        String patternString7 = "{(entity-location: $y, geo-entity: $x), isa is-located-in; $x isa city;}";
        String patternString8 = "{$x isa city; (entity-location: $y1, geo-entity: $x), isa is-located-in;}";

        ReasonerQueryImpl query5 = ReasonerQueries.create(conjunction(patternString5, graph), graph);
        ReasonerQueryImpl query6 = ReasonerQueries.create(conjunction(patternString6, graph), graph);
        ReasonerQueryImpl query7 = ReasonerQueries.create(conjunction(patternString7, graph), graph);
        ReasonerQueryImpl query8 = ReasonerQueries.create(conjunction(patternString8, graph), graph);
        assertEquals(query5, query6);
        assertEquals(query7, query8);
    }

    @Test //tests alpha-equivalence of resource atoms differing only by variable names
    public void testAlphaEquivalence_matchingResources() {
        GraknGraph graph = admissionsGraph.graph();
        String patternString = "{$x isa $x-type-ec47c2f8-4ced-46a6-a74d-0fb84233e680;" +
                "$x has GRE $x-GRE-dabaf2cf-b797-4fda-87b2-f9b01e982f45;" +
                "$x-type-ec47c2f8-4ced-46a6-a74d-0fb84233e680 label 'applicant';" +
                "$x-GRE-dabaf2cf-b797-4fda-87b2-f9b01e982f45 val > 1099;}";

        String patternString2 = "{$x isa $x-type-79e3295d-6be6-4b15-b691-69cf634c9cd6;" +
                "$x has GRE $x-GRE-388fa981-faa8-4705-984e-f14b072eb688;" +
                "$x-type-79e3295d-6be6-4b15-b691-69cf634c9cd6 label 'applicant';" +
                "$x-GRE-388fa981-faa8-4705-984e-f14b072eb688 val > 1099;}";
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerQueryImpl parentQuery = ReasonerQueries.create(pattern, graph);
        ReasonerQueryImpl childQuery = ReasonerQueries.create(pattern2, graph);
        assertEquals(parentQuery, childQuery);
        assertEquals(parentQuery.hashCode(), childQuery.hashCode());
    }

    @Test //tests alpha-equivalence of resource atoms with different predicates
    public void testAlphaEquivalence_resourcesWithDifferentPredicates() {
        GraknGraph graph = admissionsGraph.graph();
        String patternString = "{$x has GRE $gre;$gre val > 1099;}";
        String patternString2 = "{$x has GRE $gre;$gre val < 1099;}";
        String patternString3 = "{$x has GRE $gre;$gre val = 1099;}";
        String patternString4 = "{$x has GRE $gre;$gre val '1099';}";
        String patternString5 = "{$x has GRE $gre;$gre val > $var;}";

        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        Conjunction<VarAdmin> pattern4 = conjunction(patternString4, graph);
        Conjunction<VarAdmin> pattern5 = conjunction(patternString5, graph);

        ReasonerQueryImpl query = ReasonerQueries.create(pattern, graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(pattern2, graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(pattern3, graph);
        ReasonerQueryImpl query4 = ReasonerQueries.create(pattern4, graph);
        ReasonerQueryImpl query5 = ReasonerQueries.create(pattern5, graph);
        assertNotEquals(query, query2);
        assertNotEquals(query2, query3);
        assertEquals(query3, query4);
        assertNotEquals(query4, query5);
    }

    @Test //tests alpha-equivalence of queries with resources with multi predicate
    public void testAlphaEquivalence_WithMultiPredicateResources(){
        GraknGraph graph = snbGraph.graph();
        String patternString = "{$x has age $a;$a val >23; $a val <27;}";
        String patternString2 = "{$p has age $a;$a val >23;}";
        String patternString3 = "{$a has age $p;$p val <27;$p val >23;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, graph), graph);
        assertNotEquals(query, query2);
        assertEquals(query, query3);
    }

    @Test //tests alpha-equivalence of queries with indirect types
    public void testAlphaEquivalence_indirectTypes(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(entity-location: $x2, geo-entity: $x1) isa is-located-in;" +
                "$x1 isa $t1; $t1 sub geoObject;}";
        String patternString2 = "{(geo-entity: $y1, entity-location: $y2) isa is-located-in;" +
                "$y1 isa $t2; $t2 sub geoObject;}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        assertEquals(query, query2);
    }

    //Bug #11150 Relations with resources as single VarAdmin
    @Test //tests whether directly and indirectly reified relations are equivalent
    public void testAlphaEquivalence_reifiedRelation(){
        GraknGraph graph = genealogyOntology.graph();
        String patternString = "{$rel (happening: $b, protagonist: $p) isa event-protagonist has event-role 'parent';}";
        String patternString2 = "{$rel (happening: $c, protagonist: $r) isa event-protagonist; $rel has event-role 'parent';}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, graph), graph);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, graph), graph);
        assertEquals(query, query2);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private static Concept getConcept(GraknGraph graph, String typeLabel, Object val){
        return graph.graql().match(Graql.var("x").has(typeLabel, val).admin()).execute().iterator().next().get("x");
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
