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

package io.grakn.test.graql.reasoner;

import com.google.common.collect.Sets;
import io.grakn.GraknGraph;
import io.grakn.graql.AskQuery;
import io.grakn.graql.Graql;
import io.grakn.graql.MatchQuery;
import io.grakn.graql.QueryBuilder;
import io.grakn.graql.admin.Conjunction;
import io.grakn.graql.admin.PatternAdmin;
import io.grakn.graql.internal.reasoner.atom.Atomic;
import io.grakn.graql.internal.reasoner.atom.AtomicFactory;
import io.grakn.graql.internal.reasoner.atom.IdPredicate;
import io.grakn.graql.internal.reasoner.query.AtomicQuery;
import io.grakn.test.graql.reasoner.graphs.GenericGraph;
import io.grakn.test.graql.reasoner.graphs.SNBGraph;
import io.grakn.util.ErrorMessage;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AtomicQueryTest {
    private static GraknGraph graph;
    private static QueryBuilder qb;

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        graph = SNBGraph.getGraph();
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testErrorNonAtomicQuery() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation;($y, $t) isa typing;";
        exception.expect(IllegalStateException.class);
        exception.expectMessage(ErrorMessage.NON_ATOMIC_QUERY.getMessage(queryString));
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
    }

    @Test
    public void testCopyConstructor(){
        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        assert(atomicQuery.equals(new AtomicQuery(atomicQuery)));
    }

    @Test
    public void testPatternNotVar(){
        exception.expect(IllegalArgumentException.class);
        Conjunction<PatternAdmin> pattern = qb.<MatchQuery>parse("match $x isa person;").admin().getPattern();
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));
        Atomic atom = AtomicFactory.create(pattern);
    }

    @Test
    public void testErrorOnMaterialize(){
        exception.expect(IllegalStateException.class);
        String queryString = "match ($x, $y) isa recommendation;";
        IdPredicate sub = new IdPredicate("x", graph.getConcept("Bob"));
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        AtomicQuery atomicQuery2 = new AtomicQuery(atomicQuery);
        atomicQuery2.addAtomConstraints(Sets.newHashSet(sub));
        exception.expectMessage(ErrorMessage.MATERIALIZATION_ERROR.getMessage(atomicQuery2.toString()));
        atomicQuery.materialise(Sets.newHashSet(new IdPredicate("x", graph.getConcept("Bob"))));
    }

    @Test
    public void testMaterialize(){
        assert(!qb.<AskQuery>parse("match ($x, $y) isa recommendation;$x id 'Bob';$y id 'Colour of Magic'; ask;").execute());

        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        atomicQuery.materialise(Sets.newHashSet(new IdPredicate("x", graph.getConcept("Bob"))
                                                , new IdPredicate("y", graph.getConcept("Colour of Magic"))));
        assert(qb.<AskQuery>parse("match ($x, $y) isa recommendation;$x id 'Bob';$y id 'Colour of Magic'; ask;").execute());
    }

    @Test
    public void testUnification(){
        GraknGraph localGraph = GenericGraph.getGraph("ancestor-friend-test.gql");
        AtomicQuery parentQuery = new AtomicQuery("match ($Y, $z) isa Friend; $Y id 'd'; select $z;", localGraph);
        AtomicQuery childQuery = new AtomicQuery("match ($X, $Y) isa Friend; $Y id 'd'; select $X;", localGraph);

        Atomic parentAtom = parentQuery.getAtom();
        Atomic childAtom = childQuery.getAtom();
        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);
        Map<String, String> correctUnifiers = new HashMap<>();
        correctUnifiers.put("X", "z");
        assertTrue(unifiers.equals(correctUnifiers));
    }

}
