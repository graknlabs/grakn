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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Reasoner;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.AdmissionsGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.Query;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;


public class AdmissionsInferenceTest extends AbstractEngineTest{

    @BeforeClass
    public static void onStartup(){
        assumeTrue(usingTinker());
    }

    @Test
    public void testConditionalAdmission() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'conditional';";
        Query query = new Query(queryString, graph);
        String explicitQuery = "match $x isa applicant, has name 'Bob';";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testDeniedAdmission() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'denied';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Alice';";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testProvisionalAdmission() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'provisional';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Denis';";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testWaitForTranscriptAdmission() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has admissionStatus 'wait for transcript';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name 'Frank';";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testFullStatusAdmission() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x isa applicant;$x has name $name;$x has admissionStatus 'full';";
        MatchQuery query = qb.parse(queryString);
        String explicitQuery = "match $x isa applicant, has name $name;{$name value 'Charlie';} or {$name value 'Eva';};";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testAdmissions() {
        GraknGraph graph = AdmissionsGraph.getGraph();
        QueryBuilder qb = graph.graql().infer(false);
        Reasoner reasoner = new Reasoner(graph);

        String queryString = "match $x has admissionStatus $y;$x has name $name;";
        Query query = new Query(queryString, graph);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(queryString).stream());
    }

    private void assertQueriesEqual(Stream<Map<String, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }
}
