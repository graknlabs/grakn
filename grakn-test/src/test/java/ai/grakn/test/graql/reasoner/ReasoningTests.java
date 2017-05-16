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

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.GraphContext;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
public class ReasoningTests {

    @ClassRule
    public static final GraphContext testSet1 = GraphContext.preLoad("testSet1.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet2 = GraphContext.preLoad("testSet2.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet3 = GraphContext.preLoad("testSet3.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet4 = GraphContext.preLoad("testSet4.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet5 = GraphContext.preLoad("testSet5.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet6 = GraphContext.preLoad("testSet6.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet7 = GraphContext.preLoad("testSet7.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet8 = GraphContext.preLoad("testSet8.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet9 = GraphContext.preLoad("testSet9.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet10 = GraphContext.preLoad("testSet10.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet11 = GraphContext.preLoad("testSet11.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet12 = GraphContext.preLoad("testSet12.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet13 = GraphContext.preLoad("testSet13.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet14 = GraphContext.preLoad("testSet14.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet15 = GraphContext.preLoad("testSet15.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet16 = GraphContext.preLoad("testSet16.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet17 = GraphContext.preLoad("testSet17.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet18 = GraphContext.preLoad("testSet18.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet19 = GraphContext.preLoad("testSet19.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet20 = GraphContext.preLoad("testSet20.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet21 = GraphContext.preLoad("testSet21.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet22 = GraphContext.preLoad("testSet22.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet23 = GraphContext.preLoad("testSet23.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet24 = GraphContext.preLoad("testSet24.gql").assumeTrue(usingTinker());

    @ClassRule
    public static final GraphContext testSet25 = GraphContext.preLoad("testSet25.gql").assumeTrue(usingTinker());

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Ignore
    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationWithVarDuplicates() {
        QueryBuilder qb = testSet1.graph().graql().infer(true);
        String query1String = "match (role1:$x, role2:$x);";
        String query2String = "match (role1:$x, role2:$y);";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));

        assertNotEquals(answers1.size() * answers2.size(), 0);
        answers1.forEach(x -> assertTrue(x.keySet().size() ==1));
        answers2.forEach(x -> answers1.forEach(y -> assertTrue(x.values().containsAll(y.values()))));
        answers2.forEach(x -> assertTrue(x.keySet().size() ==2));

    }

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = testSet2.graph().graql().infer(true);
        String query1String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        assertEquals(answers1.size(), 1);
    }

    @Test //Expected result: The queries should return different matches, unique per query.
    public void generatingFreshEntity() {
        QueryBuilder qb = testSet3.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertEquals(answers1.size(), answers2.size());
        assertNotEquals(answers1, answers2);
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdge() {
        QueryBuilder qb = testSet4.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertEquals(answers1, answers2);
    }

    @Test //Expected result: The query should return a unique match (or possibly nothing if we enforce range-restriction).
    public void generatingFreshEntity2() {
        QueryBuilder qb = testSet5.graph().graql().infer(false);
        QueryBuilder iqb = testSet5.graph().graql().infer(true);
        String queryString = "match $x isa entity2;";
        String explicitQuery = "match $x isa entity1;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(explicitQuery));

        assertTrue(!answers2.containsAll(answers));
        assertTrue(!answers.isEmpty());
        assertEquals(answers2.size(), 3);
    }

    @Test //Expected result: The query should return three different instances of relation1 with unique ids.
    public void generatingFreshRelation() {
        QueryBuilder qb = testSet6.graph().graql().infer(true);
        String queryString = "match $x isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 3);
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        QueryBuilder iqb = testSet7.graph().graql().infer(true);
        QueryBuilder qb = testSet7.graph().graql().infer(true);
        String queryString = "match $x isa relation1; limit 10;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 10);
        assertEquals(answers.size(), queryAnswers(qb.parse(queryString)).size());

    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved1() {
        QueryBuilder qb = testSet8.graph().graql().infer(true);
        String queryString = "match (role2:$x, role3:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> assertTrue(y.values().size()<=1));
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved2() {
        QueryBuilder qb = testSet9.graph().graql().infer(true);
        String queryString = "match (role1:$x, role1:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> assertTrue(y.values().size()<=1));
    }

    @Test //Expected result: The query should return a single match
    public void roleUnificationWithRoleHierarchiesInvolved3() {
        QueryBuilder qb = testSet9.graph().graql().infer(true);
        String queryString = "match (role1:$x) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        QueryBuilder iqb = testSet10.graph().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation2;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        QueryBuilder qb = testSet11.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        QueryBuilder qb = testSet12.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 2);
    }

    @Ignore
    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        QueryBuilder qb = testSet13.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources1() {
        QueryBuilder qb = testSet14.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res1 $y;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        String queryString2 = "match $x isa res1;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));

        assertEquals(answers2.size(), 1);
        assertEquals(answers1.size(), 2);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources2() {
        QueryBuilder qb = testSet15.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res2 $y;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        assertEquals(answers1.size(), 1);
        String queryString2 = "match $x isa res2;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 1);
        assertTrue(answers2.iterator().next().get(VarName.of("x")).isResource());
        String queryString3 = "match $x isa res1; $y isa res2;";
        QueryAnswers answers3 = queryAnswers(qb.parse(queryString3));
        assertEquals(answers3.size(), 1);

        assertTrue(answers3.iterator().next().get(VarName.of("x")).isResource());
        assertTrue(answers3.iterator().next().get(VarName.of("y")).isResource());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources3() {
        QueryBuilder qb = testSet16.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res1 $y; $z isa relation1;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        assertEquals(answers1.size(), 1);
        answers1.forEach(ans ->
                {
                    assertTrue(ans.get(VarName.of("x")).isEntity());
                    assertTrue(ans.get(VarName.of("y")).isResource());
                    assertTrue(ans.get(VarName.of("z")).isRelation());
                }
        );
        String queryString2 = "match $x isa relation1, has res1 $y;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 1);
        answers2.forEach(ans ->
                {
                    assertTrue(ans.get(VarName.of("x")).isRelation());
                    assertTrue(ans.get(VarName.of("y")).isResource());
                }
        );
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources4() {
        QueryBuilder qb = testSet17.graph().graql().infer(true);
        String queryString1 = "match $x has res2 $r;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString1));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void inferringSpecificResourceValue() {
        QueryBuilder qb = testSet18.graph().graql().infer(true);
        String queryString = "match $x has res1 'value';";
        String queryString2 = "match $x has res1 $r;";
        MatchQuery query = qb.parse(queryString);
        MatchQuery query2 = qb.parse(queryString2);
        List<Answer> answers = query.execute();
        List<Answer> answers2 = query2.execute();
        List<Answer> requeriedAnswers = query.execute();
        assertEquals(answers.size(), 2);
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers.size(), requeriedAnswers.size());
        assertTrue(answers.containsAll(requeriedAnswers));
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected(){
        QueryBuilder qb = testSet19.graph().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 2);
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected2(){
        QueryBuilder qb = testSet19.graph().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa subEntity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverRelationHierarchy(){
        QueryBuilder qb = testSet20.graph().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation1;";
        String queryString2 = "match (role1: $x, role2: $y) isa sub-relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers.size(), 1);
        assertEquals(answers, answers2);
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverEntityHierarchy(){
        QueryBuilder qb = testSet21.graph().graql().infer(true);
        String queryString = "match $x isa entity1;";
        String queryString2 = "match $x isa sub-entity1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers.size(), 1);
        assertEquals(answers, answers2);
    }

    @Test //Expected result: Returns db and inferred relations + their inverses and relations with self for all entities
    public void reasoningWithRepeatingRoles(){
        QueryBuilder qb = testSet22.graph().graql().infer(true);
        String queryString = "match (friend:$x1, friend:$x2) isa knows-trans;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 16);
    }

    @Test //Expected result: The same set of results is always returned
    public void reasoningWithLimitHigherThanNumberOfResultsReturnsConsistentResults(){
        QueryBuilder qb = testSet23.graph().graql().infer(true);
        String queryString = "match (friend1:$x1, friend2:$x2) isa knows-trans;limit 60;";
        QueryAnswers oldAnswers = queryAnswers(qb.parse(queryString));
        for(int i = 0; i < 5 ; i++) {
            QueryAnswers answers =queryAnswers(qb.parse(queryString));
            assertEquals(answers.size(), 6);
            assertEquals(answers, oldAnswers);
        }
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes() {
        QueryBuilder qb = testSet24.graph().graql().infer(true);
        QueryBuilder qbm = testSet24.graph().graql().infer(true).materialise(true);
        String queryString = "match (role1:$x1, role2:$x2) isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qbm.parse(queryString));
        assertEquals(answers.size(), 9);
        assertEquals(answers2.size(), 9);
        assertEquals(answers, answers2);
    }

    @Test //Expected result: Timeline is correctly recognised via applying resource comparisons in the rule body
    public void reasoningWithResourceValueComparison() {
        QueryBuilder qb = testSet25.graph().graql().infer(true);
        String queryString = "match (predecessor:$x1, successor:$x2) isa message-succession;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 10);
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().stream().collect(toSet()));
    }
}
