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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Type;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.rule.SampleKBContext;
import java.util.List;
import java.util.stream.Stream;
import org.apache.cassandra.cql.Relation;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.GraqlTestUtil.assertQueriesEqual;
import static org.junit.Assert.assertEquals;

public class OntologicalQueryTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

    /** HasAtom **/

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type has name; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        //1 x noRoleEntity + 3 x 3 (hierarchy) anotherTwoRoleEntities
        assertEquals(answers.size(), 10);
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType_needInferenceToGetAllResults(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);

        String queryString = "match $x isa $type; $type has description; get;";
        String specificQueryString = "match $x isa reifiable-relation;get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(answers.size(), qb.<GetQuery>parse(specificQueryString).execute().size() * tx.getRelationshipType("reifiable-relation").subs().count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    /** SubAtom **/

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type sub noRoleEntity; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType_needInferenceToGetAllResults(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type sub relationship; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(answers.size(), tx.getRelationshipType("relationship").subs().flatMap(RelationshipType::instances).count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    /** PlaysAtom **/

    @Test
    public void allInstancesOfTypesThatPlayGivenRole(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type plays role1; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> reifiableRelations = qb.<GetQuery>parse("match $x isa reifiable-relation;get;").execute();
        assertEquals(answers.size(), tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count() + reifiableRelations.size());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    /** RelatesAtom **/

    @Test
    public void allInstancesOfRelationsThatRelateGivenRole(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type relates role1; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(),  15);
        System.out.println(answers.size());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
        List<Answer> relations = qb.<GetQuery>parse("match $x isa relationship;get;").execute();
        //plus extra 3 cause there are 3 binary relations which are not extra counted as reifiable-relations
        assertEquals(answers.size(), relations.stream().filter(ans -> !ans.get("x").asRelationship().type().isImplicit()).count() + 3);

    }

    @Test
    public void sanityCheck0(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa relationship;get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 13);
    }

    @Test
    public void sanityCheck(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($u, $v) isa $type; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void sanityCheck2(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $r ($u, $v) isa binary;get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
    }
}
