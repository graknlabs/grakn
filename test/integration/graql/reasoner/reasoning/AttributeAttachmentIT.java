/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.reasoning;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
@SuppressWarnings("CheckReturnValue")
public class AttributeAttachmentIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static Session attributeAttachmentSession;
    @BeforeClass
    public static void loadContext(){
        attributeAttachmentSession = server.sessionWithNewKeyspace();
        String resourcePath = "test/integration/graql/reasoner/stubs/";
        loadFromFileAndCommit(resourcePath, "resourceAttachment.gql", attributeAttachmentSession);
    }

    @AfterClass
    public static void closeSession(){
        attributeAttachmentSession.close();
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void whenUsingNonPersistedValueType_noDuplicatesAreCreated() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            String queryString2 = "match $x isa reattachable-resource-string; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());

            //two attributes for each entity
            assertEquals(tx.getEntityType("genericEntity").instances().count() * 2, answers.size());
            //one base resource, one sub
            assertEquals(2, answers2.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttribute_reattachingAttributeToEntity() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            String queryString2 = "match $x isa reattachable-resource-string; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());

            //two attributes for each entity
            assertEquals(tx.getEntityType("genericEntity").instances().count() * 2, answers.size());
            //one base resource, one sub
            assertEquals(2, answers2.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_queryingForGenericOwnership() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has attribute $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertEquals(6, answers.size());
            //three attributes for each entity
            assertEquals(
                    tx.getEntityType("genericEntity").instances().count() * 3,
                    answers.stream().filter(answer -> answer.get("y").isAttribute()).count()
            );
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_usingExistingAttributeToCreateSubAttribute() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match $x isa genericEntity, has subResource $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());

            String queryString2 = "match $x isa subResource; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertEquals(1, answers2.size());
            assertTrue(answers2.iterator().next().get("x").isAttribute());

            String queryString3 = "match $x isa reattachable-resource-string; $y isa subResource;get;";
            List<ConceptMap> answers3 = tx.execute(Graql.parse(queryString3).asGet());
            //2 RRS instances - one base, one sub hence two answers
            assertEquals(2, answers3.size());

            assertTrue(answers3.iterator().next().get("x").isAttribute());
            assertTrue(answers3.iterator().next().get("y").isAttribute());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_usingExistingAttributeToCreateUnrelatedAttribute() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match $x isa genericEntity, has unrelated-reattachable-string $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());

            String queryString2 = "match $x isa unrelated-reattachable-string; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertEquals(1, answers2.size());
            assertTrue(answers2.iterator().next().get("x").isAttribute());

            String queryString3 = "match $x isa reattachable-resource-string; $y isa unrelated-reattachable-string;get;";
            List<ConceptMap> answers3 = tx.execute(Graql.parse(queryString3).asGet());
            //2 RRS instances - one base, one sub hence two answers
            assertEquals(2, answers3.size());

            assertTrue(answers3.iterator().next().get("x").isAttribute());
            assertTrue(answers3.iterator().next().get("y").isAttribute());
        }
    }


    @Test
    public void whenReasoningWithAttributes_ResultsAreComplete() {
        try (Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            Statement has = var("x").has("reattachable-resource-string", var("y"));
            List<ConceptMap> answers = tx.execute(Graql.match(has).get());
            assertEquals(5, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingAttributes_attachingExistingAttributeToARelation() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; $z isa relation0; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            //two attributes for each entity
            assertEquals(tx.getEntityType("genericEntity").instances().count() * 2, answers.size());
            answers.forEach(ans ->
                    {
                        assertTrue(ans.get("x").isEntity());
                        assertTrue(ans.get("y").isAttribute());
                        assertTrue(ans.get("z").isRelation());
                    }
            );

            String queryString2 = "match $x isa relation0, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertEquals(1, answers2.size());
            answers2.forEach(ans ->
                    {
                        assertTrue(ans.get("x").isRelation());
                        assertTrue(ans.get("y").isAttribute());
                    }
            );
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_derivingAttributeFromOtherAttributeWithConditionalValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x has derived-resource-boolean $r; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(1, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void derivingAttributeWithSpecificValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match $x has derived-resource-string 'value'; get;";
            String queryString2 = "match $x has derived-resource-string $r; get;";
            GraqlGet query = Graql.parse(queryString).asGet();
            GraqlGet query2 = Graql.parse(queryString2).asGet();
            List<ConceptMap> answers = tx.execute(query);
            List<ConceptMap> answers2 = tx.execute(query2);
            List<ConceptMap> requeriedAnswers = tx.execute(query);
            assertEquals(2, answers.size());
            assertEquals(4, answers2.size());
            assertEquals(answers.size(), requeriedAnswers.size());
            assertTrue(answers.containsAll(requeriedAnswers));
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_attachingStrayAttributeToEntityDoesntThrowErrors() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x isa yetAnotherEntity, has derived-resource-string 'unattached'; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(2, answers.size());
        }
    }

}
