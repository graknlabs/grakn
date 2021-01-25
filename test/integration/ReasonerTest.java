/*
 * Copyright (C) 2021 Grakn Labs
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
 *
 */

package grakn.core.test.integration;

import grakn.core.common.concurrent.actor.EventLoopGroup;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.logic.LogicManager;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class ReasonerTest {

    private static final Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final String database = "reasoner-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    private RocksTransaction singleThreadElgTransaction(RocksSession session, Arguments.Transaction.Type transactionType) {
        RocksTransaction transaction = session.transaction(transactionType);
        transaction.reasoner().resolverRegistry().setEventLoopGroup(new EventLoopGroup(1, "grakn-elg"));
        return transaction;
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
    }

    @After
    public void tearDown() {
        grakn.close();
    }

    @Test
    public void test_basic() {
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();

        final EntityType milk = conceptMgr.putEntityType("milk");
        final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
        milk.setOwns(ageInDays);
        rocksTransaction.commit();
        session.close();

        session = grakn.session(database, Arguments.Session.Type.DATA);

        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
        rocksTransaction.commit();

        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        List<ConceptMap> ans = rocksTransaction.query().match(Graql.parseQuery("match $x has age-in-days $a;").asMatch()).toList();
        System.out.println("Got answer: " + ans);

        ans.iterator().forEachRemaining(a -> {
            assertEquals("age-in-days", a.get("a").asThing().getType().getLabel().scopedName());
            assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
        });

        assertEquals(2, ans.size());

        rocksTransaction.close();
        session.close();
    }

    @Test
    public void test_has_explicit_rule() {
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();

        final EntityType milk = conceptMgr.putEntityType("milk");
        final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
        final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
        milk.setOwns(ageInDays);
        milk.setOwns(isStillGood);
        logicMgr.putRule(
                "old-milk-is-not-good",
                Graql.parsePattern("{ $x isa milk, has age-in-days >= 10; }").asConjunction(),
                Graql.parseVariable("$x has is-still-good false").asThing());
        rocksTransaction.commit();
        session.close();

        session = grakn.session(database, Arguments.Session.Type.DATA);

        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
        rocksTransaction.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
        rocksTransaction.commit();

        rocksTransaction = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE);
        List<ConceptMap> ans = rocksTransaction.query().match(Graql.parseQuery("match $x has is-still-good $a;").asMatch()).toList();
        System.out.println("Got answer: " + ans);

        ans.iterator().forEachRemaining(a -> {
            assertFalse(a.get("a").asAttribute().asBoolean().getValue());
            assertEquals("is-still-good", a.get("a").asThing().getType().getLabel().scopedName());
            assertEquals("milk", a.get("x").asThing().getType().getLabel().scopedName());
        });

        assertEquals(2, ans.size());

        rocksTransaction.close();
        session.close();
    }

    @Test
    public void test_relation_rule() {
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                final ConceptManager conceptMgr = txn.concepts();
                final LogicManager logicMgr = txn.logic();

                final EntityType person = conceptMgr.putEntityType("person");
                final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                person.setOwns(name);
                final RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                final RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                logicMgr.putRule(
                        "marriage-is-friendship",
                        Graql.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(Graql.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match $f (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();
                System.out.println("Got answer: " + ans);

                ans.iterator().forEachRemaining(a -> {
                    assertEquals("friendship", a.get("f").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.get("p1").asThing().getType().getLabel().scopedName());
                    assertEquals("person", a.get("p2").asThing().getType().getLabel().scopedName());
                    assertEquals("name", a.get("na").asAttribute().getType().getLabel().scopedName());
                });

                assertEquals(2, ans.size());
            }
        }
    }
}
