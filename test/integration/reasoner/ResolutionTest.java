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

package grakn.core.reasoner;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.NoOpAggregator;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.rocks.RocksGrakn;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ResolutionTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("resolution-test");
    private static String database = "resolution-test";
    private static Grakn grakn;

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
    public void singleConcludable() throws InterruptedException {
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age;" +
                                "age sub attribute, value long;"));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        long atomicTraversalAnswerCount = 3L;
        long conjunctionTraversalAnswerCount = 3L;
        long answerCount = atomicTraversalAnswerCount + conjunctionTraversalAnswerCount;
        Conjunction conjunctionPattern = parseConjunction("{ $p1 has age 24; }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }


    @Test
    public void twoConcludables() throws InterruptedException {
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        long conjunctionTraversalAnswerCount = 3L;
        long atomic1TraversalAnswerCount = 3L;
        long atomic2TraversalAnswerCount = 3L;
        long answerCount = conjunctionTraversalAnswerCount + (atomic2TraversalAnswerCount * atomic1TraversalAnswerCount);
        Conjunction conjunctionPattern = parseConjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

    @Test
    public void filteringConcludable() throws InterruptedException {
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        long atomic1TraversalAnswerCount = 3L;
        long atomic2TraversalAnswerCount = 0L;
        long conjunctionTraversalAnswerCount = 0L;
        long answerCount = conjunctionTraversalAnswerCount + (atomic1TraversalAnswerCount * atomic2TraversalAnswerCount);
        Conjunction conjunctionPattern = parseConjunction("{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

    @Ignore // TODO Un-ignore
    @Test
    public void simpleRule() throws InterruptedException {
        // TODO This is what the initial test described, but since atomic2 is the conjunction the conjuntion traversal must return the same number of results as the atomic2 traversal.
        String atomic1 = "$p1 isa person, has name \"Bob\";";
        String atomic2 = "$p1 isa person, has age 42;";
        String rulePattern = "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 isa person, has age 42; };";

        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                rulePattern));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.commit();
            }
        }

        long atomic1TraversalAnswerCount = 3L;
        long ruleTraversalAnswerCount = 0L;
        long atomic2TraversalAnswerCount = 3L;
        long conjunctionTraversalAnswerCount = 0L;
        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount;
        Conjunction conjunctionPattern = parseConjunction("{ " + atomic2 + " }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

    @Ignore // TODO Un-ignore
    @Test
    public void concludableChainWithRule() throws InterruptedException {
        String atomic1 = "$p1 isa person, has name \"Bob\";";
        String atomic2 = "$p1 isa person, has age 42;";
        String atomic3 = "$p1 isa person; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins;";
        String rule = "rule bobs-are-42: when { $p1 has name \"Bob\" } then { $p1 has age 42; };";
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                rule));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.commit();
            }
        }
        long atomic1TraversalAnswerCount = 3L;
        long atomic2TraversalAnswerCount = 3L;
        long atomic3TraversalAnswerCount = 3L;
        long ruleTraversalAnswerCount = 3L;
        long conjunctionTraversalAnswerCount = 0L;
        long answerCount = conjunctionTraversalAnswerCount + (atomic3TraversalAnswerCount * (atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount));
        Conjunction conjunctionPattern = parseConjunction("{ " + atomic2 + atomic3 + " }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

    @Test
    public void shallowRerequestChain() throws InterruptedException {
        String atomic1 = "$p1 isa person; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins;";
        String atomic2 = "$p1 isa person; $p1 has name \"Alice\"";
        String atomic3 = "$p1 isa person; $p1 has age 24;";
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.commit();
            }
        }
        long atomic1TraversalAnswerCount = 3L;
        long atomic2TraversalAnswerCount = 3L;
        long atomic3TraversalAnswerCount = 3L;
        long conjunctionTraversalAnswerCount = 0L;
        long answerCount = conjunctionTraversalAnswerCount + (atomic3TraversalAnswerCount * atomic2TraversalAnswerCount * atomic1TraversalAnswerCount);
        Conjunction conjunctionPattern = parseConjunction("{ " + atomic1 + " " + atomic2 + " " + atomic3 + " }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

    @Test
    public void deepRerequestChain() throws InterruptedException {
        String atomic1 = "$p1 isa person; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins;";
        String atomic2 = "$p1 isa person; $p1 has name \"Alice\"";
        String atomic3 = "$p1 isa person; $p1 has age 24;";
        String atomic4 = "$p1 isa person; $p1 has name \"Bob\";";
        String atomic5 = "$p1 isa person; $p1 has age 72;";
        try (Grakn.Session session = schemaSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (Grakn.Session session = dataSession()) {
            try (Grakn.Transaction transaction = writeTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic2));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic3));
                transaction.query().insert(Graql.parseQuery("insert " + atomic4));
                transaction.query().insert(Graql.parseQuery("insert " + atomic4));
                transaction.query().insert(Graql.parseQuery("insert " + atomic4));
                transaction.query().insert(Graql.parseQuery("insert " + atomic5));
                transaction.query().insert(Graql.parseQuery("insert " + atomic5));
                transaction.query().insert(Graql.parseQuery("insert " + atomic5));
                transaction.commit();
            }
        }
        long atomic1TraversalAnswerCount = 3L;
        long atomic2TraversalAnswerCount = 3L;
        long atomic3TraversalAnswerCount = 3L;
        long atomic4TraversalAnswerCount = 3L;
        long atomic5TraversalAnswerCount = 3L;
        long conjunctionTraversalAnswerCount = 0L;
        long answerCount = conjunctionTraversalAnswerCount + (atomic5TraversalAnswerCount * atomic4TraversalAnswerCount * atomic3TraversalAnswerCount * atomic2TraversalAnswerCount * atomic1TraversalAnswerCount);
        Conjunction conjunctionPattern = parseConjunction("{ " + atomic5 + " " + atomic4 + " " + atomic3 + " " + atomic2 + " " + atomic1 + " }");
        setUpAndAssertResponses(conjunctionPattern, answerCount);
    }

//
//    @Test
//    public void recursiveTerminationAndDeduplication() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomicPattern = 1L;
//
//        List<Long> rulePattern = list(atomicPattern);
//        long ruleTraversalAnswerCount = 1L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomicPattern, Arrays.asList(rulePattern), atomic1TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomicPattern);
//        long conjunctionTraversalAnswerCount = 0L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        // the recursively produced answers will be identical, so will be deduplicated
//        long answerCount = conjunctionTraversalAnswerCount + atomic1TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount - atomic1TraversalAnswerCount;
//        assertResponses(root, responses, doneReceived, answerCount, registry);
//    }
//
//    @Test
//    public void answerRecorderTest() throws InterruptedException {
//        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
//        AtomicLong doneReceived = new AtomicLong(0L);
//        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
//        ResolverRegistry registry = new ResolverRegistry(elg);
//
//        long atomic1Pattern = 10L;
//        long atomic1TraversalAnswerCount = 1L;
//        registerConcludable(atomic1Pattern, list(), atomic1TraversalAnswerCount, registry);
//
//        List<Long> rulePattern = list(10L);
//        long ruleTraversalAnswerCount = 0L;
//        registerRule(rulePattern, ruleTraversalAnswerCount, registry);
//
//        long atomic2Pattern = 2010L;
//        long atomic2TraversalAnswerCount = 0L;
//        registerConcludable(atomic2Pattern, Arrays.asList(rulePattern), atomic2TraversalAnswerCount, registry);
//
//        List<Long> conjunctionPattern = list(atomic2Pattern);
//        long conjunctionTraversalAnswerCount = 1L;
//        Actor<RootResolver> root = registerRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet, registry);
//
//        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount + atomic1TraversalAnswerCount;
//
//        for (int i = 0; i < answerCount; i++) {
//            root.tell(actor ->
//                              actor.executeReceiveRequest(
//                                      new Request(new Request.Path(root), new ConceptMap(), null),
//                                      registry
//                              )
//            );
//            ResolutionAnswer answer = responses.take();
//
//            // TODO write more meaningful explanation tests
//            System.out.println(answer);
//        }
//    }

    private Grakn.Session schemaSession() {
        return grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    private Grakn.Session dataSession() {
        return grakn.session(database, Arguments.Session.Type.DATA);
    }

    private Grakn.Transaction writeTransaction(Grakn.Session session) {
        return session.transaction(Arguments.Transaction.Type.WRITE);
    }

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private void setUpAndAssertResponses(Conjunction conjunctionPattern, long answerCount) throws InterruptedException {
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");
        ResolverRegistry registry = new ResolverRegistry(elg);
        Actor<RootResolver> root = registry.createRoot(conjunctionPattern, responses::add, doneReceived::incrementAndGet);
        assertResponses(root, responses, doneReceived, answerCount, registry);
    }

    //    private void registerConcludable(long pattern, List<List<Long>> rules, long traversalAnswerCount, ResolverRegistry registry) {
//        registry.registerConcludable(pattern, rules, traversalAnswerCount);
//    }
//
//    private void registerRule(List<Long> pattern, long traversalAnswerCount, ResolverRegistry registry) {
//        registry.registerRule(pattern, traversalAnswerCount);
//    }

    private void assertResponses(final Actor<RootResolver> root, final LinkedBlockingQueue<ResolutionAnswer> responses,
                                 final AtomicLong doneReceived, final long answerCount, ResolverRegistry registry)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            root.tell(actor ->
                              actor.executeReceiveRequest(
                                      new Request(new Request.Path(root), NoOpAggregator.create(), ResolutionAnswer.Derivation.EMPTY),
                                      registry
                              )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            ResolutionAnswer answer = responses.take();
        }
        Thread.sleep(1000);
        assertEquals(1, doneReceived.get());
        assertTrue(responses.isEmpty());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}