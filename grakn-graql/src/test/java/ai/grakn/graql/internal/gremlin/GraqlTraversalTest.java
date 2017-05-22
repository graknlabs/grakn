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

package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.util.CommonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.gremlin.GraqlMatchers.feature;
import static ai.grakn.graql.internal.gremlin.GraqlMatchers.satisfies;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.id;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inIsa;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inRelates;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inRolePlayer;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsa;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outRelates;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outRolePlayer;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.value;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraqlTraversalTest {

    private static final Var a = Var.of("a");
    private static final Var b = Var.of("b");
    private static final Var c = Var.of("c");
    private static final Var x = Var.of("x");
    private static final Var y = Var.of("y");
    private static final Var z = Var.of("z");
    private static final Var xx = Var.of("xx");
    private static final Var yy = Var.of("yy");
    private static final Var zz = Var.of("zz");
    private static final Fragment xId = id(x, ConceptId.of("Titanic"));
    private static final Fragment xValue = value(x, eq("hello").admin());
    private static final Fragment yId = id(y, ConceptId.of("movie"));
    private static final Fragment xIsaY = outIsa(x, y);
    private static final Fragment yTypeOfX = inIsa(y, x);

    private static final GraqlTraversal fastIsaTraversal = traversal(yId, yTypeOfX);
    private static GraknGraph graph;

    @BeforeClass
    public static void setUp() {
        graph = mock(GraknGraph.class);

        // We have to mock out the `subTypes` call because the shortcut edge optimisation checks it
        when(graph.getType(any())).thenAnswer(invocation -> {
            Type type = mock(Type.class);
            //noinspection unchecked
            when(type.subTypes()).thenReturn((Collection) ImmutableSet.of(type));
            when(type.getLabel()).thenReturn(invocation.getArgument(0));
            return type;
        });
    }

    @Test
    public void testComplexityIndexVsIsa() {
        GraqlTraversal indexTraversal = traversal(xId);
        assertFaster(indexTraversal, fastIsaTraversal);
    }

    @Test
    public void testComplexityFastIsaVsSlowIsa() {
        GraqlTraversal slowIsaTraversal = traversal(xIsaY, yId);
        assertFaster(fastIsaTraversal, slowIsaTraversal);
    }

    @Test
    public void testComplexityConnectedVsDisconnected() {
        GraqlTraversal connectedDoubleIsa = traversal(xIsaY, outIsa(y, z));
        GraqlTraversal disconnectedDoubleIsa = traversal(xIsaY, inIsa(z, y));
        assertFaster(connectedDoubleIsa, disconnectedDoubleIsa);
    }

    @Test
    public void testGloballyOptimalIsFasterThanLocallyOptimal() {
        GraqlTraversal locallyOptimalSpecificInstance = traversal(yId, yTypeOfX, xId);
        GraqlTraversal globallyOptimalSpecificInstance = traversal(xId, xIsaY, yId);
        assertFaster(globallyOptimalSpecificInstance, locallyOptimalSpecificInstance);
    }

    @Test
    public void testRelatesFasterFromRoleType() {
        GraqlTraversal relatesFromRelationType = traversal(yId, outRelates(y, x), xId);
        GraqlTraversal relatesFromRoleType = traversal(xId, inRelates(x, y), yId);
        assertFaster(relatesFromRoleType, relatesFromRelationType);
    }

    @Test
    public void testResourceWithTypeFasterFromType() {
        GraqlTraversal fromInstance =
                traversal(outIsa(x, xx), id(xx, ConceptId.of("_")), inShortcut(x, z), outShortcut(z, y));
        GraqlTraversal fromType =
                traversal(id(xx, ConceptId.of("_")), inIsa(xx, x), inShortcut(x, z), outShortcut(z, y));
        assertFaster(fromType, fromInstance);
    }

    @Test
    public void valueFilteringIsBetterThanANonFilteringOperation() {
        GraqlTraversal valueFilterFirst = traversal(value(x, gt(1).admin()), inShortcut(x, b), outShortcut(b, y), outIsa(y, z));
        GraqlTraversal shortcutFirst = traversal(outIsa(y, z), inShortcut(y, b), outShortcut(b, x), value(x, gt(1).admin()));

        assertFaster(valueFilterFirst, shortcutFirst);
    }

    @Test
    public void testCheckDistinctCastingEarlyFaster() {
        Var c1 = Var.of("c1");
        Var c2 = Var.of("c2");
        Var r = Var.of("r");

        Fragment neq = Fragments.neq(c2, c1);
        Fragment inRolePlayer = inRolePlayer(x, c1);
        Fragment inCasting = inCasting(c1, r);
        Fragment outCasting = outCasting(r, c2);
        Fragment outRolePlayer = outRolePlayer(c2, y);

        GraqlTraversal distinctEarly =
                traversal(xId, inRolePlayer, inCasting, outCasting, neq, outRolePlayer);
        GraqlTraversal distinctLate =
                traversal(xId, inRolePlayer, inCasting, outCasting, outRolePlayer, neq);

        assertFaster(distinctEarly, distinctLate);
    }

    @Test
    public void testAllTraversalsSimpleQuery() {
        VarPattern pattern = Patterns.var(x).id(ConceptId.of("Titanic")).isa(Patterns.var(y).id(ConceptId.of("movie")));
        Set<GraqlTraversal> traversals = allGraqlTraversals(pattern).collect(toSet());

        assertEquals(12, traversals.size());

        Set<GraqlTraversal> expected = ImmutableSet.of(
                traversal(xId, xIsaY, yId),
                traversal(xId, yTypeOfX, yId),
                traversal(xId, yId, xIsaY),
                traversal(xId, yId, yTypeOfX),
                traversal(xIsaY, xId, yId),
                traversal(xIsaY, yId, xId),
                traversal(yTypeOfX, xId, yId),
                traversal(yTypeOfX, yId, xId),
                traversal(yId, xId, xIsaY),
                traversal(yId, xId, yTypeOfX),
                traversal(yId, xIsaY, xId),
                traversal(yId, yTypeOfX, xId)
        );

        assertEquals(expected, traversals);
    }

    @Test
    public void testOptimalShortQuery() {
        assertNearlyOptimal(var(x).isa(var(y).id(ConceptId.of("movie"))));
    }

    @Test
    public void testOptimalBothId() {
        assertNearlyOptimal(var(x).id(ConceptId.of("Titanic")).isa(var(y).id(ConceptId.of("movie"))));
    }

    @Test
    public void testOptimalByValue() {
        assertNearlyOptimal(var(x).val("hello").isa(var(y).id(ConceptId.of("movie"))));
    }

    @Test
    public void testOptimalAttachedResource() {
        assertNearlyOptimal(var()
                .rel(var(x).isa(var(y).id(ConceptId.of("movie"))))
                .rel(var(z).val("Titanic").isa(var("a").id(ConceptId.of("title")))));
    }

    @Ignore // TODO: This is now super-slow
    @Test
    public void makeSureTypeIsCheckedBeforeFollowingAShortcut() {
        assertNearlyOptimal(and(
                var(x).id(ConceptId.of("xid")),
                var().rel(var(x)).rel(var(y)),
                var(y).isa(var(b).label("person")),
                var().rel(var(y)).rel(var(z))
        ));
    }

    @Test
    public void whenPlanningSimpleUnaryRelation_ApplyShortcutOptimisation() {
        VarPattern rel = var("x").rel("y");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        // I know this is horrible, unfortunately I can't think of a better way...
        // The issue is that some things we want to inspect are not public, mainly:
        // 1. The variable name assigned to the casting
        // 2. The shortcut fragment classes
        // Both of these things should not be made public if possible, so I see this regex as the lesser evil
        assertThat(graqlTraversal, anyOf(
                matches("\\{\\$x-\\[shortcut:\\$.*]->\\$y}"),
                matches("\\{\\$y<-\\[shortcut:\\$.*]-\\$x}")
        ));
    }

    @Test
    public void whenPlanningSimpleBinaryRelationQuery_ApplyShortcutOptimisation() {
        VarPattern rel = var("x").rel("y").rel("z");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches("\\{\\$x-\\[shortcut:\\$.*]->\\$.* \\$x-\\[shortcut:\\$.*]->\\$.* \\$.*\\[neq:\\$.*]}"),
                matches("\\{\\$.*<-\\[shortcut:\\$.*]-\\$x-\\[shortcut:\\$.*]->\\$.* \\$.*\\[neq:\\$.*]}")
        ));
    }

    @Test
    public void whenPlanningBinaryRelationQueryWithType_ApplyShortcutOptimisation() {
        VarPattern rel = var("x").rel("y").rel("z").isa("marriage");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches(".*\\$x-\\[shortcut:\\$.* marriage]->\\$.* \\$x-\\[shortcut:\\$.* marriage]->\\$.* \\$.*\\[neq:\\$.*].*"),
                matches(".*\\$.*<-\\[shortcut:\\$.* marriage]-\\$x-\\[shortcut:\\$.* marriage]->\\$.* \\$.*\\[neq:\\$.*].*")
        ));
    }

    @Test
    public void testShortcutOptimisationWithRoles() {
        VarPattern rel = var("x").rel("y").rel("wife", "z");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches(".*\\$x-\\[shortcut:\\$.* wife]->\\$.* \\$x-\\[shortcut:\\$.*]->\\$.* \\$.*\\[neq:\\$.*].*"),
                matches(".*\\$.*<-\\[shortcut:\\$.* wife]-\\$x-\\[shortcut:\\$.*]->\\$.* \\$.*\\[neq:\\$.*].*")
        ));
    }

    private static GraqlTraversal semiOptimal(Pattern pattern) {
        return GreedyTraversalPlan.createTraversal(pattern.admin(), graph);
    }

    private static GraqlTraversal traversal(Fragment... fragments) {
        return traversal(ImmutableList.copyOf(fragments));
    }

    @SafeVarargs
    private static GraqlTraversal traversal(ImmutableList<Fragment>... fragments) {
        ImmutableSet<ImmutableList<Fragment>> fragmentsSet = ImmutableSet.copyOf(fragments);
        return GraqlTraversal.create(fragmentsSet);
    }

    private static Stream<GraqlTraversal> allGraqlTraversals(Pattern pattern) {
        Collection<Conjunction<VarPatternAdmin>> patterns = pattern.admin().getDisjunctiveNormalForm().getPatterns();

        List<Set<List<Fragment>>> collect = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, graph))
                .map(ConjunctionQuery::allFragmentOrders)
                .collect(toList());

        Set<List<List<Fragment>>> lists = Sets.cartesianProduct(collect);

        return lists.stream()
                .map(Sets::newHashSet)
                .map(GraqlTraversalTest::createTraversal)
                .flatMap(CommonUtil::optionalToStream);
    }

    // Returns a traversal only if the fragment ordering is valid
    private static Optional<GraqlTraversal> createTraversal(Set<List<Fragment>> fragments) {

        // Make sure all dependencies are met
        for (List<Fragment> fragmentList : fragments) {
            Set<Var> visited = new HashSet<>();

            for (Fragment fragment : fragmentList) {
                if (!visited.containsAll(fragment.getDependencies())) {
                    return Optional.empty();
                }

                visited.addAll(fragment.getVariableNames());
            }
        }

        return Optional.of(GraqlTraversal.create(fragments));
    }

    private static Fragment outShortcut(Var relation, Var rolePlayer) {
        return Fragments.outShortcut(relation, a, rolePlayer, Optional.empty(), Optional.empty());
    }

    private static Fragment inShortcut(Var rolePlayer, Var relation) {
        return Fragments.inShortcut(rolePlayer, c, relation, Optional.empty(), Optional.empty());
    }

    private static void assertNearlyOptimal(Pattern pattern) {
        GraqlTraversal traversal = semiOptimal(pattern);

        //noinspection OptionalGetWithoutIsPresent
        GraqlTraversal globalOptimum = allGraqlTraversals(pattern).min(comparing(GraqlTraversal::getComplexity)).get();

        double globalComplexity = globalOptimum.getComplexity();
        double complexity = traversal.getComplexity();

        // We use logarithms because we are only concerned with orders of magnitude of complexity
        assertTrue(
                "Expected\n " +
                        complexity + ":\t" + traversal + "\nto be similar speed to\n " +
                        globalComplexity + ":\t" + globalOptimum,
                Math.log(complexity) < Math.log(globalComplexity) * 2
        );
    }

    private static void assertFaster(GraqlTraversal fast, GraqlTraversal slow) {
        double fastComplexity = fast.getComplexity();
        double slowComplexity = slow.getComplexity();
        boolean condition = fastComplexity < slowComplexity;

        assertTrue(
                "Expected\n" + fastComplexity + ":\t" + fast + "\nto be faster than\n" + slowComplexity + ":\t" + slow,
                condition
        );
    }

    private <T> Matcher<T> matches(String regex) {
        return feature(satisfies(string -> string.matches(regex)), "matching " + regex, Object::toString);
    }
}
