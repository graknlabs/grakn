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

package io.mindmaps.test.graql.query;

import com.google.common.collect.Lists;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.test.AbstractMovieGraphTest;
import io.mindmaps.util.Schema;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.all;
import static io.mindmaps.graql.Graql.and;
import static io.mindmaps.graql.Graql.any;
import static io.mindmaps.graql.Graql.contains;
import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.Graql.gt;
import static io.mindmaps.graql.Graql.gte;
import static io.mindmaps.graql.Graql.id;
import static io.mindmaps.graql.Graql.lt;
import static io.mindmaps.graql.Graql.lte;
import static io.mindmaps.graql.Graql.neq;
import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.regex;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;
import static io.mindmaps.util.Schema.MetaSchema.ENTITY_TYPE;
import static io.mindmaps.util.Schema.MetaSchema.RESOURCE_TYPE;
import static io.mindmaps.util.Schema.MetaSchema.RULE_TYPE;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class MatchQueryTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    @Before
    public void setUp() {
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testMovieQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"));

        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testProductionQuery() {
        MatchQuery query = qb.match(var("x").isa("production"));

        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testValueQuery() {
        MatchQuery query = qb.match(var("tgf").value("Godfather"));
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        Concept tgf = result.get("tgf");

        assertEquals("title", tgf.type().getId());
        assertEquals("Godfather", tgf.asResource().getValue());
    }

    @Test
    public void testRoleOnlyQuery() {
        MatchQuery query = qb.match(var().rel("actor", "x"));

        QueryUtil.assertResultsMatch(
                query, "x", "person",
                "Marlon-Brando", "Al-Pacino", "Miss-Piggy", "Kermit-The-Frog", "Martin-Sheen", "Robert-de-Niro",
                "Jude-Law", "Miranda-Heart", "Bette-Midler", "Sarah-Jessica-Parker"
        );
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery query = qb.match(
                var("x").isa("movie")
                        .has("title", any(lt("Juno").and(gt("Godfather")), eq("Apocalypse Now"), eq("Spy")).and(neq("Apocalypse Now")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Heat", "Spy");
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", all(lte("Juno"), gte("Godfather"), neq("Heat")).or(eq("The Muppets")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Godfather", "The-Muppets");
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                var("x").isa("genre").has("name", regex("^f.*y$"))
        );

        QueryUtil.assertResultsMatch(query, "x", "genre", "family", "fantasy");
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                var("x").isa("character").has("name", contains("ar"))
        );

        QueryUtil.assertResultsMatch(query, "x", "character", "Sarah", "Benjamin-L-Willard", "Harry");
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery query = qb.match(
                var("type").playsRole("character-being-played")
        );

        QueryUtil.assertResultsMatch(query, "type", ENTITY_TYPE.getId(), "character", "person");
    }

    @Test
    public void testRelationshipQuery() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("character").id("Don-Vito-Corleone"),
                var().rel("x").rel("y").rel("z")
        ).select("x", "y");
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        assertEquals("Godfather", result.get("x").getId());
        assertEquals("Marlon-Brando", result.get("y").getId());
    }

    @Test
    public void testIdQuery() {
        MatchQuery query = qb.match(or(var("x").id("character"), var("x").id("person")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), "character", "person");
    }

    @Test
    public void testKnowledgeQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var().rel("x").rel("y"),
                var("y").isa("movie"),
                var().rel("y").rel("z"),
                var("z").isa("person").id("Marlon-Brando")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", "Marlon-Brando", "Al-Pacino", "Martin-Sheen");
    }

    @Test
    public void testRoleQuery() {
        MatchQuery query = qb.match(
                var().rel("actor", "x").rel("y"),
                var("y").id("Apocalypse-Now")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", "Marlon-Brando", "Martin-Sheen");
    }

    @Test
    public void testResourceMatchQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime())
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Spy");
    }

    @Test
    public void testNameQuery() {
        MatchQuery query = qb.match(var("x").has("title", "Godfather"));
        QueryUtil.assertResultsMatch(query, "x", "movie", "Godfather");
    }


    @Test
    public void testIntPredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Apocalypse-Now", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testDoublePredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-average", gt(7.8))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Apocalypse-Now", "Godfather");
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", gte(DATE_FORMAT.parse("Tue Jun 23 12:34:56 GMT 1984").getTime()))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Spy", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testGlobalPredicateQuery() {
        Stream<Concept> query = qb.match(
                var("x").value(gt(500L).and(lt(1000000L)))
        ).get("x");

        // Results will contain any numbers greater than 500, but no strings
        List<Concept> results = query.collect(toList());

        assertEquals(1, results.size());
        Concept result = results.get(0);
        assertEquals(1000L, result.asResource().getValue());
        assertEquals("tmdb-vote-count", result.type().getId());
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", "x").rel("y"),
                var("y").id("Miss-Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "movie", "The-Muppets");
    }

    @Test
    public void testAndOrPattern() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").has("name", "drama"), var().rel("x").rel("y")),
                        var("x").has("title", "The Muppets")
                )
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Godfather", "Apocalypse-Now", "Heat", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(id("genre").playsRole(var("x")));
        QueryUtil.assertResultsMatch(query, "x", null, "genre-of-production", "has-name-owner");
    }

    @Test
    public void testVariableAsRoleType() {
        MatchQuery query = qb.match(var().rel(var().id("genre-of-production"), "y"));
        QueryUtil.assertResultsMatch(
                query, "y", null,
                "crime", "drama", "war", "action", "comedy", "family", "musical", "comedy", "fantasy"
        );
    }

    @Test
    public void testVariableAsRoleplayer() {
        MatchQuery query = qb.match(
                var().rel(var("x").isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, "Godfather", "Heat");
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(id("production-with-genre"), var("x").isa(var().sub(id("production"))))
                        .rel(var().has("name", "crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, "Godfather", "Heat");
    }

    @Test
    public void testSubSelf() {
        MatchQuery query = qb.match(id("movie").sub(var("x")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), "movie", "production");
    }

    @Test
    public void testHasValue() {
        MatchQuery query = qb.match(var("x").value()).limit(10);
        assertEquals(10, query.stream().count());
        assertTrue(query.stream().allMatch(results -> results.get("x").asResource().getValue() != null));
    }

    @Test
    public void testHasReleaseDate() {
        MatchQuery query = qb.match(var("x").has("release-date"));
        assertEquals(4, query.stream().count());
        assertTrue(query.stream().map(results -> results.get("x")).allMatch(
                x -> x.asEntity().resources().stream().anyMatch(
                        resource -> resource.type().getId().equals("release-date")
                )
        ));
    }

    @Test
    public void testAllowedToReferToNonExistentRoleplayer() {
        long count = qb.match(var().rel("actor", id("doesnt-exist"))).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").id("Robert-de-Niro")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", null, "Heat", "Neil-McCauley");
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").id("Kermit-The-Frog")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", null, "The-Muppets", "Kermit-The-Frog");
    }

    @Test
    public void testMatchDataType() {
        MatchQuery query = qb.match(var("x").datatype(ResourceType.DataType.DOUBLE));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-average");

        query = qb.match(var("x").datatype(ResourceType.DataType.LONG));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-count", "runtime", "release-date");

        query = qb.match(var("x").datatype(ResourceType.DataType.BOOLEAN));
        assertEquals(0, query.stream().count());

        query = qb.match(var("x").datatype(ResourceType.DataType.STRING));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "title", "gender", "real-name", "name");
    }

    @Test
    public void testSelectRuleTypes() {
        MatchQuery query = qb.match(var("x").isa(RULE_TYPE.getId()));
        QueryUtil.assertResultsMatch(query, "x", RULE_TYPE.getId(), "a-rule-type", "inference-rule", "constraint-rule");
    }

    @Test
    public void testMatchRuleRightHandSide() {
        MatchQuery query = qb.match(var("x").lhs("$x id 'expect-lhs';").rhs("$x id 'expect-rhs';"));
        QueryUtil.assertResultsMatch(query, "x", "a-rule-type", "expectation-rule");
        assertTrue(query.iterator().next().get("x").asRule().getExpectation());
    }

    @Test
    public void testDisconnectedQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"), var("y").isa("person"));
        int numPeople = 10;
        assertEquals(QueryUtil.movies.length * numPeople, query.stream().count());
    }

    @Test
    public void testSubRelationType() {
        // Work with a fresh graph for this test
        rollbackGraph();

        qb.insert(
                id("ownership").isa("relation-type").hasRole("owner").hasRole("possession"),
                id("organization-with-shares").sub("possession"),
                id("possession").isa("role-type"),

                id("share-ownership").sub("ownership").hasRole("shareholder").hasRole("organization-with-shares"),
                id("shareholder").sub("owner"),
                id("owner").isa("role-type"),

                id("person").isa("entity-type").playsRole("shareholder"),
                id("company").isa("entity-type").playsRole("organization-with-shares"),

                id("apple").isa("company"),
                id("bob").isa("person"),

                var().rel("organization-with-shares", id("apple")).rel("shareholder", id("bob")).isa("share-ownership")
        ).execute();

        // This should work despite subs
        qb.match(var().rel("x").rel("shareholder", "y").isa("ownership")).stream().count();
    }

    @Test
    public void testHasVariable() {
        MatchQuery query = qb.match(var().id("Godfather").has("tmdb-vote-count", var("x")));
        assertEquals(1000L, query.get("x").findFirst().get().asResource().getValue());
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.match(var("x").regex("(fe)?male"));
        assertEquals(1, query.stream().count());
        assertEquals("gender", query.get("x").findFirst().get().getId());
    }

    @Test
    public void testPlaysRoleSub() {
        qb.insert(
                id("c").sub(id("b").sub(id("a").isa("entity-type"))),
                id("f").sub(id("e").sub(id("d").isa("role-type"))),
                id("b").playsRole("e")
        ).execute();

        // Make sure SUBs are followed correctly...
        assertTrue(qb.match(id("b").playsRole("e")).ask().execute());
        assertTrue(qb.match(id("b").playsRole("f")).ask().execute());
        assertTrue(qb.match(id("c").playsRole("e")).ask().execute());
        assertTrue(qb.match(id("c").playsRole("f")).ask().execute());

        // ...and not incorrectly
        assertFalse(qb.match(id("a").playsRole("d")).ask().execute());
        assertFalse(qb.match(id("a").playsRole("e")).ask().execute());
        assertFalse(qb.match(id("a").playsRole("f")).ask().execute());
        assertFalse(qb.match(id("b").playsRole("d")).ask().execute());
        assertFalse(qb.match(id("c").playsRole("d")).ask().execute());
    }

    @Test
    public void testMatchQueryExecuteAndParallelStream() {
        MatchQuery query = qb.match(var("x").isa("movie"));
        List<Map<String, Concept>> list = query.execute();
        assertEquals(list, query.parallelStream().collect(toList()));
    }

    @Test
    public void testDistinctRoleplayers() {
        MatchQuery query = qb.match(var().rel("x").rel("y").rel("z").isa("has-cast"));

        assertNotEquals(0, query.stream().count());

        // Make sure none of the resulting relationships have 3 role-players all the same
        query.forEach(result -> {
            Concept x = result.get("x");
            Concept y = result.get("y");
            Concept z = result.get("z");
            assertFalse(x + " = " + y + " = " + z, x.equals(y) && x.equals(z));
        });
    }

    @Test
    public void testRelatedToSelf() {
        MatchQuery query = qb.match(var().rel("x").rel("x").rel("x"));
        
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testMatchAll() {
        MatchQuery query = qb.match(var("x"));

        // Make sure there a reasonable number of results
        assertTrue(query.stream().count() > 10);

        query.get("x").forEach(concept -> {
            // Make sure results never contain castings
            if (concept.type() != null) {
                assertFalse(concept.type().isRoleType());
            }
        });
    }

    @Test
    public void testMatchAllPairs() {
        long numConcepts = qb.match(var("x")).stream().count();
        MatchQuery pairs = qb.match(var("x"), var("y"));

        // We expect there to be a result for every pair of concepts
        assertEquals(numConcepts * numConcepts, pairs.stream().count());
    }

    @Test
    public void testNoInstancesOfRoleType() {
        MatchQuery query = qb.match(var("x").isa(var("y")), var("y").id("actor"));
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testNoInstancesOfRoleTypeUnselectedVariable() {
        MatchQuery query = qb.match(var().isa(var("y")), var("y").id("actor"));
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testNoInstancesOfRoleTypeStartingFromCasting() {
        MatchQuery query = qb.match(var("x").isa(var("y")));

        query.get("y").forEach(concept -> {
            assertFalse(concept.isRoleType());
        });
    }

    @Test
    public void testCannotLookUpCastingById() {
        String castingId = graph.getTinkerTraversal()
                .hasLabel(Schema.BaseType.CASTING.name()).<String>values(ITEM_IDENTIFIER.name()).next();

        MatchQuery query = qb.match(var("x").id(castingId));
        assertEquals(0, query.stream().count());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMatchEmpty() {
        qb.match().execute();
    }
}