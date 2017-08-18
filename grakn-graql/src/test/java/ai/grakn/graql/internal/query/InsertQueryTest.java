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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static ai.grakn.concept.AttributeType.DataType.BOOLEAN;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static ai.grakn.util.Schema.ImplicitType.KEY;
import static ai.grakn.util.Schema.ImplicitType.KEY_OWNER;
import static ai.grakn.util.Schema.ImplicitType.KEY_VALUE;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.RELATIONSHIP;
import static ai.grakn.util.Schema.MetaSchema.ROLE;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class InsertQueryTest {

    private QueryBuilder qb;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext movieKB = SampleKBContext.preLoad(MovieKB.get());

    @Before
    public void setUp() {
        qb = movieKB.tx().graql();
    }

    @After
    public void clear(){
        movieKB.rollback();
    }

    @Test
    public void testInsertId() {
        assertInsert(var("x").has("name", "abc").isa("genre"));
    }

    @Test
    public void testInsertValue() {
        assertInsert(var("x").val(LocalDateTime.of(1992, 10, 7, 13, 14, 15)).isa("release-date"));
    }

    @Test
    public void testInsertIsa() {
        assertInsert(var("x").has("title", "Titanic").isa("movie"));
    }

    @Test
    public void testInsertSub() {
        assertInsert(var("x").label("cool-movie").sub("movie"));
    }

    @Test
    public void testInsertMultiple() {
        assertInsert(
                var("x").has("name", "123").isa("person"),
                var("y").val(123L).isa("runtime"),
                var("z").isa("language")
        );
    }

    @Test
    public void testInsertResource() {
        assertInsert(var("x").isa("movie").has("title", "Gladiator").has("runtime", 100L));
    }

    @Test
    public void testInsertName() {
        assertInsert(var("x").isa("movie").has("title", "Hello"));
    }

    @Test
    public void testInsertRelation() {
        VarPattern rel = var("r").isa("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y");
        VarPattern x = var("x").has("title", "Godfather").isa("movie");
        VarPattern y = var("y").has("name", "comedy").isa("genre");
        VarPattern[] vars = new VarPattern[] {rel, x, y};
        Pattern[] patterns = new Pattern[] {rel, x, y};

        assertNotExists(qb.match(patterns));

        qb.insert(vars).execute();
        assertExists(qb, patterns);

        qb.match(patterns).delete("r").execute();
        assertNotExists(qb, patterns);
    }

    @Test
    public void testInsertSameVarName() {
        qb.insert(var("x").has("title", "SW"), var("x").has("title", "Star Wars").isa("movie")).execute();

        assertExists(qb, var().isa("movie").has("title", "SW"));
        assertExists(qb, var().isa("movie").has("title", "Star Wars"));
        assertExists(qb, var().isa("movie").has("title", "SW").has("title", "Star Wars"));
    }

    @Test
    public void testInsertRepeat() {
        VarPattern language = var("x").has("name", "123").isa("language");
        InsertQuery query = qb.insert(language);

        assertEquals(0, qb.match(language).stream().count());
        query.execute();
        assertEquals(1, qb.match(language).stream().count());
        query.execute();
        assertEquals(2, qb.match(language).stream().count());
        query.execute();
        assertEquals(3, qb.match(language).stream().count());

        qb.match(language).delete("x").execute();
        assertEquals(0, qb.match(language).stream().count());
    }

    @Test
    public void testMatchInsertQuery() {
        VarPattern language1 = var().isa("language").has("name", "123");
        VarPattern language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertExists(qb, language1);
        assertExists(qb, language2);

        qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO")).execute();
        assertExists(qb, var().isa("language").has("name", "123").has("name", "HELLO"));
        assertExists(qb, var().isa("language").has("name", "456").has("name", "HELLO"));

        qb.match(var("x").isa("language")).delete("x").execute();
        assertNotExists(qb, language1);
        assertNotExists(qb, language2);
    }

    @Test
    public void testInsertSchema() {
        qb.insert(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name"),

                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        ).execute();

        assertExists(qb, label("pokemon").sub(ENTITY.getLabel().getValue()));
        assertExists(qb, label("evolution").sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, label("evolves-from").sub(ROLE.getLabel().getValue()));
        assertExists(qb, label("evolves-to").sub(ROLE.getLabel().getValue()));
        assertExists(qb, label("evolution").relates("evolves-from").relates("evolves-to"));
        assertExists(qb, label("pokemon").plays("evolves-from").plays("evolves-to"));

        assertExists(qb,
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon")
        );

        assertExists(qb,
                var("x").has("name", "Pichu").isa("pokemon"),
                var("y").has("name", "Pikachu").isa("pokemon"),
                var().rel("evolves-from", "x").rel("evolves-to", "y").isa("evolution")
        );

        assertExists(qb,
                var("y").has("name", "Pikachu").isa("pokemon"),
                var("z").has("name", "Raichu").isa("pokemon"),
                var().rel("evolves-from", "y").rel("evolves-to", "z").isa("evolution")
        );
    }

    @Test
    public void testInsertIsAbstract() {
        qb.insert(
                label("concrete-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getLabel().getValue())
        ).execute();

        assertNotExists(qb, label("concrete-type").isAbstract());
        assertExists(qb, label("abstract-type").isAbstract());
    }

    @Test
    public void testInsertDatatype() {
        qb.insert(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ).execute();

        MatchQuery query = qb.match(var("x").label("my-type"));
        AttributeType.DataType datatype = query.iterator().next().get("x").asAttributeType().getDataType();

        Assert.assertEquals(AttributeType.DataType.LONG, datatype);
    }

    @Test
    public void testInsertSubResourceType() {
        qb.insert(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("sub-type").sub("my-type")
        ).execute();

        MatchQuery query = qb.match(var("x").label("sub-type"));
        AttributeType.DataType datatype = query.iterator().next().get("x").asAttributeType().getDataType();

        Assert.assertEquals(AttributeType.DataType.STRING, datatype);
    }

    @Test
    public void testInsertSubRoleType() {
        qb.insert(
                label("marriage").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).relates("spouse1").relates("spouse2"),
                label("spouse").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("spouse1").sub("spouse"),
                label("spouse2").sub("spouse")
        ).execute();

        assertExists(qb, label("spouse1"));
    }

    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        qb.insert(
                var("abc").sub("entity"),
                var("abc").label("123"),
                label("123").plays("actor"),
                var("abc").plays("director")
        ).execute();

        assertExists(qb, label("123").sub("entity"));
        assertExists(qb, label("123").plays("actor"));
        assertExists(qb, label("123").plays("director"));
    }

    @Test
    public void testIterateInsertResults() {
        InsertQuery insert = qb.insert(
                var("x").has("name", "123").isa("person"),
                var("z").has("name", "xyz").isa("language")
        );

        Set<Answer> results = insert.stream().collect(toSet());
        assertEquals(1, results.size());
        Answer result = results.iterator().next();
        assertEquals(ImmutableSet.of(var("x"), var("z")), result.keySet());
        assertThat(result.values(), Matchers.everyItem(notNullValue(Concept.class)));
    }

    @Test
    public void testIterateMatchInsertResults() {
        VarPattern language1 = var().isa("language").has("name", "123");
        VarPattern language2 = var().isa("language").has("name", "456");

        qb.insert(language1, language2).execute();
        assertExists(qb, language1);
        assertExists(qb, language2);

        InsertQuery query = qb.match(var("x").isa("language")).insert(var("x").has("name", "HELLO"));
        Iterator<Answer> results = query.iterator();

        assertNotExists(qb, var().isa("language").has("name", "123").has("name", "HELLO"));
        assertNotExists(qb, var().isa("language").has("name", "456").has("name", "HELLO"));

        Answer result1 = results.next();
        assertEquals(ImmutableSet.of(var("x")), result1.keySet());

        boolean query123 = qb.match(var().isa("language").has("name", "123").has("name", "HELLO")).iterator().hasNext();
        boolean query456 = qb.match(var().isa("language").has("name", "456").has("name", "HELLO")).iterator().hasNext();

        //Check if one of the matches have had the insert executed correctly
        boolean oneExists = query123 != query456;
        assertTrue("A match insert was not executed correctly for only one match", oneExists);

        //Check that both are inserted correctly
        Answer result2 = results.next();
        assertEquals(ImmutableSet.of(var("x")), result1.keySet());
        assertExists(qb, var().isa("language").has("name", "123").has("name", "HELLO"));
        assertExists(qb, var().isa("language").has("name", "456").has("name", "HELLO"));
        assertFalse(results.hasNext());

        assertNotEquals(result1.get("x"), result2.get("x"));
    }

    @Test
    public void testErrorWhenInsertWithPredicate() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("predicate");
        qb.insert(var().id(ConceptId.of("123")).val(gt(3))).execute();
    }

    @Test
    public void testErrorWhenInsertWithMultipleIds() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("123"), containsString("456")));
        qb.insert(var().id(ConceptId.of("123")).id(ConceptId.of("456")).isa("movie")).execute();
    }

    @Test
    public void whenInsertingAResourceWithMultipleValues_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(isOneOf(
                GraqlQueryException.insertMultipleProperties("val", "123", "456").getMessage(),
                GraqlQueryException.insertMultipleProperties("val", "456", "123").getMessage()
        ));

        qb.insert(var().val("123").val("456").isa("title")).execute();
    }

    @Test
    public void testErrorWhenSubRelation() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("isa"), containsString("relation")));
        qb.insert(
                var().sub("has-genre").rel("genre-of-production", "x").rel("production-with-genre", "y"),
                var("x").id(ConceptId.of("Godfather")).isa("movie"),
                var("y").id(ConceptId.of("comedy")).isa("genre")
        ).execute();
    }

    @Test
    public void testInsertReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        qb.insert(
                label("new-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("new-type").plays(roleTypeLabel),
                var("x").isa("new-type")
        ).execute();

        MatchQuery typeQuery = qb.match(var("n").label("new-type"));

        assertEquals(1, typeQuery.stream().count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = typeQuery.get("n").findFirst().get().asEntityType();

        assertTrue(newType.plays().anyMatch(role -> role.equals(movieKB.tx().getRole(roleTypeLabel))));

        assertExists(qb, var().isa("new-type"));
    }

    @Test
    public void testInsertRuleType() {
        assertInsert(var("x").label("my-inference-rule").sub(RULE.getLabel().getValue()));
    }

    @Test
    public void testInsertRule() {
        String ruleTypeId = "a-rule-type";
        Pattern when = qb.parsePattern("$x isa entity");
        Pattern then = qb.parsePattern("$x isa entity");
        VarPattern vars = var("x").isa(ruleTypeId).when(when).then(then);
        qb.insert(vars).execute();

        RuleType ruleType = movieKB.tx().getRuleType(ruleTypeId);
        boolean found = ruleType.instances().
                anyMatch(rule -> when.equals(rule.getWhen()) && then.equals(rule.getThen()));

        assertTrue("Unable to find rule with when [" + when + "] and then [" + then + "]", found);
    }

    @Test
    public void testInsertRuleSub() {
        assertInsert(var("x").label("an-sub-rule-type").sub("a-rule-type"));
    }

    @Test
    public void testInsertRepeatType() {
        assertInsert(var("x").has("title", "WOW A TITLE").isa("movie").isa("movie"));
    }

    @Test
    public void testInsertResourceTypeAndInstance() {
        qb.insert(
                label("movie").has("my-resource"),
                label("my-resource").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                var("x").isa("movie").has("my-resource", "look a string")
        ).execute();
    }

    @Test
    public void testHas() {
        String resourceType = "a-new-resource-type";

        qb.insert(
                label("a-new-type").sub("entity").has(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("an-unconnected-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ).execute();

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertExists(qb, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(qb, label("a-new-type").has("title"));
        assertNotExists(qb, label("movie").has(resourceType));
        assertNotExists(qb, label("a-new-type").has("an-unconnected-resource-type"));

        VarPattern hasResource = Graql.label(HAS.getLabel(resourceType));
        VarPattern hasResourceOwner = Graql.label(HAS_OWNER.getLabel(resourceType));
        VarPattern hasResourceValue = Graql.label(HAS_VALUE.getLabel(resourceType));

        // Make sure the expected schema elements are created
        assertExists(qb, hasResource.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, hasResourceOwner.sub(ROLE.getLabel().getValue()));
        assertExists(qb, hasResourceValue.sub(ROLE.getLabel().getValue()));
        assertExists(qb, hasResource.relates(hasResourceOwner));
        assertExists(qb, hasResource.relates(hasResourceValue));
        assertExists(qb, label("a-new-type").plays(hasResourceOwner));
        assertExists(qb, label(resourceType).plays(hasResourceValue));
    }

    @Test
    public void testKey() {
        String resourceType = "a-new-resource-type";

        qb.insert(
                label("a-new-type").sub("entity").key(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertExists(qb, label("a-new-type").sub("entity").key(resourceType));
        assertExists(qb, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(qb, label("a-new-type").sub("entity").key("title"));
        assertNotExists(qb, label("movie").sub("entity").key(resourceType));

        VarPattern key = Graql.label(KEY.getLabel(resourceType));
        VarPattern keyOwner = Graql.label(KEY_OWNER.getLabel(resourceType));
        VarPattern keyValue = Graql.label(KEY_VALUE.getLabel(resourceType));

        // Make sure the expected schema elements are created
        assertExists(qb, key.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, keyOwner.sub(ROLE.getLabel().getValue()));
        assertExists(qb, keyValue.sub(ROLE.getLabel().getValue()));
        assertExists(qb, key.relates(keyOwner));
        assertExists(qb, key.relates(keyValue));
        assertExists(qb, label("a-new-type").plays(keyOwner));
        assertExists(qb, label(resourceType).plays(keyValue));
    }

    @Test
    public void testKeyCorrectUsage() throws InvalidKBException {
        // This should only run on tinker because it commits
        assumeTrue(GraknTestSetup.usingTinker());

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();
    }

    @Test
    public void whenInsertingAThingWithTwoKeyResources_Throw() throws InvalidKBException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                var().isa("a-new-type").has("a-new-resource-type", "hello").has("a-new-resource-type", "goodbye")
        ).execute();

        exception.expect(InvalidKBException.class);
        movieKB.tx().commit();
    }

    @Ignore // TODO: Un-ignore this when constraints are designed and implemented
    @Test
    public void testKeyUniqueValue() throws InvalidKBException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub("resource").datatype(AttributeType.DataType.STRING),
                var("x").isa("a-new-type").has("a-new-resource-type", "hello"),
                var("y").isa("a-new-type").has("a-new-resource-type", "hello")
        ).execute();

        exception.expect(InvalidKBException.class);
        movieKB.tx().commit();
    }

    @Test
    public void testKeyRequiredOwner() throws InvalidKBException {
        assumeTrue(GraknTestSetup.usingTinker()); // This should only run on tinker because it commits

        qb.insert(
                label("a-new-type").sub("entity").key("a-new-resource-type"),
                label("a-new-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                var().isa("a-new-type")
        ).execute();

        exception.expect(InvalidKBException.class);
        movieKB.tx().commit();
    }

    @Test
    public void testResourceTypeRegex() {
        qb.insert(label("greeting").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING).regex("hello|good day")).execute();

        MatchQuery match = qb.match(var("x").label("greeting"));
        assertEquals("hello|good day", match.get("x").findFirst().get().asAttributeType().getRegex());
    }

    @Test
    public void whenExecutingAnInsertQuery_ResultContainsAllInsertedVars() {
        Var x = var("x");
        Var type = var("type");
        Var type2 = var("type2");

        // Note that two variables refer to the same type. They should both be in the result
        InsertQuery query = qb.insert(x.isa(type), type.label("my-type").sub("entity"), type2.label("my-type"));

        Answer result = Iterables.getOnlyElement(query);
        assertThat(result.keySet(), containsInAnyOrder(x, type, type2));
        assertEquals(result.get(type), result.get(x).asEntity().type());
        assertEquals(result.get(type), result.get(type2));
    }

    @Test
    public void whenChangingTheSuperOfAnExistingConcept_ApplyTheChange() {
        EntityType newType = movieKB.tx().putEntityType("a-new-type");
        EntityType movie = movieKB.tx().getEntityType("movie");

        qb.match(var("x").label("a-new-type")).insert(var("x").sub("movie")).execute();
        
        assertEquals(movie, newType.sup());
    }

    @Test
    public void testErrorWhenInsertRelationWithEmptyRolePlayer() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("$y"), containsString("id"), containsString("isa"), containsString("sub"))
        );
        qb.insert(
                var().rel("genre-of-production", "x").rel("production-with-genre", "y").isa("has-genre"),
                var("x").isa("genre").has("name", "drama")
        ).execute();
    }

    @Test
    public void testErrorResourceTypeWithoutDataType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        qb.insert(label("my-resource").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue())).execute();
    }

    @Test
    public void testErrorWhenAddingInstanceOfConcept() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("meta-type"), containsString("my-thing"), containsString(Schema.MetaSchema.THING.getLabel().getValue()))
        );
        qb.insert(var("my-thing").isa(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.insert(label("thingy").sub("thingy")).execute();
    }

    @Test
    public void whenInsertingAnSchemaConceptWithoutALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("label")));
        qb.insert(var().sub("entity")).execute();
    }

    @Test
    public void whenInsertingAResourceWithoutAValue_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("name"), containsString("val")));
        qb.insert(var("x").isa("name")).execute();
    }

    @Test
    public void whenInsertingAnInstanceWithALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("label"), containsString("abc")));
        qb.insert(label("abc").isa("movie")).execute();
    }

    @Test
    public void whenInsertingAResourceWithALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("label"), containsString("bobby")));
        qb.insert(label("bobby").val("bob").isa("name")).execute();
    }

    @Test
    public void testInsertDuplicatePattern() {
        qb.insert(var().isa("person").has("name", "a name"), var().isa("person").has("name", "a name")).execute();
        assertEquals(2, qb.match(var().has("name", "a name")).stream().count());
    }

    @Test
    public void testInsertResourceOnExistingId() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x").findAny().get().getId();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertResourceOnExistingIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now")).get("x").findAny().get().getId();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).isa("movie").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertResourceOnExistingResourceId() {
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x").findAny().get().getId();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertResourceOnExistingResourceIdWithType() {
        ConceptId apocalypseNow = qb.match(var("x").val("Apocalypse Now")).get("x").findAny().get().getId();

        assertNotExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
        qb.insert(var().id(apocalypseNow).isa("title").has("title", "Apocalypse Maybe Tomorrow")).execute();
        assertExists(qb, var().id(apocalypseNow).has("title", "Apocalypse Maybe Tomorrow"));
    }

    @Test
    public void testInsertInstanceWithoutType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("isa")));
        qb.insert(var().has("name", "Bob")).execute();
    }

    @Test
    public void testInsertRuleWithoutLhs() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        qb.insert(var().isa("inference-rule").then(var("x").isa("movie"))).execute();
    }

    @Test
    public void testInsertRuleWithoutRhs() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        qb.insert(var().isa("inference-rule").when(var("x").isa("movie"))).execute();
    }

    @Test
    public void whenInsertingANonRuleWithAWhenPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("unexpected property"), containsString("when")));
        qb.insert(var().isa("movie").when(var("x"))).execute();
    }

    @Test
    public void whenInsertingANonRuleWithAThenPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("unexpected property"), containsString("then")));
        qb.insert(label("thingy").sub("movie").then(var("x"))).execute();
    }

    @Test
    public void testErrorWhenNonExistentResource() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("nothing");
        qb.insert(label("blah this").sub("entity").has("nothing")).execute();
    }

    @Test
    public void whenInsertingMetaType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", Schema.MetaSchema.THING.getLabel().getValue()));
        qb.insert(label("my-metatype").sub(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void whenInsertingMultipleRolePlayers_BothRolePlayersAreAdded() {
        List<Answer> results = qb.match(
                var("g").has("title", "Godfather"),
                var("m").has("title", "The Muppets")
        ).insert(
                var("c").isa("cluster").has("name", "2"),
                var("r").rel("cluster-of-production", "c").rel("production-with-cluster", "g").rel("production-with-cluster", "m").isa("has-cluster")
        ).execute();

        Thing cluster = results.get(0).get("c").asThing();
        Thing godfather = results.get(0).get("g").asThing();
        Thing muppets = results.get(0).get("m").asThing();
        Relationship relationship = results.get(0).get("r").asRelationship();

        Role clusterOfProduction = movieKB.tx().getRole("cluster-of-production");
        Role productionWithCluster = movieKB.tx().getRole("production-with-cluster");

        assertEquals(relationship.rolePlayers().collect(toSet()), ImmutableSet.of(cluster, godfather, muppets));
        assertEquals(relationship.rolePlayers(clusterOfProduction).collect(toSet()), ImmutableSet.of(cluster));
        assertEquals(relationship.rolePlayers(productionWithCluster).collect(toSet()), ImmutableSet.of(godfather, muppets));
    }

    @Test(expected = Exception.class)
    public void matchInsertNullVar() {
        movieKB.tx().graql().match(var("x").isa("movie")).insert((VarPattern) null).execute();
    }

    @Test(expected = Exception.class)
    public void matchInsertNullCollection() {
        movieKB.tx().graql().match(var("x").isa("movie")).insert((Collection<? extends VarPattern>) null).execute();
    }

    @Test
    public void whenMatchInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        movieKB.tx().graql().match(var()).insert(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullVar() {
        movieKB.tx().graql().insert((VarPattern) null).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullCollection() {
        movieKB.tx().graql().insert((Collection<? extends VarPattern>) null).execute();
    }

    @Test
    public void whenInsertingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        movieKB.tx().graql().insert(Collections.EMPTY_SET).execute();
    }

    @Test
    public void whenSettingTwoTypes_Throw() {
        EntityType movie = movieKB.tx().getEntityType("movie");
        EntityType person = movieKB.tx().getEntityType("person");

        // We don't know in what order the message will be
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(isOneOf(
                GraqlQueryException.insertMultipleProperties("isa", movie, person).getMessage(),
                GraqlQueryException.insertMultipleProperties("isa", person, movie).getMessage()
        ));

        movieKB.tx().graql().insert(var("x").isa("movie"), var("x").isa("person")).execute();
    }

    @Test
    public void whenSpecifyingExistingConceptIdWithIncorrectType_Throw() {
        EntityType movie = movieKB.tx().getEntityType("movie");
        EntityType person = movieKB.tx().getEntityType("person");

        Concept aMovie = movie.instances().iterator().next();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.insertPropertyOnExistingConcept("isa", person, aMovie).getMessage());

        movieKB.tx().graql().insert(var("x").id(aMovie.getId()).isa("person")).execute();
    }

    @Test
    public void whenSpecifyingExistingTypeWithIncorrectDataType_Throw() {
        AttributeType name = movieKB.tx().getAttributeType("name");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                GraqlQueryException.insertPropertyOnExistingConcept("datatype", BOOLEAN, name).getMessage()
        );

        movieKB.tx().graql().insert(label("name").datatype(BOOLEAN)).execute();
    }

    @Test
    public void whenSpecifyingDataTypeOnAnEntityType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("unexpected property"), containsString("datatype"), containsString("my-type"))
        );

        movieKB.tx().graql().insert(label("my-type").sub("entity").datatype(BOOLEAN)).execute();
    }

    private void assertInsert(VarPattern... vars) {
        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertNotExists(qb, var);
        }

        // Insert all vars
        qb.insert(vars).execute();

        // Make sure all vars exist
        for (VarPattern var : vars) {
            assertExists(qb, var);
        }

        // Delete all vars
        for (VarPattern var : vars) {
            qb.match(var).delete(var.admin().var()).execute();
        }

        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertNotExists(qb, var);
        }
    }
}
