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
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static ai.grakn.concept.AttributeType.DataType.INTEGER;
import static ai.grakn.concept.AttributeType.DataType.STRING;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Felix Chapman
 */
public class UndefineQueryTest {

    private static final VarPattern THING = Graql.label(Schema.MetaSchema.THING.getLabel());
    private static final VarPattern ENTITY = Graql.label(Schema.MetaSchema.ENTITY.getLabel());
    private static final VarPattern RELATIONSHIP = Graql.label(Schema.MetaSchema.RELATIONSHIP.getLabel());
    private static final VarPattern ATTRIBUTE = Graql.label(Schema.MetaSchema.ATTRIBUTE.getLabel());
    private static final VarPattern ROLE = Graql.label(Schema.MetaSchema.ROLE.getLabel());
    private static final Label NEW_TYPE = Label.of("new-type");

    @Rule
    public final SampleKBContext movieKB = SampleKBContext.preLoad(MovieKB.get());
    public static final Var x = var("x");

    private QueryBuilder qb;
    private GraknTx tx;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        tx = movieKB.tx();
        qb = tx.graql();
    }

    @Test
    public void whenUndefiningDataType_DoNothing() {
        qb.undefine(label("name").datatype(STRING)).execute();

        assertEquals(STRING, tx.getAttributeType("name").getDataType());
    }

    @Test
    public void whenUndefiningHas_TheHasLinkIsDeleted() {
        qb.define(label(NEW_TYPE).sub(ENTITY).has("name")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        qb.undefine(label(NEW_TYPE).has("name")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningHasWhichDoesntExist_DoNothing() {
        qb.define(label(NEW_TYPE).sub(ENTITY).has("name")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        qb.undefine(label(NEW_TYPE).has("title")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = qb.define(x.label(NEW_TYPE).sub(ENTITY)).execute().get(x).asType();

        assertNotNull(tx.getType(NEW_TYPE));

        qb.undefine(var().id(newType.getId()).sub(ENTITY)).execute();

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningIsAbstract_TheTypeIsNoLongerAbstract() {
        qb.define(label(NEW_TYPE).sub(ENTITY).isAbstract()).execute();

        assertTrue(tx.getType(NEW_TYPE).isAbstract());

        qb.undefine(label(NEW_TYPE).isAbstract()).execute();

        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningIsAbstractOnNonAbstractType_DoNothing() {
        qb.define(label(NEW_TYPE).sub(ENTITY)).execute();

        assertFalse(tx.getType(NEW_TYPE).isAbstract());

        qb.undefine(label(NEW_TYPE).isAbstract()).execute();

        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningPlays_TheTypeNoLongerPlaysTheRole() {
        qb.define(label(NEW_TYPE).sub(ENTITY).plays("actor")).execute();

        assertThat(tx.getType(NEW_TYPE).plays().toArray(), hasItemInArray(tx.getRole("actor")));

        qb.undefine(label(NEW_TYPE).plays("actor")).execute();

        assertThat(tx.getType(NEW_TYPE).plays().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningPlaysWhichDoesntExist_DoNothing() {
        qb.define(label(NEW_TYPE).sub(ENTITY).plays("production-with-cast")).execute();

        assertThat(tx.getType(NEW_TYPE).plays().toArray(), hasItemInArray(tx.getRole("production-with-cast")));

        qb.undefine(label(NEW_TYPE).plays("actor")).execute();

        assertThat(tx.getType(NEW_TYPE).plays().toArray(), hasItemInArray(tx.getRole("production-with-cast")));
    }

    @Test
    public void whenUndefiningRegexProperty_TheAttributeTypeHasNoRegex() {
        qb.define(label(NEW_TYPE).sub(ATTRIBUTE).datatype(STRING).regex("abc")).execute();

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).getRegex());

        qb.undefine(label(NEW_TYPE).regex("abc")).execute();

        assertNull(tx.<AttributeType>getType(NEW_TYPE).getRegex());
    }

    @Test
    public void whenUndefiningRegexPropertyWithWrongRegex_DoNothing() {
        qb.define(label(NEW_TYPE).sub(ATTRIBUTE).datatype(STRING).regex("abc")).execute();

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).getRegex());

        qb.undefine(label(NEW_TYPE).regex("xyz")).execute();

        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).getRegex());
    }

    @Test
    public void whenUndefiningRelatesProperty_TheRelationshipTypeNoLongerRelatesTheRole() {
        qb.define(label(NEW_TYPE).sub(RELATIONSHIP).relates("actor")).execute();

        assertThat(tx.<RelationshipType>getType(NEW_TYPE).relates().toArray(), hasItemInArray(tx.getRole("actor")));

        qb.undefine(label(NEW_TYPE).relates("actor")).execute();

        assertThat(tx.<RelationshipType>getType(NEW_TYPE).relates().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningSub_TheSchemaConceptIsDeleted() {
        qb.define(label(NEW_TYPE).sub(ENTITY)).execute();

        assertNotNull(tx.getType(NEW_TYPE));

        qb.undefine(label(NEW_TYPE).sub(ENTITY)).execute();

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningSubWhichDoesntExist_DoNothing() {
        qb.define(label(NEW_TYPE).sub(ENTITY)).execute();

        assertNotNull(tx.getType(NEW_TYPE));

        qb.undefine(label(NEW_TYPE).sub(THING)).execute();

        assertNotNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningComplexSchema_TheEntireSchemaIsRemoved() {
        Collection<VarPattern> schema = ImmutableList.of(
                label("pokemon").sub(ENTITY).has("pokedex-no").plays("ancestor").plays("descendant"),
                label("pokedex-no").sub(ATTRIBUTE).datatype(INTEGER),
                label("evolution").sub(RELATIONSHIP).relates("ancestor").relates("descendant"),
                label("ancestor").sub(ROLE),
                label("descendant").sub(ROLE)
        );

        qb.define(schema).execute();

        EntityType pokemon = tx.getEntityType("pokemon");
        RelationshipType evolution = tx.getRelationshipType("evolution");
        AttributeType<Long> pokedexNo = tx.getAttributeType("pokedex-no");
        Role ancestor = tx.getRole("ancestor");
        Role descendant = tx.getRole("descendant");

        assertThat(pokemon.attributes().toArray(), arrayContaining(pokedexNo));
        assertThat(evolution.relates().toArray(), arrayContainingInAnyOrder(ancestor, descendant));
        assertThat(pokemon.plays().filter(r -> !r.isImplicit()).toArray(), arrayContainingInAnyOrder(ancestor, descendant));

        qb.undefine(schema).execute();

        assertNull(tx.getEntityType("pokemon"));
        assertNull(tx.getEntityType("evolution"));
        assertNull(tx.getAttributeType("pokedex-no"));
        assertNull(tx.getRole("ancestor"));
        assertNull(tx.getRole("descendant"));
    }

    @Test
    public void whenUndefiningAnInstanceProperty_Throw() {
        Concept movie = qb.insert(x.isa("movie")).execute().get(0).get(x);

        exception.expect(RuntimeException.class); // TODO
        qb.undefine(var().id(movie.getId()).isa("movie")).execute();
    }
}
