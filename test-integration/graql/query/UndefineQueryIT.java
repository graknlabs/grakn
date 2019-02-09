/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query;

import com.google.common.collect.ImmutableList;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class UndefineQueryIT {
    // TODO: This test class should be cleaned up.
    //       Either use a shared dataset across all test, or make all tests independent.

    private static final Statement THING = type(Schema.MetaSchema.THING.getLabel().getValue());
    private static final Statement ENTITY = type(Schema.MetaSchema.ENTITY.getLabel().getValue());
    private static final Statement RELATIONSHIP = type(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue());
    private static final Statement ATTRIBUTE = type(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue());
    private static final Statement ROLE = type(Schema.MetaSchema.ROLE.getLabel().getValue());
    private static final Label NEW_TYPE = Label.of("new-type");
    private static final Statement x = var("x");

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();

    public static SessionImpl session;
    private TransactionOLTP tx;

    @Before
    public void newTransaction() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void closeTransaction() {
        tx.close();
        session.close();
    }

    @Test
    public void whenUndefiningDataType_DoNothing() {
        tx.execute(Graql.undefine(type("name").datatype(Query.DataType.STRING)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertEquals(AttributeType.DataType.STRING, tx.getAttributeType("name").dataType());
    }

    @Test
    public void whenUndefiningHas_TheHasLinkIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).has("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).has("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningHasWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).has("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).has("title")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test
    public void whenUndefiningKey_TheKeyLinkIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).key("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).key("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningKeyWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).key("name")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).key("title")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).keys().toArray(), hasItemInArray(tx.getAttributeType("name")));
    }

    @Test @Ignore // TODO: investigate how this is possible in the first place
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = tx.execute(Graql.define(x.type(NEW_TYPE.getValue()).sub(ENTITY))).get(0).get(x.var()).asType();
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(var().id(newType.id().getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningIsAbstract_TheTypeIsNoLongerAbstract() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).isAbstract()));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertTrue(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).isAbstract()));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningIsAbstractOnNonAbstractType_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertFalse(tx.getType(NEW_TYPE).isAbstract());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).isAbstract()));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertFalse(tx.getType(NEW_TYPE).isAbstract());
    }

    @Test
    public void whenUndefiningPlays_TheTypeNoLongerPlaysTheRole() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).plays("actor")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).plays("actor")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningPlaysWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY).plays("production-with-cast")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).plays("actor")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.getType(NEW_TYPE).playing().toArray(), hasItemInArray(tx.getRole("production-with-cast")));
    }

    @Test
    public void whenUndefiningRegexProperty_TheAttributeTypeHasNoRegex() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ATTRIBUTE).datatype(Query.DataType.STRING).regex("abc")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).regex("abc")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRegexPropertyWithWrongRegex_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ATTRIBUTE).datatype(Query.DataType.STRING).regex("abc")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).regex("xyz")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertEquals("abc", tx.<AttributeType>getType(NEW_TYPE).regex());
    }

    @Test
    public void whenUndefiningRelatesPropertyWithoutCommit_TheRelationshipTypeNoLongerRelatesTheRole() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(RELATIONSHIP).relates("actor")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), hasItemInArray(tx.getRole("actor")));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).relates("actor")));
        assertThat(tx.<RelationType>getType(NEW_TYPE).roles().toArray(), not(hasItemInArray(tx.getRole("actor"))));
    }

    @Test
    public void whenUndefiningSub_TheSchemaConceptIsDeleted() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningSubWhichDoesntExist_DoNothing() {
        tx.execute(Graql.define(type(NEW_TYPE.getValue()).sub(ENTITY)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(NEW_TYPE));

        tx.execute(Graql.undefine(type(NEW_TYPE.getValue()).sub(THING)));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void undefineRelationshipAndRoles() {
        tx.execute(Graql.define(type("employment").sub("relationship").relates("employee")));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(Label.of("employment")));

        UndefineQuery undefineQuery = Graql.undefine(
                type("employment").sub("relationship").relates("employee"),
                type("employee").sub("role")
        );

        tx.execute(undefineQuery);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.getType(Label.of("employment")));
    }

    @Test
    public void undefineTypeAndTheirAttributes() {
        tx.execute(Graql.define(
                type("company").sub("entity").has("registration"),
                type("registration").sub("attribute").datatype(Query.DataType.STRING)
        ));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(Label.of("company")));
        assertNotNull(tx.getType(Label.of("registration")));

        UndefineQuery undefineQuery = Graql.undefine(
                type("company").sub("entity").has("registration"),
                type("registration").sub("attribute")
        );

        tx.execute(undefineQuery);
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNull(tx.getType(Label.of("company")));
        assertNull(tx.getType(Label.of("registration")));
    }

    @Test
    public void undefineTypeAndTheirAttributeAndRolesAndRelationships() {
        Collection<Statement> schema = ImmutableList.of(
                type("pokemon").sub(ENTITY).has("pokedex-no").plays("ancestor").plays("descendant"),
                type("pokedex-no").sub(ATTRIBUTE).datatype(Query.DataType.LONG),
                type("evolution").sub(RELATIONSHIP).relates("ancestor").relates("descendant"),
                type("ancestor").sub(ROLE),
                type("descendant").sub(ROLE)
        );
        tx.execute(Graql.define(schema));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        EntityType pokemon = tx.getEntityType("pokemon");
        RelationType evolution = tx.getRelationshipType("evolution");
        AttributeType<Long> pokedexNo = tx.getAttributeType("pokedex-no");
        Role ancestor = tx.getRole("ancestor");
        Role descendant = tx.getRole("descendant");

        assertThat(pokemon.attributes().toArray(), arrayContaining(pokedexNo));
        assertThat(evolution.roles().toArray(), arrayContainingInAnyOrder(ancestor, descendant));
        assertThat(pokemon.playing().filter(r -> !r.isImplicit()).toArray(), arrayContainingInAnyOrder(ancestor, descendant));

        tx.execute(Graql.undefine(schema));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        assertNull(tx.getEntityType("pokemon"));
        assertNull(tx.getEntityType("evolution"));
        assertNull(tx.getAttributeType("pokedex-no"));
        assertNull(tx.getRole("ancestor"));
        assertNull(tx.getRole("descendant"));
    }

    @Test
    public void undefineTypeAndTheirAttributeWhenThereIsAnInstanceOfThem_Throw() {
        tx.execute(Graql.define(
                type("registration").sub("attribute").datatype(Query.DataType.STRING),
                type("company").sub("entity").has("registration")
        ));
        tx.execute(Graql.insert(
                var("x").isa("company").has("registration", "12345")
        ));
        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);
        assertNotNull(tx.getType(Label.of("company")));
        assertNotNull(tx.getType(Label.of("registration")));
        assertTrue(tx.getType(Label.of("company")).instances().iterator().hasNext());

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("Failed to: undefine"),
                                      containsString("company"),
                                      containsString("registration")));
        tx.execute(Graql.undefine(type("company").has("registration")));
    }

    @Test
    public void whenUndefiningATypeWithInstances_Throw() {
        assertExists(tx, x.type("movie").sub("entity"));
        assertExists(tx, x.isa("movie"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("movie"), containsString("delet")));
        tx.execute(Graql.undefine(type("movie").sub("production")));
    }

    @Test
    public void whenUndefiningASuperConcept_Throw() {
        assertExists(tx, x.type("production").sub("entity"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("production"), containsString("delet")));
        tx.execute(Graql.undefine(type("production").sub(ENTITY)));
    }

    @Test
    public void whenUndefiningARoleWithPlayers_Throw() {
        assertExists(tx, x.type("actor"));

        exception.expect(TransactionException.class);
        exception.expectMessage(allOf(containsString("actor"), containsString("delet")));
        tx.execute(Graql.undefine(type("actor").sub(ROLE)));
    }

    @Test
    public void whenUndefiningAnInstanceProperty_Throw() {
        Concept movie = tx.execute(Graql.insert(x.isa("movie"))).get(0).get(x.var());

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.defineUnsupportedProperty(Query.Property.ISA.toString()).getMessage());

        tx.execute(Graql.undefine(var().id(movie.id().getValue()).isa("movie")));
    }
}
