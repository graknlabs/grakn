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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DeleteQueryIT {

    public static final Statement ENTITY = label(Schema.MetaSchema.ENTITY.getLabel());
    public static final Variable x = var("x");
    public static final Variable y = var("y");

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static Session session;
    public Transaction tx;
    public QueryBuilder qb;

    @Rule
    public  final ExpectedException exception = ExpectedException.none();

    private Match kurtz;
    private Match marlonBrando;
    private Match apocalypseNow;
    private Match kurtzCastRelation;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }
    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
        qb = tx.graql();

        kurtz = qb.match(x.has("name", "Colonel Walter E. Kurtz"));
        marlonBrando = qb.match(x.has("name", "Marlon Brando"));
        apocalypseNow = qb.match(x.has("title", "Apocalypse Now"));
        kurtzCastRelation =
                qb.match(var("a").rel("character-being-played", var().has("name", "Colonel Walter E. Kurtz")));
    }

    @After
    public void closeTransaction(){
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Test
    public void testDeleteMultiple() {
        qb.define(label("fake-type").sub(ENTITY)).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).execute();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type")).delete(x).execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void testDeleteEntity() {

        assertExists(qb, var().has("title", "Godfather"));
        assertExists(qb, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(qb, var().has("name", "Don Vito Corleone"));

        qb.match(x.has("title", "Godfather")).delete(x).execute();

        assertNotExists(qb, var().has("title", "Godfather"));
        assertNotExists(qb, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(qb, var().has("name", "Don Vito Corleone"));
    }

    @Test
    public void testDeleteRelation() {
        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(kurtzCastRelation);

        kurtzCastRelation.delete("a").execute();

        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertNotExists(kurtzCastRelation);
    }

    @Test
    public void testDeleteAllRolePlayers() {
        ConceptId id = kurtzCastRelation.get("a").stream().map(ans -> ans.get("a")).findFirst().get().id();
        Match relation = qb.match(var().id(id));

        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        kurtz.delete(x).execute();

        assertNotExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        marlonBrando.delete(x).execute();

        assertNotExists(kurtz);
        assertNotExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        apocalypseNow.delete(x).execute();

        assertNotExists(kurtz);
        assertNotExists(marlonBrando);
        assertNotExists(apocalypseNow);
        assertNotExists(relation);
    }

    @Test
    public void whenDeletingAResource_TheResourceAndImplicitRelationsAreDeleted() {
        ConceptId id = qb.match(
                x.has("title", "Godfather"),
                var("a").rel(x).rel(y).isa(Schema.ImplicitType.HAS.getLabel("tmdb-vote-count").getValue())
        ).get("a").stream().map(ans -> ans.get("a")).findFirst().get().id();

        assertExists(qb, var().has("title", "Godfather"));
        assertExists(qb, var().id(id));
        assertExists(qb, var().val(1000L).isa("tmdb-vote-count"));

        qb.match(x.val(1000L).isa("tmdb-vote-count")).delete(x).execute();

        assertExists(qb, var().has("title", "Godfather"));
        assertNotExists(qb, var().id(id));
        assertNotExists(qb, var().val(1000L).isa("tmdb-vote-count"));
    }

    @Test
    public void afterDeletingAllInstances_TheTypeCanBeUndefined() {
        Match movie = qb.match(x.isa("movie"));

        assertNotNull(tx.getEntityType("movie"));
        assertExists(movie);

        movie.delete(x).execute();

        assertNotNull(tx.getEntityType("movie"));
        assertNotExists(movie);

        qb.undefine(label("movie").sub("production")).execute();

        assertNull(tx.getEntityType("movie"));
    }

    @Test
    public void whenDeletingMultipleVariables_AllVariablesGetDeleted() {
        qb.define(label("fake-type").sub(ENTITY)).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).execute();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type"), y.isa("fake-type"), x.neq(y)).limit(1).delete(x, y).execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingWithNoArguments_AllVariablesGetDeleted() {
        qb.define(label("fake-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).execute();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type"), y.isa("fake-type"), x.neq(y)).limit(1).delete().execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(y));
        tx.graql().match(x.isa("movie")).delete(y).execute();
    }

    @Test
    public void whenDeletingASchemaConcept_Throw() {
        SchemaConcept newType = qb.define(x.label("new-type").sub(ENTITY)).execute().get(0).get(x).asSchemaConcept();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.deleteSchemaConcept(newType).getMessage());
        qb.match(x.label("new-type")).delete(x).execute();
    }

    @Test(expected = Exception.class)
    public void deleteVarNameNullSet() {
        tx.graql().match(var()).delete(null).execute();
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        tx.graql().match(var()).delete((String) null).execute();
    }

}
