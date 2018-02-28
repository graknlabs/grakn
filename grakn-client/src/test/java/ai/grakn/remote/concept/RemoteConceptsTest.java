/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.remote.concept;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.AttributeType.DataType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.BaseType;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptProperty.DATA_TYPE;
import static ai.grakn.grpc.ConceptProperty.IS_ABSTRACT;
import static ai.grakn.grpc.ConceptProperty.IS_IMPLICIT;
import static ai.grakn.grpc.ConceptProperty.IS_INFERRED;
import static ai.grakn.grpc.ConceptProperty.REGEX;
import static ai.grakn.grpc.ConceptProperty.THEN;
import static ai.grakn.grpc.ConceptProperty.VALUE;
import static ai.grakn.grpc.ConceptProperty.WHEN;
import static ai.grakn.remote.concept.RemoteConcept.ME;
import static ai.grakn.remote.concept.RemoteConcept.TARGET;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.Attribute;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.EntityType;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.MetaType;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.Relationship;
import static ai.grakn.util.Schema.MetaSchema.ATTRIBUTE;
import static ai.grakn.util.Schema.MetaSchema.THING;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class RemoteConceptsTest {

    private static final ConceptId ID = ConceptId.of("V123");
    private static final Pattern PATTERN = var("x").isa("person");

    @Rule
    public final GrpcServerMock server = GrpcServerMock.create();

    private RemoteGraknSession session;
    private RemoteGraknTx tx;
    private static final SimpleURI URI = new SimpleURI("localhost", 999);
    private static final Label LABEL = Label.of("too-tired-for-funny-test-names-today");

    @Before
    public void setUp() {
        session = RemoteGraknSession.create(Keyspace.of("whatever"), URI, server.channel());
        tx = session.open(GraknTxType.WRITE);
        verify(server.requests()).onNext(any()); // The open request
    }

    @After
    public void closeTx() {
        tx.close();
    }

    @After
    public void closeSession() {
        session.close();
    }

    @Test
    public void whenGettingLabel_ReturnTheExpectedLabel() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);
        mockLabelResponse(ID, LABEL);

        assertEquals(LABEL, concept.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_IMPLICIT), IS_IMPLICIT.createTxResponse(true));
        assertTrue(concept.isImplicit());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_IMPLICIT), IS_IMPLICIT.createTxResponse(false));
        assertFalse(concept.isImplicit());
    }

    @Test
    public void whenCallingIsInferred_GetTheExpectedResult() {
        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_INFERRED), IS_INFERRED.createTxResponse(true));
        assertTrue(concept.isInferred());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_INFERRED), IS_INFERRED.createTxResponse(false));
        assertFalse(concept.isInferred());
    }

    @Test
    public void whenCallingIsAbstract_GetTheExpectedResult() {
        Type concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_ABSTRACT), IS_ABSTRACT.createTxResponse(true));
        assertTrue(concept.isAbstract());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_ABSTRACT), IS_ABSTRACT.createTxResponse(false));
        assertFalse(concept.isAbstract());
    }

    @Test
    public void whenCallingGetValue_GetTheExpectedResult() {
        Attribute<?> concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, VALUE), VALUE.createTxResponse(123));
        assertEquals(123, concept.getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        AttributeType<?> concept = RemoteConcepts.createAttributeType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, DATA_TYPE), DATA_TYPE.createTxResponse(DataType.LONG));
        assertEquals(DataType.LONG, concept.getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        Attribute<?> concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, DATA_TYPE), DATA_TYPE.createTxResponse(DataType.LONG));
        assertEquals(DataType.LONG, concept.dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        AttributeType<?> concept = RemoteConcepts.createAttributeType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, REGEX), REGEX.createTxResponse("hello"));
        assertEquals("hello", concept.getRegex());
    }

    @Test
    public void whenCallingGetWhen_GetTheExpectedResult() {
        ai.grakn.concept.Rule concept = RemoteConcepts.createRule(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, WHEN), WHEN.createTxResponse(PATTERN));
        assertEquals(PATTERN, concept.getWhen());
    }

    @Test
    public void whenCallingGetThen_GetTheExpectedResult() {
        ai.grakn.concept.Rule concept = RemoteConcepts.createRule(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, THEN), THEN.createTxResponse(PATTERN));
        assertEquals(PATTERN, concept.getThen());
    }

    @Test
    public void whenCallingIsDeleted_ExecuteAnAskQuery() {
        String query = match(var().id(ID)).aggregate(ask()).toString();

        Concept concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponse(GrpcUtil.execQueryRequest(query), queryResultResponse("true"));
        assertFalse(concept.isDeleted());

        server.setResponse(GrpcUtil.execQueryRequest(query), queryResultResponse("false"));
        assertTrue(concept.isDeleted());
    }

    @Test
    public void whenCallingSups_ExecuteAQuery() {
        String query = match(ME.id(ID), ME.sub(TARGET)).get().toString();

        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        Label labelId = Label.of("yes");
        ConceptId a = ConceptId.of("A");
        Label labelA = Label.of("A");
        ConceptId b = ConceptId.of("B");
        Label labelB = Label.of("B");
        ConceptId metaType = ConceptId.of("C");
        Label labelMetaType = THING.getLabel();

        mockLabelResponse(ID, labelId);
        mockLabelResponse(a, labelA);
        mockLabelResponse(b, labelB);
        mockLabelResponse(metaType, labelMetaType);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(ID, EntityType),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        Set<ConceptId> sups = concept.sups().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(ID, a, b));
        assertThat(sups, not(hasItem(metaType)));
    }

    @Test
    public void whenCallingSubs_ExecuteAQuery() {
        String query = match(ME.id(ID), TARGET.sub(ME)).get().toString();

        SchemaConcept concept = RemoteConcepts.createRelationshipType(tx, ID);

        Label labelId = Label.of("yes");
        ConceptId a = ConceptId.of("A");
        Label labelA = Label.of("A");
        ConceptId b = ConceptId.of("B");
        Label labelB = Label.of("B");

        mockLabelResponse(ID, labelId);
        mockLabelResponse(a, labelA);
        mockLabelResponse(b, labelB);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(ID, BaseType.RelationshipType),
                queryResultResponse(a, BaseType.RelationshipType),
                queryResultResponse(b, BaseType.RelationshipType)
        );

        Set<ConceptId> sups = concept.subs().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(ID, a, b));
    }

    @Test
    public void whenCallingSup_ExecuteSeveralQueries() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        Label labelId = Label.of("yes");
        ConceptId a = ConceptId.of("A");
        Label labelA = Label.of("A");
        ConceptId b = ConceptId.of("B");
        Label labelB = Label.of("B");
        ConceptId metaType = ConceptId.of("C");
        Label labelMetaType = THING.getLabel();

        String supsId = match(ME.id(ID), ME.sub(TARGET)).get().toString();
        String supsA = match(ME.id(a), ME.sub(TARGET)).get().toString();
        String supsB = match(ME.id(b), ME.sub(TARGET)).get().toString();
        String supsMetaType = match(ME.id(metaType), ME.sub(TARGET)).get().toString();

        mockLabelResponse(ID, labelId);
        mockLabelResponse(a, labelA);
        mockLabelResponse(b, labelB);
        mockLabelResponse(metaType, labelMetaType);

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsId),
                queryResultResponse(ID, EntityType),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsA),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsB),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsMetaType),
                queryResultResponse(metaType, MetaType)
        );

        assertEquals(a, concept.sup().getId());
    }

    @Test
    public void whenCallingIsa_ExecuteSeveralQueries() {
        Thing concept = RemoteConcepts.createEntity(tx, ID);

        ConceptId a = ConceptId.of("A");
        Label labelA = Label.of("A");
        ConceptId b = ConceptId.of("B");
        Label labelB = Label.of("B");
        ConceptId metaType = ConceptId.of("C");
        Label labelMetaType = THING.getLabel();

        String typeId = match(ME.id(ID), ME.isa(TARGET)).get().toString();
        String supsA = match(ME.id(a), ME.sub(TARGET)).get().toString();
        String supsB = match(ME.id(b), ME.sub(TARGET)).get().toString();
        String supsMetaType = match(ME.id(metaType), ME.sub(TARGET)).get().toString();

        mockLabelResponse(a, labelA);
        mockLabelResponse(b, labelB);
        mockLabelResponse(metaType, labelMetaType);

        server.setResponseSequence(GrpcUtil.execQueryRequest(typeId),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsA),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsB),
                queryResultResponse(b, EntityType),
                queryResultResponse(metaType, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsMetaType),
                queryResultResponse(metaType, MetaType)
        );

        assertEquals(a, concept.type().getId());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_ExecuteAQueryForAllAttributes() {
        String query = match(ME.id(ID), ME.has(ATTRIBUTE.getLabel(), TARGET)).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, Attribute),
                queryResultResponse(b, Attribute),
                queryResultResponse(c, Attribute)
        );

        Set<ConceptId> sups = concept.attributes().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAttributesWithArguments_ExecuteAQueryForThoseTypesOnly() {
        ConceptId fooId = ConceptId.of("foo");
        Label fooLabel = Label.of("foo");
        AttributeType<?> foo = RemoteConcepts.createAttributeType(tx, fooId);
        ConceptId barId = ConceptId.of("bar");
        Label barLabel = Label.of("bar");
        AttributeType<?> bar = RemoteConcepts.createAttributeType(tx, barId);
        ConceptId bazId = ConceptId.of("baz");
        Label bazLabel = Label.of("baz");
        AttributeType<?> baz = RemoteConcepts.createAttributeType(tx, bazId);

        mockLabelResponse(fooId, fooLabel);
        mockLabelResponse(barId, barLabel);
        mockLabelResponse(bazId, bazLabel);

        String query = match(
                ME.id(ID),
                or(
                        ME.has(fooLabel, TARGET),
                        ME.has(barLabel, TARGET),
                        ME.has(bazLabel, TARGET)
                )
        ).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, Attribute),
                queryResultResponse(b, Attribute),
                queryResultResponse(c, Attribute)
        );

        Set<ConceptId> sups = concept.attributes(foo, bar, baz).map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlays_ExecuteAQuery() {
        String query = match(ME.id(ID), ME.plays(TARGET)).get().toString();

        Type concept = RemoteConcepts.createEntityType(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.Role),
                queryResultResponse(b, BaseType.Role),
                queryResultResponse(c, BaseType.Role)
        );

        Set<ConceptId> sups = concept.plays().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingInstances_ExecuteAQuery() {
        String query = match(ME.id(ID), TARGET.isa(ME)).get().toString();

        Type concept = RemoteConcepts.createRelationshipType(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, Relationship),
                queryResultResponse(b, Relationship),
                queryResultResponse(c, Relationship)
        );

        Set<ConceptId> sups = concept.instances().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingThingPlays_ExecuteAQuery() {
        String query = match(ME.id(ID), var().rel(TARGET, ME)).get().toString();

        Thing concept = RemoteConcepts.createAttribute(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.Role),
                queryResultResponse(b, BaseType.Role),
                queryResultResponse(c, BaseType.Role)
        );

        Set<ConceptId> sups = concept.plays().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_ExecuteAQueryForAllRelationships() {
        String query = match(ME.id(ID), TARGET.rel(ME)).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, Relationship),
                queryResultResponse(b, Relationship),
                queryResultResponse(c, Relationship)
        );

        Set<ConceptId> sups = concept.relationships().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipsWithRoles_ExecuteAQueryForAllRelationshipsOfTheSpecifiedRoles() {
        ConceptId fooId = ConceptId.of("foo");
        Label fooLabel = Label.of("foo");
        Role foo = RemoteConcepts.createRole(tx, fooId);
        ConceptId barId = ConceptId.of("bar");
        Label barLabel = Label.of("bar");
        Role bar = RemoteConcepts.createRole(tx, barId);
        ConceptId bazId = ConceptId.of("baz");
        Label bazLabel = Label.of("baz");
        Role baz = RemoteConcepts.createRole(tx, bazId);

        mockLabelResponse(fooId, fooLabel);
        mockLabelResponse(barId, barLabel);
        mockLabelResponse(bazId, bazLabel);

        Var role = var("role");
        String query = match(
                ME.id(ID),
                TARGET.rel(role, ME),
                or(
                        role.label(fooLabel),
                        role.label(barLabel),
                        role.label(bazLabel)
                )
        ).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, Relationship),
                queryResultResponse(b, Relationship),
                queryResultResponse(c, Relationship)
        );

        Set<ConceptId> sups = concept.relationships(foo, bar, baz).map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipTypes_ExecuteAQuery() {
        String query = match(ME.id(ID), TARGET.relates(ME)).get().toString();

        Role concept = RemoteConcepts.createRole(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.RelationshipType),
                queryResultResponse(b, BaseType.RelationshipType),
                queryResultResponse(c, BaseType.RelationshipType)
        );

        Set<ConceptId> sups = concept.relationshipTypes().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlayedByTypes_ExecuteAQuery() {
        String query = match(ME.id(ID), TARGET.plays(ME)).get().toString();

        Role concept = RemoteConcepts.createRole(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, EntityType),
                queryResultResponse(b, BaseType.RelationshipType),
                queryResultResponse(c, BaseType.AttributeType)
        );

        Set<ConceptId> sups = concept.playedByTypes().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelates_ExecuteAQuery() {
        String query = match(ME.id(ID), ME.relates(TARGET)).get().toString();

        RelationshipType concept = RemoteConcepts.createRelationshipType(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.Role),
                queryResultResponse(b, BaseType.Role),
                queryResultResponse(c, BaseType.Role)
        );

        Set<ConceptId> sups = concept.relates().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAllRolePlayers_ExecuteAQuery() {
        Var role = var("role");
        Var player = var("player");

        String query = match(ME.id(ID), ME.rel(role, player)).get().toString();

        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));

        Thing a = RemoteConcepts.createEntity(tx, ConceptId.of("A"));
        Thing b = RemoteConcepts.createRelationship(tx, ConceptId.of("B"));
        Thing c = RemoteConcepts.createAttribute(tx, ConceptId.of("C"));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(new QueryAnswer(ImmutableMap.of(role, foo, player, a))),
                queryResultResponse(new QueryAnswer(ImmutableMap.of(role, bar, player, b))),
                queryResultResponse(new QueryAnswer(ImmutableMap.of(role, bar, player, c)))
        );

        Map<Role, Set<Thing>> expected = ImmutableMap.of(
                foo, ImmutableSet.of(a),
                bar, ImmutableSet.of(b, c)
        );

        Map<Role, Set<Thing>> allRolePlayers = concept.allRolePlayers();
        assertEquals(expected, allRolePlayers);
    }

    @Test
    public void whenCallingRolePlayersWithNoArguments_ExecuteAQueryForAllRolePlayers() {
        String query = match(ME.id(ID), ME.rel(TARGET)).get().toString();

        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.Entity),
                queryResultResponse(b, Relationship),
                queryResultResponse(c, Attribute)
        );

        Set<ConceptId> sups = concept.rolePlayers().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRolePlayersWithRoles_ExecuteAQueryForAllSpecifiedRoles() {
        ConceptId fooId = ConceptId.of("foo");
        Label fooLabel = Label.of("foo");
        Role foo = RemoteConcepts.createRole(tx, fooId);
        ConceptId barId = ConceptId.of("bar");
        Label barLabel = Label.of("bar");
        Role bar = RemoteConcepts.createRole(tx, barId);
        ConceptId bazId = ConceptId.of("baz");
        Label bazLabel = Label.of("baz");
        Role baz = RemoteConcepts.createRole(tx, bazId);

        mockLabelResponse(fooId, fooLabel);
        mockLabelResponse(barId, barLabel);
        mockLabelResponse(bazId, bazLabel);

        Var role = var("role");
        String query = match(
                ME.id(ID),
                ME.rel(role, TARGET),
                or(
                        role.label(fooLabel),
                        role.label(barLabel),
                        role.label(bazLabel)
                )
        ).get().toString();

        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

        ConceptId a = ConceptId.of("A");
        ConceptId b = ConceptId.of("B");
        ConceptId c = ConceptId.of("C");

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(a, BaseType.Entity),
                queryResultResponse(b, Relationship),
                queryResultResponse(c, Attribute)
        );

        Set<ConceptId> sups = concept.rolePlayers(foo, bar, baz).map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(a, b, c));
    }

    private void mockLabelResponse(ConceptId id, Label label) {
        server.setResponse(
                GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.LABEL),
                ConceptProperty.LABEL.createTxResponse(label)
        );
    }

    private static TxResponse queryResultResponse(String value) {
        QueryResult queryResult = QueryResult.newBuilder().setOtherResult(value).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private static TxResponse queryResultResponse(Answer answer) {
        GraknOuterClass.Answer.Builder grpcAnswer = GraknOuterClass.Answer.newBuilder();

        answer.forEach((var, concept) -> {
            grpcAnswer.putAnswer(var.getValue(), GrpcUtil.convert(concept));
        });

        return queryResultResponse(grpcAnswer.build());
    }

    private static TxResponse queryResultResponse(ConceptId id, BaseType baseType) {
        GraknOuterClass.ConceptId conceptId = GraknOuterClass.ConceptId.newBuilder().setValue(id.getValue()).build();

        GraknOuterClass.Concept concept =
                GraknOuterClass.Concept.newBuilder().setId(conceptId).setBaseType(baseType).build();

        GraknOuterClass.Answer answer =
                GraknOuterClass.Answer.newBuilder().putAnswer(TARGET.getValue(), concept).build();

        return queryResultResponse(answer);
    }

    private static TxResponse queryResultResponse(GraknOuterClass.Answer answer) {
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(answer).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

}