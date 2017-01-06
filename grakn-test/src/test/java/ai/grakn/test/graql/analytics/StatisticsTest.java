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

package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.analytics.GraknVertexProgram;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static ai.grakn.test.GraknTestEnv.*;

public class StatisticsTest extends AbstractGraphTest {

    private static final String thing = "thing";
    private static final String anotherThing = "anotherThing";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private static final double delta = 0.000001;

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;

    private String keyspace;

    @Before
    public void setUp() {
        // TODO: Fix tests in orientdb
        assumeFalse(usingOrientDB());

        keyspace = graph.getKeyspace();

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(GraknVertexProgram.class);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) org.slf4j.LoggerFactory.getLogger(ComputeQuery.class);
        logger.setLevel(Level.DEBUG);
    }

    @Test
    public void testStatisticsExceptions() throws Exception {
        addOntologyAndEntities();
        addResourceRelations();

        //TODO: add more detailed error messages
        // resources-type is not set
        assertIllegalStateExceptionThrown(graph.graql().compute().max().in(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().min().in(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().mean().in(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().sum().in(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().std().in(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().median().in(thing)::execute);

        // if it's not a resource-type
        assertIllegalStateExceptionThrown(graph.graql().compute().max().of(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().min().of(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().mean().of(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().sum().of(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().std().of(thing)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().median().of(thing)::execute);

        // resource-type has no instance
        assertFalse(graph.graql().compute().max().of(resourceType7).execute().isPresent());
        assertFalse(graph.graql().compute().min().of(resourceType7).execute().isPresent());
        assertFalse(graph.graql().compute().sum().of(resourceType7).execute().isPresent());
        assertFalse(graph.graql().compute().std().of(resourceType7).execute().isPresent());
        assertFalse(graph.graql().compute().median().of(resourceType7).execute().isPresent());
        assertFalse(graph.graql().compute().mean().of(resourceType7).execute().isPresent());

        // resources are not connected to any entities
        assertFalse(graph.graql().compute().max().of(resourceType3).execute().isPresent());
        assertFalse(graph.graql().compute().min().of(resourceType3).execute().isPresent());
        assertFalse(graph.graql().compute().sum().of(resourceType3).execute().isPresent());
        assertFalse(graph.graql().compute().std().of(resourceType3).execute().isPresent());
        assertFalse(graph.graql().compute().median().of(resourceType3).execute().isPresent());
        assertFalse(graph.graql().compute().mean().of(resourceType3).execute().isPresent());

        // resource-type has incorrect data type
        assertIllegalStateExceptionThrown(graph.graql().compute().max().of(resourceType4)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().min().of(resourceType4)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().mean().of(resourceType4)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().sum().of(resourceType4)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().std().of(resourceType4)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().median().of(resourceType4)::execute);

        // resource-types have different data types
        Set<String> resourceTypes = Sets.newHashSet(resourceType1, resourceType2);
        assertIllegalStateExceptionThrown(graph.graql().compute().max().of(resourceTypes)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().min().of(resourceTypes)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().mean().of(resourceTypes)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().sum().of(resourceTypes)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().std().of(resourceTypes)::execute);
        assertIllegalStateExceptionThrown(graph.graql().compute().median().of(resourceTypes)::execute);
    }

    private void assertIllegalStateExceptionThrown(Supplier<Optional> method) {
        boolean exceptionThrown = false;
        try {
            method.get();
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testMinAndMax() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        result = Graql.compute().min().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().min().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).min().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().min().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().min().of(resourceType2).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().min().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().min().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).min().of(resourceType2).execute();
        assertFalse(result.isPresent());

        result = Graql.compute().max().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().max().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).max().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().max().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().max().of(resourceType2).in(Collections.emptyList()).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().max().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().max().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).max().of(resourceType2).execute();
        assertFalse(result.isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        result = Graql.compute().min().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().min().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().min().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().min().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        result = Graql.compute().max().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().max().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().max().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().max().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        // connect entity and resources
        addResourceRelations();

        result = graph.graql().compute().min().of(resourceType1).in(Collections.emptySet()).execute();
        assertEquals(1.2, result.get().doubleValue(), delta);
        result = Graql.compute().min().in(thing).of(resourceType2).withGraph(graph).execute();
        assertEquals(-1L, result.get());
        result = graph.graql().compute().min().in(thing).of(resourceType2, resourceType5).execute();
        assertEquals(-7L, result.get());
        result = graph.graql().compute().min().in(thing, thing, thing).of(resourceType2, resourceType5).execute();
        assertEquals(-7L, result.get());

        result = Graql.compute().max().in().withGraph(graph).of(resourceType1).execute();
        assertEquals(1.8, result.get().doubleValue(), delta);
        result = graph.graql().compute().max().of(resourceType1, resourceType6).execute();
        assertEquals(7.5, result.get().doubleValue(), delta);
        result = graph.graql().compute().max().of(Lists.newArrayList(resourceType1, resourceType6)).execute();
        assertEquals(7.5, result.get().doubleValue(), delta);

        // TODO: fix this test: we need to check the type of the resource owner, not just the type or relation
        result = graph.graql().compute().max().in(anotherThing).of(resourceType2).execute();
        assertEquals(4L, result.get());
    }

    @Test
    public void testSum() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        result = Graql.compute().sum().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).sum().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().sum().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().sum().of(resourceType2).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().sum().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().sum().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).sum().of(resourceType2).execute();
        assertFalse(result.isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().sum().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().sum().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().sum().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        // connect entity and resources
        addResourceRelations();

        result = Graql.compute().sum().of(resourceType1).withGraph(graph).execute();
        assertEquals(4.5, result.get().doubleValue(), delta);
        result = Graql.compute().sum().of(resourceType2).in(thing).withGraph(graph).execute();
        assertEquals(3L, result.get());
        result = graph.graql().compute().sum().of(resourceType1, resourceType6).execute();
        assertEquals(27.0, result.get().doubleValue(), delta);
        result = graph.graql().compute().sum().of(resourceType2, resourceType5).in(thing, anotherThing).execute();
        assertEquals(-18L, result.get());
    }

    @Test
    public void testMean() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Optional<Double> result;

        // resource-type has no instance
        addOntologyAndEntities();

        result = Graql.compute().mean().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().mean().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).mean().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().mean().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().mean().of(resourceType2).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().mean().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().mean().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).mean().of(resourceType2).execute();
        assertFalse(result.isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        result = Graql.compute().mean().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().mean().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().mean().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().mean().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        // connect entity and resources
        addResourceRelations();

        result = Graql.compute().withGraph(graph).mean().of(resourceType1).execute();
        assertEquals(1.5, result.get(), delta);
        result = Graql.compute().mean().in(thing).of(resourceType2).withGraph(graph).execute();
        assertEquals(1D, result.get(), delta);
        result = graph.graql().compute().mean().of(resourceType1, resourceType6).execute();
        assertEquals(4.5, result.get(), delta);
        result = graph.graql().compute().mean().in(anotherThing).of(resourceType2, resourceType5).execute();
        assertEquals(-3D, result.get(), delta);
    }

    @Test
    public void testStd() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Optional<Double> result;

        // resource-type has no instance
        addOntologyAndEntities();

        result = Graql.compute().std().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).std().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().std().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().std().of(resourceType2).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().std().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().std().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).std().of(resourceType2).execute();
        assertFalse(result.isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().std().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().std().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().std().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        // connect entity and resources
        addResourceRelations();

        result = Graql.compute().std().of(resourceType1).withGraph(graph).execute();
        assertEquals(Math.sqrt(0.18 / 3), result.get(), delta);
        result = Graql.compute().std().of(resourceType2).withGraph(graph).in(thing).execute();
        assertEquals(Math.sqrt(14.0 / 3), result.get(), delta);
        result = graph.graql().compute().std().of(resourceType1, resourceType6).execute();
        assertEquals(Math.sqrt(54.18 / 6), result.get(), delta);
        result = graph.graql().compute().std().of(resourceType2, resourceType5).in(thing, anotherThing).execute();
        assertEquals(Math.sqrt(110.0 / 6), result.get(), delta);
    }

    @Test
    public void testMedian() throws Exception {
        // TODO: Fix in TinkerGraphComputer
        assumeFalse(usingTinker());

        Optional<Number> result;

        // resource-type has no instance
        addOntologyAndEntities();

        result = Graql.compute().median().of(resourceType1).in(Collections.emptyList()).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().median().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().withGraph(graph).median().of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().median().withGraph(graph).of(resourceType1).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().median().of(resourceType2).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().median().of(Sets.newHashSet(resourceType2, resourceType5)).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().median().of(resourceType2).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().withGraph(graph).median().of(resourceType2).execute();
        assertFalse(result.isPresent());

        // add resources, but resources are not connected to any entities
        addResourcesInstances();

        result = Graql.compute().median().of(resourceType1).withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().median().of(resourceType1).in().withGraph(graph).execute();
        assertFalse(result.isPresent());
        result = graph.graql().compute().median().of(resourceType2).in(thing, anotherThing).execute();
        assertFalse(result.isPresent());
        result = Graql.compute().median().of(resourceType2).withGraph(graph).in(anotherThing).execute();
        assertFalse(result.isPresent());

        // connect entity and resources
        addResourceRelations();

        result = graph.graql().compute().median().of(resourceType1).in().execute();
        assertEquals(1.5D, result.get().doubleValue(), delta);
        result = Graql.compute().withGraph(graph).median().of(resourceType6).execute();
        assertEquals(7.5D, result.get().doubleValue(), delta);
        result = graph.graql().compute().median().of(resourceType1, resourceType6).execute();
        assertEquals(1.8D, result.get().doubleValue(), delta);
        result = Graql.compute().withGraph(graph).median().of(resourceType2).execute();
        assertEquals(0L, result.get().longValue());
        result = Graql.compute().withGraph(graph).median().in(thing).of(resourceType5).execute();
        assertEquals(-7L, result.get().longValue());
        result = graph.graql().compute().median().in(thing, anotherThing).of(resourceType2, resourceType5).execute();
        assertEquals(-7L, result.get().longValue());
    }

    private void addOntologyAndEntities() throws GraknValidationException {
        EntityType entityType1 = graph.putEntityType(thing);
        EntityType entityType2 = graph.putEntityType(anotherThing);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType1.addEntity();
        Entity entity3 = entityType1.addEntity();
        Entity entity4 = entityType2.addEntity();
        entityId1 = entity1.getId();
        entityId2 = entity2.getId();
        entityId3 = entity3.getId();
        entityId4 = entity4.getId();

        RoleType relation1 = graph.putRoleType("relation1");
        RoleType relation2 = graph.putRoleType("relation2");
        entityType1.playsRole(relation1).playsRole(relation2);
        entityType2.playsRole(relation1).playsRole(relation2);
        RelationType related = graph.putRelationType("related").hasRole(relation1).hasRole(relation2);

        related.addRelation()
                .putRolePlayer(relation1, entity1)
                .putRolePlayer(relation2, entity2);
        related.addRelation()
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity3);
        related.addRelation()
                .putRolePlayer(relation1, entity2)
                .putRolePlayer(relation2, entity4);

        List<ResourceType> resourceTypeList = new ArrayList<>();
        resourceTypeList.add(graph.putResourceType(resourceType1, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType2, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType3, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType4, ResourceType.DataType.STRING));
        resourceTypeList.add(graph.putResourceType(resourceType5, ResourceType.DataType.LONG));
        resourceTypeList.add(graph.putResourceType(resourceType6, ResourceType.DataType.DOUBLE));
        resourceTypeList.add(graph.putResourceType(resourceType7, ResourceType.DataType.DOUBLE));

        RoleType resourceOwner1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));
        RoleType resourceOwner7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType7));

        RoleType resourceValue1 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));
        RoleType resourceValue7 = graph.putRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType7));

        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1))
                .hasRole(resourceOwner1).hasRole(resourceValue1);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2))
                .hasRole(resourceOwner2).hasRole(resourceValue2);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType3))
                .hasRole(resourceOwner3).hasRole(resourceValue3);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType4))
                .hasRole(resourceOwner4).hasRole(resourceValue4);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5))
                .hasRole(resourceOwner5).hasRole(resourceValue5);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6))
                .hasRole(resourceOwner6).hasRole(resourceValue6);
        graph.putRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType7))
                .hasRole(resourceOwner7).hasRole(resourceValue7);

        entityType1.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);
        entityType2.playsRole(resourceOwner1)
                .playsRole(resourceOwner2)
                .playsRole(resourceOwner3)
                .playsRole(resourceOwner4)
                .playsRole(resourceOwner5)
                .playsRole(resourceOwner6)
                .playsRole(resourceOwner7);

        resourceTypeList.forEach(resourceType -> resourceType
                .playsRole(resourceValue1)
                .playsRole(resourceValue2)
                .playsRole(resourceValue3)
                .playsRole(resourceValue4)
                .playsRole(resourceValue5)
                .playsRole(resourceValue6)
                .playsRole(resourceValue7));

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourcesInstances() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        graph.getResourceType(resourceType1).putResource(1.2);
        graph.getResourceType(resourceType1).putResource(1.5);
        graph.getResourceType(resourceType1).putResource(1.8);

        graph.getResourceType(resourceType2).putResource(4L);
        graph.getResourceType(resourceType2).putResource(-1L);
        graph.getResourceType(resourceType2).putResource(0L);

        graph.getResourceType(resourceType5).putResource(6L);
        graph.getResourceType(resourceType5).putResource(7L);
        graph.getResourceType(resourceType5).putResource(8L);

        graph.getResourceType(resourceType6).putResource(7.2);
        graph.getResourceType(resourceType6).putResource(7.5);
        graph.getResourceType(resourceType6).putResource(7.8);

        graph.getResourceType(resourceType4).putResource("a");
        graph.getResourceType(resourceType4).putResource("b");
        graph.getResourceType(resourceType4).putResource("c");

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    private void addResourceRelations() throws GraknValidationException {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        Entity entity1 = graph.getConcept(entityId1);
        Entity entity2 = graph.getConcept(entityId2);
        Entity entity3 = graph.getConcept(entityId3);
        Entity entity4 = graph.getConcept(entityId4);

        RoleType resourceOwner1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType1));
        RoleType resourceOwner2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType2));
        RoleType resourceOwner3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType3));
        RoleType resourceOwner4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType4));
        RoleType resourceOwner5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType5));
        RoleType resourceOwner6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType6));

        RoleType resourceValue1 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType1));
        RoleType resourceValue2 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType2));
        RoleType resourceValue3 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType3));
        RoleType resourceValue4 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType4));
        RoleType resourceValue5 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType5));
        RoleType resourceValue6 = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType6));

        RelationType relationType1 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType1));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.2));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity1)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.5));
        relationType1.addRelation()
                .putRolePlayer(resourceOwner1, entity3)
                .putRolePlayer(resourceValue1, graph.getResourceType(resourceType1).putResource(1.8));

        RelationType relationType2 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType2));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(4L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity1)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(-1L));
        relationType2.addRelation()
                .putRolePlayer(resourceOwner2, entity4)
                .putRolePlayer(resourceValue2, graph.getResourceType(resourceType2).putResource(0L));

        graph.getResourceType(resourceType3).putResource(100L);

        RelationType relationType5 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType5));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity1)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity2)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));
        relationType5.addRelation()
                .putRolePlayer(resourceOwner5, entity4)
                .putRolePlayer(resourceValue5, graph.getResourceType(resourceType5).putResource(-7L));

        RelationType relationType6 = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceType6));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity1)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity2)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));
        relationType6.addRelation()
                .putRolePlayer(resourceOwner6, entity4)
                .putRolePlayer(resourceValue6, graph.getResourceType(resourceType6).putResource(7.5));

        // some resources in, but not connect them to any instances
        graph.getResourceType(resourceType1).putResource(2.8);
        graph.getResourceType(resourceType2).putResource(-5L);
        graph.getResourceType(resourceType5).putResource(10L);
        graph.getResourceType(resourceType6).putResource(0.8);

        graph.commit();
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }
}
