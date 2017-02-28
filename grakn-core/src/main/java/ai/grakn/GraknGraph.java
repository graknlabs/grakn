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

package ai.grakn;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graql.QueryBuilder;

import java.util.Collection;

/**
 * <p>
 *     A Grakn Graph
 * </p>
 *
 * <p>
 *     This is produced by {@link Grakn#factory(String, String)} and allows the user to construct and perform
 *     basic look ups to a Grakn Graph. This also allows the execution of Graql queries.
 * </p>
 *
 * @author fppt
 *
 */
public interface GraknGraph extends AutoCloseable{

    //------------------------------------- Concept Construction ----------------------------------
    // TODO: For all 'put' methods state the expected behaviour when there is a type with the same name but a different
    // kind or params (e.g. putRelationType("person"), putResourceType("name", BOOLEAN))

    /**
     * Create a new {@link EntityType} with super-type {@code entity}, or return a pre-existing {@link EntityType},
     * with the specified name.
     *
     * @param name A unique name for the {@link EntityType}
     * @return A new or existing {@link EntityType} with the provided name
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link EntityType}.
     */
    EntityType putEntityType(String name);

    /**
     * Create a new {@link EntityType} with super-type {@code entity}, or return a pre-existing {@link EntityType},
     * with the specified name.
     *
     * @param name A unique name for the {@link EntityType}
     * @return A new or existing {@link EntityType} with the provided name
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link EntityType}.
     */
    EntityType putEntityType(TypeName name);

    /**
     * Create a new non-unique {@link ResourceType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link ResourceType}, with the specified name and data type.
     *
     * @param name A unique name for the {@link ResourceType}
     * @param dataType The data type of the {@link ResourceType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link ResourceType} with the provided name and data type.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link ResourceType}.
     * @throws ConceptException if the {@param name} is already in use by an existing {@link ResourceType} which is
     *                          unique or has a different datatype.
     */
    <V> ResourceType<V> putResourceType(String name, ResourceType.DataType<V> dataType);

    /**
     * Create a new non-unique {@link ResourceType} with super-type {@code resource}, or return a pre-existing
     * non-unique {@link ResourceType}, with the specified name and data type.
     *
     * @param name A unique name for the {@link ResourceType}
     * @param dataType The data type of the {@link ResourceType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link ResourceType} with the provided name and data type.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link ResourceType}.
     * @throws ConceptException if the {@param name} is already in use by an existing {@link ResourceType} which is
     *                          unique or has a different datatype.
     */
    <V> ResourceType<V> putResourceType(TypeName name, ResourceType.DataType<V> dataType);

    /**
     * Create a unique {@link ResourceType} with super-type {@code resource}, or return a pre-existing
     * unique {@link ResourceType}, with the specified name and data type.
     * The {@link ResourceType} is guaranteed to be unique, in that its instances can be connected to one entity.
     *
     * @param name A unique name for the {@link ResourceType}
     * @param dataType The data type of the {@link ResourceType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link ResourceType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link ResourceType}.
     * @throws ConceptException if the {@param name} is already in use by an existing {@link ResourceType} which is
     *                          not unique or has a different datatype.
     */
    <V> ResourceType <V> putResourceTypeUnique(String name, ResourceType.DataType<V> dataType);

    /**
     * Create a unique {@link ResourceType} with super-type {@code resource}, or return a pre-existing
     * unique {@link ResourceType}, with the specified name and data type.
     * The {@link ResourceType} is guaranteed to be unique, in that its instances can be connected to one entity.
     *
     * @param name A unique name for the {@link ResourceType}
     * @param dataType The data type of the {@link ResourceType}.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing {@link ResourceType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link ResourceType}.
     * @throws ConceptException if the {@param name} is already in use by an existing {@link ResourceType} which is
     *                          not unique or has a different datatype.
     */
    <V> ResourceType <V> putResourceTypeUnique(TypeName name, ResourceType.DataType<V> dataType);

    /**
     * Create a {@link RuleType} with super-type {@code rule}, or return a pre-existing {@link RuleType}, with the
     * specified name.
     *
     * @param name A unique name for the {@link RuleType}
     * @return new or existing {@link RuleType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RuleType}.
     */
    RuleType putRuleType(String name);

    /**
     * Create a {@link RuleType} with super-type {@code rule}, or return a pre-existing {@link RuleType}, with the
     * specified name.
     *
     * @param name A unique name for the {@link RuleType}
     * @return new or existing {@link RuleType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RuleType}.
     */
    RuleType putRuleType(TypeName name);

    /**
     * Create a {@link RelationType} with super-type {@code relation}, or return a pre-existing {@link RelationType},
     * with the specified name.
     *
     * @param name A unique name for the {@link RelationType}
     * @return A new or existing {@link RelationType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RelationType}.
     */
    RelationType putRelationType(String name);

    /**
     * Create a {@link RelationType} with super-type {@code relation}, or return a pre-existing {@link RelationType},
     * with the specified name.
     *
     * @param name A unique name for the {@link RelationType}
     * @return A new or existing {@link RelationType} with the provided name.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RelationType}.
     */
    RelationType putRelationType(TypeName name);

    /**
     * Create a {@link RoleType} with super-type {@code role}, or return a pre-existing {@link RoleType}, with the
     * specified name.
     *
     * @param name A unique name for the {@link RoleType}
     * @return new or existing {@link RoleType} with the provided Id.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RoleType}.
     */
    RoleType putRoleType(String name);

    /**
     * Create a {@link RoleType} with super-type {@code role}, or return a pre-existing {@link RoleType}, with the
     * specified name.
     *
     * @param name A unique name for the {@link RoleType}
     * @return new or existing {@link RoleType} with the provided Id.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ConceptNotUniqueException if the {@param name} is already in use by an existing non-{@link RoleType}.
     */
    RoleType putRoleType(TypeName name);

    //------------------------------------- Concept Lookup ----------------------------------
    /**
     * Get the {@link Concept} with identifier provided, if it exists.
     *
     * @param id A unique identifier for the {@link Concept} in the graph.
     * @return The {@link Concept} with the provided id or null if no such {@link Concept} exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ClassCastException if the concept is not an instance of {@link T}
     */
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the {@link Type} with the name provided, if it exists.
     *
     * @param name A unique name which identifies the {@link Type} in the graph.
     * @return The {@link Type} with the provided name or null if no such {@link Type} exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     * @throws ClassCastException if the type is not an instance of {@link T}
     */
    <T extends Type> T getType(TypeName name);

    /**
     * Get all Resources holding the value provided, if they exist.
     *
     * @param value A value which a Resource in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resources holding the provided value or an empty collection if no such Resource exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    <V> Collection<Resource<V>> getResourcesByValue(V value);

    /**
     * Get the Entity Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided name or null if no such Entity Type exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    EntityType getEntityType(String name);

    /**
     * Get the Relation Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Relation Type in the graph.
     * @return The Relation Type with the provided name or null if no such Relation Type exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    RelationType getRelationType(String name);

    /**
     * Get the Resource Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Resource Type in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Type with the provided name or null if no such Resource Type exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    <V> ResourceType<V> getResourceType(String name);

    /**
     * Get the Role Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Role Type in the graph.
     * @return The Role Type  with the provided name or null if no such Role Type exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    RoleType getRoleType(String name);

    /**
     * Get the Rule Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Rule Type in the graph.
     * @return The Rule Type with the provided name or null if no such Rule Type exists.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    RuleType getRuleType(String name);

    //------------------------------------- Utilities ----------------------------------
    // TODO: what does this do when the graph is closed?
    /**
     * Returns access to the low-level details of the graph via GraknAdmin
     * @see GraknAdmin
     *
     * @return The admin interface which allows you to access more low level details of the graph.
     */
    GraknAdmin admin();

    // TODO: what does this do when the graph is closed?
    /**
     * Utility function to specify whether implicit and system-generated types should be returned.
     * @param flag Specifies if implicit and system-generated types should be returned.
     */
    void showImplicitConcepts(boolean flag);

    // TODO: what does this do when the graph is closed?
    /**
     * Utility function to specify whether implicit concepts should be exposed.
     *
     * @return true if implicit concepts are exposed.
     */
    boolean implicitConceptsVisible();

    /**
     * Closes and clears the current graph.
     *
     * @throws GraphRuntimeException if the graph is closed
     */
    void clear();

    // TODO: what does this do when the graph is closed?
    /**
     * Utility function to get the name of the keyspace where the graph is persisted.
     *
     * @return The name of the keyspace where the graph is persisted
     */
    String getKeyspace();

    /**
     * Utility function to determine whether the graph has been closed.
     *
     * @return True if the graph has been closed
     */
    boolean isClosed();

    // TODO: what does this do when the graph is closed?
    /**
     * Returns a QueryBuilder
     *
     * @return returns a query builder to allow for the creation of graql queries
     * @see QueryBuilder
     */
    QueryBuilder graql();

    // TODO: what does this do when the graph is closed?
    /**
     * Closes the current transaction. Rendering this graph unusable. You must use the {@link GraknGraphFactory} to
     * get a new open transaction.
     *
     * This will result in a commit if {@link GraknGraph#commitOnClose()} was called before hand. Otherwise the
     * transaction will be rolled back.
     */
    void close() throws GraknValidationException;

    // TODO: what does this do when the graph is closed?
    /**
     * Will cause the current transaction to be committed when closing the transaction.
     */
    void commitOnClose();
}
