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

package ai.grakn.concept;


import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A ResourceType is a schema element which represents the category of resources in the graph.
 * <p>
 * For the methods below, @param <D> is the data type of this resource represented by the Resource Type.
 * Supported Types include: String, Long, Double, and Boolean
 * </p>
 */
public interface ResourceType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the ResourceType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the ResourceType is to be abstract (true) or not (false).
     *
     * @return The ResourceType itself.
     */
    ResourceType<D> setAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of the ResourceType to be the ResourceType specified.
     *
     * @param type The super type of this ResourceType.
     * @return The ResourceType itself.
     */
    ResourceType<D> superType(ResourceType<D> type);

    /**
     * Sets the RoleType which instances of this ResourceType may play.
     *
     * @param roleType The Role Type which the instances of this ResourceType are allowed to play.
     * @return The ResourceType itself.
     */
    ResourceType<D> playsRole(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances of this ResourceType from playing it.
     *
     * @param roleType The Role Type which the instances of this ResourceType should no longer be allowed to play.
     * @return The ResourceType itself.
     */
    ResourceType<D> deletePlaysRole(RoleType roleType);

    /**
     * Set the regular expression that instances of the ResourceType must conform to.
     *
     * @param regex The regular expression that instances of this ResourceType must conform to.
     * @return The ResourceType itself.
     */
    ResourceType<D> setRegex(String regex);

    /**
     * Set the value for the Resource, unique to its type.
     *
     * @param value A value for the Resource which is unique to its type
     * @return new or existing Resource of this type with the provided value.
     */
    Resource<D> putResource(D value);

    //------------------------------------- Accessors ---------------------------------
    /**
     * Returns the supertype of this ResourceType.
     *
     * @return The supertype of this ResourceType,
     */
    ResourceType<D> superType();

    /**
     * Get the Resource with the value provided, and its type, or return NULL
     * @see Resource
     *
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @param value A value which a Resource in the graph may be holding
     * @return The Resource with the provided value and type or null if no such Resource exists.
     */
    <V> Resource<V> getResource(V value);

    /**
     * Returns a collection of subtypes of this ResourceType.
     *
     * @return The subtypes of this ResourceType
     */
    Collection<ResourceType<D>> subTypes();

    /**
     * Returns a collection of all Resource Instances of this ResourceType.
     *
     * @return The resource instances of this ResourceType
     */
    Collection<Resource<D>> instances();

    /**
     * Get the data type to which instances of the ResourceType must conform.
     *
     * @return The data type to which instances of this resource must conform.
     */
    DataType<D> getDataType();

    /**
     * Retrieve the regular expression to which instances of this ResourceType must conform.
     *
     * @return The regular expression to which instances of this ResourceType must conform.
     */
    String getRegex();

    /**
     * Returns whether the ResourceType is unique.
     *
     * @return True if the ResourceType is unique and its instances are limited to one connection to an entity
     */
    Boolean isUnique();

    /**
     * A class used to hold the supported data types of resources and any other concepts.
     * This is used tp constrain value data types to only those we explicitly support.
     * @param <D> The data type.
     */
    class DataType<D> {
        public static final DataType<String> STRING = new DataType<>(String.class.getName(), Schema.ConceptProperty.VALUE_STRING);
        public static final DataType<Boolean> BOOLEAN = new DataType<>(Boolean.class.getName(), Schema.ConceptProperty.VALUE_BOOLEAN);
        public static final DataType<Long> LONG = new DataType<>(Long.class.getName(), Schema.ConceptProperty.VALUE_LONG);
        public static final DataType<Double> DOUBLE = new DataType<>(Double.class.getName(), Schema.ConceptProperty.VALUE_DOUBLE);
        public static final Map<String, DataType<?>> SUPPORTED_TYPES = new HashMap<>();

        static {
            SUPPORTED_TYPES.put(STRING.getName(), STRING);
            SUPPORTED_TYPES.put(BOOLEAN.getName(), BOOLEAN);
            SUPPORTED_TYPES.put(LONG.getName(), LONG);
            SUPPORTED_TYPES.put(DOUBLE.getName(), DOUBLE);
            SUPPORTED_TYPES.put(Integer.class.getName(), LONG);
        }

        private final String dataType;
        private final Schema.ConceptProperty conceptProperty;

        private DataType(String dataType, Schema.ConceptProperty conceptProperty){
            this.dataType = dataType;
            this.conceptProperty = conceptProperty;
        }

        public String getName(){
            return dataType;
        }

        public Schema.ConceptProperty getConceptProperty(){
            return conceptProperty;
        }
    }
}
