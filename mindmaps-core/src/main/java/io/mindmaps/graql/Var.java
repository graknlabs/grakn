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
 *
 */

package io.mindmaps.graql;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.admin.VarAdmin;

/**
 * A wildcard variable to refers to a concept in a query.
 * <p>
 * A {@code Var} may be given a variable name, or left as an "anonymous" variable. {@code Graql} provides
 * static methods for constructing {@code Var} objects.
 * <p>
 * The methods on {@code Var} are used to set its properties. A {@code Var} behaves differently depending on the type of
 * query its used in. In a {@code MatchQuery}, a {@code Var} describes the properties any matching concept must have. In
 * an {@code InsertQuery}, it describes the properties that should be set on the inserted concept. In a
 * {@code DeleteQuery}, it describes the properties that should be deleted.
 */
@SuppressWarnings("UnusedReturnValue")
public interface Var extends Pattern {

    /**
     * @param id a string that this variable's ID must match
     * @return this
     */
    Var id(String id);

    /**
     * this variable must have a value
     * @return this
     */
    Var value();

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    Var value(Object value);

    /**
     * @param predicate a predicate this variable's value must match
     * @return this
     */
    Var value(ValuePredicate predicate);

    /**
     * @param type a resource type that this variable must have an instance of
     * @return this
     */
    Var has(String type);

    /**
     * the variable must have a resource or name of the given type with an exact matching value
     *
     * @param type a resource type in the ontology
     * @param value a value of a resource
     * @return this
     */
    Var has(String type, Object value);

    /**
     * the variable must have a resource or name of the given type that matches the given predicate
     *
     * @param type a resource type in the ontology
     * @param predicate a predicate on the value of a resource
     * @return this
     */
    Var has(String type, ValuePredicate predicate);

    /**
     * the variable must have a resource or name of the given type that matches the given predicate
     *
     * @param type a resource type in the ontology
     * @param var a variable representing a resource
     * @return this
     */
    Var has(String type, Var var);

    /**
     * @param type a concept type id that the variable must be of this type
     * @return this
     */
    Var isa(String type);

    /**
     * @param type a concept type that this variable must be an instance of
     * @return this
     */
    Var isa(Var type);

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    Var ako(String type);

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    Var ako(Var type);

    /**
     * @param type a role type id that this relation type variable must have
     * @return this
     */
    Var hasRole(String type);

    /**
     * @param type a role type that this relation type variable must have
     * @return this
     */
    Var hasRole(Var type);

    /**
     * @param type a role type id that this concept type variable must play
     * @return this
     */
    Var playsRole(String type);

    /**
     * @param type a role type that this concept type variable must play
     * @return this
     */
    Var playsRole(Var type);

    /**
     * @param type a scope that this variable must have
     * @return this
     */
    Var hasScope(Var type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    Var hasResource(String type);

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    Var hasResource(Var type);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(Var roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(String roletype, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(Var roletype, String roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a role type in the ontology
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(String roletype, Var roleplayer);

    /**
     * the variable must be a relation with the given roleplayer playing the given roletype
     *
     * @param roletype   a variable representing a roletype
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    Var rel(Var roletype, Var roleplayer);

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     * @return this
     */
    Var isAbstract();

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    Var datatype(ResourceType.DataType<?> datatype);

    /**
     * Specify the regular expression instances of this resource type must match
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    Var regex(String regex);

    /**
     * @param lhs the left-hand side of this rule
     * @return this
     */
    Var lhs(String lhs);

    /**
     * @param rhs the right-hand side of this rule
     * @return this
     */
    Var rhs(String rhs);

    /**
     * @return an Admin class to allow inspection of this Var
     */
    VarAdmin admin();
}
