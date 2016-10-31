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

package io.mindmaps.graql.admin;

import io.mindmaps.graql.Var;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Admin class for inspecting a Var
 */
public interface VarAdmin extends PatternAdmin, Var {
    @Override
    default boolean isVar() {
        return true;
    }

    @Override
    default VarAdmin asVar() {
        return this;
    }

    /**
     * @return the variable name of this variable
     */
    String getName();

    /**
     * @param name the new variable name of this variable
     */
    void setName(String name);

    /**
     * @return whether the user specified a name for this variable
     */
    boolean isUserDefinedName();

    /**
     * Get a stream of all properties on this variable
     */
    Stream<VarProperty> getProperties();

    /**
     * Get a stream of all properties of a particular type on this variable
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    <T extends VarProperty> Stream<T> getProperties(Class<T> type);

    /**
     * Get a unique property of a particular type on this variable, if it exists
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type);

    /**
     * @return the type of this variable, if it has one specified
     */
    Optional<VarAdmin> getType();

    /**
     * @return the ID this variable represents, if it represents something with a specific ID
     */
    Optional<String> getId();

    /**
     * @return if this var has only an ID set and no other properties, return that ID, else return nothing
     */
    Optional<String> getIdOnly();

    /**
     * @return all variables that this variable references
     */
    Set<VarAdmin> getInnerVars();

    /**
     * Get all inner variables, including implicit variables such as in a has-resource property
     */
    Set<VarAdmin> getImplicitInnerVars();

    /**
     * @return all type IDs that this variable refers to
     */
    Set<String> getTypeIds();

    /**
     * @return whether this variable represents a relation
     */
    boolean isRelation();

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    String getPrintableName();

    /**
     * @return true if this variable has no properties set
     */
    boolean hasNoProperties();

    /**
     * @return all predicates on the value of this variable
     */
    Set<ValuePredicateAdmin> getValuePredicates();

    /**
     * @return the values that this variable must have
     */
    Set<?> getValueEqualsPredicates();

    /**
     * @return all predicates on resources of this variable (where the key is the resource type)
     */
    Map<VarAdmin, Set<ValuePredicateAdmin>> getResourcePredicates();

    /**
     * @return all castings described on this relation (that is, pairs of role types and role players)
     */
    Set<Casting> getCastings();

    /**
     * A casting, a pair of role type and role player (where the role type may not be present)
     */
    interface Casting {
        /**
         * @return the role type, if specified
         */
        Optional<VarAdmin> getRoleType();

        /**
         * @return the role player
         */
        VarAdmin getRolePlayer();
    }
}
