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

package io.mindmaps.graql.admin;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQueryDefault;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating an InsertQuery
 */
public interface InsertQueryAdmin extends InsertQuery {
    /**
     * @return the match query that this insert query is using, if it was provided one
     */
    Optional<? extends MatchQueryDefault> getMatchQuery();

    /**
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes();

    /**
     * @return the variables to insert in the insert query
     */
    Collection<VarAdmin> getVars();

    /**
     * @return a collection of Vars to insert, including any nested vars
     */
    Collection<VarAdmin> getAllVars();

    /**
     * @return the transaction set on this query, if it was provided one
     */
    Optional<MindmapsTransaction> getTransaction();
}
