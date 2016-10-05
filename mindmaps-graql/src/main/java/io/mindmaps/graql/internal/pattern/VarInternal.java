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

package io.mindmaps.graql.internal.pattern;

import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;

import java.util.Set;

/**
 * Internal interface for Var
 */
public interface VarInternal extends VarAdmin {

    /**
     * @return the gremlin traversals that describe this variable
     */
    Set<MultiTraversal> getMultiTraversals();

    /**
     * Get all inner variables, including implicit variables such as in a has-resource property
     */
    Set<VarAdmin> getImplicitInnerVars();
}
