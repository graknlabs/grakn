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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.answer.ConceptSet;

import javax.annotation.CheckReturnValue;

/**
 * A query for deleting concepts from a {@link Match}.
 * <p>
 * A {@link DeleteQuery} is built from a {@link Match} and will perform a delete operation for every result of
 * the {@link Match}.
 * <p>
 * The delete operation to perform is based on what {@link VarPattern} objects are provided to it. If only variable names
 * are provided, then the delete query will delete the concept bound to each given variable name. If property flags
 * are provided, e.g. {@code var("x").has("name")} then only those properties are deleted.
 *
 * @author Felix Chapman
 */
public interface DeleteQuery extends Query<ConceptSet> {

    /**
     * @param tx the graph to execute the query on
     * @return a new DeleteQuery with the graph set
     */
    @Override
    DeleteQuery withTx(GraknTx tx);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    DeleteQueryAdmin admin();
}
