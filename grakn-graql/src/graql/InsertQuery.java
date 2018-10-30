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
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.answer.ConceptMap;

import javax.annotation.CheckReturnValue;

/**
 * A query for inserting data.
 * <p>
 * A {@link InsertQuery} can be built from a {@link QueryBuilder} or a {@link Match}.
 * <p>
 * When built from a {@code QueryBuilder}, the insert query will execute once, inserting all the variables provided.
 * <p>
 * When built from a {@link Match}, the {@link InsertQuery} will execute for each result of the {@link Match},
 * where variable names in the {@link InsertQuery} are bound to the concept in the result of the {@link Match}.
 *
 * @author Felix Chapman
 */
public interface InsertQuery extends Query<ConceptMap> {

    /**
     * @param tx the graph to execute the query on
     * @return a new InsertQuery with the graph set
     */
    @Override
    InsertQuery withTx(GraknTx tx);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    InsertQueryAdmin admin();

}
