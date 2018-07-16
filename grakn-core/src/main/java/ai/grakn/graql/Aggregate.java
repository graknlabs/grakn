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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import ai.grakn.graql.admin.Answer;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * An aggregate operation to perform on a query.
 * @param <T> the type of the result of the aggregate operation
 *
 * @author Felix Chapman
 */
public interface Aggregate<T> {
    /**
     * The function to apply to the stream of results to produce the aggregate result.
     * @param stream a stream of query results
     * @return the result of the aggregate operation
     */
    @CheckReturnValue
    T apply(Stream<? extends Answer> stream);

    /**
     * Return a {@link NamedAggregate}. This is used when operating on a query with multiple aggregates.
     * @param name the name of the aggregate
     * @return a new named aggregate
     */
    @CheckReturnValue
    NamedAggregate<T> as(String name);
}
