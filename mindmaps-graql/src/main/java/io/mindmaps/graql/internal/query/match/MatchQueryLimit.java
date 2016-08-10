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
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.api.query.MatchQuery;

import java.util.Map;
import java.util.stream.Stream;

/**
 * "Limit" modifier for match query that limits the results of a query.
 */
public class MatchQueryLimit extends MatchQueryDefault {

    private final long limit;

    public MatchQueryLimit(MatchQuery.Admin inner, long limit) {
        super(inner);
        this.limit = limit;
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return inner.stream().limit(limit);
    }

    @Override
    public String toString() {
        return inner.toString() + " limit " + limit;
    }

    @Override
    protected MatchQuery setInner(MatchQuery.Admin inner) {
        return new MatchQueryLimit(inner, limit);
    }
}
