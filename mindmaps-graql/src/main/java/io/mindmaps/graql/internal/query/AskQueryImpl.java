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

package io.mindmaps.graql.internal.query;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.AskQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.AskQueryAdmin;
import io.mindmaps.graql.internal.util.ANSI;

import java.util.stream.Stream;

/**
 * An AskQuery to check if a given pattern matches anywhere in the graph
 */
class AskQueryImpl implements AskQueryAdmin {

    private final MatchQuery matchQuery;

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    AskQueryImpl(MatchQuery matchQuery) {
        this.matchQuery = matchQuery;
    }

    @Override
    public Boolean execute() {
        return matchQuery.iterator().hasNext();
    }

    @Override
    public Stream<String> resultsString() {
        if (execute()) {
            return Stream.of(ANSI.color("True", ANSI.GREEN));
        } else {
            return Stream.of(ANSI.color("False", ANSI.RED));
        }
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public AskQuery withGraph(MindmapsGraph graph) {
        return new AskQueryImpl(matchQuery.withGraph(graph));
    }

    @Override
    public AskQueryAdmin admin() {
        return this;
    }

    @Override
    public String toString() {
        return matchQuery.toString() + " ask;";
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }
}