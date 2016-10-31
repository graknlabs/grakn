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

package io.grakn.graql.internal.query.match;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.grakn.concept.Concept;
import io.grakn.util.ErrorMessage;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * "Select" modifier for a match query that selects particular variables from the result.
 */
class MatchQuerySelect extends MatchQueryModifier {

    private final ImmutableSet<String> names;

    MatchQuerySelect(MatchQueryInternal inner, ImmutableSet<String> names) {
        super(inner);

        if (names.isEmpty()) {
            throw new IllegalArgumentException(ErrorMessage.SELECT_NONE_SELECTED.getMessage());
        }

        this.names = names;
    }

    @Override
    public Stream<Map<String, Concept>> transformStream(Stream<Map<String, Concept>> stream) {
        return stream.map(result -> Maps.filterKeys(result, names::contains));
    }

    @Override
    protected String modifierString() {
        return "select " + names.stream().map(s -> "$" + s).collect(joining(", "));
    }

    @Override
    public Set<String> getSelectedNames() {
        return names;
    }
}
