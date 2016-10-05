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

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
import io.mindmaps.graql.internal.util.StringConverter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.ConceptProperty.REGEX;

public class RegexProperty extends AbstractVarProperty implements UniqueVarProperty, SingleTraversalProperty, NamedProperty {

    private final String regex;

    public RegexProperty(String regex) {
        this.regex = regex;
    }

    public String getRegex() {
        return regex;
    }

    @Override
    public String getName() {
        return "regex";
    }

    @Override
    public String getProperty() {
        return StringConverter.valueToString(regex);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.has(REGEX.name(), P.eq(regex));
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        concept.asResourceType().setRegex(regex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegexProperty that = (RegexProperty) o;

        return regex.equals(that.regex);

    }

    @Override
    public int hashCode() {
        return regex.hashCode();
    }
}
