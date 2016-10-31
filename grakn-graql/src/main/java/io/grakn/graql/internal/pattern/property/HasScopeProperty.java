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

package io.grakn.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.grakn.GraknGraph;
import io.grakn.concept.Concept;
import io.grakn.concept.Instance;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import io.grakn.graql.internal.gremlin.fragment.Fragments;
import io.grakn.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

public class HasScopeProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin scope;

    public HasScopeProperty(VarAdmin scope) {
        this.scope = scope;
    }

    public VarAdmin getScope() {
        return scope;
    }

    @Override
    public String getName() {
        return "has-scope";
    }

    @Override
    public String getProperty() {
        return scope.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(String start) {
        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.outHasScope(start, scope.getName()),
                Fragments.inHasScope(scope.getName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(scope);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Instance scopeInstance = insertQueryExecutor.getConcept(scope).asInstance();
        concept.asRelation().scope(scopeInstance);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        String scopeId = scope.getId().orElseThrow(() -> failDelete(this));
        concept.asRelation().deleteScope(graph.getInstance(scopeId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasScopeProperty that = (HasScopeProperty) o;

        return scope.equals(that.scope);

    }

    @Override
    public int hashCode() {
        return scope.hashCode();
    }
}
