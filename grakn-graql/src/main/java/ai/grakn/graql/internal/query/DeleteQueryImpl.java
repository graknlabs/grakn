/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A DeleteQuery that will execute deletions for every result of a MatchQuery
 */
class DeleteQueryImpl implements DeleteQueryAdmin {
    private final ImmutableMap<VarName, VarAdmin> deleters;
    private final MatchQueryAdmin matchQuery;

    /**
     * @param deleters a collection of variable patterns to delete
     * @param matchQuery a pattern to match and delete for each result
     */
    DeleteQueryImpl(Collection<VarAdmin> deleters, MatchQuery matchQuery) {
        this.deleters = Maps.uniqueIndex(deleters, VarAdmin::getVarName);
        this.matchQuery = matchQuery.admin();
    }

    @Override
    public Void execute() {
        List<Map<VarName, Concept>> results = matchQuery.streamWithVarNames().collect(toList());
        results.forEach(result -> result.forEach(this::deleteResult));
        return null;
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        execute();
        return Stream.empty();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public DeleteQuery withGraph(GraknGraph graph) {
        return Queries.delete(deleters.values(), matchQuery.withGraph(graph));
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    /**
     * Delete a result from a query. This may involve deleting the whole concept or specific edges, depending
     * on what deleters were provided.
     * @param name the variable name to delete
     * @param result the concept that matches the variable in the graph
     */
    private void deleteResult(VarName name, Concept result) {
        VarAdmin deleter = deleters.get(name);

        // Check if this has been requested to be deleted
        if (deleter == null) return;

        if (!deleter.getProperties().findAny().isPresent()) {
            // Delete whole concept if nothing specified to delete
            result.delete();
        } else {
            deleter.getProperties().forEach(property ->
                    ((VarPropertyInternal) property).delete(getGraph(), result)
            );
        }
    }

    private GraknGraph getGraph() {
        return matchQuery.getGraph().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );
    }

    @Override
    public Collection<VarAdmin> getDeleters() {
        return deleters.values();
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteQueryImpl that = (DeleteQueryImpl) o;

        if (!deleters.equals(that.deleters)) return false;
        return matchQuery.equals(that.matchQuery);
    }

    @Override
    public int hashCode() {
        int result = deleters.hashCode();
        result = 31 * result + matchQuery.hashCode();
        return result;
    }
}
