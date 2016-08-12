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

package io.mindmaps.graql.internal.query.match;

import com.google.common.collect.Sets;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.internal.gremlin.Query;
import io.mindmaps.graql.internal.validation.MatchQueryValidator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.constants.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static java.util.stream.Collectors.toSet;

/**
 * Base MatchQuery implementation that executes the gremlin traversal
 */
public class MatchQueryBase implements MatchQuery.Admin {

    private final Pattern.Conjunction<Pattern.Admin> pattern;

    /**
     * @param pattern a pattern to match in the graph
     */
    public MatchQueryBase(Pattern.Conjunction<Pattern.Admin> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.pattern = pattern;
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<MindmapsTransaction> optionalTransaction, Optional<MatchOrder> order
    ) {
        MindmapsTransaction transaction = optionalTransaction.orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage())
        );

        new MatchQueryValidator(admin()).validate(transaction);

        GraphTraversal<Vertex, Map<String, Vertex>> traversal = getQuery(transaction, order).getTraversals();
        return traversal.toStream().map(vertices -> makeResults(transaction, vertices)).sequential();
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public Set<Type> getTypes(MindmapsTransaction transaction) {
        Query query = getQuery(transaction, Optional.empty());
        return query.getConcepts().map(transaction::getType).filter(t -> t != null).collect(toSet());
    }

    @Override
    public Set<Type> getTypes() {
        throw new IllegalStateException(ErrorMessage.NO_TRANSACTION.getMessage());
    }

    @Override
    public Set<String> getSelectedNames() {
        // Default selected names are all user defined variable names shared between disjunctions.
        // For example, in a query of the form
        // {..$x..$y..} or {..$x..}
        // $x will appear in the results, but not $y because it is not guaranteed to appear in all disjunctions

        // Get conjunctions within disjunction
        Set<Pattern.Conjunction<Var.Admin>> conjunctions = pattern.getDisjunctiveNormalForm().getPatterns();

        // Get all selected names from each conjunction
        Stream<Set<String>> vars = conjunctions.stream().map(this::getDefinedNamesFromConjunction);

        // Get the intersection of all conjunctions to find any variables shared between them
        // This will fail if there are no conjunctions (so the query is empty)
        return vars.reduce(Sets::intersection).orElseThrow(
                () -> new RuntimeException(ErrorMessage.MATCH_NO_PATTERNS.getMessage())
        );
    }

    @Override
    public Pattern.Conjunction<Pattern.Admin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "match " + pattern;
    }

    /**
     * @param conjunction a conjunction containing variables
     * @return all user-defined variable names in the given conjunction
     */
    private Set<String> getDefinedNamesFromConjunction(Pattern.Conjunction<Var.Admin> conjunction) {
        return conjunction.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .filter(Var.Admin::isUserDefinedName)
                .map(Var.Admin::getName)
                .collect(Collectors.toSet());
    }

    /**
     * @param transaction the transaction to execute the query on
     * @param order an optional ordering of the query
     * @return the query that will match the specified patterns
     */
    private Query getQuery(MindmapsTransaction transaction, Optional<MatchOrder> order) {
        return new Query(transaction, this.pattern, getSelectedNames(), order);
    }

    /**
     * @param transaction the transaction to get results from
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(MindmapsTransaction transaction, Map<String, Vertex> vertices) {
        return getSelectedNames().stream().collect(Collectors.toMap(
                name -> name,
                name -> transaction.getConcept(vertices.get(name).value(ITEM_IDENTIFIER.name()))
        ));
    }
}

