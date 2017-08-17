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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchQueryAdmin;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * a query used for finding data in a graph that matches the given patterns.
 * <p>
 * The {@code MatchQuery} is a pattern-matching query. The patterns are described in a declarative fashion, forming a
 * subgraph, then the {@code MatchQuery} will traverse the graph in an efficient fashion to find any matching subgraphs.
 * <p>
 * Each matching subgraph will produce a map, where keys are variable names and values are concepts in the graph.
 *
 * @author Felix Chapman
 */
public interface MatchQuery extends Query<List<Answer>>, Streamable<Answer> {

    /**
     * @param names an array of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    @CheckReturnValue
    MatchQuery select(String... names);

    /**
     * @param names a set of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    @CheckReturnValue
    MatchQuery select(Set<Var> names);

    /**
     * @param name a variable name to get
     * @return a stream of concepts
     */
    @CheckReturnValue
    Stream<Concept> get(String name);

    /**
     * @param vars an array of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    @CheckReturnValue
    InsertQuery insert(VarPattern... vars);

    /**
     * @param vars a collection of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    @CheckReturnValue
    InsertQuery insert(Collection<? extends VarPattern> vars);

    /**
     * @param names an array of variable names to delete for each result of this match query
     * @return a delete query that will delete the given variable names for each result of this match query
     */
    @CheckReturnValue
    DeleteQuery delete(String... names);

    /**
     * @param deleters an array of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    @CheckReturnValue
    DeleteQuery delete(VarPattern... deleters);

    /**
     * @param deleters a collection of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    @CheckReturnValue
    DeleteQuery delete(Collection<? extends VarPattern> deleters);

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new MatchQuery with the given ordering
     */
    @CheckReturnValue
    MatchQuery orderBy(String varName);

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new MatchQuery with the given ordering
     */
    @CheckReturnValue
    MatchQuery orderBy(Var varName);

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param order the ordering to use
     * @return a new MatchQuery with the given ordering
     */
    @CheckReturnValue
    MatchQuery orderBy(String varName, Order order);

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param order the ordering to use
     * @return a new MatchQuery with the given ordering
     */
    @CheckReturnValue
    MatchQuery orderBy(Var varName, Order order);

    /**
     * @param graph the graph to execute the query on
     * @return a new MatchQuery with the graph set
     */
    @Override
    MatchQuery withGraph(GraknTx graph);

    /**
     * @param limit the maximum number of results the query should return
     * @return a new MatchQuery with the limit set
     */
    @CheckReturnValue
    MatchQuery limit(long limit);

    /**
     * @param offset the number of results to skip
     * @return a new MatchQuery with the offset set
     */
    @CheckReturnValue
    MatchQuery offset(long offset);

    /**
     * remove any duplicate results from the query
     * @return a new MatchQuery without duplicate results
     */
    @CheckReturnValue
    MatchQuery distinct();

    /**
     * Aggregate results of a query.
     * @param aggregate the aggregate operation to apply
     * @param <S> the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    @CheckReturnValue
    <S> AggregateQuery<S> aggregate(Aggregate<? super Answer, S> aggregate);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    @CheckReturnValue
    MatchQueryAdmin admin();
}
