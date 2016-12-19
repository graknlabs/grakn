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
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.AskQueryAdmin;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.query.match.MatchQueryBase;
import com.google.common.collect.ImmutableCollection;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Internal query factory
 */
public class Queries {

    private Queries() {
    }

    /**
     * @param pattern a pattern to match in the graph
     */
    public static MatchQueryAdmin match(Conjunction<PatternAdmin> pattern, boolean infer, boolean materialise) {
        MatchQueryBase query = new MatchQueryBase(pattern);
        return infer ? query.infer(materialise).admin() : query;
    }

    /**
     * @param matchQuery the match query that the ask query will search for in the graph
     */
    public static AskQueryAdmin ask(MatchQuery matchQuery) {
        return new AskQueryImpl(matchQuery);
    }

    /**
     * @param vars       a collection of Vars to insert
     * @param matchQuery the match query to insert for each result
     */
    public static InsertQueryAdmin insert(ImmutableCollection<VarAdmin> vars, MatchQueryAdmin matchQuery) {
        return new InsertQueryImpl(vars, Optional.of(matchQuery), Optional.empty());
    }

    /**
     * @param graph the graph to execute on
     * @param vars  a collection of Vars to insert
     */
    public static InsertQueryAdmin insert(ImmutableCollection<VarAdmin> vars, Optional<GraknGraph> graph) {
        return new InsertQueryImpl(vars, Optional.empty(), graph);
    }

    public static DeleteQueryAdmin delete(Collection<VarAdmin> deleters, MatchQuery matchQuery) {
        return new DeleteQueryImpl(deleters, matchQuery);
    }

    public static ComputeQueryBuilder compute(Optional<GraknGraph> graph) {
        return new ComputeQueryBuilderImpl(graph);
    }

    public static <T> AggregateQuery<T> aggregate(MatchQueryAdmin matchQuery, Aggregate<? super Map<String, Concept>, T> aggregate) {
        return new AggregateQueryImpl<>(matchQuery, aggregate);
    }
}
