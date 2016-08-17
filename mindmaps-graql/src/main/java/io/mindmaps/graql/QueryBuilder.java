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

package io.mindmaps.graql;

import com.google.common.collect.ImmutableSet;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.admin.AdminConverter;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.InsertQueryImpl;
import io.mindmaps.graql.internal.query.match.MatchQueryBase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * A starting point for creating queries.
 * <p>
 * A {@code QueryBuiler} is constructed with a {@code MindmapsTransaction}. All operations are performed using this
 * transaction. The user must explicitly commit or rollback changes after executing queries.
 * <p>
 * {@code QueryBuilder} also provides static methods for creating {@code Vars}.
 */
public class QueryBuilder {

    private final Optional<MindmapsTransaction> transaction;

    QueryBuilder() {
        this.transaction = Optional.empty();
    }

    QueryBuilder(MindmapsTransaction transaction) {
        this.transaction = Optional.of(transaction);
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public MatchQueryDefault match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public MatchQueryDefault match(Collection<? extends Pattern> patterns) {
        MatchQueryBase query = new MatchQueryBase(PatternAdmin.conjunction(AdminConverter.getPatternAdmins(patterns)));
        return transaction.map(query::withTransaction).orElse(query);
    }

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableSet<VarAdmin> varAdmins = ImmutableSet.copyOf(AdminConverter.getVarAdmins(vars));
        return new InsertQueryImpl(varAdmins, transaction);
    }

}