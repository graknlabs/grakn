/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.match.MatchBase;
import grakn.core.graql.internal.parser.QueryParserImpl;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.util.AdminConverter;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;

import static grakn.core.graql.query.Syntax.Compute.Method;

/**
 * A starting point for creating queries.
 * A {@code QueryBuiler} is constructed with a {@code Transaction}. All operations are performed using this
 * transaction. The user must explicitly commit or rollback changes after executing queries.
 * {@code QueryBuilderImpl} also provides static methods for creating {@code Vars}.
 */
public class QueryBuilder {

    @Nullable
    private final Transaction tx;
    private final QueryParser queryParser = QueryParserImpl.create(this);
    private boolean infer = true;

    public QueryBuilder() {
        this.tx = null;
    }

    @SuppressWarnings("unused")
    /** used by {@link TransactionImpl#graql()}*/
    public QueryBuilder(Transaction tx) {
        this.tx = tx;
    }

    /**
     * Enable or disable inference
     */
    public QueryBuilder infer(boolean infer) {
        this.infer = infer;
        return this;
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @javax.annotation.CheckReturnValue
    public Match match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @javax.annotation.CheckReturnValue
    public Match match(Collection<? extends Pattern> patterns) {
        Conjunction<PatternAdmin> conjunction = Patterns.conjunction(Sets.newHashSet(AdminConverter.getPatternAdmins(patterns)));
        MatchBase base = new MatchBase(conjunction);
        Match match = infer ? base.infer().admin() : base;
        return (tx != null) ? match.withTx(tx) : match;
    }

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @javax.annotation.CheckReturnValue
    public InsertQuery insert(VarPattern... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @javax.annotation.CheckReturnValue
    public InsertQuery insert(Collection<? extends VarPattern> vars) {
        ImmutableList<VarPatternAdmin> varAdmins = ImmutableList.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(tx, varAdmins);
    }

    /**
     * @param varPatterns an array of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @javax.annotation.CheckReturnValue
    public DefineQuery define(VarPattern... varPatterns) {
        return define(Arrays.asList(varPatterns));
    }

    /**
     * @param varPatterns a collection of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @javax.annotation.CheckReturnValue
    public DefineQuery define(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return DefineQuery.of(admins, tx);
    }

    /**
     * @param varPatterns an array of {@link VarPattern}s defining {@link SchemaConcept}s to undefine
     * @return an {@link UndefineQuery} that will remove the changes described in the {@code varPatterns}
     */
    @javax.annotation.CheckReturnValue
    public UndefineQuery undefine(VarPattern... varPatterns) {
        return undefine(Arrays.asList(varPatterns));
    }

    /**
     * @param varPatterns a collection of {@link VarPattern}s defining {@link SchemaConcept}s to undefine
     * @return an {@link UndefineQuery} that will remove the changes described in the {@code varPatterns}
     */
    @javax.annotation.CheckReturnValue
    public UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return UndefineQuery.of(admins, tx);
    }

    /**
     * @return a compute query builder for building analytics query
     */
    @javax.annotation.CheckReturnValue
    public <T extends Answer> ComputeQuery<T> compute(Method<T> method) {
        return new ComputeQuery<>(tx, method);
    }

    /**
     * Get a {@link QueryParser} for parsing queries from strings
     */
    public QueryParser parser() {
        return queryParser;
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    @javax.annotation.CheckReturnValue
    public <T extends Query<?>> T parse(String queryString) {
        return queryParser.parseQuery(queryString);
    }

}