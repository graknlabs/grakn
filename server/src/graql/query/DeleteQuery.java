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

import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A query for deleting concepts from a match clause clause.
 * The delete operation to perform is based on what Statement objects
 * are provided to it. If only variable names are provided, then the delete
 * query will delete the concept bound to each given variable name. If property
 * flags are provided, e.g. {@code var("x").has("name")} then only those
 * properties are deleted.
 */
public class DeleteQuery implements Query<ConceptSet> {

    private final MatchClause match;
    private final Set<Variable> vars;

    public DeleteQuery(MatchClause match, Set<Variable> vars) {
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        this.match = match;
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
        }
        this.vars = vars;
    }

    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @CheckReturnValue
    public Set<Variable> vars() {
        return vars;
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();
        query.append(match()).append(" ").append("delete");
        if (!vars().isEmpty())
            query.append(" ").append(vars().stream().map(Variable::toString).collect(joining(", ")).trim());
        query.append(";");

        return query.toString();
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DeleteQuery) {
            DeleteQuery that = (DeleteQuery) o;
            return (this.match.equals(that.match()))
                    && (this.vars.equals(that.vars()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.match.hashCode();
        h *= 1000003;
        h ^= this.vars.hashCode();
        return h;
    }
}
