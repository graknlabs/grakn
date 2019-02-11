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

package grakn.core.graql.query.query;

import grakn.core.graql.query.query.builder.AggregateBuilder;
import grakn.core.graql.query.statement.Variable;
import graql.lang.exception.GraqlException;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A query used for finding data in a knowledge base that matches the given patterns. The Get Query is a
 * pattern-matching query. The patterns are described in a declarative fashion, then the query will traverse
 * the knowledge base in an efficient fashion to find any matching answers.
 */
public class GraqlGet extends GraqlQuery implements AggregateBuilder<GraqlGet.Aggregate> {

    private final LinkedHashSet<Variable> vars;
    private final MatchClause match;

    public GraqlGet(MatchClause match) {
        this(match, new LinkedHashSet<>());
    }

    public GraqlGet(MatchClause match, LinkedHashSet<Variable> vars) {
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        this.vars = vars;
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlException.varNotInQuery(var.toString());
            }
        }
        this.match = match;
    }

    @Override
    public Aggregate aggregate(Token.Statistics.Method method, Variable var) {
        return new Aggregate(this, method, var);
    }

    public Group group(String var) {
        return group(new Variable(var));
    }

    public Group group(Variable var) {
        return new Group(this, var);
    }

    @CheckReturnValue // TODO: Return LinkedHashSet
    public Set<Variable> vars() {
        if (vars.isEmpty()) return match.getPatterns().variables();
        else return vars;
    }

    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @Override @SuppressWarnings("Duplicates")
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(match());
        if (match().getPatterns().getPatterns().size()>1) query.append(Token.Char.NEW_LINE);
        else query.append(Token.Char.SPACE);

        // It is important that we use vars (the property) and not vars() (the method)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to print vars (the property) as the user defined
        query.append(Token.Command.GET);
        if (!vars.isEmpty()) {
            query.append(Token.Char.SPACE).append(
                    vars.stream().map(Variable::toString)
                            .collect(joining(Token.Char.COMMA_SPACE.toString()))
            );
        }
        query.append(Token.Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlGet that = (GraqlGet) o;

        // It is important that we use vars() (the method) and not vars (the property)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to compare vars() (the method) which determines the final value
        return (this.vars().equals(that.vars()) &&
                this.match().equals(that.match()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        // It is important that we use vars() (the method) and not vars (the property)
        // For reasons explained in the equals() method above
        h ^= this.vars().hashCode();
        h *= 1000003;
        h ^= this.match().hashCode();
        return h;
    }

    public static class Aggregate extends GraqlQuery {

        private final GraqlGet query;
        private final Token.Statistics.Method method;
        private final Variable var;

        public Aggregate(GraqlGet query, Token.Statistics.Method method, Variable var) {
            if (query == null) {
                throw new NullPointerException("GetQuery is null");
            }
            this.query = query;

            if (method == null) {
                throw new NullPointerException("Method is null");
            }
            this.method = method;

            if (var == null && !method.equals(Token.Statistics.Method.COUNT)) {
                throw new NullPointerException("Variable is null");
            } else if (var != null && method.equals(Token.Statistics.Method.COUNT)) {
                throw new IllegalArgumentException("Aggregate COUNT does not accept a Variable");
            } else if (var != null && !query.vars().contains(var)) {
                throw new IllegalArgumentException("Aggregate variable should be contained in GET query");
            }

            this.var = var;
        }

        public GraqlGet query() {
            return query;
        }

        public Token.Statistics.Method method() {
            return method;
        }

        public Variable var() {
            return var;
        }

        @Override
        public final String toString() {
            StringBuilder query = new StringBuilder();

            query.append(query()).append(Token.Char.SPACE).append(method);

            if (var != null) query.append(Token.Char.SPACE).append(var);
            query.append(Token.Char.SEMICOLON);

            return query.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Aggregate that = (Aggregate) o;

            return (this.query.equals(that.query()) &&
                    this.method.equals(that.method()) &&
                    this.var == null ?
                        that.var() == null :
                        this.var.equals(that.var()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.query.hashCode();
            h *= 1000003;
            h ^= this.method.hashCode();
            h *= 1000003;
            h ^= this.var.hashCode();
            return h;
        }

    }

    public static class Group extends GraqlQuery implements AggregateBuilder<GraqlGet.Group.Aggregate> {

        private final GraqlGet query;
        private final Variable var;

        public Group(GraqlGet query, Variable var) {
            if (query == null) {
                throw new NullPointerException("GetQuery is null");
            }
            this.query = query;

            if (var == null) {
                throw new NullPointerException("Variable is null");
            }
            this.var = var;
        }

        public GraqlGet query() {
            return query;
        }

        public Variable var() {
            return var;
        }

        @Override
        public Aggregate aggregate(Token.Statistics.Method method, Variable var) {
            return new Aggregate(this, method, var);
        }

        @Override
        public String toString() {
            StringBuilder query = new StringBuilder();

            query.append(query()).append(Token.Char.SPACE)
                    .append(Token.Command.GROUP).append(Token.Char.SPACE)
                    .append(var).append(Token.Char.SEMICOLON);

            return query.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Group that = (Group) o;

            return (this.query.equals(that.query()) &&
                    this.var.equals(that.var()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.query.hashCode();
            h *= 1000003;
            h ^= this.var.hashCode();
            return h;
        }

        public static class Aggregate extends GraqlQuery {

            private final GraqlGet.Group group;
            private final Token.Statistics.Method method;
            private final Variable var;

            public Aggregate(GraqlGet.Group group, Token.Statistics.Method method, Variable var) {

                if (group == null) {
                    throw new NullPointerException("GraqlGet.Group is null");
                }
                this.group = group;

                if (method == null) {
                    throw new NullPointerException("Method is null");
                }
                this.method = method;

                if (var == null && !this.method.equals(Token.Statistics.Method.COUNT)) {
                    throw new NullPointerException("Variable is null");
                } else if (var != null && this.method.equals(Token.Statistics.Method.COUNT)) {
                    throw new IllegalArgumentException("Aggregate COUNT does not accept a Variable");
                } else if (var != null && !group.query().vars().contains(var)) {
                    throw new IllegalArgumentException("Aggregate variable should be contained in GET query");
                }
                this.var = var;
            }

            public GraqlGet.Group group() {
                return group;
            }

            public Token.Statistics.Method method() {
                return method;
            }

            public Variable var() {
                return var;
            }

            @Override
            public final String toString() {
                StringBuilder query = new StringBuilder();

                query.append(group().query()).append(Token.Char.SPACE)
                        .append(Token.Command.GROUP).append(Token.Char.SPACE)
                        .append(group().var()).append(Token.Char.SEMICOLON).append(Token.Char.SPACE)
                        .append(method);

                if (var != null) query.append(Token.Char.SPACE).append(var);
                query.append(Token.Char.SEMICOLON);

                return query.toString();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Aggregate that = (Aggregate) o;

                return (this.group().equals(that.group()) &&
                        this.method().equals(that.method()) &&
                        this.var() == null ?
                            that.var() == null :
                            this.var().equals(that.var()));
            }

            @Override
            public int hashCode() {
                int h = 1;
                h *= 1000003;
                h ^= this.group.hashCode();
                h *= 1000003;
                h ^= this.method.hashCode();
                h *= 1000003;
                h ^= this.var.hashCode();
                return h;
            }
        }
    }
}
