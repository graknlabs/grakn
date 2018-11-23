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

package grakn.core.graql.internal.pattern;

import grakn.core.graql.query.Var;
import grakn.core.graql.query.VarPattern;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Disjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.admin.VarProperty;
import grakn.core.graql.query.Pattern;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for instances of {@link Pattern}.
 *
 * Also includes helper methods to operate on a {@link Pattern} or {@link VarPattern}.
 *
 */
public class Patterns {

    private static final AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);

    public static final Var RELATION_EDGE = reservedVar("RELATION_EDGE");
    public static final Var RELATION_DIRECTION = reservedVar("RELATION_DIRECTION");

    private Patterns() {}

    public static <T extends PatternAdmin> Conjunction<T> conjunction(Set<T> patterns) {
        return new AutoValue_ConjunctionImpl<>(patterns);
    }

    public static <T extends PatternAdmin> Disjunction<T> disjunction(Set<T> patterns) {
        return new AutoValue_DisjunctionImpl<>(patterns);
    }

    public static Var var() {
        return VarImpl.of(Long.toString(counter.getAndIncrement()), Var.Kind.Generated);
    }

    public static Var var(String value) {
        return VarImpl.of(value, Var.Kind.UserDefined);
    }

    public static VarPatternAdmin varPattern(Var name, Set<VarProperty> properties) {
        if (properties.isEmpty()) {
            return name.admin();
        } else {
            return new AutoValue_VarPatternImpl(name, properties);
        }
    }

    private static Var reservedVar(String value) {
        return VarImpl.of(value, Var.Kind.Reserved);
    }
}
