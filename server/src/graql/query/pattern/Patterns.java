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

package grakn.core.graql.query.pattern;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for instances of {@link Pattern}.
 *
 * Also includes helper methods to operate on a {@link Pattern} or {@link VarPattern}.
 *
 */
public class Patterns {

    private static final AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);

    private Patterns() {}

    public static Var var() {
        return new Var(Long.toString(counter.getAndIncrement()), Var.Kind.Generated);
    }

    public static Var var(String value) {
        return new Var(value, Var.Kind.UserDefined);
    }
}
