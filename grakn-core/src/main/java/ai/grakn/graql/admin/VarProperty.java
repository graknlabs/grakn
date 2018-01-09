/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.admin;

import javax.annotation.CheckReturnValue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A property of a {@link VarPatternAdmin}, such as "isa movie" or "has name 'Jim'"
 *
 * @author Felix Chapman
 */
public interface VarProperty {

    /**
     * Build a Graql string representation of this property
     * @param builder a string builder to append to
     */
    void buildString(StringBuilder builder);

    /**
     * Get the Graql string representation of this property
     */
    @CheckReturnValue
    default String graqlString() {
        StringBuilder builder = new StringBuilder();
        buildString(builder);
        return builder.toString();
    }

    /**
     * Get a stream of {@link VarPatternAdmin} that must be types.
     */
    @CheckReturnValue
    Stream<VarPatternAdmin> getTypes();

    /**
     * Get a stream of any inner {@link VarPatternAdmin} within this `VarProperty`.
     */
    @CheckReturnValue
    Stream<VarPatternAdmin> innerVarPatterns();

    /**
     * Get a stream of any inner {@link VarPatternAdmin} within this `VarProperty`, including any that may have been
     * implicitly created (such as with "has").
     */
    @CheckReturnValue
    Stream<VarPatternAdmin> implicitInnerVarPatterns();

    /**
     * True if there is at most one of these properties for each {@link VarPatternAdmin}
     */
    @CheckReturnValue
    default boolean isUnique() {
        return false;
    }

    /**
     * maps this var property to a reasoner atom
     * @param var {@link VarPatternAdmin} this property belongs to
     * @param vars VarAdmins constituting the pattern this property belongs to
     * @param parent reasoner query this atom should belong to
     * @return created atom
     */
    @CheckReturnValue
    Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent);
}
