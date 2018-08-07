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

package ai.grakn.graql.admin;

import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;

import javax.annotation.CheckReturnValue;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Admin class for inspecting and manipulating a Pattern
 *
 * @author Felix Chapman
 */
public interface PatternAdmin extends Pattern {
    /**
     * Get the disjunctive normal form of this pattern group.
     * This means the pattern group will be transformed into a number of conjunctive patterns, where each is disjunct.
     *
     * e.g.
     * p = (A or B) and (C or D)
     * p.getDisjunctiveNormalForm() = (A and C) or (A and D) or (B and C) or (B and D)
     *
     * @return the pattern group in disjunctive normal form
     */
    @CheckReturnValue
    Disjunction<Conjunction<VarPatternAdmin>> getDisjunctiveNormalForm();

    /**
     * Get all common, user-defined variable names in the pattern.
     */
    @CheckReturnValue
    Set<Var> commonVars();

    /**
     * @return true if this Pattern.Admin is a Conjunction
     */
    @CheckReturnValue
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Disjunction
     */
    @CheckReturnValue
    default boolean isConjunction() {
        return false;
    }

    /**
     * @return true if this {@link PatternAdmin} is a {@link VarPatternAdmin}
     */
    @CheckReturnValue
    default boolean isVarPattern() {
        return false;
    }

    /**
     * @return this Pattern.Admin as a Disjunction, if it is one.
     */
    @CheckReturnValue
    default Disjunction<?> asDisjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern.Admin as a Conjunction, if it is one.
     */
    @CheckReturnValue
    default Conjunction<?> asConjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this {@link PatternAdmin} as a {@link VarPatternAdmin}, if it is one.
     */
    @CheckReturnValue
    default VarPatternAdmin asVarPattern() {
        throw new UnsupportedOperationException();
    }
    /**
     * @return all variables referenced in the pattern
     */
    @CheckReturnValue
    default Set<VarPatternAdmin> varPatterns() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(toSet());
    }
}
