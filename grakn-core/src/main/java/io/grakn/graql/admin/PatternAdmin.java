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
 *
 */

package io.grakn.graql.admin;

import io.grakn.graql.Pattern;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Admin class for inspecting and manipulating a Pattern
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
    Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm();

    /**
     * @return true if this Pattern.Admin is a Conjunction
     */
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Disjunction
     */
    default boolean isConjunction() {
        return false;
    }

    /**
     * @return true if this Pattern.Admin is a Var.Admin
     */
    default boolean isVar() {
        return false;
    }

    /**
     * @return this Pattern.Admin as a Disjunction, if it is one.
     */
    default Disjunction<?> asDisjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern.Admin as a Conjunction, if it is one.
     */
    default Conjunction<?> asConjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern.Admin as a Var.Admin, if it is one.
     */
    default VarAdmin asVar() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return all variables referenced in the pattern
     */
    default Set<VarAdmin> getVars() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(toSet());
    }
}
