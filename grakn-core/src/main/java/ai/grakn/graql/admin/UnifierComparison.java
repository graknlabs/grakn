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

import ai.grakn.concept.Type;
import ai.grakn.graql.Var;

/**
 *
 * <p>
 * Interface for defining unifier comparisons.
 * </p>
 *
 *@author Kasper Piskorski
 *
 */
public interface UnifierComparison {

    /**
     * @param parent {@link Type} of parent expression
     * @param child {@link Type} of child expression
     * @return true if {@link Type}s are compatible
     */
    boolean typeCompatibility(Type parent, Type child);

    /**
     * @param parent {@link Atomic} of parent expression
     * @param child {@link Atomic} of child expression
     * @return true if {@link Atomic}s compatible
     */
    boolean atomicCompatibility(Atomic parent, Atomic child);

    /**
     * @param query to be checked
     * @param var variable of interest
     * @param type which playability is toto be checked
     * @return true if typing the typeVar with type is compatible with role configuration of the provided query
     */
    boolean typePlayability(ReasonerQuery query, Var var, Type type);
}
