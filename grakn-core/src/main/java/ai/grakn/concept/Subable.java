/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.concept;

import javax.annotation.CheckReturnValue;
import java.util.Collection;

/**
 * <p>
 *     Facilitates construction of hierarchies
 * </p>
 *
 * <p>
 *     Allows you to create a hierarchy of concepts by specifying what the super and sub types of the concept are.
 * </p>
 *
 * @param <T> the super and sub of the concept
 *
 * @author fppt
 */
public interface Subable<T extends Subable> {

    /**
     * Returns the unique id of this Type.
     *
     * @return The unique id of this type
     */
    //TODO: rename this ugly thing.
    @CheckReturnValue
    TypeId getTypeId();

    /**
     * Returns the unique label of this Type.
     *
     * @return The unique label of this type
     */
    @CheckReturnValue
    Label getLabel();

    /**
     *
     * @return The direct super of this concept
     */
    @CheckReturnValue
    T superType();

    /**
     * Get all indirect subs of this concept.
     *
     * The indirect subs are the concept itself and all indirect subs of direct subs.
     *
     * @return All the indirect sub-types of this Type
     */
    @CheckReturnValue
    Collection<? extends T> subTypes();
}
