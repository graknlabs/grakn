/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
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

package ai.grakn.graql;

import ai.grakn.concept.Concept;
import ai.grakn.graql.admin.Answer;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * An interface for converting objects in Graql responses (e.g. {@link Integer}s and {@link Answer}s into a
 * given type {@link T}).
 *
 * <p>
 *     The intermediate {@link Builder} type is used when the final type is different to the "in-progress" type when
 *     creating it. For example, if {@link T} is {@link String}, then you may want to use a link {@link StringBuilder}
 *     for {@link Builder} (for efficiency).
 * </p>
 *
 * <p>
 *     If you don't need a {@link Builder} type, then set it to the same type as {@link T} and implement
 *     {@link #build(Builder)} to just return its argument.
 * </p>
 *
 * @param <Builder> An intermediate builder type that can be changed into a {@link T}
 * @param <T> The type to convert into
 *
 * @author Felix Chapman
 */
public interface GraqlConverter<Builder, T> {

    /**
     * Convert any object into the given type
     * @param object the object to convert
     * @return the converted object
     */
    @CheckReturnValue
    default T convert(Object object) {
        Builder builder = convert(false, object);
        return build(builder);
    }

    /**
     * Convert any object into a builder
     * @param inner whether this object is within a collection
     * @param object the object to convert into a builder
     * @return the object as a builder
     */
    @CheckReturnValue
    default Builder convert(boolean inner, Object object) {
        if (object instanceof Concept) {
            return convert(inner, (Concept) object);
        } else if (object instanceof Boolean) {
            return convert(inner, (boolean) object);
        } else if (object instanceof Optional) {
            return convert(inner, (Optional<?>) object);
        } else if (object instanceof Collection) {
            return convert(inner, (Collection<?>) object);
        } else if (object instanceof Answer) {
            return convert(inner, (Answer) object);
        } else if (object instanceof Map) {
            return convert(inner, (Map<?, ?>) object);
        } else {
            return convertDefault(inner, object);
        }
    }

    /**
     * Convert a builder into the final type
     * @param builder the builder to convert into the final type
     * @return the converted builder
     */
    @CheckReturnValue
    T build(Builder builder);

    /**
     * Convert any concept into a builder
     * @param inner whether this concept is within a collection
     * @param concept the concept to convert into a builder
     * @return the concept as a builder
     */
    @CheckReturnValue
    Builder convert(boolean inner, Concept concept);

    /**
     * Convert any boolean into a builder
     * @param inner whether this boolean is within a collection
     * @param bool the boolean to convert into a builder
     * @return the boolean as a builder
     */
    @CheckReturnValue
    Builder convert(boolean inner, boolean bool);

    /**
     * Convert any optional into a builder
     * @param inner whether this optional is within a collection
     * @param optional the optional to convert into a builder
     * @return the optional as a builder
     */
    @CheckReturnValue
    Builder convert(boolean inner, Optional<?> optional);

    /**
     * Convert any collection into a builder
     * @param inner whether this collection is within a collection
     * @param collection the collection to convert into a builder
     * @return the collection as a builder
     */
    @CheckReturnValue
    Builder convert(boolean inner, Collection<?> collection);

    /**
     * Convert any map into a builder
     * @param inner whether this map is within a collection
     * @param map the map to convert into a builder
     * @return the map as a builder
     */
    @CheckReturnValue
    Builder convert(boolean inner, Map<?, ?> map);

    /**
     * Convert any {@link Answer} into a builder
     * @param inner whether this map is within a collection
     * @param answer the answer to convert into a builder
     * @return the map as a builder
     */
    @CheckReturnValue
    default Builder convert(boolean inner, Answer answer) {
        return convert(inner, answer.map());
    }

    /**
     * Default conversion behaviour if none of the more specific methods can be used
     * @param inner whether this object is within a collection
     * @param object the object to convert into a builder
     * @return the object as a builder
     */
    @CheckReturnValue
    Builder convertDefault(boolean inner, Object object);
}
