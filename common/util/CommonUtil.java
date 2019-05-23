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

package grakn.core.common.util;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Common utility methods used within Grakn.
 *
 * Some of these methods are Grakn-specific, others add important "missing" methods to Java/Guava classes.
 *
 */
public class CommonUtil {

    private CommonUtil() {}

    /**
     * @param optional the optional to change into a stream
     * @param <T> the type in the optional
     * @return a stream of one item if the optional has an element, else an empty stream
     */
    public static <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    /**
     * Helper which lazily checks if a {@link Stream} contains the number specified
     * WARNING: This consumes the stream rendering it unusable afterwards
     *
     * @param stream the {@link Stream} to check the count against
     * @param size the expected number of elements in the stream
     * @return true if the expected size is found
     */
    public static boolean containsOnly(Stream stream, long size){
        long count = 0L;
        Iterator it = stream.iterator();

        while(it.hasNext()){
            it.next();
            if(++count > size) return false;
        }

        return size == count;
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(Throwable cause) {
        return unreachableStatement(null, cause);
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(String message) {
        return unreachableStatement(message, null);
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(@Nullable String message, Throwable cause) {
        return new RuntimeException("Statement expected to be unreachable: " + message, cause);
    }

}
