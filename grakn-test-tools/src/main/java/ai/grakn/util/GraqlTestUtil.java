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
 *
 */

package ai.grakn.util;

import ai.grakn.GraknTx;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Helper methods for writing tests for Graql
 *
 * @author Felix Chapman
 */
public class GraqlTestUtil {

    public static void assertExists(GraknTx tx, Pattern... patterns) {
        assertExists(tx.graql(), patterns);
    }

    public static void assertExists(QueryBuilder qb, Pattern... patterns) {
        assertExists(qb.match(patterns));
    }

    public static void assertExists(Iterable<?> iterable) {
        assertTrue(iterable.iterator().hasNext());
    }

    public static void assertNotExists(GraknTx tx, Pattern... patterns) {
        assertNotExists(tx.graql(), patterns);
    }

    public static void assertNotExists(QueryBuilder qb, Pattern... patterns) {
        assertNotExists(qb.match(patterns));
    }

    public static void assertNotExists(MatchQuery query) {
        assertFalse(query.iterator().hasNext());
    }
}
