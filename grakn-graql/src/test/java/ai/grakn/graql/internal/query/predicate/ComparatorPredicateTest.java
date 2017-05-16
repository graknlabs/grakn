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

package ai.grakn.graql.internal.query.predicate;

import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import java.time.LocalDate;

/**
 * @author Felix Chapman
 */
public class ComparatorPredicateTest {

    @Test
    public void whenPassedAString_DontThrow() {
        new MyComparatorPredicate("hello");
    }

    @Test
    public void whenPassedAnInt_DontThrow() {
        new MyComparatorPredicate(1);
    }

    @Test
    public void whenPassedALong_DontThrow() {
        new MyComparatorPredicate(1L);
    }

    @Test
    public void whenPassedADouble_DontThrow() {
        new MyComparatorPredicate(1d);
    }

    @Test
    public void whenPassedABoolean_DontThrow() {
        new MyComparatorPredicate(false);
    }

    @Test
    public void whenPassedALocalDateTime_DontThrow() {
        new MyComparatorPredicate(LocalDate.of(2012, 12, 21).atStartOfDay());
    }

    @Test(expected = NullPointerException.class)
    public void whenPassedANull_Throw() {
        new MyComparatorPredicate(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenPassedAnUnsupportedType_Throw() {
        new MyComparatorPredicate(ImmutableList.of(1, 2, 3));
    }
}

class MyComparatorPredicate extends ComparatorPredicate {

    MyComparatorPredicate(Object value) {
        super(value);
    }

    @Override
    protected String getSymbol() {
        return "@";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.eq(value);
    }
}