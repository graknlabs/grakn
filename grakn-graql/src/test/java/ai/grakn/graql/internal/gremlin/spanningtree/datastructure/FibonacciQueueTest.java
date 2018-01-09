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

package ai.grakn.graql.internal.gremlin.spanningtree.datastructure;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.Range.closedOpen;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FibonacciQueueTest {
    @Test
    public void testIterator() {
        // insert lots of numbers in order
        final Set<Integer> values = ContiguousSet.create(closedOpen(0, 1000), integers());
        final FibonacciQueue<Integer> queue = FibonacciQueue.create();
        assertTrue(queue.addAll(values));
        assertEquals(values, ImmutableSet.copyOf(queue.iterator()));
        assertEquals(values, ImmutableSet.copyOf(queue));
    }

    @Test
    public void testLotsOfRandomInserts() {
        int lots = 50000;
        final FibonacciQueue<Integer> queue = FibonacciQueue.create();
        // Insert lots of random numbers.
        final ImmutableMultiset.Builder<Integer> insertedBuilder = ImmutableMultiset.builder();
        final Random random = new Random();
        for (int i = 0; i < lots; i++) {
            int r = random.nextInt();
            insertedBuilder.add(r);
            queue.add(r);
        }
        final Multiset<Integer> inserted = insertedBuilder.build();
        assertEquals(lots, queue.size());
        // Ensure it contains the same multiset of values that we put in
        assertEquals(inserted, ImmutableMultiset.copyOf(queue));
        // Ensure the numbers come out in increasing order.
        final List<Integer> polled = Lists.newLinkedList();
        while (!queue.isEmpty()) {
            polled.add(queue.poll());
        }
        assertTrue(Ordering.<Integer>natural().isOrdered(polled));
        // Ensure the same multiset of values came out that we put in
        assertEquals(inserted, ImmutableMultiset.copyOf(polled));
        assertEquals(0, queue.size());
    }
}
