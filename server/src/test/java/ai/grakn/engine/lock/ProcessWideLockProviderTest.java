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

package ai.grakn.engine.lock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;

public class ProcessWideLockProviderTest {

    private final String LOCK_NAME = "lock";

    @Rule
    public final SystemOutRule systemOut = new SystemOutRule().enableLog();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGivenLock_ReturnsLockWithSameClass(){
        assertThat(new ProcessWideLockProvider().getLock(LOCK_NAME).getClass(), equalTo(ReentrantLock.class));
    }

    @Test
    public void whenUsingSameString_LocksAreTheSame(){
        ProcessWideLockProvider l = new ProcessWideLockProvider();

        Lock lock1 = l.getLock(LOCK_NAME);
        Lock lock2 = l.getLock(LOCK_NAME);

        assertEquals(lock1, lock2);
    }
}
