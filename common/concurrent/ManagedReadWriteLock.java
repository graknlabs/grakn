/*
 * Copyright (C) 2021 Grakn Labs
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
 *
 */

package grakn.core.common.concurrent;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@code ReentrantReadWriteLock} that wrapped in a {@code ManagedBlocker}.
 *
 * When a thread is blocked while waiting to acquire a lock, this class will
 * possibly arrange for a spare thread to be activated if necessary, to ensure
 * sufficient parallelism while the current thread is blocked. There are 2 blocking
 * methods we would like to manage for a {@code ReentrantReadWriteLock}, they are
 * {@code reentrantLock.readLock().lock()} and {@code reentrantLock.writeLock().lock()}.
 * Each of them needs to be wrapped in a {@code ManagedBlocker}, and every thread
 * needs to have one instance of each blocker. Thus, we hold each {@code ManagedBlocker}
 * in a {@code ThreadLocal} object.
 */
public class ManagedReadWriteLock {

    private final ReentrantReadWriteLock reentrantLock;
    private final ThreadLocal<ReadLocker> localReadLocker;
    private final ThreadLocal<WriteLocker> localWriteLocker;

    public ManagedReadWriteLock() {
        reentrantLock = new ReentrantReadWriteLock();
        localReadLocker = ThreadLocal.withInitial(ReadLocker::new);
        localWriteLocker = ThreadLocal.withInitial(WriteLocker::new);
    }

    public void lockRead() throws InterruptedException {
        ForkJoinPool.managedBlock(localReadLocker.get());
    }

    public void lockWrite() throws InterruptedException {
        ForkJoinPool.managedBlock(localWriteLocker.get());
    }

    public void unlockRead() {
        localReadLocker.get().unlock();
    }

    public void unlockWrite() {
        localWriteLocker.get().unlock();
    }

    private class ReadLocker implements ForkJoinPool.ManagedBlocker {

        boolean hasLock = false;

        @Override
        public boolean block() {
            if (!hasLock) reentrantLock.readLock().lock();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return hasLock || (hasLock = reentrantLock.readLock().tryLock());
        }

        void unlock() {
            reentrantLock.readLock().unlock();
            hasLock = false;
        }
    }

    private class WriteLocker implements ForkJoinPool.ManagedBlocker {

        boolean hasLock = false;

        @Override
        public boolean block() {
            if (!hasLock) reentrantLock.writeLock().lock();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return hasLock || (hasLock = reentrantLock.writeLock().tryLock());
        }

        void unlock() {
            reentrantLock.writeLock().unlock();
            hasLock = false;
        }
    }
}
