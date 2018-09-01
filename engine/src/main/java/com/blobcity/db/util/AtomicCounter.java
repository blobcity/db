/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.util;

import com.blobcity.db.locks.ReadWriteSemaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements an atomic counter along with a lock that is used to allow only synchronous updates to the count value.
 * External program is responsible for maintaining synchronisation by making use of the {@link ReadWriteSemaphore}
 * provided by this object. The counting value is of <code>long</code> type.
 *
 * @author sanketsarang
 * @see ReadWriteSemaphore
 */
public final class AtomicCounter {

    private static final int DEFAULT_READ_PERMITS = 100;

    private final AtomicLong count;
    private final ReadWriteSemaphore semaphore;
    private final int readPermits;

    /**
     * Constructor used to initialize the class with just the initial count value. The number of read permits will be
     * set to the default value as set by the internal constant property <code>DEFAULT_READ_PERMITS</code>
     *
     * @param value the count value with which the class is to be initialized
     */
    public AtomicCounter(long value) {
        count = new AtomicLong(value);
        this.readPermits = DEFAULT_READ_PERMITS;
        semaphore = new ReadWriteSemaphore(readPermits);
    }

    /**
     * Constructor used to initialize the class with an initial count value along with setting the permitted read
     * concurrency. The minimum value of read permits that will be set is 1.
     *
     * @param value the count value with which the class is to be initialized
     * @param readPermits the number of read permits, or the number of threads to which concurrent access is to be
     * granted to read the current count value
     */
    public AtomicCounter(long value, int readPermits) {
        if (readPermits < 1) {
            readPermits = 1;
        }
        count = new AtomicLong(value);
        this.readPermits = readPermits;
        semaphore = new ReadWriteSemaphore(readPermits);
    }

    /**
     * The number of read permits as set during the construction of this object. Also used to retrieve the default value
     * as specified by the internal constant property <code>DEFAULT_READ_PERMITS</code>. This value is not indicative of
     * the read permits currently available, but the total number of read permits set onto the object.
     *
     * @return the concurrent read permits or concurrent read access granted by {@link ReadWriteSemaphore}
     */
    public int getReadPermits() {
        return readPermits;
    }

    /**
     * Gets the current count value. This method is thread safe as the internal counter is an {@link AtomicLong}.
     * However complete thread safety in a transactional environment is the responsibility of the external program which
     * needs to manage the {@link ReadWriteSemaphore} appropriately.
     *
     * @return the current count value
     */
    public long getValue() {
        return count.get();
    }

    /**
     * Atomically increments by one the current value.
     *
     * @return the updated value
     */
    public long incrementAndGet() {
        return count.incrementAndGet();
    }

    /**
     * Atomically decrements by one the current value.
     *
     * @return the updated value
     */
    public long decrementAndGet() {
        return count.decrementAndGet();
    }

    /**
     * Gets the internal instance of {@link ReadWriteSemaphore}
     *
     * @return internal final instance of {@link ReadWriteSemaphore}
     */
    public ReadWriteSemaphore getSemaphore() {
        return semaphore;
    }
}
