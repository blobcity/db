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

package com.blobcity.db.locks;

import java.util.concurrent.Semaphore;

/**
 * <p>
 * Adds two types of locking modes for a {@link Semaphore}. While the semaphore may permit multiple read accesses it
 * will only permit a single write access at any given point in time. This allow multiple readers to the controlled data
 * but at any point in time only a single write, while ensuring that no one reads the controlled value while it is being
 * written to.
 *
 * @author sanketsarang
 */
public final class ReadWriteSemaphore {

    private final Semaphore semaphore;
    private final int readPermits;
    private LockType lockType;
    private long lastOperatedAt = System.currentTimeMillis();

    /**
     * Construct used to consider the semaphore with the permitted read concurrency or in other words with the number of
     * available permits
     *
     * @param readPermits the total number of available permits in the semaphore. This number is also equal to the
     * permitted read concurrency as every read lock requires and acquire of one permit.
     */
    public ReadWriteSemaphore(final int readPermits) {
        this.readPermits = readPermits;
        semaphore = new Semaphore(readPermits);
    }

    /**
     * <p>
     * Gets the current lock status of the semaphore. Represented by {@link LockType}, one of <code>NONE</code>,
     * <code>READ</code> and <code>WRITE</code></p>
     *
     * <p>
     * Lock type of <code>READ</code> indicates that at least one read lock is currently acquired by the semaphore. A
     * lock type of <code>WRITE</code> indicates that the semaphore is currently under write operation, further
     * indicating that there are no available permits for either reading or writing until the current write lock is
     * release. A lock type of <code>NONE</code> indicates that neither read or write lock is currently acquired. The
     * <code>NONE</code> state is marked by the availability of all permits and a read or write lock can be immediately
     * acquired on the semaphore.</p>
     *
     * @return the current {@link LockType} status of the semaphore
     */
    public LockType getLockType() {
        if (semaphore.availablePermits() == readPermits) {
            return LockType.NONE;
        }

        return lockType;
    }

    /**
     * Gets the total number of read permits allowed by this instance of the semaphore. This is not an indication of the
     * currently available permits, but returns the maximum read permits.
     *
     * @return the number of read permits
     */
    public int getReadPermits() {
        return readPermits;
    }

    /**
     * Gets the number of permits currently available on the semaphore. This figure will match the number of read
     * permits currently available. A value of zero may indicate that either all read permits are acquired or the
     * semaphore is currently in a write lock
     *
     * @return the currently available permits as reported by <code>semaphore.availablePermits()</code>
     * @see Semaphore
     */
    public int availablePermits() {
        return semaphore.availablePermits();
    }

    /**
     * <p>
     * Lock blocks the item from being read and written to by anyone other than the lock holder. There can be multiple
     * holders for read upto the permitted concurrent read limit, but only one holder for a lock. </p>
     *
     * <p>
     * If the read lock acquirer releases the acquired read lock before the termination of this function, the
     * {@link LockType} property of this class may result in having an inconsistent value.</p>
     *
     * <p>
     * This function is a blocking call until a read lock is acquired
     * </p>
     *
     */
    public void acquireReadLock() {
        lastOperatedAt = System.currentTimeMillis();
        semaphore.acquireUninterruptibly();
        lockType = LockType.READ;
    }

    /**
     * Lock blocks the item from being available for writing by anyone other than the lock holder. Others may still read
     * the current value of the item
     */
    public void acquireWriteLock() {
        lastOperatedAt = System.currentTimeMillis();
        semaphore.acquireUninterruptibly(readPermits);
        lockType = LockType.WRITE;
    }

    /**
     * Releases a single read lock. The release is performed only if the current lock type is <code>READ</code> and the
     * available permits are less that the permitted read permits. Unlike the default semaphore release method
     * invocation of this method will never result in the semaphore having more available permits than the initially set
     * maximum read permits.
     */
    public void releaseReadLock() {
        lastOperatedAt = System.currentTimeMillis();
        if (lockType == LockType.READ && semaphore.availablePermits() < readPermits) {
            semaphore.release();
        }
    }

    /**
     * Releases a write lock if one is already acquired. The function call is a no-op if the current lock type is not
     * <code>WRITE</code> and if the currently available permits are not zero.
     */
    public void releaseWriteLock() {
        lastOperatedAt = System.currentTimeMillis();
        if (lockType == LockType.WRITE && semaphore.availablePermits() == 0) {
            semaphore.release(readPermits);
        }
    }

    /**
     * Gets the time at which this Semaphore was last operated at. Only lock and release operations affect the
     * lastOperatedAt time reported by this function
     * @return the lastOperatedAt time in milli-seconds from epoch
     */
    public long getLastOperatedAt() {
        return this.lastOperatedAt;
    }
}
