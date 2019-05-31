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

package com.blobcity.db.data;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.util.AtomicCounter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Operation;
import org.springframework.stereotype.Component;

/**
 * Caches the number of records present in a table. Also provides methods to save and retrieve such values from permanent storage.
 *
 * TODO: Fix the locking mechanism because Spring doesn't support method locks
 *
 * @author sanketsarang
 */
@Component
public class RowCountStore {

    @Autowired
    private RowCountManager rowCountManager;
    private final Map<String, AtomicCounter> map = new ConcurrentHashMap<>();

    /**
     * Gets the number of records currently present within the specified table
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @return the current row count of the specified table
     * @throws OperationException if the app/table is inexistent or an I/O error occurs while reading the count file
     */
//    @Lock(LockType.READ)
    public long getRowCount(final String app, final String table) throws OperationException {
        final String mapKey = getKey(app, table);

        map.computeIfAbsent(mapKey, k -> {
            try {
                return new AtomicCounter(getCount(app, table));
            } catch(OperationException ex) {
                return new AtomicCounter(0);
            }
        });

        return map.get(mapKey).getValue();
    }

    /**
     * Increments the row count of a table. Function must be called when a new record is inserted in the specified table
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @throws OperationException if the app/table is inexistent or if an I/O error occurs while updating the row count file
     */
//    @Lock(LockType.READ)
    public void incrementRowCount(final String app, final String table) throws OperationException {
        final String mapKey = getKey(app, table);

        map.computeIfAbsent(mapKey, k -> {
            try {
                return new AtomicCounter(getCount(app, table));
            } catch(OperationException ex) {
                return new AtomicCounter(0);
            }
        });

        AtomicCounter counter = map.get(mapKey);
        counter.getSemaphore().acquireWriteLock();
        try {
            final long newValue = counter.incrementAndGet();
            rowCountManager.writeCount(app, table, newValue);
        } finally {
            counter.getSemaphore().releaseWriteLock();
        }
    }

    /**
     * Decrements the row count of a table. Function must be called when an existing row is deleted from the specified table
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @throws OperationException if the app/table is inexistent or if an I/O error occurs while updating the row count file
     */
//    @Lock(LockType.READ)
    public void decrementRowCount(final String app, final String table) throws OperationException {
        final String mapKey = getKey(app, table);

        map.computeIfAbsent(mapKey, k -> {
            try {
                return new AtomicCounter(getCount(app, table));
            } catch(OperationException ex) {
                return new AtomicCounter(0);
            }
        });

        AtomicCounter counter = map.get(mapKey);
        counter.getSemaphore().acquireWriteLock();
        try {
            final long newValue = counter.decrementAndGet();
            rowCountManager.writeCount(app, table, newValue);
        } finally {
            counter.getSemaphore().releaseWriteLock();
        }
    }

    /**
     * Gets the key used in the <code>map</code> that is used to cache the row count values for all tables. The key is a combination of application id and table
     * name as follows in String form: <code>app + "-" + table</code>
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @return the key used for the cache map that identifies the application and table combination. Key is a String of the form: <code>app + "-" + table</code>
     */
//    @Lock(LockType.READ)
    private String getKey(final String app, final String table) {
        return app + "-" + table;
    }

    /**
     * Performs a file system I/O operation to load the current row count from the corresponding row count file
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @throws OperationException if the specified app/table combination is inexist or an I/O error occurs while reading the row count file
     */
//    @Lock(LockType.WRITE)
    private void loadCount(final String app, final String table) throws OperationException {
        final long count = rowCountManager.readCount(app, table);
        final AtomicCounter counter = new AtomicCounter(count);
        final String mapKey = getKey(app, table);
        map.put(mapKey, counter);
    }

    private long getCount(final String app, final String table) throws OperationException {
        return rowCountManager.readCount(app, table);
    }
}
