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

package com.blobcity.db.indexing;

import com.blobcity.db.util.AtomicCounter;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>
 * Keeps count of cardinal size for every indexed entry in the database for BTree and Hashed type indexes only. This s
 * useful for returning fast responses to COUNT function invocations and for faster computation of intersection sets
 * when running "AND" clause based search operations.
 *
 * <p>
 * This store performs lazy load of index size information, hence first time fetch for any index maybe a slow operation
 *
 * @author sanketsarang
 */
@Component
public class IndexCountStore {

    private static final int LRU_SIZE = 10000;

    /* AppId-table-column -> columnValue -> indexCount */
    private final Map<String, Map<String, AtomicCounter>> map = new HashMap<>();

    /**
     * Gets the size of the specified index cardinal. -1 if an entry cannot be found for the specified cardinal. The
     * function will check for the value in its local in-memory cache. If value is not found in cache it will
     * automatically attempt loading the corresponding value from the respective index count file.
     *
     * @param app the id of the BlobCity application
     * @param table name of table within the application
     * @param column name of indexed column within the specified table
     * @param columnValue the cardinal value of the index
     * @param indexingStrategy an instance of indexing strategy corresponding to the type of index, which can be used to
     * read from the index count file
     * @return current size of the index for all entries under the specified cardinal. -1 if an entry is not found for
     * the specified index.
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public long getIndexSize(String app, String table, String column, String columnValue, IndexingStrategy indexingStrategy) throws OperationException {
//        long count;
//        final String mapKey = getKey(app, table, column);
//        final AtomicCounter indexCount;
//
//        if (map.containsKey(mapKey) && map.get(mapKey).containsKey(columnValue)) {
//            indexCount = map.get(mapKey).get(columnValue);
//            try {
//                indexCount.getSemaphore().acquireReadLock();
//                count = indexCount.getValue();
//                return count;
//            } catch (InterruptedException ex) {
//                LoggerFactory.getLogger(IndexCountStore.class.getName()).error(null, ex);
//                throw new OperationException(ErrorCode.INDEX_COUNT_ERROR);
//            } finally {
//                indexCount.getSemaphore().releaseReadLock();
//            }
//        } else if (!map.containsKey(mapKey)) {
//            synchronized (map) {
//                count = indexingStrategy.readIndexCount(app, table, column, columnValue);
//                if (count == -1) {
//                    return -1;
//                }
//
//                map.put(mapKey, newLruMap());
//                indexCount = new AtomicCounter(count);
//                map.get(mapKey).put(columnValue, indexCount);
//                return count;
//            }
//        } else {
//            synchronized (map.get(mapKey)) {
//                count = indexingStrategy.readIndexCount(app, table, column, columnValue);
//                if (count == -1) {
//                    return -1;
//                }
//
//                indexCount = new AtomicCounter(count);
//                map.get(mapKey).put(columnValue, indexCount);
//                return count;
//            }
//        }
        
        return 1;
    }

    public void incrementCount(final String app, final String table, final String column, final String columnValue, final IndexingStrategy indexingStrategy) throws OperationException {
//        AtomicCounter indexCount;
//        final String mapKey = getKey(app, table, column);
//
//        /* Create entry for column if one not already present */
//        synchronized (map) {
//            if (!map.containsKey(mapKey)) {
//                map.put(mapKey, newLruMap());
//            }
//        }
//
//        /* Add entry for columnValue if one not already present */
//        synchronized (map.get(mapKey)) {
//            if (!map.get(mapKey).containsKey(columnValue)) {
//                long value = indexingStrategy.readIndexCount(app, table, column, columnValue);
//
//                /* short-circuit optimisation. Directly adding entry with incremented value */
//                if (value == -1) {
//                    value = 1;
//                }
//                indexCount = new AtomicCounter(value);
//                map.get(mapKey).put(columnValue, indexCount);
//                indexingStrategy.writeIndexCount(app, table, column, columnValue, indexCount.getValue());
//                return;
//            }
//        }
//
//        /* Increment and write new value to index count file */
//        indexCount = map.get(mapKey).get(columnValue);
//        try {
//            indexCount.getSemaphore().acquireWriteLock();
//            indexingStrategy.writeIndexCount(app, table, column, columnValue, indexCount.incrementAndGet());
//            indexCount.getSemaphore().releaseWriteLock();
//        } catch (InterruptedException ex) {
//            LoggerFactory.getLogger(IndexCountStore.class.getName()).error(null, ex);
//            throw new OperationException(ErrorCode.INDEX_COUNT_ERROR);
//        }
    }

    public void decrementCount(final String app, final String table, final String column, final String columnValue, final IndexingStrategy indexingStrategy) throws OperationException {
//        AtomicCounter indexCount;
//        final String mapKey = getKey(app, table, column);
//
//        /* Create entry for column if one not already present */
//        synchronized (map) {
//            if (!map.containsKey(mapKey)) {
//                map.put(mapKey, newLruMap());
//            }
//        }
//
//        /* Add entry for columnValue if one not already present */
//        synchronized (map.get(mapKey)) {
//            if (!map.get(mapKey).containsKey(columnValue)) {
//                long value = indexingStrategy.readIndexCount(app, table, column, columnValue);
//
//                /* If no current entry formed, then the requested decrement is erroneous*/
//                if (value == -1) {
//                    throw new OperationException(ErrorCode.INDEX_COUNT_ERROR);
//                }
//
//                indexCount = new AtomicCounter(value - 1);//initialize with decrement value for faster operation
//                map.get(mapKey).put(columnValue, indexCount);
//                indexingStrategy.writeIndexCount(app, table, column, columnValue, indexCount.getValue());
//                return;
//            }
//        }
//
//        /* Decrement and write new value to index count file */
//        indexCount = map.get(mapKey).get(columnValue);
//        try {
//            indexCount.getSemaphore().acquireWriteLock();
//
//            /* Updates new index value. Passing a value of zero or negative will result in the corresponding index 
//             count file getting deleted */
//            indexingStrategy.writeIndexCount(app, table, column, columnValue, indexCount.decrementAndGet());
//
//            /* Remove entry from map if new value is 0 */
//            if (indexCount.getValue() <= 0) {
//                map.get(mapKey).remove(columnValue);
//                if (map.get(mapKey).isEmpty()) {
//                    map.remove(mapKey);
//                }
//            }
//
//            indexCount.getSemaphore().releaseWriteLock();
//        } catch (InterruptedException ex) {
//            LoggerFactory.getLogger(IndexCountStore.class.getName()).error(null, ex);
//            throw new OperationException(ErrorCode.INDEX_COUNT_ERROR);
//        }
    }

    private String getKey(String app, String table, String column) {
        final StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(column);
        return sb.toString();
    }

    private LinkedHashMap<String, AtomicCounter> newLruMap() {
        return new LinkedHashMap<String, AtomicCounter>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, AtomicCounter> eldest) {
                return size() > LRU_SIZE;
            }
        };
    }
}
