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

package com.blobcity.db.indexcache;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author sanketsarang
 */
@Component
public class OnDiskBtreeIndexCache {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(OnDiskBtreeIndexCache.class.getName());

    /* ds.collection -> column name -> column value -> pk set */
    private final Map<String, Map<String, Map<String, Set<String>>>> map = new ConcurrentHashMap<>();

    public void cache(final String ds, final String collection, final String column, final String columnValue, final Set<String> pkSet) {
        logger.trace("On Disk BTree Index caching: ds={}, collection={}, column={}, columnValue={}", ds, collection, column, columnValue);
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            map.put(dsCollection, new ConcurrentHashMap<>());
        }

        if(!map.get(dsCollection).containsKey(column)) {
            map.get(dsCollection).put(column, new ConcurrentHashMap<>());
        }

        map.get(dsCollection).get(column).put(columnValue, pkSet);
    }

    public Set<String> get(final String ds, final String collection, final String column, final String columnValue) {
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            return null;
        }

        if(!map.get(dsCollection).containsKey(column)) {
            return null;
        }

        logger.trace("On Disk BTree Index cache found: ds={}, collection={}, column={}, columnValue={}", ds, collection, column, columnValue);
        return map.get(dsCollection).get(column).get(columnValue);
    }

    /**
     * USE WITH CARE. THE FULL INDEX CACHE MUST BE LOADED BEFORE THIS OPERATION IS PERFORMED, ELSE THE CACHED INDEX
     * MAYBE INCONSISTENT WITH THE DISK INDEX
     * @param ds name of datastore
     * @param collection name of collection
     * @param column name of column
     * @param columnValue value of column
     * @param pk auto-defined _id of the record
     */
    public void addEntry(final String ds, final String collection, final String column, final String columnValue, final String pk) {
        Set<String> pkSet = get(ds, collection, column, columnValue);

        if(pkSet != null) {
            pkSet.add(pk);
        }
    }

    public Optional<Boolean> contains(final String ds, final String collection, final String column, final String columnValue, final String pk) {
        Set<String> pkSet = get(ds, collection, column, columnValue);

        if(pkSet == null) {
            return Optional.empty();
        }

        if(pkSet.contains(pk)) {
            return Optional.of(true);
        } else {
            return Optional.of(false);
        }
    }

    public void removeEntry(final String ds, final String collection, final String column, final String columnValue, final String pk) {
        Set<String> pkSet = get(ds, collection, column, columnValue);
        if(pkSet != null) {
            pkSet.remove(pk);
        }
    }

    public Optional<Set<String>> cardinality(final String ds, final String collection, final String column) {
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            return Optional.empty();
        }

        if(!map.get(dsCollection).containsKey(column)) {
            return Optional.empty();
        }

        return Optional.of(map.get(dsCollection).get(column).keySet());
    }

    public Optional<Set<String>> inQuery(final String ds, final String collection, final String column, final Set<String> values) {
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            return Optional.empty();
        }

        if(!map.get(dsCollection).containsKey(column)) {
            return Optional.empty();
        }

        /* Ensure that all values within the in-query are cached  */
        final Map<String, Set<String>> columnMap = map.get(dsCollection).get(column);
        if(values.stream().filter(value -> !columnMap.containsKey(value)).count() != 0) {
            return Optional.empty();
        }

        final Set<String> pkSet = new HashSet<>();
        values.forEach(value -> pkSet.addAll(columnMap.get(value)));

        return Optional.of(pkSet);
    }

    public void invalidate(final String ds, final String collection, final String column, final String columnValue) {
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            return;
        }

        if(!map.get(dsCollection).containsKey(column)) {
            return;
        }

        map.get(dsCollection).get(column).remove(columnValue);
    }

    public void invalidate(final String ds, final String collection, final String column) {
        final String dsCollection = ds + "." + collection;
        if(!map.containsKey(dsCollection)) {
            return;
        }

        map.get(dsCollection).remove(column);
    }

    public void invalidate(final String ds, final String collection) {
        final String dsCollection = ds + "." + collection;
        map.remove(dsCollection);
    }

    public void invalidate() {
        map.clear();
    }
}
