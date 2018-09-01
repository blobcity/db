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

package com.blobcity.db.cache;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author sanketsarang
 */
@Component
public class QueryResultCache {

    /* Maps SQL query to the result string */
    private final LinkedHashMap<String, String> map = createLruMap();

    /* Maintains a map of which tables correspond to which SQL queries that are cached. This is used for invalidating
    the cache when data in the corresponding tables changes.
     */
    private final Map<String, Set<String>> tableToSqlMap = new HashMap<>();

    public void cache(final String ds, final String collection, final String sqlQuery, final String result) {
        final String dsCollection = ds + "." + collection;
        if(!tableToSqlMap.containsKey(dsCollection)) {
            tableToSqlMap.put(dsCollection, new HashSet<>());
        }

        tableToSqlMap.get(dsCollection).add(sqlQuery);
        map.put(sqlQuery, result);
    }

    public void invalidate(final String ds, final String collection) {
        final String dsCollection = ds + "." + collection;
        if(!tableToSqlMap.containsKey(dsCollection)) {
            return;
        }

        tableToSqlMap.get(dsCollection).forEach(sql -> map.remove(sql));
    }

    public String get(final String sql) {
        return map.get(sql);
    }

    private LinkedHashMap<String, String> createLruMap() {
        return new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 0.98;
            }
        };
    }
}
