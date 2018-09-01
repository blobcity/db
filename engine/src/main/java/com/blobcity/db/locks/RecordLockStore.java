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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class RecordLockStore {

    public static final int READ_CONCURRENCY = 10;
    private static final int CACHE_SIZE = 10000;

    /* Keyed on {appId}-{table}-{pk} */
    private static final Map<String, Semaphore> map = new LinkedHashMap<String, Semaphore>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > CACHE_SIZE;
        }
    };

    public static Semaphore get(String app, String table, String pk) {
        final String key = generateKey(app, table, pk);

        if (!map.containsKey(key)) {
            map.put(key, new Semaphore(READ_CONCURRENCY));
        }

        return map.get(key);
    }

    private static String generateKey(String app, String table, String pk) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(pk);
        return sb.toString();
    }
}
