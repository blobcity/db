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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Component;

/**
 * <p>Locks store to acquire global locks on application and tables while critical mutation operation are in progress. </p>
 *
 * <p>A application lock must be acquired when tables are being added or removed from the application.</p>
 * <p>A table lock must be acquired when a table is being dropped, truncated or a schema change operation is being
 * performed on it.</p>
 *
 * @author sanketsarang
 */
@Component
public class MasterLockBean {

    private Semaphore globalSemaphore = new Semaphore(1);
    private Map<String, Semaphore> appMap = new HashMap<>();
    private Map<String, Map<String, Semaphore>> tableMap = new HashMap<>();

    public void acquireGlobalLock() throws InterruptedException {
        globalSemaphore.acquire();
    }

    public void releaseGlobalLock() {
        globalSemaphore.release();
    }

    public void acquireApplicationLock(final String app) throws InterruptedException {
        if (!appMap.containsKey(app)) {
            appMap.put(app, new Semaphore(1));
        }

        if (!tableMap.containsKey(app)) {
            tableMap.put(app, new HashMap<String, Semaphore>());
        }

        /* Acquire for all tables in the application */
        for (String table : tableMap.get(app).keySet()) {
            tableMap.get(app).get(table).acquire();
        }

        /* Acquire at application level */
        appMap.get(app).acquire();
    }

    public void releaseApplicationLock(final String app) {
        if (!appMap.containsKey(app)) {
            return;
        }

        /* Release for all tables in the application */
        if (tableMap.containsKey(app)) {
            for (String table : tableMap.get(app).keySet()) {
                tableMap.get(app).get(table).release();
            }
        }

        /* Release the database semaphore */
        appMap.get(app).release();
    }

    public void acquireTableLock(final String account, final String table) throws InterruptedException {
        if (!tableMap.containsKey(account)) {
            tableMap.put(account, new HashMap<String, Semaphore>());
        }

        if (!tableMap.get(account).containsKey(table)) {
            tableMap.get(account).put(table, new Semaphore(1));
        }

        tableMap.get(account).get(table).acquire();
    }

    public void releaseTableLock(final String account, final String table) {
        if (!tableMap.containsKey(account)) {
            return;
        }

        if (!tableMap.get(account).containsKey(table)) {
            return;
        }

        tableMap.get(account).get(table).release();
    }
}
