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

import com.blobcity.db.exceptions.OperationException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */

@Component
public class TransactionLocking {

    /* Keyed on {appId}-{table}-{pk} */
    private final Map<String, ReadWriteSemaphore> map = new ConcurrentHashMap<>();

    public boolean isLocked(String app, String table, String pk) {
        return map.containsKey(generateKey(app, table, pk));
    }

    public LockType getLockType(String app, String table, String pk) {
        final String key = generateKey(app, table, pk);
        ReadWriteSemaphore rwSemaphore = map.get(key);
        return rwSemaphore == null ? LockType.NONE : rwSemaphore.getLockType();

    }

    public void acquireLock(String app, String table, String pk, LockType lockType) {
        final String key = generateKey(app, table, pk);

        switch (lockType) {
            case READ:
                map.compute(key, (k, value)-> {
                    if(value == null) {
                        return new ReadWriteSemaphore(1);
                    }
                   return value;
                });
                map.get(key).acquireReadLock();
                break;
            case WRITE:
                map.compute(key, (k, value)-> {
                    if(value == null) {
                        return new ReadWriteSemaphore(1);
                    }
                    return value;
                });
                map.get(key).acquireWriteLock();
                break;
        }
    }

    public void releaseLock(String app, String table, String pk, LockType lockType) {
        final String key = generateKey(app, table, pk);
        ReadWriteSemaphore rwSemaphore = map.get(key);
        if(rwSemaphore == null) {
            return;
        }

        switch(lockType) {
            case READ:
                rwSemaphore.releaseReadLock();
                break;
            case WRITE:
                rwSemaphore.releaseWriteLock();
                break;
        }
    }

    private String generateKey(String app, String table, String pk) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(pk);
        return sb.toString();
    }

    @Scheduled(cron = "0 * * * * *")
    private void cleanUp() {
        final long removeBefore = System.currentTimeMillis() - 30000; //30 seconds
        map.entrySet().removeIf(item -> item.getValue().getLockType() == LockType.NONE && item.getValue().getLastOperatedAt() < removeBefore);
    }
}
