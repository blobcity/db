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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class TransactionLocking {

    /* Keyed on {appId}-{table}-{pk} */
    private final Map<String, ReadWriteSemaphore> map = new HashMap<>();

    public boolean isLocked(String app, String table, String pk) {
        return map.containsKey(generateKey(app, table, pk));
    }

    public LockType getLockType(String app, String table, String pk) {
        final String key = generateKey(app, table, pk);
        if (map.containsKey(key)) {
            return map.get(key).getLockType();
        }
        return LockType.NONE;
    }

//    @Lock(javax.ejb.LockType.WRITE)
    public String acquireLock(String app, String table, String pk, LockType lockType) throws OperationException, InterruptedException {
        final String key = generateKey(app, table, pk);
        ReadWriteSemaphore readWriteSemaphore;

        switch (lockType) {
            case READ:
                readWriteSemaphore = new ReadWriteSemaphore(1);
                map.put(key, readWriteSemaphore);
                readWriteSemaphore.acquireReadLock();
                return key;
            case WRITE:
                readWriteSemaphore = new ReadWriteSemaphore(1);
                map.put(key, readWriteSemaphore);
                readWriteSemaphore.acquireWriteLock();
                return key;
            default:
                throw new OperationException(ErrorCode.LOCKING_ERROR, "Lock type can only be one of READ, WRITE");
        }
    }

    public String releaseLock(String app, String table, String pk, LockType lockType) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Method releaseLock(...) inside TransactionLocking not yet implemeneted.");
    }

    private String generateKey(String app, String table, String pk) {
        StringBuilder sb = new StringBuilder(app);
        sb.append("-");
        sb.append(table);
        sb.append("-");
        sb.append(pk);
        return sb.toString();
    }
}
