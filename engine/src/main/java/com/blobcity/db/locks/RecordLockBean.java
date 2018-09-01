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
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class RecordLockBean {

//    @Autowired
//    private RecordLockStore recordLockStore;

    public void acquireReadLock(String app, String table, String pk) {
        //RecordLockStore.get(app, table, pk).acquireUninterruptibly();
    }

    public void acquireWriteLock(String app, String table, String pk) {
        //RecordLockStore.get(app, table, pk).acquireUninterruptibly(RecordLockStore.READ_CONCURRENCY);
    }

    public void releaseReadLock(String app, String table, String pk) throws OperationException {
        //RecordLockStore.get(app, table, pk).release();
    }

    public void releaseWriteLock(String app, String table, String pk) throws OperationException {
        //RecordLockStore.get(app, table, pk).release(RecordLockStore.READ_CONCURRENCY);
    }
}
