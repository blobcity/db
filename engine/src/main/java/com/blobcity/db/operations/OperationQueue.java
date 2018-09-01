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

package com.blobcity.db.operations;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class OperationQueue {

    /* app -> Set of opids. The set used is a linked hashed set that maintains order of queue  */
    public Map<String, Set<String>> appMap = new HashMap<>();

//    @Lock(LockType.WRITE)
    public void enqueue(final String app, final String opid) {
        if (!appMap.containsKey(app)) {
            appMap.put(app, new LinkedHashSet<String>());
        }

        appMap.get(app).add(opid);
    }

//    @Lock(LockType.WRITE)
    public void dequeue(final String app, final String opid) {
        if (!appMap.containsKey(app)) {
            return;
        }

        appMap.get(app).remove(opid);
    }

    /**
     * The function will return the next in the queue followed by a dequeue.
     *
     * @param app The application id of the BlobCity application
     * @return the operation id of the operation belonging to the specified application that is next in order of execution. Returns null by default if next is
     * not present or if the specified application is not found
     */
//    @Lock(LockType.WRITE)
    public String next(final String app) {
        if (!appMap.containsKey(app)) {
            return null;
        }

        if (appMap.get(app).isEmpty()) {
            return null;
        }

        final String returnValue = appMap.get(app).toArray()[0].toString();
        appMap.get(app).remove(returnValue);
        return returnValue;
    }

//    @Lock(LockType.READ)
    public boolean hasNext(final String app) {
        return appMap.containsKey(app) && !appMap.get(app).isEmpty();
    }
}
