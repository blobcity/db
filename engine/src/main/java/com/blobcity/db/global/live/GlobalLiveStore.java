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

package com.blobcity.db.global.live;

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.operations.OperationQueue;
import com.blobcity.db.operations.OperationsManager;

import java.util.*;
import java.util.concurrent.Semaphore;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
@Component
public class GlobalLiveStore {

    /* app -> opids */
    private final Map<String, Set<String>> appOppMap = new TreeMap<>();
    /* opid -> app */
    private final Map<String, String> oppAppMap = new TreeMap<>();
    @Autowired
    private GlobalLiveManager globalLiveManager;
    @Autowired
    private OperationsManager operationsManager;
    @Autowired
    private OperationQueue operationQueue;

    private final Map<String, List<Semaphore>> completeNotifyMap = new HashMap<>();

    public void init() {
        JSONObject jsonObject;
        List<String> fileNames = globalLiveManager.getAll();
        for (String fileName : fileNames) {
            try {
                jsonObject = globalLiveManager.get(fileName);
                final String opid = jsonObject.getString("opid");
                final String app = jsonObject.getString("app");
                add(app, opid);
                operationQueue.enqueue(app, opid);
            } catch (OperationException | JSONException ex) {

                //TODO: Notify admin for manual recovery of the operation file

                LoggerFactory.getLogger(GlobalLiveStore.class.getName()).error(null, ex);
            }
        }

        /* Attempt operation start for every application */
        for (String app : appOppMap.keySet()) {
            try {
                operationsManager.tryStartNext(app);
            } catch (OperationException ex) {
                LoggerFactory.getLogger(GlobalLiveStore.class.getName()).error(null, ex);
            }
        }
    }

//    @Lock(LockType.WRITE)
    public void add(final String app, final String opid) {
        if (oppAppMap.containsKey(opid)) {
            return;
        }

        if (!appOppMap.containsKey(app)) {
            appOppMap.put(app, new HashSet<>());
        }

        appOppMap.get(app).add(opid);
        oppAppMap.put(opid, app);
    }

//    @Lock(LockType.WRITE)
    public void removeOperation(final String opid) {
        if (!oppAppMap.containsKey(opid)) {
            return;
        }

        final String app = oppAppMap.get(opid);
        appOppMap.get(app).remove(opid);
        oppAppMap.remove(opid);
    }

//    @Lock(LockType.WRITE)
    public void removeApplication(final String app) {
        if (!appOppMap.containsKey(app)) {
            return;
        }

        Set<String> opids = appOppMap.get(app);
        for (String opid : opids) {
            oppAppMap.remove(opid);
        }
        appOppMap.remove(app);
    }

//    @Lock(LockType.READ)
    public boolean contains(final String opid) {
        return oppAppMap.containsKey(opid);
    }

//    @Lock(LockType.READ)
    public String getApp(final String opid) {
        if (!oppAppMap.containsKey(opid)) {
            return null;
        }

        return oppAppMap.get(opid);
    }

//    @Lock(LockType.READ)
    public Set<String> getOps(final String app) {
        if (!appOppMap.containsKey(app)) {
            return null;
        }

        return appOppMap.get(app);
    }

    public void registerNotification(final String opid, final Semaphore semaphore) {
        if(!completeNotifyMap.containsKey(opid)) {
            completeNotifyMap.put(opid, new ArrayList<Semaphore>());
        }

        completeNotifyMap.get(opid).add(semaphore);
    }

    public void notifyComplete(final String opid) {
        if(!completeNotifyMap.containsKey(opid)) {
            return;
        }

        completeNotifyMap.get(opid).forEach(semaphore -> semaphore.release());
        completeNotifyMap.remove(opid);
    }
}
