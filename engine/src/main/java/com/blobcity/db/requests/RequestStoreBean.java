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

package com.blobcity.db.requests;

import com.blobcity.db.security.Credentials;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.requests.RequestData;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * Singleton storing data for all requests
 *
 * @author sanketsarang
 */
@Component
public class RequestStoreBean implements RequestStore {

    private final Map<String, Query> map = new ConcurrentHashMap<>();

    public String registerRequest(final Query query) {
        final String requestId = UUID.randomUUID().toString();
        map.put(requestId, query);
        return requestId;
    }

    @Deprecated
    public String registerNewRequest(final Credentials credentials) {
        return null;
//        final String requestId = UUID.randomUUID().toString();
//        RequestData requestData = new RequestData(requestId, "", credentials.getUsername(), "", "");
//        map.put(requestId, requestData);
//        return requestId;
    }
//
//    /**
//     * Creates a new request ID and stores the specified request credentials into the request store mapped to the new
//     * request Id
//     *
//     * @param database the database to which the connection is opened
//     * @param username the username of the connecting user
//     * @param password the password used for connecting
//     * @param table the table is specified for the request
//     * @return the new request ID under which the data is stored
//     */
    @Override
    @Deprecated // this class should not be used to create new requests
    public String registerNewRequest(final String database, final String username, final String password, final String table) {
        return null;
//        final String requestId = UUID.randomUUID().toString();
//        final RequestData requestData = new RequestData(requestId, database, username, password, table);
//        map.put(requestId, requestData);
//        return requestId;
    }

    @Deprecated
    public boolean registerRequest(final String requestId, final String datastore, final String username, final String password, final String collection) {
        return false;
    }
//
//    /**
//     * Creates a {@link RequestData} object and stores it inside the store
//     *
//     * @param requestId the request id of an already existing request
//     * @param database the database to which the connection is opened
//     * @param username the username of the connecting user
//     * @param password the password used for connecting
//     * @param table the table is specified for the request
//     * @return <code>false</code> if a request with the specified requestId is already registered, <code>true</code>
//     * otherwise
//     */
//    @Override
//    @Deprecated //should not be here.
//    public boolean registerRequest(final String requestId, final String database, final String username, final String password, final String table) {
//        if (map.containsKey(requestId)) {
//            return false;
//        }
//
//        if(map.put(requestId, new RequestData(requestId, database, username, password, table)) == null) {
//            return true;
//        }
//        return false;
//    }
//
//    /**
//     * Used to store request data mapped to a request id.
//     *
//     * @param requestData the {@link RequestData} object
//     * @return <code>true</code> if the request is registered; <code>false</code> otherwise
//     */
    @Deprecated
    public boolean registerRequest(RequestData requestData) {
        return false;
//        map.containsKey(requestData.getRequestId()) {
//            return false;
//        }
//
//        map.put(requestData.getRequestId(), requestData);
//        return true;
    }

    /**
     * Gets the {@link RequestData} for the specified requestId
     *
     * @param requestId the id of the request
     * @return the {@link Query} mapped to the requestId if present; null otherwise
     */
    @Override
    public Query getRequest(final String requestId) {
        return map.get(requestId);
    }

    /**
     * Remove the request data from the store corresponding to the specified requestId
     *
     * @param requestId the id of the request
     */
    @Override
    public void unregisterRequest(final String requestId) {
        map.remove(requestId);
    }

    @Override
    public int size() {
        return map.size();
    }
}
