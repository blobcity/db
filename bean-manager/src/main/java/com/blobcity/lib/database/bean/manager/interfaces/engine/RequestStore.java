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

package com.blobcity.lib.database.bean.manager.interfaces.engine;

import com.blobcity.lib.query.Query;
import com.blobcity.lib.requests.RequestData;

/**
 * API definition for register request data within a request store.
 * 
 * @author sanketsarang
 */
public interface RequestStore {

    /**
     * Registers a new request, by creating a new requestId and storing the Query object against the requestId
     * @param query the {@link Query} object for which the request is to be registered
     * @return the <code>requestId</code> under which the request is registered
     */
    public String registerRequest(Query query);
    
//    /**
//     * [non-clustered]
//     * Registers a new request for the specified user. Does not validate, but simply stores the specified credentials
//     * for the request. This function executes only on the invocation node.
//     * @param datastore the name of the data store
//     * @param collection the users name
//     * @param password the users password that was used for authentication
//     * @param table the name of the collection
//     * @return a newly generated unique request id of type {@link UUID}
//     */
    @Deprecated
    public String registerNewRequest(final String datastore, final String collection, final String password, final String table);
//
//    /**
//     * [non-clustered]
//     * Registers a request with the specified requestId if a request with the same id is not already registered in the
//     * local RequestStore
//     * @param requestId the requestId with which the request is to be registered
//     * @param datastore the name of the datastore
//     * @param username the name of the user
//     * @param password the users password that was used for authentication
//     * @param collection the name of the collection
//     * @return <code>true</code> if the request is registered; <code>false</code> otherwise
//     */
    @Deprecated
    public boolean registerRequest(final String requestId, final String datastore, final String username, final String password, final String collection);
            
    /**
     * [non-clustered]
     * Gets data corresponding to the specified requestId.
     * @param requestId the requestId to be looked up
     * @return an instance of {@link Query} if the requestId is existent; <code>null</code> otherwise
     */
    public Query getRequest(final String requestId);
    
    /**
     * [non-clustered]
     * Purges data corresponding to the requestId if found
     * @param requestId the requestId of which data has to be purged
     */
    public void unregisterRequest(final String requestId);
}
