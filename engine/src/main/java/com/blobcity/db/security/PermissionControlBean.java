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

package com.blobcity.db.security;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryType;
import com.blobcity.db.requests.RequestStoreBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Provides functions to validate permissions for end to end database operations
 *
 * @author sanketsarang
 */
@Service
public class PermissionControlBean {

    @Autowired
    private RequestStoreBean requestStoreBean;

    public boolean requiresDsAccess(final String requestId, final String ds, final QueryType queryType) {
        return true;
    }

    public boolean requiresCollectionAccess(final String requestId, final String ds, final String collection, final QueryType queryType) {
        return true;
    }

    public void requiresRoles(final String requestId, Roles... roles) throws OperationException {

        Query query = requestStoreBean.getRequest(requestId);
        if(query == null) {
            throw new OperationException(ErrorCode.INVALID_REQUEST_ID, "Could not find request with the specified id: " + requestId);
        }

        for(Roles role : roles){
            if(!userHasRole(query.getUser(), role)) {
                throw new OperationException(ErrorCode.NOT_AUTHORISED,"User " + query.getUser() + " not authorised for required role: " + role.name());
            }
        }
    }

    public boolean userHasRole(final String username, final Roles role) {

        //TODO: Implement this. At the moment grants all roles to all users

        return true;
    }
}
