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

package com.blobcity.lib.requests;

/**
 * Stores the connection credentials for a request
 * 
 * @author sanketsarang
 */
@Deprecated //needs to be fully removed and replaced with Query
public class RequestData {
    private String requestId;
    private String masterNodeId;
    private String ds;
    private String user;
    private RequestStatus status;

    public RequestData requestId(final String requestId) {
        this.requestId = requestId;
        return this;
    }

    public RequestData masterNodeId(final String masterNodeId) {
        this.masterNodeId = masterNodeId;
        return this;
    }

    public RequestData ds(final String ds) {
        this.ds = ds;
        return this;
    }

    public RequestData user(final String user) {
        this.user = user;
        return this;
    }

    public RequestData status(final RequestStatus status) {
        this.status = status;
        return this;
    }

    public String getRequestId() {
        return requestId;
    }
    
    public String getDs() {
        return ds;
    }

    public String getUser() {
        return user;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public void setStatus(RequestStatus status) {
        this.status = status;
    }
}
