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
 * Represents status of a query request
 *
 * @author sanketsarang
 */
public enum RequestStatus {
    /**
     * The requested is currently executing
     */
    IN_PROGRESS, 
    
    /**
     * The request completed successfully. This state is cached for few minutes for other nodes to look up the request 
     * should they have an execution delay. 
     */
    COMPLETED_SUCCESS, 
    
    /**
     * The request completed with error. This state is cached for few minutes for other nodes to look up the request 
     * should they have an execution delay.
     */
    COMPLETED_ERROR
}
