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

/**
 * Represents possible status values for any long running operatios
 *
 * @author sanketsarang
 */
public enum OperationStatus {

    NOT_STARTED("not-started"),
    RUNNING("running"),
    PAUSED("paused"),
    STOPPED("stopped"),
    ERROR("error"),
    COMPLETED("completed");
    private String statusCode;

    OperationStatus(final String statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public static OperationStatus fromStatusCode(final String statusCode) {
        for (OperationStatus value : values()) {
            if (value.getStatusCode().equals(statusCode)) {
                return value;
            }
        }
        
        throw new IllegalArgumentException("Unrecognized OperationStatus code: " + statusCode);
    }
}
