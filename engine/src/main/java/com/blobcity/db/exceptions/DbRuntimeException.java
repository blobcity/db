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

package com.blobcity.db.exceptions;

/**
 * Database Run time exceptions that cannot be caught
 *
 * @author akshaydewan
 */
public class DbRuntimeException extends RuntimeException {

    private final ErrorCode errorCode;

    public DbRuntimeException(final OperationException ex) {
        super(ex);
        this.errorCode = ex.getErrorCode();
    }

    public DbRuntimeException(final Exception ex) {
        super(ex);
        errorCode = ErrorCode.INTERNAL_OPERATION_ERROR;
    }

    public DbRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.INTERNAL_OPERATION_ERROR;
    }

    public DbRuntimeException(final String message) {
        super(message);
        this.errorCode = ErrorCode.INTERNAL_OPERATION_ERROR;
    }

    public DbRuntimeException(final ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public DbRuntimeException(final ErrorCode errorCode, final String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
