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

package com.blobcity.db.lang.datatypes.converters;

import com.blobcity.db.exceptions.OperationException;

/**
 * Generic interface to define any String to database supported data type converter. This is necessary because everything inside the database is expressed as a
 * String.
 *
 * @author sanketsarang
 * @param <T>
 */
public interface TypeConverter<T> {

    /**
     * Implementation must convert the input string to the target type
     *
     * @param input The input in string format
     * @return the converted value from String to the specified target type <code>T</code>
     * @throws OperationException if the type conversation fails. Exception thrown with error code <code>ErrorCode.DATATYPE_MISMATCH</code>
     */
    public T getValue(String input) throws OperationException;

    public Class getType();
}
