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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Used to convert values stored as string in the database to Long
 *
 * @author sanketsarang
 */
@Qualifier("LongType")
public class LongConverter implements TypeConverter<Long> {

    /**
     * Converts the input string to long
     *
     * @param input The Long value in String format
     * @return The Long value corresponding to the input String
     * @throws OperationException if the input String value is now a valid Long and fails parsing .Error reported with
     * code <code>ErrorCode.DATATYPE_MISMATCH</code>
     */
    @Override
    public Long getValue(String input) throws OperationException {
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException ex) {
                        throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Could not parse '" + input + "' to a long");

        }
    }

    @Override
    public Class getType() {
        return Long.class;
    }
}
