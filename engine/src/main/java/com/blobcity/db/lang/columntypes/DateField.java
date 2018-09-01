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

package com.blobcity.db.lang.columntypes;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Types;
import java.sql.Date;
import org.json.JSONObject;

/**
 * Represents a field of a <code>Date</code> type. The field may have an optional default value associated.
 *
 * @author sanketsarang
 */
public class DateField implements FieldType<Date> {

    private final Types type;
    private final Date defaultValue;
    private final boolean hasDefaultValue;

    /**
     * Creates an instance with data type set to <code>DATE</code> of {@link Types} without having a default value
     * associated.
     */
    public DateField() {
        this.type = Types.DATE;
        this.defaultValue = null;
        hasDefaultValue = false;
    }

    /**
     * Creates an instance with data type set to <code>DATE</code> of {@link Types} with having a default value
     * associated
     *
     * @param defaultValue the default value of {@link java.sql.Date} type to be associated with the field
     */
    public DateField(final Date defaultValue) {
        this.type = Types.DATE;
        this.defaultValue = defaultValue;
        hasDefaultValue = true;
    }

    /**
     * Gets the {@link Types} associated with this field
     *
     * @return the {@link Types} associated with the field, always <code>DATE</code>
     */
    @Override
    public Types getType() {
        return type;
    }

    /**
     * Getter for the default value specified for the field
     *
     * @return the default value specified for the field; <code>null</code> by default
     */
    @Override
    public Date getDefaultValue() {
        return defaultValue;
    }

    /**
     * Function to check if the field has a explicitely specified default value associated with it
     *
     * @return <code>true</code> if a default value was explicitely specified during object construction;
     * <code>false</code> otherwise
     */
    @Override
    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    /**
     * Converts the given {@link Object} value to a valid {@link java.sql.Date} value, subject to the conversion being
     * possible.
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link String} type, the function will use the
     * <code>Date.valueOf(string)</code> function to create the <code>Date</code> object and thus supports all formats
     * supported by the function, with support format at date of writing of this document being yyyy-[m]m-[d]d. The
     * leading zero for <code>mm</code> and <code>dd</code> may also be omitted.
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link Long} type the function will use overloaded constructor of the
     * {@link java.sql.Date} class to create the <code>Date</code> from the timestamp passed.
     *
     * <p>
     * If the <code>objValue</code> is already of {@link java.sql.Date} type the function will return it as is without
     * any modification
     *
     * @param objValue the value to be converted into {@link java.sql.Date}. Must be one of {@link String}, {@link Long}
     * or {@link java.sql.Date} type
     * @return an object of {@link java.sql.Date} if the <code>objValue</code> can be successful converted to a
     * {@link java.sql.Date}
     * @throws OperationException if the date format is incorrect when passed parameter is of {@link String} type, with
     * error code <code>INVALID_DATE_FORMAT</code>; or if the parameter <code>objValue</code> could not be converted to
     * a date because it was of an unsupported type for successful conversion in which case exception is thrown with
     * error code <code>DATATYPE_MISMATCH</code>
     */
    @Override
    public Date convert(Object objValue) throws OperationException {
        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }
        
        if (objValue instanceof String) {
            try {
                return Date.valueOf(objValue.toString());
            } catch (IllegalArgumentException ex) {
                throw new OperationException(ErrorCode.INVALID_DATE_FORMAT, "Could not parse " + objValue.toString()
                        + " to a valid date. Supported format is yyyy-[m]m-[d]d");
            }
        } else if (objValue instanceof Long) {
            return new Date((Long) objValue);
        } else if (objValue instanceof Date) {
            return (Date) objValue;
        } else {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Cannot cast to a valid Date field. Value: " + objValue);
        }
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link DateField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        if (hasDefaultValue()) {
            jsonObject.put("default", getDefaultValue());
        }
        return jsonObject;
    }
}
