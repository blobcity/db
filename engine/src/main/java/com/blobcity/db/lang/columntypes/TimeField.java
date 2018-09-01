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
import java.sql.Time;
import org.json.JSONObject;

/**
 * <p>
 * Represents a field of a <code>Timestamp</code> type. The field can be initialized with time zone or without time zone
 * processing of values option. The field may have an optional default value associated. The supported format for time
 * is hh:mm:ss. This class internally implements {@link java.sql.Time}.
 *
 * <p>
 * A value of TIMESTAMP WITHOUT TIME ZONE represents a local time, whereas a value of TIMESTAMP WITH TIME ZONE
 * represents UTC.
 *
 * @author sanketsarang
 * @see java.sql.Time
 */
public class TimeField implements FieldType<Time> {

    private final Types type;
    private final boolean withTimeZone;
    private final Time defaultValue;
    private final boolean hasDefaultValue;

    /**
     * Constructs an instance of a time field, by using default initialization configuration.
     */
    public TimeField() {
        this.type = Types.TIME;
        this.withTimeZone = false;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIME</code> of {@link Types}, and the specified default value
     *
     * @param defaultValue the default value to be used for the field
     */
    public TimeField(final Time defaultValue) {
        this.type = Types.TIME;
        this.withTimeZone = false;
        this.defaultValue = defaultValue;
        this.hasDefaultValue = true;
    }

    /**
     * Initializes object with data type of <code>TIME</code> of {@link Types}, along with time zone setting
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     */
    public TimeField(final boolean withTimeZone) {
        this.type = Types.TIME;
        this.withTimeZone = withTimeZone;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIME</code> of {@link Types}, along with time zone setting and a
     * default value associated with the field
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     * @param defaultValue the default value to be used for the field
     */
    public TimeField(final boolean withTimeZone, final Time defaultValue) {
        this.type = Types.TIME;
        this.withTimeZone = withTimeZone;
        this.defaultValue = defaultValue;
        this.hasDefaultValue = true;
    }

    /**
     * Gets the {@link Types} associated with this field
     *
     * @return the {@link Types} associated with the field, always <code>TIME</code>
     */
    @Override
    public Types getType() {
        return type;
    }

    /**
     * Indicates if the field is configured to operation with or without time zone.
     *
     * @return <code>true</code> if configured <code>WITH TIME ZONE</code>; <code>false</code> otherwise
     */
    public boolean isWithTimeZone() {
        return withTimeZone;
    }

    /**
     * Getter for the default value specified for the field
     *
     * @return the default value specified for the field; <code>null</code> by default
     */
    @Override
    public Time getDefaultValue() {
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
     * Converts the given {@link Object} value to a valid {@link java.sql.Time} value, subject to the conversion being
     * possible.
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link String} type, the function will use the
     * <code>Time.valueOf(string)</code> function to create the <code>Time</code> object and thus supports all formats
     * supported by the function, with support format at date of writing of this document being hh:mm:ss
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link Long} type the function will use overloaded constructor of the
     * {@link java.sql.Time} class to create the <code>Time</code> from the long value passed.
     *
     * <p>
     * If the <code>objValue</code> is already of {@link java.sql.Time} type the function will return it as is without
     * modification
     *
     * @param objValue the value to be converted into {@link java.sql.Time}. Must be one of {@link String}, {@link Long}
     * or {@link java.sql.Time} type
     * @return an object of {@link java.sql.Time} if the <code>objValue</code> can be successful converted to a
     * {@link java.sql.Time}
     * @throws OperationException if the timestamp format is incorrect when passed parameter is of {@link String} type,
     * with error code <code>INVALID_TIME_FORMAT</code>; or if the parameter <code>objValue</code> could not be
     * converted to a time because it was of an unsupported type for successful conversion in which case exception is
     * thrown with error code <code>DATATYPE_MISMATCH</code>
     */
    @Override
    public Time convert(Object objValue) throws OperationException {
        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }
        
        if (objValue instanceof String) {
            try {
                return Time.valueOf(objValue.toString());
            } catch (IllegalArgumentException ex) {
                throw new OperationException(ErrorCode.INVALID_TIME_FORMAT, "The string " + objValue.toString()
                        + " could not be recongized as a valid time format. Supported format is hh:mm:ss");
            }
        }

        if (objValue instanceof Long) {
            return new Time((Long) objValue);
        }

        if (objValue instanceof Time) {
            return (Time) objValue;
        }

        throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Could not convert " + objValue + " to a valid time type");
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>with-zone</code>: always present
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link TimeField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        jsonObject.put("with-zone", isWithTimeZone());
        if (hasDefaultValue()) {
            jsonObject.put("default", getDefaultValue());
        }
        return jsonObject;
    }
}
