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
import com.blobcity.db.util.PreciseNumber;
import java.math.BigDecimal;
import java.sql.Timestamp;
import org.json.JSONObject;

/**
 * <p>
 * Represents a field of a <code>Timestamp</code> type. The field can be initialized with time zone or without time zone
 * processing of values option. The field may have an optional default value associated. The supported format for
 * timestamp is yyyy-[m]m-[d]d hh:mm:ss[.f...], with leading zeros of months and days being optional and the fractional
 * part ahead of seconds being optional. The fractional part can be precision configured using the precision parameter
 * in the constructor. If no precision is specified then a default precision of 6 is used. This class internally
 * implements {@link java.sql.Timestamp}.
 *
 * <p>
 * A value of TIMESTAMP WITHOUT TIME ZONE represents a local time, whereas a value of TIMESTAMP WITH TIME ZONE
 * represents UTC.
 *
 * @author sanketsarang
 * @see java.sql.Timestamp
 */
public class TimestampField implements FieldType<Timestamp> {

    private final Types type;
    private final boolean withTimeZone;
    private final int precision; //default value of 6
    private final Timestamp defaultValue;
    private final boolean hasDefaultValue;

    /**
     * Constructs an instance of a timestamp field, by using default initialization configuration.
     */
    public TimestampField() {
        this.type = Types.TIMESTAMP;
        this.precision = 6;
        this.withTimeZone = false;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, and the specified default value
     *
     * @param defaultValue the default value to be used for the field
     * @throws OperationException never thrown since input is of type {@link Timestamp}. This is re-thrown from
     * <code>convert(objValue)</code> function
     */
    public TimestampField(final Timestamp defaultValue) throws OperationException {
        this.type = Types.TIMESTAMP;
        this.precision = 6;
        this.withTimeZone = false;
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, along with time zone setting
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     */
    public TimestampField(final boolean withTimeZone) {
        this.type = Types.TIMESTAMP;
        this.precision = 6;
        this.withTimeZone = withTimeZone;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, along with specified precision for
     * the fractional part of seconds component
     *
     * @param precision a {@link int} value specifying the precision of the fractional part of seconds
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_ERROR</code> if the precision value is
     * out of bounds
     */
    public TimestampField(final int precision) throws OperationException {
        this.type = Types.TIMESTAMP;
        if (precision < 0 || precision > 9) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Precision value for timestamp field out of bounds. Allowed values are [0 - 9]");
        }
        this.precision = precision;
        this.withTimeZone = false;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, along with time zone setting and a
     * default value associated with the field
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     * @param defaultValue the default value to be used for the field
     * @throws OperationException never thrown since input is of type {@link Timestamp}. This is re-thrown from
     * <code>convert(objValue)</code> function
     */
    public TimestampField(final boolean withTimeZone, final Timestamp defaultValue) throws OperationException {
        this.type = Types.TIMESTAMP;
        this.precision = 6;
        this.withTimeZone = withTimeZone;
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, along with time zone setting and
     * with specified precision for the fractional part of seconds component, and a default value associated with the
     * field
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     * @param precision a {@link int} value specifying the precision of the fractional part of seconds
     */
    public TimestampField(final boolean withTimeZone, final int precision) throws OperationException {
        this.type = Types.TIMESTAMP;
        if (precision < 0 || precision > 9) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Precision value for timestamp field out of bounds. Allowed values are [0 - 9]");
        }
        this.precision = precision;
        this.withTimeZone = withTimeZone;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Initializes object with data type of <code>TIMESTAMP</code> of {@link Types}, along with specified precision for
     * the fractional part of seconds component, and a default value associated with the field
     *
     * @param withTimeZone one of <code>true</code> or <code>false</code> mapping to <code>WITH TIMEZONE</code> and
     * <code>WITHOUT TIMEZONE</code> setting respectively
     * @param precision a {@link int} value specifying the precision of the fractional part of seconds
     * @param defaultValue the default value to be used for the field
     * @throws OperationException never thrown since input is of type {@link Timestamp}. This is re-thrown from
     * <code>convert(objValue)</code> function
     */
    public TimestampField(final boolean withTimeZone, final int precision, final Timestamp defaultValue) throws OperationException {
        this.type = Types.TIMESTAMP;
        if (precision < 0 || precision > 9) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Precision value for timestamp field out of bounds. Allowed values are [0 - 9]");
        }
        this.precision = precision;
        this.withTimeZone = withTimeZone;
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Gets the {@link Types} associated with this field
     *
     * @return the {@link Types} associated with the field, always <code>TIMESTAMP</code>
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
     * Gets the precision value configuration for the factional part of the seconds component
     *
     * @return the set precision value; 6 by default
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Getter for the default value specified for the field
     *
     * @return the default value specified for the field; <code>null</code> by default
     */
    @Override
    public Timestamp getDefaultValue() {
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
     * Converts the given {@link Object} value to a valid {@link java.sql.Timestamp} value, subject to the conversion
     * being possible.
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link String} type, the function will use the
     * <code>Timestamp.valueOf(string)</code> function to create the <code>Timestamp</code> object and thus supports all
     * formats supported by the function, with support format at date of writing of this document being yyyy-[m]m-[d]d
     * hh:mm:ss[.f...], with leading zeros of months and days being optional and the fractional part ahead of seconds
     * being optional. The value returned will satisfy the precision specification and may result in rounding of the
     * fractional seconds component.
     *
     * <p>
     * If the <code>objValue</code> passed is of {@link Long} type the function will use overloaded constructor of the
     * {@link java.sql.Timestamp} class to create the <code>Timestamp</code> from the timestamp passed. The value
     * returned would satisfy the precision specification.
     *
     * <p>
     * If the <code>objValue</code> is already of {@link java.sql.Timestamp} type the function will return it post
     * checking precision constraint
     *
     * @param objValue the value to be converted into {@link java.sql.Timestamp}. Must be one of
     * {@link String}, {@link Long} or {@link java.sql.Timestamp} type
     * @return an object of {@link java.sql.Timestamp} if the <code>objValue</code> can be successful converted to a
     * {@link java.sql.Timestamp}
     * @throws OperationException if the timestamp format is incorrect when passed parameter is of {@link String} type,
     * with error code <code>INVALID_TIMESTAMP_FORMAT</code>; or if the parameter <code>objValue</code> could not be
     * converted to a timestamp because it was of an unsupported type for successful conversion in which case exception
     * is thrown with error code <code>DATATYPE_MISMATCH</code>
     */
    @Override
    public final Timestamp convert(Object objValue) throws OperationException {
        Timestamp timestamp;

        /* Conversion from String to Timestamp */
        try {
            if (objValue instanceof String) {
                timestamp = Timestamp.valueOf(objValue.toString());
                timestamp.setNanos(applyPrecisionRounding(timestamp.getNanos()));
                return timestamp;
            }
        } catch (IllegalArgumentException ex) {
            throw new OperationException(ErrorCode.INVALID_TIMESTAMP_FORMAT, "The value " + objValue.toString()
                    + " could not parsed to a valid timestamp format. Supported format yyyy-[m]m-[d]d hh:mm:ss[.f...]");
        }

        /* Converstion from Long to Timestamp */
        if (objValue instanceof Long) {
            timestamp = new Timestamp((Long) objValue);
            timestamp.setNanos(applyPrecisionRounding(timestamp.getNanos()));
            return timestamp;
        }

        if (objValue instanceof Timestamp) {
            timestamp = (Timestamp) objValue;
            timestamp.setNanos(applyPrecisionRounding(timestamp.getNanos()));
            return timestamp;
        }

        throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Could not convert " + objValue + " to a valid time type");
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>with-zone</code>: always present
     * <li><code>precision</code>: always present
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link TimestampField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        jsonObject.put("precision", getPrecision());
        jsonObject.put("with-zone", isWithTimeZone());
        if (hasDefaultValue()) {
            jsonObject.put("default", getDefaultValue());
        }
        return jsonObject;
    }

    /**
     * Rounds the specified integer value to the precision setting mentioned in the class. Used specifically to round
     * the factional part of seconds to the required precision specification
     *
     * @param nanos the fractional part of seconds to be rounded to the set precision
     * @return the value after applying rounding. The value maybe unchanged if the passed value was falling under the
     * precision specification and did not require rounding
     * @throws OperationException if {@link PreciseNumber} cannot be initialized due to an unexpected error
     */
    private int applyPrecisionRounding(int nanos) throws OperationException {
        try {
            PreciseNumber preciseNumber = new PreciseNumber(new BigDecimal(nanos), 9, precision - 9);
            return preciseNumber.get().intValue();
        } catch (OperationException ex) {
            return 0; //roll-over for maximum permitted, so setting to zero. Eg: Rounding of 9999 by scale -1 for precision 4.
        }
    }
}
