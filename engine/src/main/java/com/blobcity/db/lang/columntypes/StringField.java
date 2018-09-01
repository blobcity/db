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
import org.json.JSONObject;

/**
 * <p>
 * Represents a field storing character sequence data. The valid {@link Types} for use with this class are:
 *
 * <code>CHAR, CHARACTER, CHARACTER_VARYING, CHAR_VARYING, VARCHAR, CHARACTER_LARGE_OBJECT, CHAR_LARGE_OBJECT, CLOB,
 * NATIONAL_CHARACTER, NATIONAL_CHAR, NCHAR, NATIONAL_CHARACTER_VARYING, NATIONAL_CHAR_VARYING, NCHAR_VARYING,
 * NATIONAL_CHARACTER_LARGE_OBJECT, NCHAR_LARGE_OBJECT, NCLOB, STRING</code>
 *
 * <p>
 * An optional parameter of length indicates the maximum length of content that is allowed for storage within the field.
 * If the length specified is zero or negative, no length constraint will be applied and the field can store data of
 * virtually infinite length.
 *
 * <p>
 * Maximum length specification needs to be given in string form. It can be a {@link Integer} value represented in
 * string form in bytes or along with a multiplier. The valid multipliers are <code>K</code>,<code>M</code> and
 * <code>G</code>. Verbally they stand for Kilo, Mega and Giga bytes respectively. Mentioned below are example length
 * values.
 * <ul>
 * <li> length 100 = 100 bytes
 * <li> length 100K = 100 * 1024 bytes
 * <li> length 100M = 100 * 1024 * 1024 bytes
 * <li> length 100G = 100 * 1024 * 1024 * 1024 bytes
 * </ul>
 *
 * @author sanketsarang
 */
public final class StringField implements FieldType<String> {

    private final Types type;
    private final long length;
    private final String defaultValue;
    private final boolean hasDefaultValue;

    /**
     * Constructs an instance with a specified valid string type
     *
     * @param type the {@link Types} of the field
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid string type.
     */
    public StringField(final Types type) throws OperationException {
        enforceStringField(type);
        this.type = type;
        this.length = -1;
        this.defaultValue = null;
        hasDefaultValue = false;
    }

    /**
     * Constructs an instance with a specified valid string type and associated default value
     *
     * @param defaultValue the default value of the field
     * @param type the {@link Types} of the field
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid string type.
     */
    public StringField(final String defaultValue, final Types type) throws OperationException {
        enforceStringField(type);
        this.type = type;
        this.length = -1;
        this.defaultValue = defaultValue; //convert() function not required here as length is not restricted
        hasDefaultValue = true;
    }

    /**
     * Constructs an instance with a specified valid string type and associated default value
     *
     * @param type the {@link Types} of the field
     * @param length the maximum length specification in valid string format <code>length[{K|M|G}]</code>
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid string type, and with {@link ErrorCode} <code>INVALID_FIELD_LENGTH_FORMAT</code>
     * if the length specification string is of an invalid format
     */
    public StringField(final Types type, final String length) throws OperationException {
        enforceStringField(type);
        this.type = type;
        this.defaultValue = null;
        this.hasDefaultValue = false;
        this.length = lengthFromString(length);
    }

    /**
     * Constructs an instance with a specified valid string type and associated default value
     *
     * @param type the {@link Types} of the field
     * @param length the maximum length specification in valid string format <code>length[{K|M|G}]</code>
     * @param defaultValue the default value of the field
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid string type.
     */
    public StringField(final Types type, final String length, final String defaultValue) throws OperationException {
        enforceStringField(type);
        this.type = type;
        this.length = lengthFromString(length);
        this.defaultValue = convert(defaultValue); //specifically used to validate length constraint
        hasDefaultValue = true;
    }

    /**
     * Gets the {@link Types} associated with the field
     *
     * @return the {@link Types} associated with the field
     */
    @Override
    public Types getType() {
        return type;
    }

    /**
     * Gets the maximum permitted length of the field
     *
     * @return the maximum permitted length in bytes
     */
    public long getLength() {
        return length;
    }

    /**
     * Gets the default value assigned to the field which is to be used in case the executing program does not specify a
     * value for the field.
     *
     * @return the default value of type {@link String} associated with the field; <code>null</code> if default is not
     * set
     */
    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Indicates whether the field has a default value assigned.
     *
     * @return <code>true</code> if default value was set during object construction; <code>false</code> otherwise
     */
    @Override
    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    /**
     * Converts the given {@link Object} value to an equivalent {@link String} value. The function uses the
     * <code>object.toString</code> method to do the conversion, so every object having a valid <code>toString</code>
     * method is a legal input to the function
     *
     * @param objValue The value as {@link Object} that is to be converted to {@link String}
     * @return {@link String} form of the <code>objValue</code> passed
     * @throws OperationException if the converted {@link String} exceeds the maximum permitted length if constraint is
     * applied, reported with error code <code>DATATYPE_CONSTRAINT_VIOLATION</code>; with code
     * <code>DATATYPE_MISMATCH</code> if the input value is <code>null</code>
     */
    @Override
    public String convert(Object objValue) throws OperationException {
        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }

        final String str = objValue.toString();
        if (length > 0 && str.length() > length) {
            throw new OperationException(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, "Maximum permitted length is "
                    + length + " but found content with length " + str.length());
        }
        return str;
    }

    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>length</code>: always present
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link StringField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        jsonObject.put("length", getLength());
        if (hasDefaultValue()) {
            jsonObject.put("default", getDefaultValue());
        }
        return jsonObject;
    }

    /**
     * Converts the maximum permitted length represented in {@link String} form to an equivalent {@link Long} value if
     * the input abides by the length specification format.
     *
     * @param length the length string in format <code>length[{K|M|G}]</code>
     * @return the length in {@link Long} from if the input abides by the length specification format
     * @throws OperationException thrown with {@link ErrorCode} <code>INVALID_FIELD_LENGTH_FORMAT</code> if the length
     * specification string does is not of a valid format and cannot be converted to a valid {@link Long} length value.
     */
    private long lengthFromString(final String length) throws OperationException {
        try {
            switch (length.charAt(length.length() - 1)) {
                case 'K':
                    return Integer.parseInt(length.substring(0, length.length() - 1)) * 1024;
                case 'M':
                    return Integer.parseInt(length.substring(0, length.length() - 1)) * 1024 * 1024;
                case 'G':
                    return Integer.parseInt(length.substring(0, length.length() - 1)) * 1024 * 1024 * 1024;
                default:
                    return Integer.parseInt(length);
            }
        } catch (NumberFormatException ex) {
            throw new OperationException(ErrorCode.INVALID_FIELD_LENGTH_FORMAT, length + " is not a valid length representation. Supported format is: length[{K|M|G}]");
        }
    }

    /**
     * Throws an {@link OperationException} if the specified {@link Types} is not a valid string field
     *
     * @param type the {@link Types} to be checked to be a string type
     * @throws OperationException throw with {@link ErrorCode}
     * <code>DATATYPE_MISMATCH<code> if the specified {@link Types} is not a valid string type
     */
    private void enforceStringField(Types type) throws OperationException {
        switch (type) {
            case CHAR:
            case CHARACTER:
            case CHARACTER_VARYING:
            case CHAR_VARYING:
            case VARCHAR:
            case CHARACTER_LARGE_OBJECT:
            case CHAR_LARGE_OBJECT:
            case CLOB:
            case NATIONAL_CHARACTER:
            case NATIONAL_CHAR:
            case NCHAR:
            case NATIONAL_CHARACTER_VARYING:
            case NATIONAL_CHAR_VARYING:
            case NCHAR_VARYING:
            case NATIONAL_CHARACTER_LARGE_OBJECT:
            case NCHAR_LARGE_OBJECT:
            case NCLOB:
            case STRING:
                break;
            default:
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, type.getType()
                        + " is not a valid character sequence / string type");
        }
    }
}
