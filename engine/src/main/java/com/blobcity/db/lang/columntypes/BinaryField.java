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
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * <p>
 * Represents a binary sequence of data and is used to represent <code>BINARY LARGE OBJECT</code> and <code>BLOB</code>
 * data type. An optional parameter of length indicates the maximum length of content that is allowed for storage within
 * the field. If the length specified is zero or negative, no length constraint will be applied and the field can store
 * data of virtually infinite length.
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
public class BinaryField implements FieldType<byte[]> {

    private final Types type;
    private final long length;

    /**
     * Constructs an instance with a specified valid binary type
     *
     * @param type the {@link Types} of the field
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid binary type.
     */
    public BinaryField(Types type) throws OperationException {
        enforceBinaryType(type);
        this.type = type;
        this.length = -1;
    }

    /**
     * Constructs an instance with a specified valid binary type and the specified maximum content length
     *
     * @param type the {@link Types} of the field
     * @param lengthString the maximum length specification in valid string format <code>length[{K|M|G}]</code>
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISTMATCH</code> if the specified
     * <code>type</code> is not a valid binary type; or with {@link ErrorCode} <code>INVALID_FIELD_LENGTH_FORMAT</code>
     * if the length string is not of a valid format
     */
    public BinaryField(Types type, String lengthString) throws OperationException {
        enforceBinaryType(type);
        this.type = type;

        /* Process the length string */
        try {
            switch (lengthString.charAt(lengthString.length() - 1)) {
                case 'K':
                    length = Integer.parseInt(lengthString.substring(0, lengthString.length() - 1)) * 1024;
                    break;
                case 'M':
                    length = Integer.parseInt(lengthString.substring(0, lengthString.length() - 1)) * 1024 * 1024;
                    break;
                case 'G':
                    length = Integer.parseInt(lengthString.substring(0, lengthString.length() - 1)) * 1024 * 1024 * 1024;
                    break;
                default:
                    length = Integer.parseInt(lengthString);
            }
        } catch (NumberFormatException ex) {
            throw new OperationException(ErrorCode.INVALID_FIELD_LENGTH_FORMAT);
        }
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
     * value for the field. This data type does not support default values so the response is always <code>null</code>
     *
     * @return always <code>null</code>
     */
    @Override
    public byte[] getDefaultValue() {
        return null;
    }

    /**
     * Indicates whether the field has a default value assigned. This data type does not support default values so
     * response is always <code>false</code>.
     *
     * @return always <code>false</code>
     */
    @Override
    public boolean hasDefaultValue() {
        return false;
    }

    /**
     * Converts the given object to a byte []. The function call with throw an exception if the passed object cannot be
     * converted to a byte [].
     *
     * The supported types for <code>objValue</code> are <code>byte []</code> and {@link JSONArray}
     *
     * @param objValue The value as {@link Object} that is to be converted to a <code>byte []</code>
     * @return <code>byte []</code> form of the <code>objValue</code> passed
     * @throws OperationException if conversion is not possible error is reported with code
     * <code>DATATYPE_MISMATCH</code>, or if the <code>byte []</code> exceeds the maximum permitted length if constraint
     * is applied, reported with error code <code>DATATYPE_CONSTRAINT_VIOLATION</code>
     */
    @Override
    public byte[] convert(Object objValue) throws OperationException {
        byte[] bytes;
        JSONArray jsonArray;
        
        if(objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }

        /* Check if objValue is already of byte [] type in which case only length validation is to be performed */
        if (objValue instanceof byte[]) {
            bytes = (byte[]) objValue;
            if (getLength() > 0 && bytes.length > getLength()) {
                throw new OperationException(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, "Maximum permitted length " + getLength() + ", but found data of length " + bytes.length);
            }

            return bytes;
        }

        /* Post this point objValue must only be of JSONArray type for the conversion to succeed */
        if (!(objValue instanceof JSONArray)) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Could not parse " + objValue + " to a valid byte []");
        }

        jsonArray = (JSONArray) objValue;

        /* Ensure that lenght constraint is satisfied */
        if (getLength() > 0 && jsonArray.length() > getLength()) {
            throw new OperationException(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, "Maximum permitted length " + getLength() + ", but found data of length " + jsonArray.length());
        }

        bytes = new byte[jsonArray.length()];
        final int arrayLength = jsonArray.length();
        for (int i = 0; i < arrayLength; i++) {
            try {
                bytes[i] = new Byte(jsonArray.getString(i));
            } catch (NumberFormatException ex) {
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Cannot cast " + objValue + " to a byte []. "
                        + "Failure at index " + i + " for casting " + jsonArray.getString(i) + " to a byte");
            }
        }

        return bytes;
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>length</code>: always present
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link BinaryField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        jsonObject.put("length", getLength());
        return jsonObject;
    }
    
    /**
     * Throws an {@link OperationException} if the specified {@link Types} is not a valid binary field
     *
     * @param type the {@link Types} to be checked to be a binary type
     * @throws OperationException throw with {@link ErrorCode}
     * <code>DATATYPE_MISMATCH<code> if the specified {@link Types} is not a valid binary type
     */
    private void enforceBinaryType(Types type) throws OperationException {
        switch (type) {
            case BINARY_LARGE_OBJECT:
            case BLOB:
                break;
            default:
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, type.getType() + " is not a valid binary field type");
        }
    }
}
