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
 * Used to represent fields that can take one of two binary values <code>true</code> or <code>false</code>. The field
 * has no constraint parameters but has default value that can be assigned incase of non-assignment by the query.
 *
 * <p>
 * This field should be associated with only <code>BOOLEAN</code> value of {@link Types}
 *
 * @author sanketsarang
 */
public class BooleanField implements FieldType<Boolean> {

    private final Types type;
    private Boolean defaultValue = null;

    /**
     * Initializes object with data type of <code>BOOLEAN</code> of {@link Types}, and default value of
     * <code>null</code>
     */
    public BooleanField() {
        this.type = Types.BOOLEAN;
    }

    /**
     * Initializes object with data type of <code>BOOLEAN</code> of {@link Types}, and the specified default value
     *
     * @param defaultValue the default value to be used for the field, one of <code>true</code>, <code>false</code>
     */
    public BooleanField(final Boolean defaultValue) {
        this.type = Types.BOOLEAN;
        this.defaultValue = defaultValue;
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
     * Gets the default value assigned to the field which is to be used in case the executing program does not specify a
     * value for the field.
     *
     * @return the set default value; <code>null</code> by default
     */
    @Override
    public Boolean getDefaultValue() {
        return defaultValue;
    }

    /**
     * Indicates if a default value is set of the field
     *
     * @return <code>true</code> if default value is set; <code>false</code> otherwise
     */
    @Override
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /**
     * Converts either {@link String} or {@link Boolean} value to a {@link Boolean} value. Only the strings
     * <code>"true"</code> and <code>"false"</code> are supported for conversion to {@link Boolean}
     *
     * @param objValue the value in either {@link String} or {@link Boolean} form passed as an {@link Object} type which
     * needs to be converted to type {@link Boolean}
     * @return the {@link Boolean} equivalent to the <code>objValue</code> parameter passed to the function
     * @throws OperationException reported with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the passed
     * <code>objValue</code> parameter is not of the required type for conversion to a {@link Boolean}, or is
     * <code>null</code>, or if the {@link String} value passed cannot be successfully type-casted to a {@link Boolean}
     */
    @Override
    public Boolean convert(Object objValue) throws OperationException {
        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }

        if (objValue instanceof String) {
            switch (objValue.toString().toLowerCase()) {
                case "true":
                    return true;
                case "false":
                    return false;
                default:
                    throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Cannot cast " + objValue.toString() + " to a boolean value");
            }
        } else if (objValue instanceof Boolean) {
            return (Boolean) objValue;
        }

        throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Cannot convert " + objValue + " to a valid boolean type");
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link BooleanField}
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
