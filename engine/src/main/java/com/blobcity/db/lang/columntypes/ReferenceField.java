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
import java.util.UUID;
import org.json.JSONObject;

/**
 * <p>
 * Represents a field storing a reference pointer to another data item within the database. The valid {@link Types} for
 * use with this class is <code>REF</code>
 *
 * The class stores an additional property of <code>scope</code> which is used for specifying / restricting the scope of
 * the reference to a specific table within the database
 *
 * @author sanketsarang
 */
public final class ReferenceField implements FieldType<String> {

    private static final String REGEX_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

    private final Types type;
    private final String scope;

    /**
     * Constructs an instance of reference type without any scope specifications
     */
    public ReferenceField() {
        this.type = Types.REF;
        this.scope = null;
    }

    /**
     * Constructs an instance of reference type along with the the specified scope property
     *
     * @param scope the scope restriction of the reference mapping. Must be a table name
     */
    public ReferenceField(final String scope) {
        this.type = Types.REF;
        this.scope = scope;
    }

    /**
     * Gets the {@link Types} associated with this field
     *
     * @return the {@link Types} associated with the field, always <code>REF</code>
     */
    @Override
    public Types getType() {
        return type;
    }

    /**
     * Getter for the scope property of the field
     *
     * @return the scope property set on the field; <code>null</code> by default if no property was set during object
     * construction
     */
    public String getScope() {
        return scope;
    }

    public boolean isScoped() {
        return scope != null;
    }

    /**
     * Getter for the default value specified for the field. <b>This field does not support a default value
     * specification</b>
     *
     * @return always <code>null</code>
     */
    @Override
    public String getDefaultValue() {
        return null;
    }

    /**
     * Getter to check if the field has an associated default value. <b>This field does not support a default value
     * specification</b>
     *
     * @return always <code>false</code>
     */
    @Override
    public boolean hasDefaultValue() {
        return false;
    }

    /**
     * <p>
     * Converts the given value to a reference mapping. The function supports only conversion from a {@link String} type
     * to the return of {@link String} type.
     *
     * <p>
     * The supported reference format is <code>[UUID]:[UUID]</code> where each <code>UUID</code> must be a valid
     * {@link UUID} of 36 characters in length. The first UUID maps to the table id while the second UUID maps to the
     * row id within the table. The two UUIDs are separated by a <code>:</code>. The function will not check for
     * existence of the table or record with the specified id, but will just validate the string format
     *
     * @param objValue The object to be converted to a {@link String} which maps to a reference if the input is of
     * {@link String} of a valid reference format
     * @return an instance of {@link String} created from <code>objValue</code> which is used as reference mapping to a
     * record within a table
     * @throws OperationException thrown with {@link ErrorCode} <code>INVALID_REF_FORMAT</code> if the input string is
     * not of a valid reference specification format; or with code <code>DATATYPE_MISMATCH</code> if the input is not of
     * [@link String} type
     */
    @Override
    public String convert(Object objValue) throws OperationException {
        String value;
        if (objValue == null || !(objValue instanceof String)) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Input must be of String type. Cannot convert " + objValue + " to a String value");
        }

        value = (String) objValue;
        if (!value.matches(REGEX_PATTERN)) {
            throw new OperationException(ErrorCode.INVALID_REF_FORMAT,
                    value + " is not of a valid REF format specification. Supported format [UUID]:[UUID]");
        }

        return value;
    }
    
    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>scope</code>: if the field has a scope constraint set
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link ReferenceField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        if (isScoped()) {
            jsonObject.put("scope", getScope());
        }
        return jsonObject;
    }
}
