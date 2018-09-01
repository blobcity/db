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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * <p>
 * Used to represent field that stores collection data. The supported {@link Types} by this class are <code>ARRAY</code>
 * and <code>MULTISET</code>.
 *
 * <p>
 * The <code>ARRAY</code> is implemented as a {@link ArrayList} and the <code>MULTISET</code> is implemented as an
 * {@link HashSet}, with each following all the rules of their Java counter parts.
 *
 * @author sanketsarang
 */
public class CollectionField implements FieldType {

    private final Types type;
    /**
     * The subType property represents the type of the collection. At this moment collections are only allowed to be of
     * primitive types and not user defined types, hence the sub type of a collection has to be of {@link Types} and not
     * {@link FieldType} although in future the {@link FieldType} would be supported.
     */
    private final Types subType;

    /**
     * Constructs an instance with a specified valid collection type with sub-type specification for collection items
     *
     * @param type the {@link Types} of the field
     * @param subType the {@link Types} for the items inside the collection
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the specified
     * <code>type</code> is not a valid collection type.
     */
    public CollectionField(final Types type, final Types subType) throws OperationException {
        enforceCollectionType(type);
        this.type = type;
        this.subType = subType;
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
     * Gets the {@link Types} of the collection items
     *
     * @return the {@link Types} associated with the collection items
     */
    public Types getSubType() {
        return subType;
    }

    /**
     * Gets the default value assigned to the field which is to be used in case the executing program does not specify a
     * value for the field. This data type does not support default values so the response is always <code>null</code>
     *
     * @return always <code>null</code>
     */
    @Override
    public Object getDefaultValue() {
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
     * Converts the given {@link Object} value to a valid {@link Collection} subject to the conversion being possible.
     * The input parameter can be of type {@link JSONArray} or {@link Collection}
     *
     * @param objValue the value to be converted into {@link Collection}. Must be one of {@link JSONArray} or
     * {@link Collection}
     * @return an object of {@link Collection} if the <code>objValue</code> can be successful converted to a
     * {@link Collection}
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code> if the passed
     * <code>objValue</code> cannot be converted to a valid {@link Collection}, or if <code>objValue</code> is
     * <code>null</code>
     */
    @Override
    public Object convert(Object objValue) throws OperationException {
        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }

        if (objValue instanceof JSONArray) {
            if (type == Types.ARRAY) {
                return listFromJsonArray((JSONArray) objValue);
            } else if (type == Types.MULTISET) {
                return setFromJsonArray((JSONArray) objValue);
            } else {
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Data type " + type.name() + " is not a valid collection type");
            }
        } else if (objValue instanceof Collection) {
            Collection collection = (Collection) objValue;
            if (type == Types.ARRAY) {

                /* Short circuit to prevent recreation of list when 'if' condition is satisfied */
                if (objValue instanceof List) {
                    return objValue;
                }

                return new ArrayList<>(collection);
            } else if (type == Types.MULTISET) {

                /* Short circuit to prevent recreation of list when 'if' condition is satisfied */
                if (objValue instanceof Set) {
                    return objValue;
                }

                return new HashSet<>(collection);
            } else {
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, "Data type " + type.name() + " is not a valid collection type");
            }
        } else {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Cannot cast " + objValue + " to a valid collection type");
        }
    }

    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>sub-type</code>: always present
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link CollectionField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        jsonObject.put("sub-type", getSubType().name());
        return jsonObject;
    }

    /**
     * Converts a {@link JSONArray} to a {@link List} of {@link Object}
     *
     * @param jsonArray the {@link JSONArray} to be converted to {@link List}
     * @return a <code>List<Object></code> in converted form from the passed {@link JSONArray}
     */
    private List<Object> listFromJsonArray(JSONArray jsonArray) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            list.add(jsonArray.get(i));
        }
        return list;
    }

    /**
     * Converts a {@link JSONArray} to a {@link Set} of {@link Object}
     *
     * @param jsonArray the {@link JSONArray} to be converted to {@link Set}
     * @return a <code>Set<Object></code> in converted form from the passed {@link JSONArray}
     */
    private Set<Object> setFromJsonArray(JSONArray jsonArray) {
        Set<Object> set = new HashSet<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            set.add(jsonArray.get(i));
        }
        return set;
    }

    /**
     * Throws an {@link OperationException} if the specified {@link Types} is not a valid collection field
     *
     * @param type the {@link Types} to be checked to be a collection type
     * @throws OperationException throw with {@link ErrorCode}
     * <code>DATATYPE_MISMATCH<code> if the specified {@link Types} is not a valid collection type
     */
    private void enforceCollectionType(Types type) throws OperationException {
        switch (type) {
            case MULTISET:
            case ARRAY:
                break;
            default:
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, type.getType() + " is not a valid collection field type");
        }
    }
}
