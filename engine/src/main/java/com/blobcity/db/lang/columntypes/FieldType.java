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

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Types;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 * @param <T> data type of content stored by the target field. This will also be the data type of the default value
 * stored by the field
 */
public interface FieldType<T> {

    /**
     * Gets the {@link Types} of the field which is indicative of the type of data stored in the field
     *
     * @return an {@link Types} with represents the type of data stored in the field
     */
    public Types getType();

    /**
     * Gets the default value if any associated with the corresponding field which should be used in case a value is not
     * specified for the respective field.
     *
     * @return the value of type <code>T</code> which is to be used as a default value. Default type of <code>T</code>
     * is {@link Object}. <code>null</code> if no default value is specified or if <code>null</code> itself is the
     * default value. To check if the default value set is <code>null</code> or if a default value is not set at all,
     * the <code>hasDefaultValue()</code> function should be used.
     */
    public T getDefaultValue();

    /**
     * Returns a boolean indicating whether the field has a default value associated with it.
     *
     * @return <code>true<code> if default value is set on the field, <code>false</code> otherwise
     */
    public boolean hasDefaultValue();

    /**
     * Converts an object value into the desired type. If conversion is not possible due to data type definition
     * violation the function will throw an exception. Examples where an exception can be expected is passing an invalid
     * date string for conversion into a date, or passing a decimal number with precision greater than the specified
     * precision for the respective field.
     *
     * @param objValue the value to be converted to <code>T</code>
     * @return a value of type <code>T</code> post successful conversion of <code>objValue</code> into it.
     * @throws OperationException if the value cannot be converted to the desired type
     */
    public T convert(final Object objValue) throws OperationException;

    /**
     * Represents the {@link FieldType} in JSON form, with the JSON containing all properties of the respective
     * {@link FieldType}
     *
     * @return instance of {@link JSONObject} representing the {@link FieldType} in JSON
     */
    public JSONObject toJson();
}
