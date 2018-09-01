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

import com.blobcity.db.schema.Types;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Defines factory method to retrieve {@link TypeConverter} for converting String to the requested {@link DataTypes}
 *
 * @author sanketsarang
 */
@Component
public class TypeConverterFactory {

    @Autowired
    @Qualifier("IntType")
    private TypeConverter<Integer> intConverter;
    @Autowired
    @Qualifier("LongType")
    private TypeConverter<Long> longConverter;
    @Autowired
    @Qualifier("FloatType")
    private TypeConverter<Float> floatConverter;
    @Autowired
    @Qualifier("DoubleType")
    private TypeConverter<Double> doubleConverter;
    @Autowired
    @Qualifier("StringType")
    private TypeConverter<String> stringConverter;

    /**
     * Factory method to get {@link TypeConverter} for the specifed type. Input {@link DataTypes} parameter should be
     * one of <code>INTEGER, LONG, FLOAT, DOUBLE</code>. For all other data types defined in {@link DataTypes} the
     * function will return a <code>null</code> by default
     *
     * @param dataType
     * @return
     */
    public TypeConverter getTypeConverter(Types dataType) {
        switch (dataType) {
            //TODO: Support all types here
            case INT:
            case INTEGER:
                return intConverter;
            case LONG:
            case BIGINT:
                return longConverter;
            case FLOAT:
                return floatConverter;
            case DOUBLE:
                return floatConverter;
            case STRING:
            case VARCHAR:
            case CHARACTER_LARGE_OBJECT:
            case CHARACTER_VARYING:
            case CHAR_LARGE_OBJECT:
            case CHAR_VARYING:
            case CLOB:
            case NATIONAL_CHARACTER_LARGE_OBJECT:
            case NATIONAL_CHARACTER_VARYING:
            case NATIONAL_CHAR_VARYING:
            case NCHAR_LARGE_OBJECT:
            case NCHAR_VARYING:
            case CHAR:
            case CHARACTER:
            case NATIONAL_CHAR:
            case NATIONAL_CHARACTER:
                return stringConverter;
            default:
                return null;
        }
    }
}
