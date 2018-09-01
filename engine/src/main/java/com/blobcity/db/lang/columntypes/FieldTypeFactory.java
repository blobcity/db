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
import com.blobcity.db.sql.util.DataTypeMapper;
import com.foundationdb.sql.types.DataTypeDescriptor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * Utility class to convert external input to internal {@link FieldType} and to express {@link FieldType} in a json
 * format
 *
 * @author sanketsarang
 */
public class FieldTypeFactory {

    /**
     * Converts a JSONObject to a valid {@link FieldType} if the input is of the correct JSON format. The supported
     * field types along with properties for every field type can be found at:
     * {@link http://docs.blobcity.com/display/DB/Data+Types}
     *
     * The JSON object must have a <code>type</code> parameter mapping to the SQL compliant field type name for any of
     * the field types supported by BlobCity. The additional parameters should be added with the respective keys as
     * mentioned in the documentation found at the above mentioned link.
     *
     * @param dataTypeJson the data type of the field expressed in JSON format
     * @return {@link FieldType} created from the requested JSON, if the JSON is of the appropriate format
     * @throws OperationException if the JSON format is incorrect.
     */
    public static FieldType fromJson(final JSONObject dataTypeJson) throws OperationException {
        FieldType dataType = null;
        Types type;
        try {
            type = Types.fromString(dataTypeJson.getString("type"));
            switch (type) {
                case CHAR:
                case CHARACTER:
                case CHARACTER_VARYING:
                case CHAR_VARYING:
                case VARCHAR:
                case NATIONAL_CHAR:
                case NATIONAL_CHARACTER:
                case NCHAR:
                case NATIONAL_CHARACTER_VARYING:
                case NATIONAL_CHAR_VARYING:
                case NCHAR_VARYING:
                    return new StringField(type, dataTypeJson.get("length").toString());
                case CHARACTER_LARGE_OBJECT:
                case CHAR_LARGE_OBJECT:
                case CLOB:
                case STRING:
                case NATIONAL_CHARACTER_LARGE_OBJECT:
                case NCHAR_LARGE_OBJECT:
                case NCLOB:
                    if (dataTypeJson.has("length")) {
                        return new StringField(type, dataTypeJson.get("length").toString());
                    } else {
                        return new StringField(type);
                    }
                case BINARY_LARGE_OBJECT:
                case BLOB:
                    if (dataTypeJson.has("length")) {
                        return new BinaryField(type, dataTypeJson.get("length").toString());
                    } else {
                        return new BinaryField(type);
                    }
                case NUMERIC:
                case DECIMAL:
                case DEC:
                    return new NumberField(type, dataTypeJson.getInt("precision"), dataTypeJson.getInt("scale"));
                case SMALLINT:
                case INTEGER:
                case INT:
                case BIGINT:
                case LONG:
                case REAL:
                case DOUBLE:
                case DOUBLE_PRECISION:
                    return new NumberField(type);
                case FLOAT:
                    return new NumberField(type, dataTypeJson.getInt("precision"));
                case BOOLEAN:
                    return new BooleanField();
                case DATE:
                    return new DateField();
                case TIME:
                    if (dataTypeJson.has("with-zone")) {
                        return new TimeField(dataTypeJson.getBoolean("with-zone"));
                    }

                    return new TimeField();
                case TIMESTAMP:
                    if (dataTypeJson.has("with-zone") && dataTypeJson.has("precision")) {
                        return new TimestampField(dataTypeJson.getBoolean("with-zone"), (short) dataTypeJson.getInt("precision"));
                    }

                    if (dataTypeJson.has("with-zone")) {
                        return new TimestampField(dataTypeJson.getBoolean("with-zone"));
                    }

                    if (dataTypeJson.has("precision")) {
                        return new TimestampField((short) dataTypeJson.getInt("precision"));
                    }

                    return new TimestampField();
                case REF:
                    if (dataTypeJson.has("scope")) {
                        return new ReferenceField(dataTypeJson.getString("scope"));
                    } else {
                        return new ReferenceField();
                    }
                case ARRAY:
                case MULTISET:
                    return new CollectionField(type, Types.fromString(dataTypeJson.getString("sub-type")));
                case INTERVAL:
                case ROW:
                case XML:
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, type.getType() + " data type not yet supported");

            }
        } catch (OperationException ex) {
            Logger.getLogger(FieldTypeFactory.class.getName()).log(Level.SEVERE, null, ex);
            throw new OperationException(ErrorCode.DATATYPE_ERROR);
        }

        return dataType;
    }

    /**
     * <p>
     * Converts a {@link DataTypeDescriptor} as provided by the Foundation library SQL parser into an internal
     * equivalent {@link FieldType}.
     *
     * <p>
     * <b>Capabilities of this method are limited to capabilities of {@link DataTypeDescriptor} and are lesser than
     * capabilities of {@link FieldType}</b>
     *
     * @param typeDescriptor an instance of {@link DataTypeDescriptor} which is to be converted to {@link FieldType}
     * @return an instance of {@link FieldType} which is equivalent to the {@link DataTypeDescriptor} properties
     * @throws OperationException if the specified data type cannot be successfully mapped to a BlobCity supported type,
     * or a validation error occurs in creating a {@link FieldType} instance.
     */
    public static FieldType fromTypeDescriptor(DataTypeDescriptor typeDescriptor) throws OperationException {
        int jdbcTypeId = typeDescriptor.getJDBCTypeId();
        switch (jdbcTypeId) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
                return new StringField(DataTypeMapper.map(typeDescriptor), "" + typeDescriptor.getMaximumWidth());
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return new BinaryField(DataTypeMapper.map(typeDescriptor), "" + typeDescriptor.getMaximumWidth());
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.BIGINT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.INTEGER:
            case java.sql.Types.REAL:
                return new NumberField(DataTypeMapper.map(typeDescriptor));
            case java.sql.Types.FLOAT:
                return new NumberField(DataTypeMapper.map(typeDescriptor), typeDescriptor.getPrecision());
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return new NumberField(DataTypeMapper.map(typeDescriptor), typeDescriptor.getPrecision(), typeDescriptor.getScale());
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                return new BooleanField();
            case java.sql.Types.DATE:
                return new DateField();
            case java.sql.Types.TIME:
                return new TimeField();
            case java.sql.Types.TIMESTAMP:
                return new TimestampField();
            case java.sql.Types.REF:
                return new ReferenceField();
            case java.sql.Types.ARRAY: //TODO figure out how to map to list
            case java.sql.Types.DATALINK:
            case java.sql.Types.DISTINCT:
            case java.sql.Types.JAVA_OBJECT:
            case java.sql.Types.NULL:
            case java.sql.Types.OTHER:
            case java.sql.Types.ROWID:
            case java.sql.Types.SQLXML:
            case java.sql.Types.STRUCT:
            default:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "The type " + typeDescriptor.getFullSQLTypeName() + " is not yet supported");
        }
    }

    public static FieldType fromString(String typeString) throws OperationException {
        FieldType dataType = null;
        String typeName;
        Types type;
        String[] constrainParts = null;
        String constrainString;
        try {
            if (typeString.contains("(")) {
                typeName = typeString.substring(0, typeString.indexOf("("));
                constrainString = typeString.substring(typeString.indexOf("(") + 1, typeString.indexOf(")"));
                constrainParts = constrainString.split(",");
            } else {
                typeName = typeString;
            }
            type = Types.fromString(typeName);

            validateStringConstraints(type, constrainParts);

            switch (type) {
                case CHAR:
                case CHARACTER:
                case CHARACTER_VARYING:
                case CHAR_VARYING:
                case VARCHAR:
                case NATIONAL_CHAR:
                case NATIONAL_CHARACTER:
                case NCHAR:
                case NATIONAL_CHARACTER_VARYING:
                case NATIONAL_CHAR_VARYING:
                case NCHAR_VARYING:
                    return new StringField(type, constrainParts[0]);
                case CHARACTER_LARGE_OBJECT:
                case CHAR_LARGE_OBJECT:
                case CLOB:
                case STRING:
                case NATIONAL_CHARACTER_LARGE_OBJECT:
                case NCHAR_LARGE_OBJECT:
                case NCLOB:
                    if (constrainParts != null && constrainParts.length > 0) {
                        return new StringField(type, constrainParts[0]);
                    } else {
                        return new StringField(type);
                    }
                case BINARY_LARGE_OBJECT:
                case BLOB:
                    if (constrainParts != null && constrainParts.length > 0) {
                        return new BinaryField(type, constrainParts[0]);
                    } else {
                        return new BinaryField(type);
                    }
                case NUMERIC:
                case DECIMAL:
                case DEC:
                    return new NumberField(type, Integer.valueOf(constrainParts[0]), (Number) Integer.valueOf(constrainParts[1]));
                case SMALLINT:
                case INTEGER:
                case INT:
                case BIGINT:
                case LONG:
                case REAL:
                case DOUBLE:
                case DOUBLE_PRECISION:
                    return new NumberField(type);
                case FLOAT:
                    if(constrainParts != null)
                        return new NumberField(type, (int)Integer.valueOf(constrainParts[0]));
                    else 
                        return new NumberField(type);
                case BOOLEAN:
                    return new BooleanField();
                case DATE:
                    return new DateField();
                case TIME:
                    if (constrainParts.length > 0) {
                        return new TimeField(constrainParts[0].toLowerCase().equals("with-zone"));
                    }

                    return new TimeField();
                case TIMESTAMP:
                    if (constrainParts.length == 2) {
                        return new TimestampField("with-zone".equals(constrainParts[0].toLowerCase()), Short.valueOf(constrainParts[1]));
                    }

                    if (constrainParts.length == 1) {
                        if (StringUtils.isNumeric(constrainParts[0])) {
                            return new TimestampField(Short.valueOf(constrainParts[0]));
                        } else {
                            return new TimestampField("with-zone".equals(constrainParts[0].toLowerCase()));
                        }
                    }

                    return new TimestampField();
                case REF:
                    if (constrainParts.length == 1) {
                        return new ReferenceField(constrainParts[0]);
                    }

                    return new ReferenceField();
                case ARRAY:
                case MULTISET:
                    return new CollectionField(type, Types.fromString(constrainParts[0]));
                case INTERVAL:
                case ROW:
                case XML:
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, type.getType() + " data type not yet supported");

            }
        } catch (OperationException ex) {
            Logger.getLogger(FieldTypeFactory.class.getName()).log(Level.SEVERE, null, ex);
            throw new OperationException(ErrorCode.DATATYPE_ERROR, ex.getMessage());
        }

        return dataType;
    }

    /**
     *
     * Validates the constrains specified along with the type for specification of fields in string based queries.
     *
     * @param type The data type
     * @param constrains The specified constrains
     * @throws OperationException if the constrains do not the match the type specified
     */
    private static void validateStringConstraints(final Types type, final String[] constrainParts) throws OperationException {
        switch (type) {
            case CHAR:
            case CHAR_VARYING:
            case CHARACTER:
            case CHARACTER_VARYING:
            case NCHAR:
            case NCHAR_VARYING:
            case NATIONAL_CHAR:
            case NATIONAL_CHARACTER:
            case NATIONAL_CHARACTER_VARYING:
                if( constrainParts == null || constrainParts.length == 0 || constrainParts.length > 1 ){
                    throw new OperationException(ErrorCode.DATATYPE_ERROR, type+" type must have exactly one constraint specified");
                } 
                break;
            case NUMERIC:
            case DECIMAL:
            case DEC:
                if( constrainParts == null || constrainParts.length ==0 || constrainParts.length > 2){
                    throw new OperationException(ErrorCode.DATATYPE_ERROR, type+" type must have exactly two constraint specified, precision and scale");
                }
                break;
        }
    }
}
