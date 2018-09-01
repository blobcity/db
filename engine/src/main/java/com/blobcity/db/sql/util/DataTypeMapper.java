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

package com.blobcity.db.sql.util;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Types;
import com.foundationdb.sql.types.DataTypeDescriptor;

/**
 * Map an SQL type to BlobCity's type
 *
 * @author akshaydewan
 * @author sanketsarang
 */
public class DataTypeMapper {

    /**
     * Map an SQL type to BlobCity's type
     *
     * @param typeDescriptor The DataTypeDescriptor for the node given by the parser
     * @return The BlobCity data type
     * @throws com.blobcity.db.exceptions.OperationException if the typeDescriptor is unhandled
     */
    public static Types map(DataTypeDescriptor typeDescriptor) throws OperationException {
        int jdbcTypeId = typeDescriptor.getJDBCTypeId();
        switch (jdbcTypeId) {
            case java.sql.Types.BIGINT:
                return Types.LONG;
            case java.sql.Types.CHAR:
                return Types.CHARACTER;
            case java.sql.Types.DOUBLE:
                return Types.DOUBLE;
            case java.sql.Types.FLOAT:
                return Types.FLOAT;
            case java.sql.Types.INTEGER:
                return Types.INTEGER;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                return Types.VARCHAR;
            case java.sql.Types.NCHAR:
                return Types.NCHAR;
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
                return Types.NATIONAL_CHAR_VARYING;
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                return Types.INTEGER;
            case java.sql.Types.DECIMAL:
                return Types.DECIMAL;
            case java.sql.Types.NUMERIC:
                return Types.NUMERIC;
            case java.sql.Types.REAL:
                return Types.REAL;
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                return Types.BOOLEAN;
            case java.sql.Types.CLOB:
                return Types.CLOB;
            case java.sql.Types.NCLOB:
                return Types.NCLOB;
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return Types.BINARY_LARGE_OBJECT;
            case java.sql.Types.REF:
                return Types.REF;
            case java.sql.Types.DATE:
                return Types.DATE;
            case java.sql.Types.TIME:
                return Types.TIME;
            case java.sql.Types.TIMESTAMP:
                return Types.TIMESTAMP;
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

}
