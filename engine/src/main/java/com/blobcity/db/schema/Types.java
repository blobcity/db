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

package com.blobcity.db.schema;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

/**
 * Stores an enumeration representing all valid column data types supported by the database.
 *
 * @author sanketsarang
 */
public enum Types {

    CHAR("CHAR"),
    CHARACTER("CHARACTER"),
    CHARACTER_VARYING("CHARACTER VARYING"),
    CHAR_VARYING("CHAR VARYING"),
    VARCHAR("VARCHAR"),
    CHARACTER_LARGE_OBJECT("CHARACTER LARGE OBJECT"),
    CHAR_LARGE_OBJECT("CHAR LARGE OBJECT"),
    CLOB("CLOB"),
    NATIONAL_CHARACTER("NATIONAL CHARACTER"),
    NATIONAL_CHAR("NATIONAL CHAR"),
    NCHAR("NCHAR"),
    NATIONAL_CHARACTER_VARYING("NATIONAL CHARACTER VARYING"),
    NATIONAL_CHAR_VARYING("NATIONAL CHAR VARYING"),
    NCHAR_VARYING("NCHAR VARYING"),
    NATIONAL_CHARACTER_LARGE_OBJECT("NATIONAL CHARACTER LARGE OBJECT"),
    NCHAR_LARGE_OBJECT("NCHAR LARGE OBJECT"),
    NCLOB("NCLOB"),
    BINARY_LARGE_OBJECT("BINARY LARGE OBJECT"),
    BLOB("BLOB"),
    NUMERIC("NUMERIC"),
    DECIMAL("DECIMAL"),
    DEC("DEC"),
    SMALLINT("SMALLINT"),
    INTEGER("INTEGER"),
    INT("INT"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    REAL("REAL"),
    DOUBLE_PRECISION("DOUBLE PRECISION"),
    BOOLEAN("BOOLEAN"),
    DATE("DATE"),
    TIME("TIME"),
    TIMESTAMP("TIMESTAMP"),
    INTERVAL("INTERVAL"),
    REF("REF"),
    ARRAY("ARRAY"),
    MULTISET("MULTISET"),
    ROW("ROW"),
    XML("XML"),
    /* BlobCity Specific Types */
    LONG("LONG"),
    DOUBLE("DOUBLE"),
    STRING("STRING"),
    @Deprecated
    LIST_INTEGER("LIST<INT>"),
    @Deprecated
    LIST_FLOAT("LIST<FLOAT>"),
    @Deprecated
    LIST_LONG("LIST<LONG>"),
    @Deprecated
    LIST_DOUBLE("LIST<DOUBLE>"),
    @Deprecated
    LIST_STRING("LIST<STRING>"),
    @Deprecated
    LIST_CHARACTER("LIST<CHAR>");
    private final String type;

    Types(final String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static Types fromString(String type) throws OperationException {
        if (type == null || type.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "A blank data type is not permitted");
        }

        for (Types value : values()) {
            if (type.equalsIgnoreCase(value.getType())) {
                return value;
            }
        }

        throw new OperationException(ErrorCode.INVALID_SCHEMA, "Data type " + type + " could not be mapped to any known data types");
    }
}
