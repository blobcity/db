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

package com.blobcity.db.lang;

/**
 * An enumeration representing the various operator supported by the database. Calls to internal functions must pass
 * these enumeration equivalents of the operators to the respective functions.
 *
 * @author sanketsarang
 */
public enum Operators {

    /* Less than operator */
    LT("LT"),
    /* Greater than operator */
    GT("GT"),
    /* Equals operator */
    EQ("EQ"),
    /* Not-Equals operator */
    NEQ("NEQ"),
    /* Less than equals operator */
    LTEQ("LTEQ"),
    /* Greater than equals operator */
    GTEQ("GTEQ"),
    /* LIKE operator */
    LIKE("LIKE"),
    /* IN operator */
    IN("IN"),
    /**
     * Used to perform a NOT IN type search. This is not a standard SQL operator, but use would in queries of the type
     * SELECT * FROM table WHERE NOT col IN (val1, val2)
     */
    NOT_IN("NOT-IN"),
    /* BETWEEN operator */
    BETWEEN("BETWEEN"),
    /* NOT BETWEEN operator */
    NOT_BETWEEN("NOT-BETWEEN");

    private final String code;

    Operators(final String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static Operators fromCode(final String code) {
        for (Operators operator : Operators.values()) {
            if (operator.getCode().equalsIgnoreCase(code)) {
                return operator;
            }
        }

        return null;
    }
}
