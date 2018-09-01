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
import com.blobcity.db.lang.Operators;

/**
 * Maps and SQL operator to blobcity's operators
 * @author akshaydewan
 */
public class OperatorMapper {

    /**
     * Maps and SQL operator to blobcity's operators
     * @param operator An SQL operator
     * @return A blobcity operator
     * @throws OperationException If the SQL operator is not supported in Blobcity, or if the SQL operator is invalid
     */
    public static Operators map(String operator) throws OperationException {
        switch (operator) {
            case ">":
                return Operators.GT;
            case ">=":
                return Operators.GTEQ;
            case "<":
                return Operators.LT;
            case "<=":
                return Operators.LTEQ;
            case "=":
                return Operators.EQ;
            case "<>":
                return Operators.NEQ;
            case "IN":
                return Operators.IN;
            default:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Operator '" + operator + "' support is not implemented");
        }
    }
    
}
