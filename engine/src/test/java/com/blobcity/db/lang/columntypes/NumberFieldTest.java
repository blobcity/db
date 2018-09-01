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
import java.math.BigDecimal;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Units tests for {@link NumberField}
 *
 * @author sanketsarang
 */
public class NumberFieldTest {

    /**
     * Test of NumberField(Types) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types() {
        System.out.println("NumberField(Types)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(type);
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }
        }
    }

    /**
     * Test of NumberField(Types,Number) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types_Default() throws OperationException {
        System.out.println("NumberField(Types,Number)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(type, new Integer(0));
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!isNumberType(type)) {
                continue;
            }

            /* Check for different default values */
            switch (type) {
                case SMALLINT:
                    new NumberField(type, new Short((short) -1));
                    new NumberField(type, new Short((short) 0));
                    new NumberField(type, new Short((short) 1));
                    break;
                case INTEGER:
                case INT:
                    new NumberField(type, new Integer(-1));
                    new NumberField(type, new Integer(0));
                    new NumberField(type, new Integer(1));
                    break;
                case LONG:
                case BIGINT:
                    new NumberField(type, new Long(1));
                    new NumberField(type, new Long(0));
                    new NumberField(type, new Long(1));
                    break;
                case DOUBLE:
                case DOUBLE_PRECISION:
                    new NumberField(type, new Double(-1.1));
                    new NumberField(type, new Double(0.0));
                    new NumberField(type, new Double(1.1));
                    break;
                case REAL:
                case FLOAT:
                    new NumberField(type, new Float(-1.1));
                    new NumberField(type, new Float(0.0));
                    new NumberField(type, new Float(1.1));
                    break;
                case NUMERIC:
                case DECIMAL:
                case DEC:
                    new NumberField(type, new Short((short) -1));
                    new NumberField(type, new Short((short) 0));
                    new NumberField(type, new Short((short) 1));
                    new NumberField(type, new Integer(-1));
                    new NumberField(type, new Integer(0));
                    new NumberField(type, new Integer(1));
                    new NumberField(type, new Long(1));
                    new NumberField(type, new Long(0));
                    new NumberField(type, new Long(1));
                    new NumberField(type, new Double(-1.1));
                    new NumberField(type, new Double(0.0));
                    new NumberField(type, new Double(1.1));
                    new NumberField(type, new Float(-1.1));
                    new NumberField(type, new Float(0.0));
                    new NumberField(type, new Float(1.1));
                    break;
            }

            /* Check error reporting for invalid default values */
            switch (type) {
                case SMALLINT:
                case INTEGER:
                case INT:
                case LONG:
                case BIGINT:
                    try {
                        new NumberField(type, new Double(-1.1));
                    } catch (OperationException ex) {
                        fail("Failed to approximate a floating point type to a non floating point number " + type.getType());
                    }
                    try {
                        new NumberField(type, new Double(0.0));
                    } catch (OperationException ex) {
                        fail("Failed to approximate a floating point type to a non floating point number " + type.getType());
                    }
                    try {
                        new NumberField(type, new Double(1.1));
                    } catch (OperationException ex) {
                    }
                    try {
                        new NumberField(type, new Float(-1.1));
                    } catch (OperationException ex) {
                        fail("Failed to approximate a floating point type to a non floating point number " + type.getType());
                    }
                    try {
                        new NumberField(type, new Float(0.0));
                    } catch (OperationException ex) {
                        fail("Failed to approximate a floating point type to a non floating point number " + type.getType());
                    }
                    try {
                        new NumberField(type, new Float(1.1));
                    } catch (OperationException ex) {
                        fail("Failed to approximate a floating point type to a non floating point number " + type.getType());
                    }
                    break;
                case DOUBLE:
                case DOUBLE_PRECISION:
                case REAL:
                case FLOAT:
                case NUMERIC:
                case DECIMAL:
                case DEC:
                    //these will not throw a number format error as input is always a valid {@link Number}
                    break;
            }
        }
    }

    /**
     * Test of NumberField(Types, int) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types_Precision() throws OperationException {
        System.out.println("NumberField(Types)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(type, 1);
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesPrecision(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!isNumberType(type) || !typeTakesPrecision(type)) {
                continue;
            }

            /* Ensure all legal values of precision are accepted */
            for (int i = 1; i <= (type == Types.FLOAT ? 24 : 127); i++) {
                new NumberField(type, i);
            }

            /* Validate precision validation */
            try {
                new NumberField(type, 0);
                fail("Number type accepted value with precision of 0, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, -1);
                fail("Number type accepted value with precision of -1, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 128);
                fail("Number type accepted value with precision of 128, where as maximum permitted value for precision is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of NumberField(int, Types) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Scale_Types() {
        System.out.println("NumberField(int, Types)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(1, type);
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesScale(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!typeTakesScale(type)) {
                continue;
            }

            /* Check if types accepts all scale values */
            for (int i = -84; i <= 127; i++) {
                try {
                    new NumberField(i, type);
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid scale value for type " + type.getType());
                }
            }

            /* Check for exception reporting for out of bound scale values */
            try {
                new NumberField(-85, type);
                fail("Accepted -85 as a valid scale value, when minimum permissible scale value is -84");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
            try {
                new NumberField(128, type);
                fail("Accepted 128 as a valid scale value, when maximum permissible scale value is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of NumberField(Types, int, Number) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types_Precision_Default() throws OperationException {
        System.out.println("NumberField(Types)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(type, 1, new Integer(0));
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesPrecision(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!typeTakesPrecision(type)) {
                continue;
            }

            /* Check for different default values falling within valid precision and scale specifications */
            new NumberField(type, 10, new Integer(-1));
            new NumberField(type, 10, new Integer(0));
            new NumberField(type, 10, new Integer(1));
            new NumberField(type, 10, new Float(-1.1));
            new NumberField(type, 10, new Float(0.0));
            new NumberField(type, 10, new Float(1.1));
            new NumberField(type, 10, new Double(-1.1));
            new NumberField(type, 10, new Double(0.0));
            new NumberField(type, 10, new Double(1.1));
            new NumberField(type, 10, new Short((short) -1));
            new NumberField(type, 10, new Short((short) 0));
            new NumberField(type, 10, new Short((short) 1));
            new NumberField(type, 10, new Long(1));
            new NumberField(type, 10, new Long(0));
            new NumberField(type, 10, new Long(1));

            /* Check for different default values falling outside precision specifications */
            try {
                new NumberField(type, 1, new Integer(-10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Integer(10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Float(-10.1));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Float(10.1));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Double(-10.1));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Double(10.1));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Short((short) -10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Short((short) 10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Long(10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, new Long(10));
                fail(type.getType() + " accepted numeric value violating precision constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }

            /* Ensure all legal values of precision are accepted */
            for (int i = 1; i <= (type == Types.FLOAT ? 24 : 127); i++) {
                new NumberField(type, i, new Integer(0));
            }

            /* Check for precision validation */
            try {
                new NumberField(Types.NUMERIC, 0, new Integer(0));
                fail(type.getType() + " type accepted value with precision of 0, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(Types.NUMERIC, -1, new Integer(0));
                fail(type.getType() + " type accepted value with precision of -1, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(Types.NUMERIC, 128, new Integer(0));
                fail(type.getType() + " type accepted value with precision of 128, where as maximum permitted precision is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of NumberField(int, Types, Number) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Scale_Types_Default() throws OperationException {
        System.out.println("NumberField(int, Types, Number)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(1, type, new Integer(0));
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesScale(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!typeTakesScale(type)) {
                continue;
            }

            /* Check if types accepts all scale values upto default assigned precision value */
            NumberField numberForPrecision = new NumberField(0, type, new Integer(0));
            for (int i = -84; i <= numberForPrecision.getPrecision(); i++) {
                try {
                    new NumberField(i, type, new Integer(0));
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid scale value for type " + type.getType());
                }
            }

            /* Check if types fails to accepts all scale values above default precision value */
            for (int i = numberForPrecision.getPrecision() + 1; i <= 127; i++) {
                try {
                    new NumberField(i, type, new Integer(0));
                    fail("Accepted " + i + " as a valid scale value for type " + type.getType()
                            + " when default precision value is " + numberForPrecision.getPrecision()
                            + ", and scale must be less than or equal to precision");
                } catch (OperationException ex) {
                }
            }

            /* Check for different default values. A default value can never violate a scale constraint, so there
             are not negative test cases */
            new NumberField(0, type, new Integer(-1));
            new NumberField(0, type, new Integer(0));
            new NumberField(0, type, new Integer(1));
            new NumberField(0, type, new Float(-1.1));
            new NumberField(0, type, new Float(0.0));
            new NumberField(0, type, new Float(1.1));
            new NumberField(0, type, new Double(-1.1));
            new NumberField(0, type, new Double(0.0));
            new NumberField(0, type, new Double(1.1));
            new NumberField(0, type, new Short((short) -1));
            new NumberField(0, type, new Short((short) 0));
            new NumberField(0, type, new Short((short) 1));
            new NumberField(0, type, new Long(1));
            new NumberField(0, type, new Long(0));
            new NumberField(0, type, new Long(1));

            /* Check for exception reporting for out of bound scale values */
            try {
                new NumberField(-85, type, new Integer(0));
                fail("Accepted -85 as a valid scale value, when minimum permissible scale value is -84");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
            try {
                new NumberField(128, type, new Integer(0));
                fail("Accepted 128 as a valid scale value, when maximum permissible scale value is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of NumberField(Types, int, int) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types_Precision_Scale() {
        System.out.println("NumberField(Types, int, int)");

        /* Test whether constructor accepts only valid number types */
        for (Types type : Types.values()) {
            try {
                new NumberField(type, 1, 0);
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesPrecisionAndScale(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!typeTakesPrecisionAndScale(type)) {
                continue;
            }

            /* Check if type accepts all precision values */
            for (int i = 1; i <= 127; i++) {
                try {
                    new NumberField(type, i, 0);
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid precision value for type " + type.getType());
                }
            }

            /* Check if types accepts all scale values */
            for (int i = -84; i <= 127; i++) {
                try {
                    new NumberField(type, 127, i);
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid scale value for type " + type.getType());
                }
            }

            /* Check for out of bound precision values */
            try {
                new NumberField(type, 0, 0);
                fail("Number type accepted value with precision of 0, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, -1, 0);
                fail("Number type accepted value with precision of -1, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 2, 2);
            } catch (OperationException ex) {
                fail("Number type did not accept terminal condition of precision equal to scale");
            }

            try {
                new NumberField(type, 2, 3);
                fail("Number type accepted precision greater than scale, whereas precision must be less than or equal to scale");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 128, 0);
                fail("Number type accepted value with precision of 128, where as maximum permitted precision is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            /* Check for exception reporting for out of bound scale values */
            try {
                new NumberField(type, 127, -85);
                fail("Accepted -85 as a valid scale value, when minimum permissible scale value is -84");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 127, 128);
                fail("Accepted 128 as a valid scale value, when maximum permissible scale value is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of NumberField(Types, int, int) constructor, of {@link NumberField}
     */
    @Test
    public void testConstructor_Types_Precision_Scale_Default() throws OperationException {
        System.out.println("NumberField(Types, int, int, Number)");

        for (Types type : Types.values()) {

            /* Test whether constructor accepts only valid number types */
            try {
                new NumberField(type, 1, 0, new Integer(0));
                if (!isNumberType(type)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid number type");
                }
            } catch (OperationException ex) {
                if (isNumberType(type) && !typeTakesPrecisionAndScale(type)) {
                    assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
                } else if (isNumberType(type)) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid number type");
                }
            }

            if (!typeTakesPrecisionAndScale(type)) {
                continue;
            }

            /* Check if type accepts all precision values */
            for (int i = 1; i <= 127; i++) {
                try {
                    new NumberField(type, i, 0, new Integer(0));
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid precision value for type " + type.getType());
                }
            }

            /* Check if types accepts all scale values */
            for (int i = -84; i <= 127; i++) {
                try {
                    new NumberField(type, 127, i, new Integer(0));
                } catch (OperationException ex) {
                    fail("Failed to accept " + i + " as a valid scale value for type " + type.getType());
                }
            }

            /* Check for different default values falling within valid precision and scale specifications */
            new NumberField(type, 10, 2, new Integer(-1));
            new NumberField(type, 10, 2, new Integer(0));
            new NumberField(type, 10, 2, new Integer(1));
            new NumberField(type, 10, 2, new Float(-1.1));
            new NumberField(type, 10, 2, new Float(0.0));
            new NumberField(type, 10, 2, new Float(1.1));
            new NumberField(type, 10, 2, new Double(-1.1));
            new NumberField(type, 10, 2, new Double(0.0));
            new NumberField(type, 10, 2, new Double(1.1));
            new NumberField(type, 10, 2, new Short((short) -1));
            new NumberField(type, 10, 2, new Short((short) 0));
            new NumberField(type, 10, 2, new Short((short) 1));
            new NumberField(type, 10, 2, new Long(1));
            new NumberField(type, 10, 2, new Long(0));
            new NumberField(type, 10, 2, new Long(1));

            /* Check for different default values falling outside precision specifications */
            try {
                new NumberField(type, 1, 0, new Integer(-10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 0, new Integer(10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 1, new Float(-10.1));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 1, new Float(10.1));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 1, new Double(-10.1));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 1, new Double(10.1));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 0, new Short((short) -10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 0, new Short((short) 10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 0, new Long(10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }
            try {
                new NumberField(type, 1, 0, new Long(10));
                fail(type.getType() + " accepted numeric value violating precision / scale constraint");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
            }

            /* Check for out of bound precision values */
            try {
                new NumberField(type, 0, 0, new Integer(0));
                fail(type.getType() + " type accepted value with precision of 0, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, -1, 0, new Integer(0));
                fail(type.getType() + " type accepted value with precision of -1, where as precision has to be greater than or equal to one");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 2, 2, new Integer(0));
            } catch (OperationException ex) {
                fail(type.getType() + " type did not accept terminal condition of precision equal to scale");
            }

            try {
                new NumberField(type, 2, 3, new Integer(0));
                fail(type.getType() + " type accepted precision greater than scale, whereas precision must be less than or equal to scale");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 128, 0, new Integer(0));
                fail(type.getType() + " type accepted value with precision of 128, where as maximum permitted precision is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            /* Check for exception reporting for out of bound scale values */
            try {
                new NumberField(type, 127, -85, new Integer(0));
                fail("Accepted -85 as a valid scale value, when minimum permissible scale value is -84");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }

            try {
                new NumberField(type, 127, 128, new Integer(0));
                fail("Accepted 128 as a valid scale value, when maximum permissible scale value is 127");
            } catch (OperationException ex) {
                assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
            }
        }
    }

    /**
     * Test of getQueryCode method, of class NumberField.
     */
    @Test
    public void testGetType() throws OperationException {
        System.out.println("getQueryCode");

        assertEquals(Types.NUMERIC, new NumberField(Types.NUMERIC).getType());
        assertEquals(Types.DECIMAL, new NumberField(Types.DECIMAL).getType());
        assertEquals(Types.DEC, new NumberField(Types.DEC).getType());
        assertEquals(Types.SMALLINT, new NumberField(Types.SMALLINT).getType());
        assertEquals(Types.INTEGER, new NumberField(Types.INTEGER).getType());
        assertEquals(Types.LONG, new NumberField(Types.LONG).getType());
        assertEquals(Types.INT, new NumberField(Types.INT).getType());
        assertEquals(Types.BIGINT, new NumberField(Types.BIGINT).getType());
        assertEquals(Types.FLOAT, new NumberField(Types.FLOAT).getType());
        assertEquals(Types.REAL, new NumberField(Types.REAL).getType());
        assertEquals(Types.DOUBLE_PRECISION, new NumberField(Types.DOUBLE_PRECISION).getType());
    }

    /**
     * Test of getPrecision method, of class NumberField.
     */
    @Test
    public void testGetPrecision() throws OperationException {
        System.out.println("getPrecision");

        for (Types type : Types.values()) {
            if (!isNumberType(type)) {
                continue;
            }
            assertEquals(getDefaultPrecision(type), new NumberField(type).getPrecision());
            assertEquals(getDefaultPrecision(type), new NumberField(type, new Integer(0)).getPrecision());

            if (typeTakesScale(type)) {
                assertEquals(getDefaultPrecision(type), new NumberField(2, type).getPrecision());
                assertEquals(getDefaultPrecision(type), new NumberField(2, type, new Integer(0)).getPrecision());
            }

            if (typeTakesPrecision(type)) {
                assertEquals(2, new NumberField(type, 2).getPrecision());
                assertEquals(2, new NumberField(type, 2, new Integer(0)).getPrecision());
                if (typeTakesScale(type)) {
                    assertEquals(2, new NumberField(type, 2, 2).getPrecision());
                    assertEquals(2, new NumberField(type, 2, 2, new Integer(0)).getPrecision());
                    assertEquals(2, new NumberField(type, 2, 2, new Integer(0)).getPrecision());
                }
            }
        }
    }

    /**
     * Test of getScale method, of class NumberField.
     */
    @Test
    public void testGetScale() throws OperationException {
        System.out.println("getScale");

        for (Types type : Types.values()) {
            if (!isNumberType(type)) {
                continue;
            }

            assertEquals(getDefaultScale(type, false), new NumberField(type).getScale());
            assertEquals(getDefaultScale(type, false), new NumberField(type, new Integer(0)).getScale());
            if (typeTakesScale(type)) {
                assertEquals(new Integer(0), new NumberField(0, type).getScale());
                assertEquals(new Integer(0), new NumberField(0, type, new Integer(0)).getScale());
                if (typeTakesPrecision(type)) {
                    assertEquals(new Integer(0), new NumberField(type, 1, 0).getScale());
                    assertEquals(new Integer(0), new NumberField(type, 1, 0, new Integer(0)).getScale());
                }
            } else if (typeTakesPrecision(type)) {
                if (type == Types.FLOAT) {
                    assertEquals(getDefaultScale(type, true), new NumberField(type, 24).getScale());
                    assertEquals(getDefaultScale(type, true), new NumberField(type, 24, new Integer(0)).getScale());
                } else {
                    assertEquals(getDefaultScale(type, true), new NumberField(type, 38).getScale());
                    assertEquals(getDefaultScale(type, true), new NumberField(type, 38, new Integer(0)).getScale());
                }
            }
        }
    }

    /**
     * Test of getDefaultValue method, of class NumberField.
     */
    @Test
    public void testGetDefaultValue() throws OperationException {
        System.out.println("getDefaultValue");
        assertNull(new NumberField(Types.NUMERIC).getDefaultValue());
        assertNull(new NumberField(0, Types.NUMERIC).getDefaultValue());
        assertNull(new NumberField(Types.NUMERIC, 1).getDefaultValue());
        assertNull(new NumberField(Types.NUMERIC, 1, 1).getDefaultValue());
        assertEquals(new BigDecimal(0), new NumberField(Types.NUMERIC, new Integer(0)).getDefaultValue());
        assertEquals(new BigDecimal(0), new NumberField(Types.NUMERIC, 1, new Integer(0)).getDefaultValue());
        assertEquals(new BigDecimal(0), new NumberField(0, Types.NUMERIC, new Integer(0)).getDefaultValue());
        assertEquals(new BigDecimal(0), new NumberField(Types.NUMERIC, 1, 0, new Integer(0)).getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class NumberField.
     */
    @Test
    public void testHasDefaultValue() throws OperationException {
        System.out.println("hasDefaultValue");

        assertFalse(new NumberField(Types.NUMERIC).hasDefaultValue());
        assertFalse(new NumberField(0, Types.NUMERIC).hasDefaultValue());
        assertFalse(new NumberField(Types.NUMERIC, 1).hasDefaultValue());
        assertFalse(new NumberField(Types.NUMERIC, 1, 1).hasDefaultValue());
        assertTrue(new NumberField(Types.NUMERIC, new Integer(0)).hasDefaultValue());
        assertTrue(new NumberField(Types.NUMERIC, 1, new Integer(0)).hasDefaultValue());
        assertTrue(new NumberField(0, Types.NUMERIC, new Integer(0)).hasDefaultValue());
        assertTrue(new NumberField(Types.NUMERIC, 1, 0, new Integer(0)).hasDefaultValue());
    }

    /**
     * Test of convert method, of class NumberField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        NumberField instance;

        /* NUMERIC tests */
        instance = new NumberField(Types.NUMERIC);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.NUMERIC, 38, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(1, Types.NUMERIC);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.NUMERIC, 9);
        assertEquals(new BigDecimal("7456124"), instance.convert("7456123.89"));
        instance = new NumberField(Types.NUMERIC, 9, 2);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.NUMERIC, 9, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.NUMERIC, 7, -2);
        assertEquals(new BigDecimal("7456100").setScale(-2), instance.convert("7456123.89"));
        try {
            instance = new NumberField(Types.NUMERIC, 6);
            instance.convert("7456123.89");
            fail("Accepted number with precision surpassing the specified precision");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

        /* DEC tests */
        instance = new NumberField(Types.DEC);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DEC, 38, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(1, Types.DEC);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DEC, 9);
        assertEquals(new BigDecimal("7456124"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DEC, 9, 2);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DEC, 9, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DEC, 7, -2);
        assertEquals(new BigDecimal("7456100").setScale(-2), instance.convert("7456123.89"));
        try {
            instance = new NumberField(Types.DEC, 6);
            instance.convert("7456123.89");
            fail("Accepted number with precision surpassing the specified precision");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

        /* DECIMAL tests */
        instance = new NumberField(Types.DECIMAL);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DECIMAL, 38, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(1, Types.DECIMAL);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DECIMAL, 9);
        assertEquals(new BigDecimal("7456124"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DECIMAL, 9, 2);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DECIMAL, 9, 1);
        assertEquals(new BigDecimal("7456123.9"), instance.convert("7456123.89"));
        instance = new NumberField(Types.DECIMAL, 7, -2);
        assertEquals(new BigDecimal("7456100").setScale(-2), instance.convert("7456123.89"));
        try {
            instance = new NumberField(Types.DECIMAL, 6);
            instance.convert("7456123.89");
            fail("Accepted number with precision surpassing the specified precision");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

        /* FLOAT tests */
        instance = new NumberField(Types.FLOAT);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        instance = new NumberField(Types.FLOAT, 9);
        assertEquals(new BigDecimal("7456123.89"), instance.convert("7456123.89"));
        try {
            instance = new NumberField(Types.FLOAT, 6);
            instance.convert("7456123.89");
            fail("Accepted number with precision surpassing the specified precision");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

        /* INTEGER tests */
        instance = new NumberField(Types.INTEGER);
        assertEquals(1000, instance.convert(1000));
        assertEquals(Integer.MAX_VALUE, instance.convert(Integer.MAX_VALUE));
        assertEquals(1000, instance.convert("1000"));
        try {
            instance.convert(1000.1);
        } catch (OperationException ex) {
            fail("Failed to approximate float to integer");
        }

        /* LONG tests */
        instance = new NumberField(Types.LONG);
        assertEquals(1000L, instance.convert(1000L));
        assertEquals(Long.MAX_VALUE, instance.convert(Long.MAX_VALUE));
        assertEquals(1000L, instance.convert("1000"));
        try {
            instance.convert(1000.1);
        } catch (OperationException ex) {
            fail("Failed to approximate float point value to long value");
        }

        /* SMALLINT tests */
        instance = new NumberField(Types.SMALLINT);
        assertEquals((short) 1000, instance.convert(1000));
        assertEquals(Short.MAX_VALUE, instance.convert(Short.MAX_VALUE));
        assertEquals((short) 1000, instance.convert("1000"));
        try {
            instance.convert(1000.1);
        } catch (OperationException ex) {
            fail("Failed to approximate float point value to smallint value");
        }

        /* BIGINT tests */
        instance = new NumberField(Types.BIGINT);
        assertEquals((long) 1000, instance.convert(1000));
        assertEquals(Long.MAX_VALUE, instance.convert(Long.MAX_VALUE));
        assertEquals((long) 1000, instance.convert("1000"));
        try {
            instance.convert(1000.1);
        } catch (OperationException ex) {
            fail("Failed to approximate float point value to bigint value");
        }

        /* DOUBLE_PRECISION tests */
        instance = new NumberField(Types.DOUBLE_PRECISION);
        assertEquals(Double.MAX_VALUE, instance.convert(Double.MAX_VALUE));
        try {
            instance.convert("1000ld");
            fail("Invalid conversion of non-number to double succeeded instead of reporting an error");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_NUMBER_FORMAT, ex.getErrorCode());
        }

        /* DOUBLE tests */
        instance = new NumberField(Types.DOUBLE);
        assertEquals(Double.MAX_VALUE, instance.convert(Double.MAX_VALUE));
        try {
            instance.convert("1000ld");
            fail("Invalid conversion of non-number to double succeeded instead of reporting an error");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_NUMBER_FORMAT, ex.getErrorCode());
        }
    }

    private boolean isNumberType(Types type) {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
            case SMALLINT:
            case INTEGER:
            case INT:
            case LONG:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case DOUBLE_PRECISION:
                return true;
        }

        return false;
    }

    private boolean typeTakesPrecision(Types type) {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
            case FLOAT:
                return true;
        }

        return false;
    }

    private boolean typeTakesScale(Types type) {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
                return true;
        }

        return false;
    }

    private boolean typeTakesPrecisionAndScale(Types type) {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
                return true;
        }

        return false;
    }

    private int getDefaultPrecision(Types type) {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
                return 38;
            case FLOAT:
            case REAL:
                return 24;
            default:
                return -1;
        }
    }

    private Integer getDefaultScale(Types type, boolean precisionSpecified) {
        switch (type) {
            case NUMERIC:
            case DEC:
            case DECIMAL:
                if (precisionSpecified) {
                    return 0;
                }
                return null;
            default:
                return null;
        }
    }
}
