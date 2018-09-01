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
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link StringField}
 *
 * @author sanketsarang
 */
public class StringFieldTest {

    /**
     *  Test of default constructor, of class StringField.
     */
    @Test
    public void testDefaultConstructor() {
        System.out.println("StringField(Types)");

        /* Test whether constructor accepts only valid String types */
        for (Types type : Types.values()) {
            try {
                new StringField(type);
                if (!(type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid string type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid string type");
                }
            }
        }
    }

    /**
     *  Test of constructor StringField(String,Types), of class StringField.
     */
    @Test
    public void testConstructor_String_Types() {
        System.out.println("StringField(String,Types)");
        final String DEFAULT_VALUE = "something";

        /* Test whether constructor accepts only valid String types */
        for (Types type : Types.values()) {
            try {
                new StringField(DEFAULT_VALUE, type);
                if (!(type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid string type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid string type");
                }
            }
        }
    }

    /**
     *  Test of constructor StringField(Types,String), of class StringField.
     */
    @Test
    public void testConstructor_Types_String() {
        System.out.println("StringField(Types,String)");

        /* Test whether constructor accepts only valid String types */
        for (Types type : Types.values()) {
            try {
                new StringField(type, "100");
                if (!(type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid string type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid string type");
                }
            }
        }

        /* Test whether constructor throws exception for invalid string formats */
        try {
            new StringField(Types.STRING, "something");
            new StringField(Types.STRING, "A100");
            new StringField(Types.STRING, "K100");
            new StringField(Types.STRING, "100A");
            new StringField(Types.STRING, " 100");
            new StringField(Types.STRING, "100K ");
            new StringField(Types.STRING, "100 K");
            new StringField(Types.STRING, "");
            new StringField(Types.STRING, null);
        } catch (OperationException ex) {
            if (ex.getErrorCode() != ErrorCode.INVALID_FIELD_LENGTH_FORMAT) {
                fail("Invalid field length format exception reported with incorrect error code of " + ex.getErrorCode());
            }
        }
    }

    /**
     *  Test of constructor StringField(Types,String, String), of class StringField.
     */
    @Test
    public void testConstructor_Types_String_String() {
        System.out.println("StringField(Types,String,String)");

        /* Test whether constructor accepts only valid String types */
        for (Types type : Types.values()) {
            try {
                new StringField(type, "100", "something");
                if (!(type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid string type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.CHAR
                        || type == Types.CHARACTER
                        || type == Types.CHARACTER_VARYING
                        || type == Types.CHAR_VARYING
                        || type == Types.VARCHAR
                        || type == Types.CHARACTER_LARGE_OBJECT
                        || type == Types.CHAR_LARGE_OBJECT
                        || type == Types.CLOB
                        || type == Types.NATIONAL_CHARACTER
                        || type == Types.NATIONAL_CHAR
                        || type == Types.NCHAR
                        || type == Types.NATIONAL_CHARACTER_VARYING
                        || type == Types.NATIONAL_CHAR_VARYING
                        || type == Types.NCHAR_VARYING
                        || type == Types.NATIONAL_CHARACTER_LARGE_OBJECT
                        || type == Types.NCHAR_LARGE_OBJECT
                        || type == Types.NCLOB
                        || type == Types.STRING) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid string type");
                }
            }
        }

        /* Test whether constructor throws exception for invalid string formats */
        try {
            new StringField(Types.STRING, "something", "something");
            new StringField(Types.STRING, "A100", "something");
            new StringField(Types.STRING, "K100", "something");
            new StringField(Types.STRING, "100A", "something");
            new StringField(Types.STRING, " 100", "something");
            new StringField(Types.STRING, "100K ", "something");
            new StringField(Types.STRING, "100 K", "something");
            new StringField(Types.STRING, "", "something");
            new StringField(Types.STRING, null, "something");
        } catch (OperationException ex) {
            if (ex.getErrorCode() != ErrorCode.INVALID_FIELD_LENGTH_FORMAT) {
                fail("Invalid field length format exception reported with incorrect error code of " + ex.getErrorCode());
            }
        }
    }

    /**
     * Test of getQueryCode method, of class StringField.
     */
    @Test
    public void testGetType() throws OperationException {
        System.out.println("getQueryCode");
        assertEquals(Types.CHAR, new StringField(Types.CHAR).getType());
        assertEquals(Types.CHARACTER, new StringField(Types.CHARACTER).getType());
        assertEquals(Types.CHARACTER_VARYING, new StringField(Types.CHARACTER_VARYING).getType());
        assertEquals(Types.CHAR_VARYING, new StringField(Types.CHAR_VARYING).getType());
        assertEquals(Types.VARCHAR, new StringField(Types.VARCHAR).getType());
        assertEquals(Types.CHARACTER_LARGE_OBJECT, new StringField(Types.CHARACTER_LARGE_OBJECT).getType());
        assertEquals(Types.CHAR_LARGE_OBJECT, new StringField(Types.CHAR_LARGE_OBJECT).getType());
        assertEquals(Types.CLOB, new StringField(Types.CLOB).getType());
        assertEquals(Types.NATIONAL_CHARACTER, new StringField(Types.NATIONAL_CHARACTER).getType());
        assertEquals(Types.NATIONAL_CHAR, new StringField(Types.NATIONAL_CHAR).getType());
        assertEquals(Types.NCHAR, new StringField(Types.NCHAR).getType());
        assertEquals(Types.NATIONAL_CHARACTER_VARYING, new StringField(Types.NATIONAL_CHARACTER_VARYING).getType());
        assertEquals(Types.NATIONAL_CHAR_VARYING, new StringField(Types.NATIONAL_CHAR_VARYING).getType());
        assertEquals(Types.NCHAR_VARYING, new StringField(Types.NCHAR_VARYING).getType());
        assertEquals(Types.NATIONAL_CHARACTER_LARGE_OBJECT, new StringField(Types.NATIONAL_CHARACTER_LARGE_OBJECT).getType());
        assertEquals(Types.NCHAR_LARGE_OBJECT, new StringField(Types.NCHAR_LARGE_OBJECT).getType());
        assertEquals(Types.NCLOB, new StringField(Types.NCLOB).getType());
        assertEquals(Types.STRING, new StringField(Types.STRING).getType());
    }

    /**
     * Test of getLength method, of class StringField.
     */
    @Test
    public void testGetLength() throws OperationException {
        System.out.println("getLength");
        StringField instance = new StringField(Types.STRING);
        assert (instance.getLength() < 0);

        instance = new StringField(Types.STRING, "10");
        assertEquals(10, instance.getLength());

        instance = new StringField(Types.STRING, "10K");
        assertEquals(10 * 1024, instance.getLength());

        instance = new StringField(Types.STRING, "20M");
        assertEquals(20 * 1024 * 1024, instance.getLength());

        instance = new StringField(Types.STRING, "30G");
        assertEquals(30 * 1024 * 1024 * 1024, instance.getLength());
    }

    /**
     * Test of getDefaultValue method, of class StringField.
     */
    @Test
    public void testGetDefaultValue() throws OperationException {
        System.out.println("getDefaultValue");
        StringField instance;

        instance = new StringField(Types.STRING);
        assertEquals(null, instance.getDefaultValue());

        instance = new StringField(Types.STRING, "100");
        assertEquals(null, instance.getDefaultValue());

        instance = new StringField("something", Types.STRING);
        assertEquals("something", instance.getDefaultValue());

        instance = new StringField(Types.STRING, "100", "something");
        assertEquals("something", instance.getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class StringField.
     */
    @Test
    public void testHasDefaultValue() throws OperationException {
        System.out.println("hasDefaultValue");
        StringField instance;

        instance = new StringField(Types.STRING);
        assertFalse(instance.hasDefaultValue());

        instance = new StringField(Types.STRING, "100");
        assertFalse(instance.hasDefaultValue());

        instance = new StringField("something", Types.STRING);
        assertTrue(instance.hasDefaultValue());

        instance = new StringField(Types.STRING, "100", "something");
        assertTrue(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class StringField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        StringField instance;

        instance = new StringField(Types.STRING, "36");
        UUID expectedResult = UUID.randomUUID();
        assertEquals(expectedResult.toString(), instance.convert(expectedResult));
        assertEquals(expectedResult.toString(), instance.convert(expectedResult.toString()));

        instance = new StringField(Types.STRING, "35");
        try {
            instance.convert(expectedResult.toString());
            fail("StringField failed to do constraint check");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }
    }
}
