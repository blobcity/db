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

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link BinaryField}
 *
 * @author sanketsarang
 */
public class BinaryFieldTest {

    /**
     * Test of default constructor, of class BinaryField.
     */
    @Test
    public void testDefaultConsturctor() {
        System.out.println("BinaryField(Types)");
        BinaryField instance;

        /* Test whether constructor accepts only valid binary types */
        for (Types type : Types.values()) {
            try {
                new BinaryField(type);
                if (!(type == Types.BINARY_LARGE_OBJECT || type == Types.BLOB)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid binary type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.BINARY_LARGE_OBJECT || type == Types.BLOB) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid binary type");
                }
            }
        }
    }

    /**
     * Test of constructor(Types,String), of class BinaryField.
     */
    @Test
    public void testConstructor_type_string() {
        System.out.println("BinaryField(Types,String)");
        BinaryField instance;

        /* Test whether constructor accepts only valid binary types */
        for (Types type : Types.values()) {
            try {
                new BinaryField(type, "100");
                if (!(type == Types.BINARY_LARGE_OBJECT || type == Types.BLOB)) {
                    fail("Incorrectly accepted " + type.getType() + " as a valid binary type");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() != ErrorCode.DATATYPE_MISMATCH) {
                    fail("Incorrect error code reported for data type mismatch");
                }

                if (type == Types.BINARY_LARGE_OBJECT || type == Types.BLOB) {
                    fail("Incorrectly reported " + type.getType() + " as an invalid binary type");
                }
            }
        }

        /* Test if constructor accepts all valid length formats */
        try {
            new BinaryField(Types.BLOB, "100");
            new BinaryField(Types.BLOB, "100K");
            new BinaryField(Types.BLOB, "100M");
            new BinaryField(Types.BLOB, "100G");
        } catch (OperationException ex) {
            fail("Construct rejected valid length format");
        }

        /* Test whether constructor throws exception for invalid string formats */
        try {
            new BinaryField(Types.BLOB, "something");
            new BinaryField(Types.BLOB, "A100");
            new BinaryField(Types.BLOB, "K100");
            new BinaryField(Types.BLOB, "100A");
            new BinaryField(Types.BLOB, " 100");
            new BinaryField(Types.BLOB, "100K ");
            new BinaryField(Types.BLOB, "100 K");
            new BinaryField(Types.BLOB, "");
            new BinaryField(Types.BLOB, null);
        } catch (OperationException ex) {
            if(ex.getErrorCode() != ErrorCode.INVALID_FIELD_LENGTH_FORMAT) {
                fail("Invalid field length format exception reported with incorrect error code of " + ex.getErrorCode());
            }
        }
    }

    /**
     * Test of getQueryCode method, of class BinaryField.
     */
    @Test
    public void testGetType() throws OperationException {
        System.out.println("getQueryCode");
        BinaryField instance;

        /* Test for BINARY_LARGE_OBJECT type */
        instance = new BinaryField(Types.BINARY_LARGE_OBJECT);
        assertEquals(Types.BINARY_LARGE_OBJECT, instance.getType());

        /* Test for BLOB type */
        instance = new BinaryField(Types.BLOB);
        assertEquals(Types.BLOB, instance.getType());
    }

    /**
     * Test of getLength method, of class BinaryField.
     */
    @Test
    public void testGetLength() throws OperationException {
        System.out.println("getLength");
        BinaryField instance = new BinaryField(Types.BLOB);
        assert (instance.getLength() < 0);

        instance = new BinaryField(Types.BLOB, "10");
        assertEquals(10, instance.getLength());

        instance = new BinaryField(Types.BLOB, "10K");
        assertEquals(10 * 1024, instance.getLength());

        instance = new BinaryField(Types.BLOB, "20M");
        assertEquals(20 * 1024 * 1024, instance.getLength());

        instance = new BinaryField(Types.BLOB, "30G");
        assertEquals(30 * 1024 * 1024 * 1024, instance.getLength());
    }

    /**
     * Test of getDefaultValue method, of class BinaryField.
     */
    @Test
    public void testGetDefaultValue() throws OperationException {
        System.out.println("getDefaultValue");
        BinaryField instance = new BinaryField(Types.BLOB);
        assertEquals(null, instance.getDefaultValue());

        instance = new BinaryField(Types.BLOB, "100K");
        assertEquals(null, instance.getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class BinaryField.
     */
    @Test
    public void testHasDefaultValue() throws OperationException {
        System.out.println("hasDefaultValue");
        BinaryField instance = new BinaryField(Types.BLOB);
        assertFalse(instance.hasDefaultValue());

        instance = new BinaryField(Types.BLOB, "25M");
        assertFalse(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class BinaryField.
     */
    @Test
    public void testConvert() throws OperationException, JSONException {
        System.out.println("convert");
        final byte[] expResult;
        byte[] result;
        final String sampleText = UUID.randomUUID().toString();
        BinaryField instance = new BinaryField(Types.BLOB);

        /* Create JSON and simulate getting byte array from JSON, which is the expected data source */
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bytes", sampleText.getBytes());

        /* The expected result in bytes */
        expResult = sampleText.getBytes();

        /* Check if conversion works with JSONArray as input type */
        result = instance.convert(jsonObject.get("bytes"));
        assertArrayEquals(expResult, result);

        /* Check if conversion works with byte [] as input type */
        result = instance.convert(sampleText.getBytes());
        assertArrayEquals(expResult, result);

        /* Re-test with a valid length constraint */
        instance = new BinaryField(Types.BLOB, "36"); //UUID lenght is 36
        result = instance.convert(jsonObject.get("bytes"));
        assertArrayEquals(expResult, result);
        result = instance.convert(sampleText.getBytes());
        assertArrayEquals(expResult, result);

        /* Re-test with a violating the length constraint */
        instance = new BinaryField(Types.BLOB, "35"); //UUID length is 36. This is violation of constraint
        try {
            instance.convert(jsonObject.get("bytes"));
            fail("Length constraint violation not reported in the form of an OperationException");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

        try {
            instance.convert(sampleText.getBytes());
            fail("Length constraint violation not reported in the form of an OperationException");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }
        
        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }
}
