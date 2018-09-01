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
import java.sql.Timestamp;
import java.util.Collections;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link TimestampField}
 *
 * @author sanketsarang
 */
public class TimestampFieldTest {

    /**
     * Test of TimestampField(Integer), of class TimestampField.
     */
    @Test
    public void testConstructor_Integer() {
        System.out.println("TimestampField(Integer)");

        for (int i = 0; i <= 9; i++) {
            try {
                new TimestampField(i);
            } catch (OperationException ex) {
                fail("Constructor incorrectly threw exception for valid precision input of " + i);
            }
        }

        try {
            new TimestampField(-1);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }

        try {
            new TimestampField(10);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }
    }

    /**
     * Test of TimestampField(Boolean, Integer), of class TimestampField.
     */
    @Test
    public void testConstructure_Boolean_Integer() {
        System.out.println("TimestampField(Boolean, Integer)");

        for (int i = 0; i <= 9; i++) {
            try {
                new TimestampField(true, i);
            } catch (OperationException ex) {
                fail("Constructor incorrectly threw exception for valid precision input of " + i);
            }
        }

        try {
            new TimestampField(true, -1);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }

        try {
            new TimestampField(true, 10);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }
    }

    /**
     * Test of TimestampField(Boolean, Integer, Timestamp), of class TimestampField.
     */
    @Test
    public void testConstructure_Boolean_Integer_Timestamp() {
        System.out.println("TimestampField(Boolean, Integer, Timestamp)");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        for (int i = 0; i <= 9; i++) {
            try {
                new TimestampField(true, i, timestamp);
            } catch (OperationException ex) {
                fail("Constructor incorrectly threw exception for valid precision input of " + i);
            }
        }

        try {
            new TimestampField(true, -1, timestamp);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }

        try {
            new TimestampField(true, 10, timestamp);
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }
    }

    /**
     * Test of getQueryCode method, of class TimestampField.
     */
    @Test
    public void testGetType() throws OperationException {
        System.out.println("getQueryCode");
        TimestampField instance;

        instance = new TimestampField();
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(new Timestamp(System.currentTimeMillis()));
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(true);
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(2);
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(true, new Timestamp(System.currentTimeMillis()));
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(true, 2);
        assertEquals(Types.TIMESTAMP, instance.getType());
        instance = new TimestampField(true, 2, new Timestamp(System.currentTimeMillis()));
        assertEquals(Types.TIMESTAMP, instance.getType());
    }

    @Test
    public void testIsWithTimeZone() throws OperationException {
        System.out.println("isWithTimeZone");
        TimestampField instance;

        instance = new TimestampField();
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(new Timestamp(System.currentTimeMillis()));
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(true);
        assertTrue(instance.isWithTimeZone());
        instance = new TimestampField(false);
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(2);
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(true, new Timestamp(System.currentTimeMillis()));
        assertTrue(instance.isWithTimeZone());
        instance = new TimestampField(false, new Timestamp(System.currentTimeMillis()));
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(true, 2);
        assertTrue(instance.isWithTimeZone());
        instance = new TimestampField(false, 2);
        assertFalse(instance.isWithTimeZone());
        instance = new TimestampField(true, 2, new Timestamp(System.currentTimeMillis()));
        assertTrue(instance.isWithTimeZone());
        instance = new TimestampField(false, 2, new Timestamp(System.currentTimeMillis()));
        assertFalse(instance.isWithTimeZone());
    }

    /**
     * Test of getPrecision method, of class TimestampField.
     */
    @Test
    public void testGetPrecision() throws OperationException {
        System.out.println("getPrecision");
        TimestampField instance;

        instance = new TimestampField();
        assertEquals(6, instance.getPrecision());
        instance = new TimestampField(new Timestamp(System.currentTimeMillis()));
        assertEquals(6, instance.getPrecision());
        instance = new TimestampField(true);
        assertEquals(6, instance.getPrecision());
        instance = new TimestampField(2);
        assertEquals(2, instance.getPrecision());
        instance = new TimestampField(true, new Timestamp(System.currentTimeMillis()));
        assertEquals(6, instance.getPrecision());
        instance = new TimestampField(true, 2);
        assertEquals(2, instance.getPrecision());
        instance = new TimestampField(true, 2, new Timestamp(System.currentTimeMillis()));
        assertEquals(2, instance.getPrecision());
    }

    /**
     * Test of getDefaultValue method, of class TimestampField.
     */
    @Test
    public void testGetDefaultValue() throws OperationException {
        System.out.println("getDefaultValue");
        Timestamp defaultValue = new Timestamp(System.currentTimeMillis());
        TimestampField instance;

        instance = new TimestampField();
        assertNull(instance.getDefaultValue());
        instance = new TimestampField(defaultValue);
        assertEquals(defaultValue, instance.getDefaultValue());
        instance = new TimestampField(true);
        assertNull(instance.getDefaultValue());
        instance = new TimestampField(2);
        assertNull(instance.getDefaultValue());
        instance = new TimestampField(true, defaultValue);
        assertEquals(defaultValue, instance.getDefaultValue());
        instance = new TimestampField(true, 2);
        assertEquals(2, instance.getPrecision());
        instance = new TimestampField(true, 2, defaultValue);
        assertEquals(defaultValue, instance.getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class TimestampField.
     */
    @Test
    public void testHasDefaultValue() throws OperationException {
        System.out.println("hasDefaultValue");
        TimestampField instance = new TimestampField();

        instance = new TimestampField();
        assertFalse(instance.hasDefaultValue());
        instance = new TimestampField(new Timestamp(System.currentTimeMillis()));
        assertTrue(instance.hasDefaultValue());
        instance = new TimestampField(true);
        assertFalse(instance.hasDefaultValue());
        instance = new TimestampField(2);
        assertFalse(instance.hasDefaultValue());
        instance = new TimestampField(true, new Timestamp(System.currentTimeMillis()));
        assertTrue(instance.hasDefaultValue());
        instance = new TimestampField(true, 2);
        assertFalse(instance.hasDefaultValue());
        instance = new TimestampField(true, 2, new Timestamp(System.currentTimeMillis()));
        assertTrue(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class TimestampField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        String str;
        TimestampField instance;
        Timestamp referenceValue;
        Timestamp expectedValue;

        /* With default precision 6 */
        instance = new TimestampField();
        referenceValue = new Timestamp(System.currentTimeMillis());
        assertEquals(referenceValue, instance.convert(referenceValue));

        expectedValue = new Timestamp(referenceValue.getTime());
        referenceValue.setNanos(999999999);
        expectedValue.setNanos(0);
        assertEquals(expectedValue, instance.convert(referenceValue));

        referenceValue.setNanos(123456789);
        expectedValue.setNanos(123457000);
        assertEquals(expectedValue, instance.convert(referenceValue));
        assertEquals(expectedValue, instance.convert(referenceValue.toString()));
        expectedValue.setNanos(123000000); //since getTime() will return only to milli-second precision
        assertEquals(expectedValue, instance.convert(referenceValue.getTime()));

        /* With maximum permitted precision of 9 */
        instance = new TimestampField(9);
        referenceValue = new Timestamp(System.currentTimeMillis());
        assertEquals(referenceValue, instance.convert(referenceValue));

        expectedValue = new Timestamp(referenceValue.getTime());
        referenceValue.setNanos(999999999);
        expectedValue.setNanos(999999999);
        assertEquals(expectedValue, instance.convert(referenceValue));

        referenceValue.setNanos(123456789);
        expectedValue.setNanos(123456789);
        assertEquals(expectedValue, instance.convert(referenceValue));
        assertEquals(expectedValue, instance.convert(referenceValue.toString()));
        expectedValue.setNanos(123000000); //since getTime() will return only to milli-second precision
        assertEquals(expectedValue, instance.convert(referenceValue.getTime()));

        /* With minimum permitted precision of 0 */
        instance = new TimestampField(0);
        referenceValue = new Timestamp(System.currentTimeMillis());
        assertEquals(referenceValue, instance.convert(referenceValue));

        expectedValue = new Timestamp(referenceValue.getTime());
        referenceValue.setNanos(999999999);
        expectedValue.setNanos(0);
        assertEquals(expectedValue, instance.convert(referenceValue));

        referenceValue.setNanos(123456789);
        expectedValue.setNanos(0);
        assertEquals(expectedValue, instance.convert(referenceValue));
        assertEquals(expectedValue, instance.convert(referenceValue.toString()));
        expectedValue.setNanos(0); //since getTime() will return only to milli-second precision
        assertEquals(expectedValue, instance.convert(referenceValue.getTime()));

        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }

        try {
            instance.convert(Collections.EMPTY_LIST);
            fail("accepted invalid input data type");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }
}
