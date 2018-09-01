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
import java.sql.Time;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link TimeField}
 *
 * @author sanketsarang
 */
public class TimeFieldTest {

    /**
     * Test of getQueryCode method, of class TimeField.
     */
    @Test
    public void testGetType() {
        System.out.println("getQueryCode");

        assertEquals(Types.TIME, new TimeField().getType());
        assertEquals(Types.TIME, new TimeField(new Time(0)).getType());
        assertEquals(Types.TIME, new TimeField(true).getType());
        assertEquals(Types.TIME, new TimeField(true, new Time(0)).getType());
    }

    /**
     * Test of isWithTimeZone method, of class TimeField.
     */
    @Test
    public void testIsWithTimeZone() {
        System.out.println("isWithTimeZone");

        assertFalse(new TimeField().isWithTimeZone());
        assertFalse(new TimeField(new Time(0)).isWithTimeZone());
        assertTrue(new TimeField(true).isWithTimeZone());
        assertTrue(new TimeField(true, new Time(0)).isWithTimeZone());
    }

    /**
     * Test of getDefaultValue method, of class TimeField.
     */
    @Test
    public void testGetDefaultValue() {
        System.out.println("getDefaultValue");

        assertNull(new TimeField().getDefaultValue());
        assertNull(new TimeField(true).getDefaultValue());
        assertNull(new TimeField(null).getDefaultValue());
        assertEquals(new Time(0), new TimeField(new Time(0)).getDefaultValue());
        assertEquals(new Time(-1), new TimeField(new Time(-1)).getDefaultValue());
        assertEquals(new Time(0), new TimeField(true, new Time(0)).getDefaultValue());
        assertEquals(new Time(-1), new TimeField(true, new Time(-1)).getDefaultValue());
    }

    /**
     * Test of hasDefaultValue method, of class TimeField.
     */
    @Test
    public void testHasDefaultValue() {
        System.out.println("hasDefaultValue");

        assertFalse(new TimeField().hasDefaultValue());
        assertTrue(new TimeField(new Time(0)).hasDefaultValue());
        assertTrue(new TimeField(null).hasDefaultValue());
        assertFalse(new TimeField(false).hasDefaultValue());
        assertTrue(new TimeField(true, new Time(0)).hasDefaultValue());
    }

    /**
     * Test of convert method, of class TimeField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        TimeField instance = new TimeField();

        assertEquals(Time.valueOf("12:34:56"), instance.convert("12:34:56"));
        assertEquals(Time.valueOf("12:34:56"), instance.convert(Time.valueOf("12:34:56")));
        assertEquals(Time.valueOf("12:34:56"), instance.convert(Time.valueOf("12:34:56").getTime()));
        assertEquals(Time.valueOf("23:34:56"), instance.convert(Time.valueOf("23:34:56").getTime()));
        assertEquals(Time.valueOf("23:04:56"), instance.convert(Time.valueOf("23:4:56").getTime()));
        assertEquals(Time.valueOf("23:04:05"), instance.convert(Time.valueOf("23:4:5").getTime()));
        final long currentTime = System.currentTimeMillis();
        assertEquals(new Time(currentTime), instance.convert(currentTime));
        assertEquals(new Time(-1), instance.convert((long)-1));

        try {
            instance.convert("12-34-56");
            fail("Accepted incorrect time value without reporting exception");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_TIME_FORMAT, ex.getErrorCode());
        }
        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
        try {
            instance.convert(new Integer(0));
            fail("Accepted incorrect data type of Integer");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }
}
