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
import java.sql.Date;
import java.util.Calendar;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link DateField}
 *
 * @author sanketsarang
 */
public class DateFieldTest {

    /**
     * Test of getQueryCode method, of class DateField.
     */
    @Test
    public void testGetType() {
        System.out.println("testGetType");
        DateField instance;

        instance = new DateField();
        assertEquals(Types.DATE, instance.getType());

        instance = new DateField(new Date(0));
        assertEquals(Types.DATE, instance.getType());
    }

    /**
     * Test of getDefaultValue method, of class DateField.
     */
    @Test
    public void testGetDefaultValue() {
        System.out.println("getDefaultValue");
        DateField instance = new DateField();
        assertEquals(null, instance.getDefaultValue());

        instance = new DateField(new Date(0));
        assertEquals(new Date(0).toString(), instance.getDefaultValue().toString());
    }

    /**
     * Test of hasDefaultValue method, of class DateField.
     */
    @Test
    public void testHasDefaultValue() {
        System.out.println("hasDefaultValue");
        DateField instance = new DateField();
        assertFalse(instance.hasDefaultValue());

        instance = new DateField(new Date(0));
        assertTrue(instance.hasDefaultValue());

        instance = new DateField(null);
        assertTrue(instance.hasDefaultValue());
    }

    /**
     * Test of convert method, of class DateField.
     */
    @Test
    public void testConvert() throws Exception {
        System.out.println("convert");
        DateField instance;

        instance = new DateField();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2014, 0, 1);
        final Date expectedResult = new Date(calendar.getTimeInMillis());

        assertEquals(expectedResult.toString(), instance.convert("2014-01-01").toString());
        assertEquals(expectedResult.toString(), instance.convert("2014-1-01").toString());
        assertEquals(expectedResult.toString(), instance.convert("2014-01-1").toString());
        assertEquals(expectedResult.toString(), instance.convert("2014-1-1").toString());

        try {
            instance.convert("2014/01/01");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_DATE_FORMAT, ex.getErrorCode());
        }
        
        try {
            instance.convert("2014-22-01");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_DATE_FORMAT, ex.getErrorCode());
        }
        
        try {
            instance.convert("01-01-2014");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.INVALID_DATE_FORMAT, ex.getErrorCode());
        }
        
        try {
            instance.convert(null);
            fail("null value not checked");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_MISMATCH, ex.getErrorCode());
        }
    }

}
