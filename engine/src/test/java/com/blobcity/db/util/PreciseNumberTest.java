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

package com.blobcity.db.util;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.math.BigDecimal;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link PreciseNumber}
 *
 * @author sanketsarang
 */
public class PreciseNumberTest {

    @Test
    public void testConstructor_BigDecimal_Int() {
        System.out.println("PreciseNumber(BigDecimal, Integer)");

        try {
            new PreciseNumber(BigDecimal.ZERO, 2);
        } catch (OperationException ex) {
            fail("Incorrectly reported error for valid initialisation of PreciseNumber with precision of 2");
        }

        try {
            new PreciseNumber(BigDecimal.ZERO, 0);
            fail("Did not report error for initilizing PreciseNumber with a zero precision value");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }

        try {
            new PreciseNumber(BigDecimal.ZERO, -1);
            fail("Did not report error for initilizing PreciseNumber with a negative precision value");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }
    }
    
    @Test
    public void testConstructor_BigDecimal_Integer_Integer() {
        System.out.println("PreciseNumber(BigDecimal, Integer, Integer)");

        try {
            new PreciseNumber(BigDecimal.ZERO, 2, 2);
        } catch (OperationException ex) {
            fail("Incorrectly reported error for valid initialisation of PreciseNumber with precision of 2");
        }

        try {
            new PreciseNumber(BigDecimal.ZERO, 0, 2);
            fail("Did not report error for initilizing PreciseNumber with a zero precision value");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }

        try {
            new PreciseNumber(BigDecimal.ZERO, -1, 2);
            fail("Did not report error for initilizing PreciseNumber with a negative precision value");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_ERROR, ex.getErrorCode());
        }
    }

    /**
     * Test of getBigDecimal method, of class PreciseNumber.
     */
    @Test
    public void testGetBigDecimal() throws OperationException {
        System.out.println("getBigDecimal");
        PreciseNumber instance;

        instance = new PreciseNumber(BigDecimal.ZERO);
        assertEquals(BigDecimal.ZERO, instance.getBigDecimal());
        instance = new PreciseNumber(BigDecimal.ZERO, 2);
        assertEquals(BigDecimal.ZERO, instance.getBigDecimal());
        instance = new PreciseNumber(2, BigDecimal.ZERO);
        assertEquals(BigDecimal.ZERO.setScale(2), instance.getBigDecimal());
        instance = new PreciseNumber(BigDecimal.ZERO, 2, 2);
        assertEquals(BigDecimal.ZERO.setScale(2), instance.getBigDecimal());
    }

    /**
     * Test of getPrecision method, of class PreciseNumber.
     */
    @Test
    public void testGetPrecision() throws OperationException {
        System.out.println("getPrecision");
        PreciseNumber instance;

        instance = new PreciseNumber(BigDecimal.ZERO);
        assertEquals(38, instance.getPrecision());
        instance = new PreciseNumber(BigDecimal.ZERO, 2);
        assertEquals(2, instance.getPrecision());
        instance = new PreciseNumber(2, BigDecimal.ZERO);
        assertEquals(38, instance.getPrecision());
        instance = new PreciseNumber(BigDecimal.ZERO, 2, 2);
        assertEquals(2, instance.getPrecision());
    }

    /**
     * Test of getScale method, of class PreciseNumber.
     */
    @Test
    public void testGetScale() throws OperationException {
        System.out.println("getScale");
        PreciseNumber instance;

        instance = new PreciseNumber(BigDecimal.ZERO);
        assertEquals(0, instance.getScale());
        instance = new PreciseNumber(BigDecimal.ZERO, 38);
        assertEquals(0, instance.getScale());
        instance = new PreciseNumber(2, BigDecimal.ZERO);
        assertEquals(2, instance.getScale());
        instance = new PreciseNumber(0, BigDecimal.ZERO);
        assertEquals(0, instance.getScale());
        instance = new PreciseNumber(-2, BigDecimal.ZERO);
        assertEquals(-2, instance.getScale());
        instance = new PreciseNumber(BigDecimal.ZERO, 38, 2);
        assertEquals(2, instance.getScale());
        instance = new PreciseNumber(BigDecimal.ZERO, 38, 0);
        assertEquals(0, instance.getScale());
        instance = new PreciseNumber(BigDecimal.ZERO, 38, -2);
        assertEquals(-2, instance.getScale());
    }

    /**
     * Test of get method, of class PreciseNumber.
     */
    @Test
    public void testGet() throws Exception {
        System.out.println("get");
        PreciseNumber instance;
        BigDecimal testNumber = new BigDecimal("7456123.89");

        instance = new PreciseNumber(testNumber);
        assertEquals(new BigDecimal("7456123.89").toPlainString(), instance.get().toPlainString());
        instance = new PreciseNumber(1, testNumber);
        assertEquals(new BigDecimal("7456123.9").toPlainString(), instance.get().toPlainString());
        instance = new PreciseNumber(testNumber, 9);
        assertEquals(new BigDecimal("7456123.89").toPlainString(), instance.get().toPlainString());
        instance = new PreciseNumber(testNumber, 9, 2);
        assertEquals(new BigDecimal("7456123.89").toPlainString(), instance.get().toPlainString());
        instance = new PreciseNumber(testNumber, 9, 0);
        assertEquals(new BigDecimal("7456124").toPlainString(), instance.get().toPlainString());
        try {
            new PreciseNumber(testNumber, 6);
            fail("PreciseNumber did not report exception even though precision constraint was violated");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }
        instance = new PreciseNumber(testNumber, 7, -2);
        assertEquals(new BigDecimal("7456100").toPlainString(), instance.get().toPlainString());

        try {
            new PreciseNumber(new BigDecimal("9999"), 4, -1);
            fail("PreciseNumber did not report exception even though precision constraint was violated due to upward approximation");
        } catch (OperationException ex) {
            assertEquals(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, ex.getErrorCode());
        }

    }
}
